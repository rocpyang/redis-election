// Package rediselection 提供基于 Redis 的抢占式乐观锁选主能力，
// 设计对标 Kubernetes client-go 的 tools/leaderelection：
//
//   - 租约记录（holder/version/renewTime/...）存储于单个 Redis Hash，对标 K8s Lease 对象；
//   - 单个 Lua 脚本原子完成"创建/续约/抢占/谦让"四分支判定，对标 K8s 的
//     resourceVersion 乐观并发控制（读-改-写 + 冲突重试在 Redis 中退化为一次原子 CAS）；
//   - 所有时间判定统一使用 redis.call('TIME') 获取的 Redis 服务器时钟，
//     对标 K8s 由 API Server 统一打时间戳，不受各实例本地时钟漂移影响；
//   - version 字段单调递增，可作为下游写操作的 fencing token。
package rediselection

// Event 是 TryAcquireOrRenew 返回的事件码，由 Lua 脚本返回。
type Event int

const (
	// EventInvalid 表示无效事件：TryAcquireOrRenew 出错时返回，
	// 调用方应优先检查 error（K8s 的 bool 返回将错误与谦让混淆，
	// 此处以显式无效值区分）。
	EventInvalid Event = iota
	// EventHeldByOther 表示租约被其他实例持有且尚未过期。
	// 调用方应继续等待并在下一轮重试。
	EventHeldByOther
	// EventAcquired 表示调用方成功获得租约：
	// 可能是新建租约（此前无主），也可能是抢占了他人的过期租约。
	EventAcquired
	// EventRenewed 表示调用方本身就是持有者，本次为续约成功。
	EventRenewed
)

// tryAcquireOrRenewScript 对标 K8s leaderelection 的 tryAcquireOrRenew：
// 在一次原子操作内完成"读租约 → 判定 → 写租约"。
//
// KEYS[1] = 租约 key
// ARGV[1] = 调用方 identity
// ARGV[2] = leaseDuration（毫秒）
//
// 返回 {event, version, currentHolder, transitions, acquireTime, renewTime}：
//   - {2, v, identity, t, now, now}   新建或抢占成功，当前持有者为调用方
//   - {3, v, identity, t, at, now}    续约成功（at 为原 acquireTime）
//   - {1, v, holder, t, at, rt}       他人持锁未过期（holder 为当前持有者）
//
// 事件编号与 Go 侧 Event 常量一致，0 保留给错误路径（EventInvalid），
// 不由脚本返回。时间字段使调用方每轮即可观察完整租约快照
// （对齐 K8s：elector 通过 Lock.Get 每轮可见完整 LeaderElectionRecord）。
//
// 过期判定基准 now 取自 redis.call('TIME')（Redis 服务器时钟），
// 各实例本地时钟不参与判定，避免时钟漂移导致双主或误抢。
// key 的 TTL 设为 2 × leaseDuration，仅作为残留数据兜底清理；
// 正确性完全依赖 renewTime 与 leaseDuration 的比较，不依赖 TTL。
//
// 注意：该脚本包含非确定性的 TIME 调用，要求 Redis >= 5
// （Redis 5 起默认按 effect 复制脚本，主从/哨兵场景安全）。
const tryAcquireOrRenewScript = `
local key = KEYS[1]
local identity = ARGV[1]
local leaseMs = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local holder = redis.call('HGET', key, 'holder')
local version = tonumber(redis.call('HGET', key, 'version') or '0')
local acquireTime = tonumber(redis.call('HGET', key, 'acquireTime') or '0')
local renewTime = tonumber(redis.call('HGET', key, 'renewTime') or '0')
local transitions = tonumber(redis.call('HGET', key, 'transitions') or '0')

if not holder then
  -- ① 无主：创建租约
  redis.call('HSET', key,
    'holder', identity,
    'version', 1,
    'acquireTime', now,
    'renewTime', now,
    'transitions', 0)
  redis.call('PEXPIRE', key, leaseMs * 2)
  return {2, 1, identity, 0, now, now}

elseif holder == identity then
  -- ② 自己持有：续约（version 递增 = fencing token）
  local v = version + 1
  redis.call('HSET', key, 'version', v, 'renewTime', now)
  redis.call('PEXPIRE', key, leaseMs * 2)
  return {3, v, identity, transitions, acquireTime, now}

elseif now - renewTime > leaseMs then
  -- ③ 他人租约已过期：抢占
  local v = version + 1
  redis.call('HSET', key,
    'holder', identity,
    'version', v,
    'acquireTime', now,
    'renewTime', now)
  redis.call('HINCRBY', key, 'transitions', 1)
  redis.call('PEXPIRE', key, leaseMs * 2)
  return {2, v, identity, transitions + 1, now, now}

else
  -- ④ 他人持锁且未过期：谦让
  return {1, version, holder, transitions, acquireTime, renewTime}
end
`

// releaseScript 优雅让位脚本（对标 K8s ReleaseOnCancel）：
// 仅当 holder 仍是自己时，把 renewTime 置 0（epoch），
// 使其他实例在下一轮 TryAcquireOrRenew 中立即满足过期条件完成抢占，
// 将 failover 时间从约 LeaseDuration 缩短到约一个 RetryPeriod。
//
// 刻意不删除 key：保留 version/transitions 历史，
// 保证 fencing token 全局单调递增、切换次数统计连续。
// holder 校验防止"假死实例复活后误伤新持有者"。
//
// KEYS[1] = 租约 key
// ARGV[1] = 调用方 identity
// 返回 1 表示让位成功，0 表示租约已不属于自己（无需/无法让位）。
const releaseScript = `
local holder = redis.call('HGET', KEYS[1], 'holder')
if holder == ARGV[1] then
  redis.call('HSET', KEYS[1], 'renewTime', 0)
  return 1
end
return 0
`
