# redis-election 设计文档

> 本文档是设计的完整沉淀，README 是其使用向精简版。

## 1. 背景与目标

多实例部署的常驻服务（如网关）中，定时任务、账单结算、探活等后台工作
只需一个实例执行。直接每个实例都跑会重复执行；用 `SETNX + TTL` 短锁
每轮竞争、无身份概念、无法感知 failover。

目标：基于 Redis 实现一套**抢占式乐观锁选主**，语义对标 Kubernetes
client-go `tools/leaderelection`，作为独立库供各项目复用。

核心决策（项目立项时确认）：

| 决策点 | 选择 | 理由 |
|---|---|---|
| 用途 | 后台任务选主 | 决定 API 形态为回调驱动 |
| 丢锁行为 | 降级等待重选 | 网关场景不允许退出进程 |
| 时间基准 | Redis 服务器时钟 | 排除实例时钟漂移影响 |
| 客户端 | redis/go-redis/v9 | 官方维护版本，新库社区主流 |
| API 分层 | 高层 Elector + 低层 LeaseLock | 开箱即用与灵活组合兼得 |

## 2. K8s LeaderElection → Redis 映射

| K8s 概念 | 本库对应 |
|---|---|
| Lease 资源对象 | Redis Hash（单 key） |
| holderIdentity | `holder` 字段（hostname_pid_random） |
| resourceVersion 乐观并发 | Lua 脚本内 `version` 字段原子递增（CAS） |
| API Server 统一时间戳 | Lua 内 `redis.call('TIME')` |
| leaseDurationSeconds | `LeaseDuration`（默认 15s） |
| renewDeadline | `RenewDeadline`（默认 10s） |
| retryPeriod + jitter | `RetryPeriod`（默认 2s）+ `JitterFactor`（1.2） |
| tryAcquireOrRenew 单函数 | 单 Lua 脚本四分支 |
| leaseTransitions | `transitions` 字段 |
| OnStartedLeading/OnStoppedLeading/OnNewLeader | 相同三回调 |
| ReleaseOnCancel | 相同选项（K8s 置 LeaseDurationSeconds=1，本库置 renewTime=0，效果同构：等待者下一轮即可抢占） |

关键等价关系：K8s 的"GET Lease → 本地判定 → 带 resourceVersion 的 UPDATE →
409 冲突则重试"这套乐观并发，在 Redis 中退化为**一次 Lua 原子操作**——
脚本在 Redis 单线程内执行，check-and-set 无竞态窗口，也不产生重试风暴。

## 3. 数据结构

```
Key:    <用户指定>                (Hash，TTL = 2×LeaseDuration)
  holder       string   持有者 identity
  version      int      每次写入 +1，全局单调（fencing token）
  acquireTime  int(ms)  当前持有者获租时间（Redis 时钟）
  renewTime    int(ms)  最近续约时间（Redis 时钟）—— 过期判定唯一依据
  transitions  int      租约易主次数
```

设计要点：

- **正确性不依赖 TTL**：TTL 仅为兜底清理。持有者失联时，等待者依据
  `now - renewTime > leaseDuration` 判定过期并抢占；TTL 到期时数据被清理，
  走"新建"分支，两者语义一致（新建 version 从 1 开始，此时 fencing
  连续性由 Redis 数据持久性决定，见 §8）。
- **Release 不删 key**：主动让位只把 `renewTime` 置 0（epoch），
  保留 `version`/`transitions` 历史，维持 fencing token 单调性与统计连续性。

## 4. 核心 Lua 脚本（tryAcquireOrRenew）

```
KEYS[1]=key  ARGV[1]=identity  ARGV[2]=leaseDurationMs

now = redis.call('TIME') 转 ms                 ← 统一时间基准

① 无主     → HSET holder=identity version=1 renewTime=now transitions=0
             PEXPIRE 2×lease     → return {ACQUIRED(2), 1, identity, 0, now, now}
② 自己持有 → HSET version+1 renewTime=now
             PEXPIRE 2×lease     → return {RENEWED(3), v, identity, t, at, now}
③ 他人过期 → (now - renewTime > lease)
             HSET holder=identity version+1 renewTime=now acquireTime=now
             HINCRBY transitions → return {ACQUIRED(2), v, identity, t+1, now, now}
④ 他人活跃 →                          return {HELD_BY_OTHER(1), v, holder, t, at, rt}
```

返回 6 元组含 acquireTime/renewTime（Redis 时钟 ms），调用方每轮即可
观察完整租约快照——对齐 K8s（elector 通过 Lock.Get 每轮可见完整
LeaderElectionRecord）。事件编号与 Go 侧 `Event` 常量一致（1=HeldByOther,
2=Acquired, 3=Renewed），0 保留给错误路径（`EventInvalid`），不由脚本返回；
错误路径显式返回 `EventInvalid`，不再与谦让分支混淆。

release 脚本：

```
holder == identity → HSET renewTime 0 → return 1   // 让位，等待者立即满足过期条件
否则                                  → return 0   // 非持有者，无副作用
```

版本要求：脚本含非确定性 `TIME` 调用，Redis ≥ 5 默认按 effect replication
复制，主从/哨兵安全。Cluster 下单 key 无跨 slot 问题。

## 5. 状态机（Elector）

```
acquire: 每 RetryPeriod±jitter 执行 TryAcquireOrRenew
         ACQUIRED/RENEWED → lead；HELD_BY_OTHER → OnNewLeader，继续等待
lead:    OnStartedLeading(ctx) 运行业务（异步）
         每 RetryPeriod±jitter 续约：
           成功             → 刷新 lastRenew
           存储错误          → RenewDeadline 宽限内重试，超时自贬（防抖动误判）
           HELD_BY_OTHER    → 立即自贬（存储健康、判定可信，压缩双跑窗口）
         退出时: isLeader=false → cancel(业务ctx) → (可选)Release(超时=RenewDeadline)
                 → OnStoppedLeading → 回 acquire
Run 退出时若从未 lead → 补触发一次 OnStoppedLeading
         （K8s 语义："always called when the LeaderElector exits,
          even if it did not start leading"）
```

cancel 前置于 Release 的理由（与 K8s 的刻意微差异）：Release 是网络调用
（预算 RenewDeadline，对齐 K8s release 的超时），若 cancel 压在其后会推迟
业务停止、拉长双跑窗口；前置 cancel 保住"OnStoppedLeading 触发时业务 ctx
已取消"的 K8s 不变量，failover 仅慢微秒级（cancel 不等待业务退出）。

不变式与推论：

1. `RenewDeadline < LeaseDuration` ⇒ 旧主自贬（停止业务）必然发生在
   新主可抢占之前 ⇒ Redis 抖动场景无双跑窗口；
2. 分支②"持有者无条件续约"与 K8s 一致：过期是给别人看的信号，
   持有者自己续约不受过期检查限制；
3. 被抢占（HeldByOther）时立即自贬 ⇒ 该场景双跑窗口 ≈ 一个 RetryPeriod。

与 K8s 的两点刻意差异（见 README 对应章节）：降级重选而非退出进程；
被抢占时立即自贬而非等满 RenewDeadline。

## 6. 时间线（默认参数）

```
T0        A 当选
T0+2s,4s… A 续约；B 观察未过期继续等待
T1        A 失联
T1+15s    租约过期（LeaseDuration）
T1+15~17s B 抢占接管          ← failover 最坏 ≈ Lease + Retry
优雅退出    A 主动让位(renewTime=0)，B ~2s 内接管
```

## 7. API 设计

```
低层 LeaseLock（对标 resourcelock.Interface）
  NewLeaseLock(client Client, key, identity)
  TryAcquireOrRenew(ctx, lease) (Event, LeaseRecord, error) // 出错返回 EventInvalid
  Release(ctx) error            // ErrNotHoldingLease 哨兵
  Get(ctx) (LeaseRecord, error) // ErrNoLease 哨兵
  Client = redis.Scripter + HGetAll（*redis.Client/Cluster/Universal 均满足）

高层 Elector（对标 LeaderElector）
  NewElector(lock Lock, opts...)          // Config.validate: Retry×Jitter < Renew < Lease
  Run(ctx)                                // 阻塞；降级等待重选；并发防护
  IsLeader() / LeaderID()
  With{LeaseDuration,RenewDeadline,RetryPeriod,JitterFactor,
       ReleaseOnCancel,Logger,OnStartedLeading,OnStoppedLeading,OnNewLeader}
```

依赖边界：直接依赖仅 go-redis/v9；Logger 为最小接口（默认静默），
不传染日志框架；identity 自生成不引 uuid。

## 8. 边界场景与风险

| 场景 | 处理 |
|---|---|
| 实例时钟漂移 | 无影响，判定基于 Redis 时钟 |
| 实例本地时钟回拨 | RenewDeadline 宽限判定基于本地时钟（K8s 同款，其 PollUntilContextTimeout 亦为本地定时器），大幅回拨推迟自贬；租约有效性不受影响 |
| Redis 抖动 | RenewDeadline 宽限，防误自贬 |
| Redis 完全不可用 | 全员自贬停跑；恢复重选（宁可停跑不可双跑） |
| 假死复活 | renew 得 HeldByOther 立即自贬；Release 校验 holder 不误伤新主 |
| Redis 数据丢失 | version 重新计数；强 fencing 需求需评估 Redis 持久化（AOF） |
| fencing | version 单调递增可作 token；下游携带做递增校验可防旧主写入 |

## 9. 测试策略

miniredis（支持 Lua + TIME 可控）：四分支（含时间字段）、Release 语义、
TTL 兜底、生命周期、双实例 failover、Redis 故障自贬、抢占后立即自贬 +
重选、错误路径 EventInvalid、回调顺序（cancel 先于 OnStoppedLeading）、
Run 并发防护与重启、从未 lead 的 OnStoppedLeading 触发、参数校验
（含 K8s jitter 规则）、jitter 边界。真时钟 + 短参数驱动状态机，
SetTime 定钟隔离时间。CI（GitHub Actions，ubuntu 自带 gcc）跑 `-race`。

## 10. 已知限制

- 强一致 fencing 依赖 Redis 数据持久性：Redis 崩溃丢数据时 version 归零，
  无法提供跨数据丢失的 fencing 保证（Redlock 论战中已论证 Redis 锁的
  本质边界；对"后台任务选主"场景该风险可接受）；
- 未内置 Prometheus 指标：保持零依赖，使用方可通过
  `IsLeader()/LeaderID()` + 回调自行接入；
- 同一 key 的选主组规模建议 < 100（抖动重试已防雪崩，规模更大需评估）。
