# redis-election

基于 Redis 的抢占式乐观锁选主库，设计对标 Kubernetes client-go 的
[leader election](https://github.com/kubernetes/client-go/tree/master/tools/leaderelection)：
租约（Lease）+ 乐观并发 + 周期续约 + 故障抢占，适用于多实例部署下
"定时任务 / 协调工作只需一个实例执行"的场景。

## 特性

- **K8s 同款语义**：`LeaseDuration / RenewDeadline / RetryPeriod` 三参数、
  `OnStartedLeading / OnStoppedLeading / OnNewLeader` 三回调，行为可对照 K8s 理解
- **单 Lua 脚本原子 CAS**：创建 / 续约 / 抢占 / 谦让四分支在一次原子操作内完成，
  等价于 K8s 的 resourceVersion 乐观并发控制，且无重试风暴
- **统一时间基准**：过期判定使用 `redis.call('TIME')`（Redis 服务器时钟），
  各实例时钟漂移不影响正确性，杜绝时钟偏移导致的双主
- **fencing token**：租约 `version` 单调递增，可用于下游写操作防旧主脑裂
- **优雅让位**：`ReleaseOnCancel` 在退出时置空租约，failover 从 ~LeaseDuration
  缩短到 ~RetryPeriod
- **降级等待重选**：丢锁后不退出进程，自动降级为候选者继续竞选
- **零框架绑定**：不绑定日志库/指标库；直接依赖仅 [redis/go-redis/v9](https://github.com/redis/go-redis)
- **两层 API**：高层 `Elector` 状态机开箱即用，低层 `LeaseLock` 原语可自由组合
- **充分测试**：单测覆盖率 91%+（miniredis 驱动，含四分支、failover、
  Redis 故障自贬、抢占后重选等场景）

## 安装

```
go get github.com/rocpyang/redis-election
```

要求：Go >= 1.24，Redis >= 5（effect replication，见[边界场景](#边界场景)）。

## 快速开始

```go
package main

import (
    "context"

    "github.com/redis/go-redis/v9"
    election "github.com/rocpyang/redis-election"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    lock := election.NewLeaseLock(rdb, "myapp:leader:lease", election.DefaultIdentity())

    elector, err := election.NewElector(lock,
        election.WithReleaseOnCancel(true), // SIGTERM 退出时主动让位
        election.WithOnStartedLeading(func(ctx context.Context) {
            // 成为 leader：在此启动 leader 专属工作（定时任务等），
            // 失去 leadership 时 ctx 会被取消
            runLeaderOnlyJobs(ctx)
        }),
        election.WithOnStoppedLeading(func() {
            // 失去 leader：框架已自动取消上面的 ctx，
            // 这里通常无需做额外清理，适合打点/告警
        }),
    )
    if err != nil {
        panic(err)
    }

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    elector.Run(ctx) // 阻塞运行：降级等待重选，直到 ctx 取消
}
```

多实例部署同一程序，任意时刻只有一个实例执行 `runLeaderOnlyJobs`；
leader 宕机后其余实例自动接管，failover 最坏约 `LeaseDuration + RetryPeriod`。

可运行示例见 `example/basic`（开两个终端观察抢占与接管）。

## 工作原理

### 与 K8s leader election 的概念对照

| K8s client-go | 本库 |
|---|---|
| Lease 资源对象 | Redis Hash（单 key） |
| resourceVersion 乐观并发（409 重试） | Lua 脚本内原子 CAS，天然无竞态 |
| API Server 统一时间戳 | `redis.call('TIME')` Redis 服务器时钟 |
| `tryAcquireOrRenew()` | 单 Lua 脚本四分支 |
| `leaseTransitions` | `transitions` 字段 |
| `ReleaseOnCancel` | 同名选项 |

### 租约数据结构

```
Key:    <你指定的 key>                    (Hash)
Fields:
  holder       持有者 identity（hostname_pid_random）
  version      乐观锁版本号，每次写入 +1（fencing token）
  acquireTime  当前持有者获租时间（Redis 时钟，毫秒）
  renewTime    最近续约时间（Redis 时钟，毫秒）——过期判定唯一依据
  transitions  租约易主次数
TTL:     2 × LeaseDuration（仅兜底清理，正确性不依赖 TTL）
```

### 状态机

```
          ┌──────────────────────────────────────────────┐
          ▼                                              │
   acquire（候选者）                                      │ 丢锁后降级
   每 RetryPeriod±jitter 尝试获取                          │ 重新竞选
          │ 获取成功                                       │
          ▼                                              │
        lead（持有者）                                    │
        每 RetryPeriod±jitter 续约                        │
        ├─ 被明确抢占（HeldByOther）→ 立即自贬 ─────────────┤
        ├─ 续约连续失败超 RenewDeadline → 自贬（防抖动宽限）──┤
        └─ ctx 取消 → （可选）主动让位后退出                 │
          │                                              │
          ▼                                              │
   OnStartedLeading(ctx) 运行业务，ctx 取消即停 ────────────┘
```

正常周期中，leader 续约成功刷新 `renewTime`；候选者观察到他人持锁未过期则谦让等待。
leader 失联后，候选者在 `renewTime + LeaseDuration` 后的下一次尝试中原子抢占。

### 时间线示例（默认参数）

```
T0        A 当选，job 启动
T0+2s,4s… A 持续续约；B 每轮观察到 A 未过期，继续等待
T1        A 宕机/GC 停止续约
T1+15s    租约过期（LeaseDuration）
T1+15~17s B 抢占成功接管            ← failover 最坏 ≈ Lease+Retry
（若 A 优雅退出开启让位，B 在 ~2s 内接管）
```

## 参数调优

| 参数 | 默认值 | 说明 |
|---|---|---|
| `LeaseDuration` | 15s | 租约有效期，**failover 最坏等待的主要构成**；调小切换快但更易误判 |
| `RenewDeadline` | 10s | 续约失败自贬宽限，**必须 < LeaseDuration**；保证"旧主先停、新主后上" |
| `RetryPeriod` | 2s | 尝试间隔，**必须 < RenewDeadline**；实际间隔叠加 `[0, 1.2×)` 抖动 |
| `JitterFactor` | 1.2 | 抖动系数，防止多实例同频竞争 |
| `ReleaseOnCancel` | false | 退出时主动让位；SIGTERM 场景建议开启 |

选型经验：

- 想要更快的 failover：同时调小三个参数并保持 `Retry < Renew < Lease` 的比例（如 1s/3s/5s）
- Redis 跨机房延迟高 / 抖动大：适当调大 `RenewDeadline`，避免频繁自贬
- 单实例其实也安全：`Run` 在 Redis 故障时自贬停跑业务，恢复后重新竞选——
  对 leader 专属任务而言"宁可停跑，不可双跑"

## 两层 API

### 高层 Elector（推荐）

见[快速开始](#快速开始)。`IsLeader()` / `LeaderID()` 可在任意 goroutine 查询。

### 低层 LeaseLock

需要自定义选主循环时使用：

```go
lock := election.NewLeaseLock(rdb, key, identity)

// 原子尝试：创建 / 续约 / 抢占 / 谦让
event, rec, err := lock.TryAcquireOrRenew(ctx, 15*time.Second)
switch event {
case election.EventAcquired:   // 成为持有者（新建或抢占过期租约）
case election.EventRenewed:    // 续约成功
case election.EventHeldByOther: // 他人持锁未过期，rec.Holder 为当前持有者
}

lock.Release(ctx) // 主动让位；非持有者返回 ErrNotHoldingLease（可忽略）
rec, _ := lock.Get(ctx) // 只读快照，观测/排障
```

`Elector` 只依赖最小接口 `Lock`（`Identity / TryAcquireOrRenew / Release`），
你也可以基于 etcd / 数据库等实现自己的 Lock 接入。

## 边界场景

| 场景 | 行为 |
|---|---|
| 实例时钟漂移 | 不影响：过期判定全部基于 Redis 服务器时钟 |
| Redis 抖动/主从切换 | leader 在 `RenewDeadline` 宽限内重试，超时自贬；不会误让 |
| Redis 完全不可用 | 所有实例自贬停跑 leader 任务；恢复后重新竞选 |
| leader 假死复活（GC/网络分区） | 复活后 renew 发现 `holder != self` → 立即自贬；`Release` 校验 holder，不误伤新主 |
| 被明确抢占 | 立即自贬（不等 RenewDeadline），双跑窗口压缩到接近 0 |
| Redis 数据残留 | key TTL = 2×LeaseDuration 兜底清理 |
| Redis 数据丢失（重启/驱逐） | fencing token（version）从头计数；若依赖 token 防脑裂需评估 Redis 持久化配置 |
| Cluster 模式 | 单 key 设计无跨 slot 问题 |

**与 SetNX 短锁的区别**：`SETNX + TTL` 适合"单次任务防重"（锁粒度小、无状态）；
本库提供"实例级长租约 + 周期续约 + 身份回调"，避免每轮任务都竞争锁，
且天然具备 failover 与观测能力。两者可以并存（实例级选主 + 任务级短锁）。

**Redis >= 5 的原因**：脚本含非确定性的 `TIME` 调用，Redis 5 起默认按
effect replication 复制脚本，主从/哨兵场景安全。

## 与 K8s 原生实现的两点刻意差异

1. **丢锁后降级等待重选**，而非退出进程——适配网关/常驻服务等不允许自杀的场景；
2. **被明确抢占时立即自贬**，而非等满 `RenewDeadline`——`HeldByOther` 说明
   存储系统健康、判定可信，立即退出可将被抢占场景的双跑窗口压缩到接近 0
   （Redis 故障场景仍保留 `RenewDeadline` 宽限，防止抖动误判）。

## 测试

```
make test     # go test -race（CI 环境，本地无 gcc 时可去掉 -race）
make cover    # 覆盖率
make lint     # gofmt + go vet
```

## License

[MIT](LICENSE)
