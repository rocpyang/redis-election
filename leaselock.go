package rediselection

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client 是 LeaseLock 对 Redis 客户端的最小能力要求。
// *redis.Client、*redis.ClusterClient、redis.UniversalClient
// 以及常见测试替身（如基于 miniredis 创建的客户端）均满足该接口。
type Client interface {
	redis.Scripter
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
}

// LeaseRecord 是租约某一时刻的只读快照，字段语义对标 K8s Lease 对象
// （coordination.k8s.io/v1 Lease）。
type LeaseRecord struct {
	// Holder 当前持有者的 identity，对应 K8s 的 holderIdentity。
	Holder string
	// Version 乐观锁版本号：每次写入 +1，全局单调递增。
	// 可作为下游写操作的 fencing token（携带该值做递增校验可防旧主脑裂写入）。
	Version int64
	// AcquireTime 当前持有者获得租约的时间（Redis 服务器时钟，毫秒）。
	AcquireTime int64
	// RenewTime 当前持有者最近一次续约时间（Redis 服务器时钟，毫秒），
	// 是过期判定（now - RenewTime > LeaseDuration）的唯一依据。
	RenewTime int64
	// Transitions 租约易主次数，对应 K8s 的 leaseTransitions。
	Transitions int64
}

// Lock 是 Elector 依赖的最小锁接口，LeaseLock 是其 Redis 实现。
// 用户也可以基于其他存储（etcd / 数据库等）实现该接口接入 Elector。
type Lock interface {
	// Identity 返回持有者标识，同一进程内多个 Elector 应各不相同。
	Identity() string
	// TryAcquireOrRenew 原子地尝试获取或续约租约，返回事件码与租约快照。
	// 事件码类型为 Event；出错时返回 EventInvalid 与零值快照，
	// 调用方应优先检查 error。
	TryAcquireOrRenew(ctx context.Context, leaseDuration time.Duration) (Event, LeaseRecord, error)
	// Release 在仍持有租约的前提下主动让位。
	Release(ctx context.Context) error
}

// LeaseLock 是基于 Redis Hash + Lua 的租约锁实现，对标 K8s client-go
// 的 resourcelock.Interface（RedisLock）。
//
// 底层仅使用两个 Lua 脚本（见 script.go），单 key 设计，
// Redis Cluster 下不存在跨 slot 问题。
type LeaseLock struct {
	client   Client
	key      string
	identity string
	acquire  *redis.Script
	release  *redis.Script
}

var _ Lock = (*LeaseLock)(nil)

// NewLeaseLock 创建一个租约锁。
//
//   - client: 任一满足 Client 接口的 go-redis 客户端；
//   - key:    租约存储的 Redis key，同一选主组的所有实例必须一致；
//   - identity: 持有者标识，同一选主组内必须唯一，可用 DefaultIdentity() 生成。
func NewLeaseLock(client Client, key, identity string) *LeaseLock {
	return &LeaseLock{
		client:   client,
		key:      key,
		identity: identity,
		acquire:  redis.NewScript(tryAcquireOrRenewScript),
		release:  redis.NewScript(releaseScript),
	}
}

// Identity 返回持有者标识。
func (l *LeaseLock) Identity() string { return l.identity }

// Key 返回租约 key。
func (l *LeaseLock) Key() string { return l.key }

// DefaultIdentity 生成默认持有者标识：hostname_pid_random。
// 同一进程多次调用得到不同值，适合同机多实例场景。
func DefaultIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", host, os.Getpid())
	}
	return fmt.Sprintf("%s_%d_%s", host, os.Getpid(), hex.EncodeToString(b))
}

// TryAcquireOrRenew 执行一次原子的"尝试获取/续约"，对标 K8s
// leaderelection 的 tryAcquireOrRenew。四分支判定见 script.go：
//
//	EventAcquired    新建租约或抢占过期租约成功，本实例成为持有者；
//	EventRenewed     本实例本就是持有者，续约成功；
//	EventHeldByOther 其他实例持锁且未过期，应继续等待；
//	EventInvalid     存储错误或脚本返回异常，调用方应检查 error。
//
// 返回完整的 LeaseRecord（Holder 为执行后的当前持有者，AcquireTime /
// RenewTime 为 Redis 服务器时钟毫秒值——对齐 K8s：elector 每轮可见
// 完整租约记录）；出错时返回零值快照。
// 该方法幂等且无副作用竞争，可安全地由多个 goroutine 调用
// （但通常只需 Elector 内部单 goroutine 周期调用）。
func (l *LeaseLock) TryAcquireOrRenew(ctx context.Context, leaseDuration time.Duration) (Event, LeaseRecord, error) {
	res, err := l.acquire.Run(ctx, l.client, []string{l.key},
		l.identity, leaseDuration.Milliseconds()).Result()
	if err != nil {
		return EventInvalid, LeaseRecord{}, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 6 {
		return EventInvalid, LeaseRecord{},
			fmt.Errorf("rediselection: unexpected script reply: %v", res)
	}
	event, _ := arr[0].(int64)
	version, _ := arr[1].(int64)
	holder, _ := arr[2].(string)
	transitions, _ := arr[3].(int64)
	acquireTime, _ := arr[4].(int64)
	renewTime, _ := arr[5].(int64)
	return Event(event), LeaseRecord{
		Holder:      holder,
		Version:     version,
		AcquireTime: acquireTime,
		RenewTime:   renewTime,
		Transitions: transitions,
	}, nil
}

// Release 主动让位：仅当租约仍属于本 identity 时，把 renewTime 置 0，
// 使其他实例在下一轮即可抢占（failover 从 ~LeaseDuration 缩短到 ~RetryPeriod）。
//
// 注意：Release 刻意不删除 key，保留 version / transitions 历史，
// 以维持 fencing token 的全局单调性与切换统计的连续性。
// 若租约已不属于本 identity，返回 ErrNotHoldingLease（无副作用，可忽略）。
func (l *LeaseLock) Release(ctx context.Context) error {
	res, err := l.release.Run(ctx, l.client, []string{l.key}, l.identity).Int()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrNotHoldingLease
	}
	return nil
}

// Get 返回租约的只读快照，用于观测/排障（不参与选主逻辑）。
// 租约不存在时返回 ErrNoLease。
func (l *LeaseLock) Get(ctx context.Context) (LeaseRecord, error) {
	m, err := l.client.HGetAll(ctx, l.key).Result()
	if err != nil {
		return LeaseRecord{}, err
	}
	if len(m) == 0 {
		return LeaseRecord{}, ErrNoLease
	}
	rec := LeaseRecord{
		Holder:      m["holder"],
		Version:     parseInt(m["version"]),
		AcquireTime: parseInt(m["acquireTime"]),
		RenewTime:   parseInt(m["renewTime"]),
		Transitions: parseInt(m["transitions"]),
	}
	return rec, nil
}

func parseInt(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}
