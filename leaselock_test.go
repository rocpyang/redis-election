package rediselection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestRedis 返回连接到 miniredis 的客户端与实例。
// miniredis 的 Lua 环境完整支持 redis.call('TIME')，
// 且 SetTime/FastForward 可精确控制 Redis 服务器时钟，
// 使租约过期分支可以确定性验证。
func newTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, mr
}

const testKey = "test:leader:lease"

// TestLeaseLock_FourBranches 以时间线驱动方式验证 Lua 脚本的四个分支：
// 创建 → 续约 → 谦让 → 抢占，以及过程中的 version/transitions 语义。
func TestLeaseLock_FourBranches(t *testing.T) {
	rdb, mr := newTestRedis(t)
	ctx := context.Background()
	lockA := NewLeaseLock(rdb, testKey, "A")
	lockB := NewLeaseLock(rdb, testKey, "B")

	start := time.Unix(1700000000, 0) // 固定起点，便于毫秒级精确断言
	mr.SetTime(start)
	lease := 10 * time.Second

	// ① 无主：A 创建租约
	ev, rec, err := lockA.TryAcquireOrRenew(ctx, lease)
	if err != nil {
		t.Fatalf("A acquire: %v", err)
	}
	if ev != EventAcquired || rec.Holder != "A" || rec.Version != 1 || rec.Transitions != 0 {
		t.Fatalf("A acquire: ev=%d rec=%+v", ev, rec)
	}
	if ttl := mr.TTL(testKey); ttl < lease || ttl > 2*lease {
		t.Fatalf("ttl should be in [lease, 2*lease], got %v", ttl)
	}

	// ② A 续约：version 递增
	mr.SetTime(start.Add(1 * time.Second))
	ev, rec, err = lockA.TryAcquireOrRenew(ctx, lease)
	if err != nil {
		t.Fatalf("A renew: %v", err)
	}
	if ev != EventRenewed || rec.Holder != "A" || rec.Version != 2 {
		t.Fatalf("A renew: ev=%d rec=%+v", ev, rec)
	}

	// ④ B 谦让：A 未过期，B 不得抢占
	ev, rec, err = lockB.TryAcquireOrRenew(ctx, lease)
	if err != nil {
		t.Fatalf("B hold: %v", err)
	}
	if ev != EventHeldByOther || rec.Holder != "A" || rec.Version != 2 {
		t.Fatalf("B hold: ev=%d rec=%+v", ev, rec)
	}

	// ③ B 抢占：距 A 最后续约（start+1s）超过 lease
	mr.SetTime(start.Add(12 * time.Second))
	ev, rec, err = lockB.TryAcquireOrRenew(ctx, lease)
	if err != nil {
		t.Fatalf("B preempt: %v", err)
	}
	if ev != EventAcquired || rec.Holder != "B" || rec.Version != 3 || rec.Transitions != 1 {
		t.Fatalf("B preempt: ev=%d rec=%+v", ev, rec)
	}

	// A 复活后续约 → 谦让（B 活跃）
	ev, rec, err = lockA.TryAcquireOrRenew(ctx, lease)
	if err != nil {
		t.Fatalf("A after preempt: %v", err)
	}
	if ev != EventHeldByOther || rec.Holder != "B" {
		t.Fatalf("A after preempt: ev=%d rec=%+v", ev, rec)
	}
}

// TestLeaseLock_Get 验证只读快照的完整字段（时间精确到毫秒）。
func TestLeaseLock_Get(t *testing.T) {
	rdb, mr := newTestRedis(t)
	ctx := context.Background()
	lock := NewLeaseLock(rdb, testKey, "A")

	if _, err := lock.Get(ctx); !errors.Is(err, ErrNoLease) {
		t.Fatalf("Get on missing lease: want ErrNoLease, got %v", err)
	}

	start := time.Unix(1700000000, 0)
	mr.SetTime(start)
	if _, _, err := lock.TryAcquireOrRenew(ctx, 10*time.Second); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	rec, err := lock.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := start.UnixMilli()
	if rec.Holder != "A" || rec.Version != 1 || rec.Transitions != 0 ||
		rec.AcquireTime != want || rec.RenewTime != want {
		t.Fatalf("Get: %+v, want times=%d", rec, want)
	}
}

// TestLeaseLock_Release 验证主动让位：
// 让位后其他实例无需等待 LeaseDuration 即可抢占；
// version / transitions 历史保留（fencing token 单调性、统计连续性）；
// 非持有者让位返回 ErrNotHoldingLease 且无副作用。
func TestLeaseLock_Release(t *testing.T) {
	rdb, mr := newTestRedis(t)
	ctx := context.Background()
	lockA := NewLeaseLock(rdb, testKey, "A")
	lockB := NewLeaseLock(rdb, testKey, "B")

	start := time.Unix(1700000000, 0)
	mr.SetTime(start)
	lease := 10 * time.Second

	if _, _, err := lockA.TryAcquireOrRenew(ctx, lease); err != nil {
		t.Fatalf("A acquire: %v", err)
	}

	// 非持有者让位：无副作用
	if err := lockB.Release(ctx); !errors.Is(err, ErrNotHoldingLease) {
		t.Fatalf("B release: want ErrNotHoldingLease, got %v", err)
	}

	// A 让位
	if err := lockA.Release(ctx); err != nil {
		t.Fatalf("A release: %v", err)
	}

	// renewTime 已置 0：B 无需推进时钟即可立即抢占
	ev, rec, err := lockB.TryAcquireOrRenew(ctx, lease)
	if err != nil {
		t.Fatalf("B acquire after release: %v", err)
	}
	if ev != EventAcquired || rec.Holder != "B" {
		t.Fatalf("B acquire after release: ev=%d rec=%+v", ev, rec)
	}

	// 历史保留：A 创建 version=1，B 抢占后 version=2、transitions=1
	if rec.Version != 2 || rec.Transitions != 1 {
		t.Fatalf("history lost after release: rec=%+v", rec)
	}
}

// TestLeaseLock_TTLGC 验证 PEXPIRE 兜底：
// 持有者彻底失联时 key 最终被 Redis 清理，等待者走"新建"而非"抢占"。
func TestLeaseLock_TTLGC(t *testing.T) {
	rdb, mr := newTestRedis(t)
	ctx := context.Background()
	lockA := NewLeaseLock(rdb, testKey, "A")
	lockB := NewLeaseLock(rdb, testKey, "B")

	mr.SetTime(time.Unix(1700000000, 0))
	lease := 10 * time.Second
	if _, _, err := lockA.TryAcquireOrRenew(ctx, lease); err != nil {
		t.Fatalf("A acquire: %v", err)
	}

	// 推进 2×lease（TTL 到期，且远超租约）
	mr.FastForward(2 * lease)
	if mr.Exists(testKey) {
		t.Fatal("key should be evicted after TTL")
	}

	ev, rec, err := lockB.TryAcquireOrRenew(ctx, lease)
	if err != nil {
		t.Fatalf("B acquire after GC: %v", err)
	}
	if ev != EventAcquired || rec.Version != 1 || rec.Transitions != 0 {
		t.Fatalf("B should create fresh lease: ev=%d rec=%+v", ev, rec)
	}
}

// TestLeaseLock_Identity 验证 identity 与默认生成。
func TestLeaseLock_Identity(t *testing.T) {
	rdb, _ := newTestRedis(t)
	lock := NewLeaseLock(rdb, testKey, "A")
	if lock.Identity() != "A" || lock.Key() != testKey {
		t.Fatalf("identity=%s key=%s", lock.Identity(), lock.Key())
	}

	id1, id2 := DefaultIdentity(), DefaultIdentity()
	if id1 == "" || id1 == id2 {
		t.Fatalf("DefaultIdentity should be unique: %q vs %q", id1, id2)
	}
}
