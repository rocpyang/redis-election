// example/basic 演示 redis-election 的基本用法与 failover 效果。
//
// 运行（需本地 Redis）：
//
//	go run ./example/basic
//
// 验证 failover：开两个终端分别运行，观察同一时刻只有一个实例执行
// leader 专属工作；Ctrl+C 停掉 leader 后，另一实例在一个重试周期内接管。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	election "github.com/rocpyang/redis-election"
)

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	key := getenv("LEASE_KEY", "example:leader:lease")
	identity := election.DefaultIdentity()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect to redis %s: %v\n", addr, err)
		os.Exit(1)
	}

	lock := election.NewLeaseLock(rdb, key, identity)

	elector, err := election.NewElector(lock,
		election.WithReleaseOnCancel(true), // SIGTERM 时主动让位，加速 failover
		election.WithOnStartedLeading(func(ctx context.Context) {
			fmt.Printf("[%s] I am the leader now, starting leader-only work\n", identity)
			runLeaderWork(ctx, identity)
		}),
		election.WithOnStoppedLeading(func() {
			fmt.Printf("[%s] lost leadership, leader-only work stopped\n", identity)
		}),
		election.WithOnNewLeader(func(id string) {
			fmt.Printf("[%s] observed new leader: %s\n", identity, id)
		}),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf("[%s] starting election, redis=%s key=%s\n", identity, addr, key)
	elector.Run(ctx) // 阻塞：降级等待重选，直到 ctx 取消
	fmt.Printf("[%s] exited\n", identity)
}

// runLeaderWork 模拟 leader 专属的周期任务；
// 通过 ctx 感知失去 leadership 并退出。
func runLeaderWork(ctx context.Context, identity string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n++
			fmt.Printf("[%s] leader working... tick=%d\n", identity, n)
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
