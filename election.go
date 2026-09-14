package rediselection

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"time"
)

// Elector 是选主器，对标 K8s client-go 的 leaderelection.LeaderElector。
//
// 生命周期为状态机循环：
//
//	acquire（候选者，每 RetryPeriod±jitter 尝试获取）
//	  └─ 获取成功 → lead（持有者，每 RetryPeriod±jitter 续约）
//	       ├─ 被明确抢占（HeldByOther）→ 立即自贬
//	       ├─ 续约连续失败超 RenewDeadline → 自贬（防 Redis 抖动误判的宽限）
//	       └─ ctx 取消 → 退出
//	  └─ 自贬后回到 acquire，降级等待重选（不退出进程）
//
// 与 K8s 原生实现的两点刻意差异（均为更安全的取舍，详见 README）：
//  1. 丢锁后默认降级等待重选，而非退出进程；
//  2. 观察到租约被明确抢占时立即自贬，而非等满 RenewDeadline，
//     将"被抢占场景"的双跑窗口压缩到接近 0。
//
// Elector 非并发启动多个 Run；IsLeader/LeaderID 可被任意 goroutine 查询。
type Elector struct {
	cfg Config

	isLeader atomic.Bool
	leaderID atomic.Pointer[string]
}

// NewElector 创建选主器并校验配置（时间参数约束见 Config 注释）。
func NewElector(lock Lock, opts ...ElectorOption) (*Elector, error) {
	cfg := Config{
		Lock:          lock,
		LeaseDuration: DefaultLeaseDuration,
		RenewDeadline: DefaultRenewDeadline,
		RetryPeriod:   DefaultRetryPeriod,
		JitterFactor:  DefaultJitterFactor,
		Logger:        discardLogger{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Elector{cfg: cfg}, nil
}

// Config 返回生效的配置副本（观测用，运行期修改无效果）。
func (e *Elector) Config() Config { return e.cfg }

// Run 阻塞运行选主循环，直到 ctx 取消。丢失租约后自动降级为候选者
// 继续竞选（降级等待重选）。典型用法：
//
//	go elector.Run(ctx)
//
// 或在独立 goroutine 中与信号处理配合，收到 SIGTERM 后 cancel ctx，
// 并视需要开启 WithReleaseOnCancel 加速 failover。
func (e *Elector) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if !e.acquire(ctx) {
			return
		}
		if !e.lead(ctx) {
			return
		}
		// 丢失租约：降级为候选者，回到循环头继续竞选
	}
}

// IsLeader 原子返回本实例当前是否为 leader。
func (e *Elector) IsLeader() bool { return e.isLeader.Load() }

// LeaderID 返回最近观测到的持有者 identity（可能为自己），未知时为空串。
func (e *Elector) LeaderID() string {
	if p := e.leaderID.Load(); p != nil {
		return *p
	}
	return ""
}

// acquire 对标 K8s 的 le.acquire：周期性尝试获取租约，直到成功或 ctx 取消。
// 返回 false 表示 ctx 已取消，应退出整个状态机。
func (e *Elector) acquire(ctx context.Context) bool {
	for {
		ev, rec, err := e.cfg.Lock.TryAcquireOrRenew(ctx, e.cfg.LeaseDuration)
		switch {
		case err != nil:
			e.logf("acquire: tryAcquireOrRenew error: %v", err)
		case ev == EventAcquired || ev == EventRenewed:
			return true
		default:
			e.observeLeader(rec.Holder)
		}
		if !sleepJitter(ctx, e.cfg.RetryPeriod, e.cfg.JitterFactor) {
			return false
		}
	}
}

// lead 对标 K8s 的 le.loop：持续续约维持 leadership。
// 返回 false 表示父 ctx 已取消（退出状态机）；
// 返回 true 表示丢失租约（降级，外层重新竞选）。
func (e *Elector) lead(parent context.Context) bool {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	e.isLeader.Store(true)
	e.observeLeader(e.cfg.Lock.Identity())
	e.logf("lead: became leader, identity=%s", e.cfg.Lock.Identity())
	if e.cfg.OnStartedLeading != nil {
		go e.cfg.OnStartedLeading(ctx)
	}

	lastRenew := time.Now()
	for {
		ev, rec, err := e.cfg.Lock.TryAcquireOrRenew(ctx, e.cfg.LeaseDuration)
		now := time.Now()
		switch {
		case err != nil:
			// 存储错误（如 Redis 抖动/主从切换）：在 RenewDeadline 宽限期内继续重试，
			// 超时则自贬。宽限期保证"旧主先停（RenewDeadline）、新主后上（LeaseDuration）"。
			e.logf("lead: renew error: %v", err)
			if now.Sub(lastRenew) >= e.cfg.RenewDeadline {
				e.logf("lead: renew deadline exceeded, stepping down")
				e.stepDown()
				return parent.Err() == nil
			}
		case ev == EventHeldByOther:
			// 租约被明确抢占且新持有者活跃（存储系统健康、判定可信）：
			// 立即自贬，不等满 RenewDeadline，压缩双跑窗口。
			e.logf("lead: lease now held by %q, stepping down immediately", rec.Holder)
			e.stepDown()
			return parent.Err() == nil
		default: // EventAcquired / EventRenewed：本实例仍持有租约
			lastRenew = now
		}
		if !sleepJitter(ctx, e.cfg.RetryPeriod, e.cfg.JitterFactor) {
			e.stepDown()
			return false
		}
	}
}

// stepDown 统一处理失去 leadership 的收尾：
// 清 leader 标记、（可选）主动让位、触发 OnStoppedLeading 回调。
func (e *Elector) stepDown() {
	e.isLeader.Store(false)
	if e.cfg.ReleaseOnCancel {
		// 尽力让位：使用独立超时，不依赖已取消的选主 ctx。
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := e.cfg.Lock.Release(ctx); err != nil && !errors.Is(err, ErrNotHoldingLease) {
			e.logf("release: %v", err)
		}
		cancel()
	}
	if e.cfg.OnStoppedLeading != nil {
		e.cfg.OnStoppedLeading()
	}
	e.logf("lead: stopped leading")
}

// observeLeader 更新观测到的持有者，变化时触发 OnNewLeader。
func (e *Elector) observeLeader(id string) {
	if cur := e.leaderID.Load(); cur != nil && *cur == id {
		return
	}
	e.leaderID.Store(&id)
	if e.cfg.OnNewLeader != nil {
		// 异步触发，避免用户回调阻塞选主循环（与 K8s 行为一致）。
		go e.cfg.OnNewLeader(id)
	}
}

func (e *Elector) logf(format string, v ...interface{}) {
	if l, ok := e.cfg.Logger.(Logger); ok && l != nil {
		l.Printf("rediselection: "+format, v...)
	}
}

// jitter 返回 [period, period*(1+maxFactor)) 内的随机时长，
// 对标 k8s.io/apimachinery/pkg/util/wait.Jitter。
func jitter(period time.Duration, maxFactor float64) time.Duration {
	if maxFactor <= 0 {
		return period
	}
	return period + time.Duration(rand.Float64()*maxFactor*float64(period))
}

// sleepJitter 等待一个带抖动的周期；返回 false 表示 ctx 已取消。
func sleepJitter(ctx context.Context, period time.Duration, maxFactor float64) bool {
	timer := time.NewTimer(jitter(period, maxFactor))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
