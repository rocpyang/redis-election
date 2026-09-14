package rediselection

import (
	"context"
	"fmt"
	"time"
)

// 时间参数默认值，与 K8s client-go leaderelection 的推荐配置一致。
const (
	// DefaultLeaseDuration 租约有效期：其他实例在持有者超过该时长未续约后
	// 才会判定其失效并发起抢占，是 failover 的最坏等待时间（主要构成）。
	DefaultLeaseDuration = 15 * time.Second
	// DefaultRenewDeadline 持有者的续约预算：连续失败达到该时长即自贬停止
	// leading。必须小于 LeaseDuration，以保证"旧主先停、新主后上"，
	// 消除 Redis 抖动场景下的双跑窗口。
	DefaultRenewDeadline = 10 * time.Second
	// DefaultRetryPeriod 获取/续约的尝试间隔（在其基础上叠加抖动）。
	DefaultRetryPeriod = 2 * time.Second
	// DefaultJitterFactor 抖动系数：实际等待间隔在
	// [RetryPeriod, RetryPeriod*(1+JitterFactor)) 内随机，
	// 防止多实例同频竞争。与 K8s wait.JitterUntil 的默认因子一致。
	DefaultJitterFactor = 1.2
)

// Config 是 Elector 的完整配置，由 NewElector 基于选项装配。
type Config struct {
	Lock Lock

	// LeaseDuration / RenewDeadline / RetryPeriod / JitterFactor 见常量注释。
	// 约束：RetryPeriod < RenewDeadline < LeaseDuration。
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	JitterFactor  float64

	// ReleaseOnCancel 为 true 时，停止 leading 后主动让位
	//（把 renewTime 置 0 加速 failover）。适合 SIGTERM 优雅退出场景。
	// 与 K8s 同名配置语义一致。注意：让位意味着放弃租约，
	// 仅当业务能容忍立即交出 leader 职责时开启。
	ReleaseOnCancel bool

	Logger Logger

	OnStartedLeading func(ctx context.Context)
	OnStoppedLeading func()
	OnNewLeader      func(identity string)
}

// ElectorOption 是 NewElector 的选项函数。
type ElectorOption func(*Config)

// WithLeaseDuration 设置租约有效期，必须大于 RenewDeadline。
func WithLeaseDuration(d time.Duration) ElectorOption {
	return func(c *Config) { c.LeaseDuration = d }
}

// WithRenewDeadline 设置续约失败自贬的宽限预算，必须小于 LeaseDuration 且大于 RetryPeriod。
func WithRenewDeadline(d time.Duration) ElectorOption {
	return func(c *Config) { c.RenewDeadline = d }
}

// WithRetryPeriod 设置尝试间隔，必须小于 RenewDeadline。
func WithRetryPeriod(d time.Duration) ElectorOption {
	return func(c *Config) { c.RetryPeriod = d }
}

// WithJitterFactor 设置抖动系数（0 表示不抖动），默认 1.2。
func WithJitterFactor(f float64) ElectorOption {
	return func(c *Config) { c.JitterFactor = f }
}

// WithReleaseOnCancel 开启停止 leading 后的主动让位。
func WithReleaseOnCancel(b bool) ElectorOption {
	return func(c *Config) { c.ReleaseOnCancel = b }
}

// WithLogger 注入日志实现，默认静默丢弃。
func WithLogger(l Logger) ElectorOption {
	return func(c *Config) {
		if l != nil {
			c.Logger = l
		}
	}
}

// WithOnStartedLeading 注册"成为 leader"回调：在获得租约后调用一次。
// 回调收到的 ctx 在失去租约时被取消，业务应据此停止 leader 专属工作
// （对标 K8s 的 OnStartedLeading）。回调运行在独立 goroutine 中。
func WithOnStartedLeading(f func(ctx context.Context)) ElectorOption {
	return func(c *Config) { c.OnStartedLeading = f }
}

// WithOnStoppedLeading 注册"失去 leader"回调：在自贬或丢锁后调用一次
// （对标 K8s 的 OnStoppedLeading）。
func WithOnStoppedLeading(f func()) ElectorOption {
	return func(c *Config) { c.OnStoppedLeading = f }
}

// WithOnNewLeader 注册"观察到新 leader"回调，包括自己首次当选。
// 回调不应长时间阻塞（运行在独立 goroutine 中），
// 且多次回调可能并发执行，回调内访问共享状态需自行同步。
func WithOnNewLeader(f func(identity string)) ElectorOption {
	return func(c *Config) { c.OnNewLeader = f }
}

// validate 校验配置约束，与 K8s LeaderElectionConfig 的校验规则对齐。
func (c *Config) validate() error {
	switch {
	case c.Lock == nil:
		return fmt.Errorf("%w: lock must not be nil", ErrInvalidConfig)
	case c.LeaseDuration <= 0:
		return fmt.Errorf("%w: LeaseDuration must be positive, got %s", ErrInvalidConfig, c.LeaseDuration)
	case c.RenewDeadline <= 0:
		return fmt.Errorf("%w: RenewDeadline must be positive, got %s", ErrInvalidConfig, c.RenewDeadline)
	case c.RetryPeriod <= 0:
		return fmt.Errorf("%w: RetryPeriod must be positive, got %s", ErrInvalidConfig, c.RetryPeriod)
	case c.RenewDeadline >= c.LeaseDuration:
		return fmt.Errorf("%w: RenewDeadline(%s) must be less than LeaseDuration(%s)",
			ErrInvalidConfig, c.RenewDeadline, c.LeaseDuration)
	case c.RetryPeriod >= c.RenewDeadline:
		return fmt.Errorf("%w: RetryPeriod(%s) must be less than RenewDeadline(%s)",
			ErrInvalidConfig, c.RetryPeriod, c.RenewDeadline)
	case c.JitterFactor < 0:
		return fmt.Errorf("%w: JitterFactor must not be negative, got %v", ErrInvalidConfig, c.JitterFactor)
	}
	return nil
}
