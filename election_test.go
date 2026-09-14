package rediselection

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 轮询等待条件成立，超时返回 false。
func waitFor(t *testing.T, timeout time.Duration, name string, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("waitFor %q timed out after %v", name, timeout)
	return cond()
}

// fastOptions 是基于真时钟的加速参数（jitter=0 保证时序确定性）。
func fastOptions(releaseOnCancel bool, started func(ctx context.Context), stopped func(), newLeader func(string)) []ElectorOption {
	return []ElectorOption{
		WithLeaseDuration(300 * time.Millisecond),
		WithRenewDeadline(200 * time.Millisecond),
		WithRetryPeriod(15 * time.Millisecond),
		WithJitterFactor(0),
		WithReleaseOnCancel(releaseOnCancel),
		WithOnStartedLeading(started),
		WithOnStoppedLeading(stopped),
		WithOnNewLeader(newLeader),
	}
}

// TestElector_Lifecycle 验证单实例完整生命周期：
// 当选 → IsLeader=true → OnStartedLeading 的 ctx 随 cancel 被取消 →
// OnStoppedLeading 触发 → 主动让位（renewTime=0）→ Run 退出。
func TestElector_Lifecycle(t *testing.T) {
	rdb, mr := newTestRedis(t)
	mr.SetTime(time.Unix(1700000000, 0)) // 定住时钟，隔离真实时间

	startedCtxDone := make(chan struct{})
	started := func(ctx context.Context) {
		<-ctx.Done()
		close(startedCtxDone)
	}
	var stoppedCount atomic.Int32
	stoppedCh := make(chan struct{}, 1)
	stopped := func() {
		stoppedCount.Add(1)
		select {
		case stoppedCh <- struct{}{}:
		default:
		}
	}

	lock := NewLeaseLock(rdb, testKey, "A")
	elector, err := NewElector(lock, fastOptions(true, started, stopped, nil)...)
	if err != nil {
		t.Fatalf("NewElector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { elector.Run(ctx); close(runDone) }()

	if !waitFor(t, 2*time.Second, "become leader", elector.IsLeader) {
		t.Fatal("should become leader")
	}
	if elector.LeaderID() != "A" {
		t.Fatalf("LeaderID=%q, want A", elector.LeaderID())
	}

	cancel()
	select {
	case <-startedCtxDone:
	case <-time.After(2 * time.Second):
		t.Fatal("OnStartedLeading ctx should be cancelled")
	}
	select {
	case <-stoppedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnStoppedLeading should fire")
	}
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run should return after ctx cancel")
	}
	if elector.IsLeader() {
		t.Fatal("should not be leader after cancel")
	}
	// stepDown 已触发过一次，Run 退出（当过 leader）不重复触发
	if n := stoppedCount.Load(); n != 1 {
		t.Fatalf("OnStoppedLeading should fire exactly once, got %d", n)
	}

	// ReleaseOnCancel：renewTime 被置 0，加速 failover
	rec, err := lock.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec.Holder != "A" || rec.RenewTime != 0 {
		t.Fatalf("release should keep holder but zero renewTime: %+v", rec)
	}
}

// TestElector_Failover 验证双实例 failover：
// A 当选 → B 观察到 OnNewLeader(A) → A 优雅退出（让位）→
// B 在一个重试周期内接管 → B 观察到 OnNewLeader(B)。
func TestElector_Failover(t *testing.T) {
	rdb, _ := newTestRedis(t)

	newLeaderB := make(chan string, 4)
	// OnNewLeader 回调由库异步（go）触发，与 waitFor 轮询构成跨 goroutine 读写，
	// 必须用原子变量同步（CI 开启 -race 时普通 bool 会告警）。
	var gotA, gotSelf atomic.Bool
	onNewLeaderB := func(id string) {
		select {
		case newLeaderB <- id:
		default:
		}
		if id == "A" {
			gotA.Store(true)
		}
		if id == "B" {
			gotSelf.Store(true)
		}
	}
	stoppedA := make(chan struct{}, 1)

	lockA := NewLeaseLock(rdb, testKey, "A")
	lockB := NewLeaseLock(rdb, testKey, "B")
	eA, err := NewElector(lockA, fastOptions(true, nil, func() { stoppedA <- struct{}{} }, nil)...)
	if err != nil {
		t.Fatalf("NewElector A: %v", err)
	}
	eB, err := NewElector(lockB, fastOptions(false, nil, nil, onNewLeaderB)...)
	if err != nil {
		t.Fatalf("NewElector B: %v", err)
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go eA.Run(ctxA)
	if !waitFor(t, 2*time.Second, "A becomes leader", eA.IsLeader) {
		t.Fatal("A should become leader")
	}

	go eB.Run(ctxB)
	if !waitFor(t, 2*time.Second, "B observes leader A", func() bool { return gotA.Load() }) {
		t.Fatal("B should observe A as leader")
	}
	if eB.IsLeader() {
		t.Fatal("B must not be leader while A is active")
	}

	// A 优雅退出：让位后 B 的接管时间应约为一个 RetryPeriod，远小于 LeaseDuration
	cancelA()
	if !waitFor(t, 1*time.Second, "B takes over", eB.IsLeader) {
		t.Fatal("B should take over after A steps down")
	}
	select {
	case <-stoppedA:
	case <-time.After(2 * time.Second):
		t.Fatal("A OnStoppedLeading should fire")
	}
	if !waitFor(t, 2*time.Second, "B observes self as leader", func() bool { return gotSelf.Load() }) {
		t.Fatal("B should observe itself as new leader")
	}
}

// TestElector_SelfDemoteOnRedisFailure 验证 Redis 故障时的自贬：
// renew 连续失败超过 RenewDeadline 后触发 OnStoppedLeading（宁可停跑，不可双跑）。
func TestElector_SelfDemoteOnRedisFailure(t *testing.T) {
	rdb, mr := newTestRedis(t)

	stoppedCh := make(chan struct{}, 1)
	lock := NewLeaseLock(rdb, testKey, "A")
	elector, err := NewElector(lock, fastOptions(false, nil, func() { stoppedCh <- struct{}{} }, nil)...)
	if err != nil {
		t.Fatalf("NewElector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go elector.Run(ctx)
	if !waitFor(t, 2*time.Second, "become leader", elector.IsLeader) {
		t.Fatal("should become leader")
	}

	mr.Close() // 注入 Redis 故障：所有后续命令失败

	select {
	case <-stoppedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("should self-demote after RenewDeadline of renew failures")
	}
	if elector.IsLeader() {
		t.Fatal("must not be leader after self-demotion")
	}
}

// TestElector_ImmediateStepDownAndReelect 验证"被明确抢占后立即自贬"
// （不等满 RenewDeadline，压缩双跑窗口），以及"降级等待重选"：
// 自贬后继续竞选，前任让位后可重新当选。
func TestElector_ImmediateStepDownAndReelect(t *testing.T) {
	rdb, mr := newTestRedis(t)

	// lease=1s, renew=600ms：若走"宽限自贬"路径需 >=600ms，
	// 立即自贬应在一个重试周期（20ms 量级）内完成。
	stoppedCh := make(chan time.Time, 1)
	stoppedAt := time.Time{}
	stopped := func() { stoppedCh <- time.Now() }

	lock := NewLeaseLock(rdb, testKey, "A")
	elector, err := NewElector(lock,
		WithLeaseDuration(1*time.Second),
		WithRenewDeadline(600*time.Millisecond),
		WithRetryPeriod(20*time.Millisecond),
		WithJitterFactor(0),
		WithOnStoppedLeading(stopped),
	)
	if err != nil {
		t.Fatalf("NewElector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go elector.Run(ctx)
	if !waitFor(t, 2*time.Second, "become leader", elector.IsLeader) {
		t.Fatal("should become leader")
	}

	// 定住时钟，模拟外部实例 B 已完成接管且活跃续约
	//（直接写入终态，避免与 A 的续约产生竞态）
	t0 := time.Unix(1700000000, 0)
	mr.SetTime(t0)
	injectAt := time.Now()
	rdb.HSet(context.Background(), testKey, map[string]interface{}{
		"holder":      "B",
		"version":     99,
		"acquireTime": t0.UnixMilli(),
		"renewTime":   t0.UnixMilli(),
		"transitions": 5,
	})

	select {
	case stoppedAt = <-stoppedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("should step down after being preempted")
	}
	// 立即自贬应在一个重试周期（20ms 量级）内完成；
	// 400ms 阈值（< RenewDeadline=600ms）足以区分立即自贬与宽限自贬。
	if d := stoppedAt.Sub(injectAt); d < 0 || d > 400*time.Millisecond {
		t.Fatalf("step-down took %v, immediate path expected (<400ms)", d)
	}
	if elector.IsLeader() {
		t.Fatal("must not be leader after preemption")
	}

	// 降级等待重选：B 让位（renewTime=0）后 A 重新当选
	rdb.HSet(context.Background(), testKey, "renewTime", 0)
	if !waitFor(t, 2*time.Second, "re-elected", elector.IsLeader) {
		t.Fatal("should be re-elected after holder released")
	}
	rec, err := lock.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec.Holder != "A" || rec.Transitions != 6 {
		t.Fatalf("re-election should keep history: %+v", rec)
	}
}

// TestElector_LeaderOnlyWorkWithCtx 验证 OnStartedLeading 的典型用法：
// 在回调内运行 leader 专属工作，通过 ctx 感知停止。
func TestElector_LeaderOnlyWorkWithCtx(t *testing.T) {
	rdb, _ := newTestRedis(t)

	workDone := make(chan int, 64)
	started := func(ctx context.Context) {
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		n := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				n++
				select {
				case workDone <- n:
				default:
				}
			}
		}
	}

	lock := NewLeaseLock(rdb, testKey, "A")
	elector, err := NewElector(lock, fastOptions(false, started, nil, nil)...)
	if err != nil {
		t.Fatalf("NewElector: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go elector.Run(ctx)

	if !waitFor(t, 2*time.Second, "work started", func() bool { return len(workDone) > 0 }) {
		t.Fatal("leader work should run")
	}
	cancel()
	time.Sleep(50 * time.Millisecond) // 等待回调退出
	before := len(workDone)
	time.Sleep(100 * time.Millisecond)
	if after := len(workDone); after != before {
		t.Fatalf("work should stop after ctx cancel: before=%d after=%d", before, after)
	}
}

// TestElector_StepDownOrder 验证 K8s 不变量：OnStoppedLeading 触发时，
// OnStartedLeading 收到的业务 ctx 已被取消（cancel 前置于回调与 Release）。
func TestElector_StepDownOrder(t *testing.T) {
	rdb, _ := newTestRedis(t)

	var bizCtx atomic.Pointer[context.Context]
	started := func(ctx context.Context) { bizCtx.Store(&ctx) }
	stoppedOk := make(chan bool, 1)
	stopped := func() {
		p := bizCtx.Load()
		stoppedOk <- p != nil && (*p).Err() != nil
	}

	lock := NewLeaseLock(rdb, testKey, "A")
	elector, err := NewElector(lock, fastOptions(true, started, stopped, nil)...)
	if err != nil {
		t.Fatalf("NewElector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() { elector.Run(ctx); close(runDone) }()

	if !waitFor(t, 2*time.Second, "become leader", elector.IsLeader) {
		t.Fatal("should become leader")
	}
	if !waitFor(t, 2*time.Second, "OnStartedLeading", func() bool { return bizCtx.Load() != nil }) {
		t.Fatal("OnStartedLeading should fire")
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run should return after cancel")
	}
	select {
	case ok := <-stoppedOk:
		if !ok {
			t.Fatal("OnStoppedLeading fired before business ctx was cancelled")
		}
	default:
		t.Fatal("OnStoppedLeading should fire")
	}
}

// TestElector_RunGuard 验证并发 Run 防护：后续调用立即返回且不触发回调；
// Run 退出后可再次启动。
func TestElector_RunGuard(t *testing.T) {
	rdb, _ := newTestRedis(t)

	var stoppedCount atomic.Int32
	lock := NewLeaseLock(rdb, testKey, "A")
	elector, err := NewElector(lock, fastOptions(false, nil, func() { stoppedCount.Add(1) }, nil)...)
	if err != nil {
		t.Fatalf("NewElector: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	done1 := make(chan struct{})
	go func() { elector.Run(ctx1); close(done1) }()
	if !waitFor(t, 2*time.Second, "first Run becomes leader", elector.IsLeader) {
		t.Fatal("first Run should become leader")
	}

	// 并发第二次 Run：应立即返回
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan struct{})
	go func() { elector.Run(ctx2); close(done2) }()
	select {
	case <-done2:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("concurrent Run should return immediately")
	}
	if n := stoppedCount.Load(); n != 0 {
		t.Fatalf("rejected Run must not fire OnStoppedLeading, got %d calls", n)
	}

	cancel1()
	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("first Run should exit after cancel")
	}
	if n := stoppedCount.Load(); n != 1 {
		t.Fatalf("OnStoppedLeading should fire exactly once, got %d", n)
	}

	// Run 退出后可重启
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	go elector.Run(ctx3)
	if !waitFor(t, 2*time.Second, "Run restarts and leads again", elector.IsLeader) {
		t.Fatal("Run should be restartable after exit")
	}
}

// TestElector_OnStoppedLeadingNeverLed 验证 K8s 对齐语义：
// 从未当选的实例在 Run 退出时也触发一次 OnStoppedLeading。
func TestElector_OnStoppedLeadingNeverLed(t *testing.T) {
	rdb, mr := newTestRedis(t)

	// 预置他人持有的活跃租约（定住时钟使其永不过期），本实例无法当选
	t0 := time.Unix(1700000000, 0)
	mr.SetTime(t0)
	rdb.HSet(context.Background(), testKey, map[string]interface{}{
		"holder":      "B",
		"version":     1,
		"acquireTime": t0.UnixMilli(),
		"renewTime":   t0.UnixMilli(),
		"transitions": 0,
	})

	var stoppedCount atomic.Int32
	lock := NewLeaseLock(rdb, testKey, "A")
	elector, err := NewElector(lock, fastOptions(false, nil, func() { stoppedCount.Add(1) }, nil)...)
	if err != nil {
		t.Fatalf("NewElector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { elector.Run(ctx); close(runDone) }()

	// 等待至少一轮"谦让"尝试
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run should exit after cancel")
	}
	if n := stoppedCount.Load(); n != 1 {
		t.Fatalf("OnStoppedLeading should fire once on exit without leading, got %d", n)
	}
	if elector.IsLeader() {
		t.Fatal("must never become leader")
	}
}

// TestNewElector_Validation 验证配置约束（与 K8s LeaderElectionConfig 校验对齐）。
func TestNewElector_Validation(t *testing.T) {
	rdb, _ := newTestRedis(t)
	lock := NewLeaseLock(rdb, testKey, "A")

	cases := []struct {
		name  string
		opts  []ElectorOption
		valid bool
	}{
		{"defaults", nil, true},
		{"valid custom", []ElectorOption{
			WithLeaseDuration(10 * time.Second),
			WithRenewDeadline(5 * time.Second),
			WithRetryPeriod(1 * time.Second),
		}, true},
		{"zero lease", []ElectorOption{WithLeaseDuration(0)}, false},
		{"zero renew", []ElectorOption{WithRenewDeadline(0)}, false},
		{"zero retry", []ElectorOption{WithRetryPeriod(0)}, false},
		{"renew >= lease", []ElectorOption{
			WithLeaseDuration(5 * time.Second), WithRenewDeadline(5 * time.Second),
		}, false},
		{"retry >= renew", []ElectorOption{
			WithRenewDeadline(5 * time.Second), WithRetryPeriod(5 * time.Second),
		}, false},
		{"renew <= retry*jitter", []ElectorOption{
			WithLeaseDuration(10 * time.Second),
			WithRenewDeadline(2300 * time.Millisecond), WithRetryPeriod(2 * time.Second),
		}, false},
		{"jitter 0 keeps basic rule", []ElectorOption{
			WithJitterFactor(0), WithRenewDeadline(2 * time.Second), WithRetryPeriod(3 * time.Second),
		}, false},
		{"negative jitter", []ElectorOption{WithJitterFactor(-0.1)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewElector(lock, tc.opts...)
			if tc.valid && err != nil {
				t.Fatalf("want valid, got %v", err)
			}
			if !tc.valid && !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("want ErrInvalidConfig, got %v", err)
			}
		})
	}

	if _, err := NewElector(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil lock: want ErrInvalidConfig, got %v", err)
	}
}

// TestJitter 验证抖动范围与退化行为。
func TestJitter(t *testing.T) {
	period := 100 * time.Millisecond
	if d := jitter(period, 0); d != period {
		t.Fatalf("factor=0 should return period exactly, got %v", d)
	}
	for i := 0; i < 1000; i++ {
		d := jitter(period, 1.2)
		if d < period || d >= period+time.Duration(1.2*float64(period)) {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
}

// TestSleepJitter 验证 ctx 取消即时中断等待。
func TestSleepJitter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepJitter(ctx, time.Hour, 1.2) {
		t.Fatal("cancelled ctx should return false immediately")
	}
	if !sleepJitter(context.Background(), time.Millisecond, 0) {
		t.Fatal("normal ctx should return true after period")
	}
}
