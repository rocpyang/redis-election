package rediselection

import "errors"

var (
	// ErrInvalidConfig 由 NewElector 返回，表示配置不满足约束
	// （如 RenewDeadline 未小于 LeaseDuration）。包装信息中带有具体原因。
	ErrInvalidConfig = errors.New("rediselection: invalid config")

	// ErrNotHoldingLease 由 LeaseLock.Release 返回：租约当前不属于本 identity
	// （可能已被抢占，或从未持有）。此时让位无副作用，调用方通常可直接忽略。
	ErrNotHoldingLease = errors.New("rediselection: lease is not held by this identity")

	// ErrNoLease 由 LeaseLock.Get 返回：租约记录尚不存在。
	ErrNoLease = errors.New("rediselection: lease record does not exist")
)
