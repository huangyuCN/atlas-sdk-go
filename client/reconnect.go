package client

import (
	"math/rand"
	"time"
)

// jitter 在 d 的 ±20% 范围内抖动，避免断线风暴下的重连同步。
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	delta := d / 5
	return d - delta + time.Duration(rand.Int63n(int64(2*delta)+1))
}

// sleepInterruptible 可被关闭信号打断的睡眠；被打断返回错误。
func sleepInterruptible(d time.Duration, closeCh <-chan struct{}) error {
	if d <= 0 {
		select {
		case <-closeCh:
			return errClosed
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-closeCh:
		return errClosed
	case <-t.C:
		return nil
	}
}

// errClosed 是内部关闭信号哨兵。
var errClosed = errClosedSentinel{}

type errClosedSentinel struct{}

func (errClosedSentinel) Error() string { return "client: closed" }
