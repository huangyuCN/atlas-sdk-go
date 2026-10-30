// 换网胶水的坏死兜底：两条触发（T2 无收包 / 吞错超预算）都**只返回读错误**，交给既有的
// 会话重连（supervise → reconnectRound → WithReplay）收敛；本文件不重建 KCP 会话。

package direct

import (
	"errors"
	"fmt"
	"time"
)

// ErrFlowDead 是换网流已坏死的哨兵（errors.Is 判定）；具体原因用 FlowDeadReason 取。
var ErrFlowDead = errors.New("direct: 换网流已坏死")

// DeadReason 是兜底重建的原因（枚举）。
type DeadReason string

// 全部坏死原因。
const (
	// DeadIdle 是 T2 内无任何收包（长暂停/观战、无写出的死链）。
	DeadIdle DeadReason = "idle_no_rx"
	// DeadSwallow 是窗口内吞掉的写错误超过条数上限或持续超过 2×窗口。
	DeadSwallow DeadReason = "swallow_budget"
)

// flowDeadError 是交给 KCP 读循环的持久错误：只由会话上层决定是否重建。
type flowDeadError struct {
	reason DeadReason
	cause  error
}

// Error 返回带原因的错误文本。
func (e *flowDeadError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%v: %s（%v）", ErrFlowDead, e.reason, e.cause)
	}
	return fmt.Sprintf("%v: %s", ErrFlowDead, e.reason)
}

// Unwrap 让 errors.Is(err, ErrFlowDead) 成立。
func (e *flowDeadError) Unwrap() error { return ErrFlowDead }

// FlowDeadReason 取出坏死原因（err 不是坏死错误时返回 false）。
func FlowDeadReason(err error) (DeadReason, bool) {
	var de *flowDeadError
	if errors.As(err, &de) {
		return de.reason, true
	}
	return "", false
}

// deadErr 返回已登记的坏死错误（nil = 尚未坏死）。
func (c *flowPacketConn) deadErr() error {
	if d := c.dead.Load(); d != nil {
		return d
	}
	return nil
}

// markDead 登记坏死原因并计数（幂等：只有首次登记计入 DeadRebuilds）。
func (c *flowPacketConn) markDead(reason DeadReason, cause error) error {
	err := &flowDeadError{reason: reason, cause: cause}
	if c.dead.CompareAndSwap(nil, err) {
		c.stats.noteDead(reason)
	}
	return c.dead.Load()
}

// checkTimers 在读侧定时 tick 上检查两条兜底与 T1 迁移信号。
func (c *flowPacketConn) checkTimers() error {
	now := c.now()
	recv := time.Unix(0, c.lastRecvNs.Load())
	if idle := now.Sub(recv); idle > c.limits.IdleDead {
		return c.markDead(DeadIdle, nil)
	}
	if idle := now.Sub(recv); idle > c.limits.IdleSwitch {
		if wrote := now.Sub(time.Unix(0, c.lastWriteNs.Load())); wrote <= c.limits.IdleSwitch {
			c.requestSwitch(SwitchReadTimeout)
		}
	}
	return nil
}

// noteSwallowed 记一次窗口内吞掉的写错误；超条数上限或持续超 2×窗口即判坏死。
func (c *flowPacketConn) noteSwallowed(err error) {
	c.stats.noteSwallowed()
	n := c.swallowed.Add(1)
	now := c.now()
	c.switchMu.Lock()
	if c.swallowSince.IsZero() {
		c.swallowSince = now
	}
	since := c.swallowSince
	c.switchMu.Unlock()
	if n > c.limits.SwallowMax || now.Sub(since) > 2*c.limits.Window {
		c.markDead(DeadSwallow, err)
	}
}
