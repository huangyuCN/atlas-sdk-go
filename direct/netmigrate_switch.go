// 换网切换流程：requestSwitch 合并并发信号 → 拨新 socket → 重发 hello 拿新 flow-id →
// 写切换点（换 cur，代次 +1）→ 关旧 socket。读切换点由 ReadFrom 的代次判定完成：
// 旧 socket 的阻塞读被 Close 唤起，代次不匹配即静默改读新 socket。
//
// 顺序不可颠倒：先切写、后关旧 socket（hello 之前关会白丢窗口内旧路径的回包）。

package direct

import (
	"context"
	"time"
)

// newKCPFlowConn 按会话配置装配换网胶水（初始拨号 + hello + 网络变化轮询）。
func newKCPFlowConn(addr string, ticket []byte, o options) (*flowPacketConn, error) {
	cfg := flowConnConfig{
		EdgeAddr:            addr,
		Ticket:              ticket,
		HelloTimeout:        o.switchHelloTimeout,
		InitialHelloTimeout: o.handshakeTimeout,
		EdgeHello:           o.edgeHello,
		Dial:                o.switchDialer,
		DialSocket:          o.switchDial,
		Now:                 o.switchClock,
		Stats:               o.flow,
		Watcher:             o.switchWatcher,
		PollInterval:        o.switchPollInterval,
	}
	limits := switchLimits{
		Window:     o.switchWindow,
		IdleSwitch: o.idleSwitch,
		IdleDead:   o.idleDead,
		SwallowMax: o.swallowMax,
		RetryBase:  o.switchRetryBase,
	}
	return newFlowPacketConn(cfg, limits)
}

// requestSwitch 请求一次切换：同一时刻至多一个迁移在跑（并发信号合并为「再跑一次」标记），
// 成功切换后并发信号直接作废；短时间内的重复请求按 RetryBase 防抖。
func (c *flowPacketConn) requestSwitch(reason SwitchReason) {
	if c.isClosed() {
		return
	}
	now := c.now()
	c.switchMu.Lock()
	if c.switching {
		c.pending = true
		c.pendingCause = reason
		c.switchMu.Unlock()
		return
	}
	if !c.lastSwitchAt.IsZero() && now.Sub(c.lastSwitchAt) < c.limits.RetryBase {
		c.switchMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.switching = true
	c.pending = true
	c.pendingCause = reason
	c.windowStart = now
	c.switchCancel = cancel
	c.switchMu.Unlock()
	go c.switchLoop(ctx, reason)
}

// switchLoop 串行执行迁移：成功即清空并发标记（已换好，无需再跑），失败且仍有请求才重试。
func (c *flowPacketConn) switchLoop(ctx context.Context, reason SwitchReason) {
	for {
		err := c.migrate(ctx, reason)
		c.switchMu.Lock()
		if err == nil {
			c.pending = false
		}
		next := c.pending
		if !next || c.isClosed() {
			c.switchMu.Unlock()
			c.finishSwitch()
			return
		}
		reason = c.pendingCause
		c.pending = false
		c.switchMu.Unlock()
		if err != nil {
			if serr := sleepCtx(ctx, c.limits.RetryBase); serr != nil {
				c.finishSwitch()
				return
			}
		}
	}
}

// finishSwitch 收口一次迁移：清切换标记并取消迁移 ctx。
func (c *flowPacketConn) finishSwitch() {
	c.switchMu.Lock()
	c.switching = false
	cancel := c.switchCancel
	c.switchCancel = nil
	c.switchMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// migrate 执行一次切换：拨新 socket → hello → 写切换点 → 关旧 socket → 统计。
func (c *flowPacketConn) migrate(ctx context.Context, reason SwitchReason) error {
	_ = reason // 原因只用于统计口径（SDK 无 logger，观测经 Stats 出口）
	start := c.now()
	sock, flowID, err := c.dialEndpoint(ctx, c.cfg.HelloTimeout)
	if err != nil {
		c.stats.noteSwitchFailure(err)
		c.switchMu.Lock()
		c.lastSwitchAt = c.now()
		c.switchMu.Unlock()
		return err
	}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		_ = sock.Close()
		return ErrClosed
	}
	old := c.cur
	c.cur = &flowEndpoint{sock: sock, flowID: flowID, gen: old.gen + 1}
	c.mu.Unlock()

	_ = old.sock.Close() // 严格晚于写切换点：先切写、后切读
	c.touchRecv()        // 新路径基线
	c.swallowed.Store(0)
	c.switchMu.Lock()
	c.swallowSince = time.Time{}
	c.lastSwitchAt = c.now()
	c.switchMu.Unlock()
	c.stats.noteSwitch(c.now().Sub(start))
	return nil
}

// startWatcher 启动网络变化事件源（缺省为本地地址集合轮询，1s 一拍）。
func (c *flowPacketConn) startWatcher() {
	w := c.cfg.Watcher
	if w == nil {
		c.ownedWatch = newPollWatcher(c.cfg.PollInterval)
		w = c.ownedWatch
	}
	c.watcher = w
	go func() {
		changes := w.Changes()
		for {
			select {
			case <-c.closeCh:
				return
			case _, ok := <-changes:
				if !ok {
					return
				}
				c.requestSwitch(SwitchNetworkEvent)
			}
		}
	}()
}
