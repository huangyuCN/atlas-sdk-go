package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// heartbeatLoop 周期发送传输保活心跳（Ping），连续失败判定死链并关闭当前代连接。
// 关闭当前代连接会触发 supervisor 重连（而非关闭整个 Client）。
// 注意：
//   - 传输心跳不续租业务会话（会话续租走登录协议的业务 Heartbeat，
//     由业务层调度，见规范 §5.2 双层心跳）；
//   - 心跳 Invoke 携带 WithFailFast：死链期间进入重连排队无意义，
//     心跳自身的失败计数就是重连触发器。
func (c *Client) heartbeatLoop(done chan struct{}) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	var failures int
	for {
		select {
		case <-done: // 当前代连接死亡（读循环已关闭代信号）：心跳随代退出
			return
		case <-c.closeCh:
			return
		case <-ticker.C:
			if err := c.Invoke(nil, HeartbeatOperation, nil, nil, WithFailFast()); err != nil {
				failures++
				if failures >= defaultHeartbeatFailures {
					// 死链：只关当前代连接（触发 supervisor 重连），不关 Client。
					c.closeConn()
					return
				}
				continue
			}
			failures = 0
		}
	}
}

// readLoop 为当前代连接循环读帧并分发；退出时关闭代信号（触发重连/关闭收尾）、
// 回收本代 in-flight（epoch 匹配天然隔离旧代）。
func (c *Client) readLoop(done chan struct{}) {
	defer func() {
		close(done)
		c.wg.Done()
	}()
	var exitErr error
	for {
		hdr, body, err := frame.Read(c.currentConn(), c.maxBodySize)
		if err != nil {
			if !c.closed.Load() {
				exitErr = c.classifyReadError(err)
			}
			break
		}
		switch hdr.Type {
		case frame.MsgTypeResponse:
			c.dispatchResponse(hdr, body)
		case frame.MsgTypeNotify:
			c.dispatchNotify(hdr, body)
		default:
			// Request 帧不应出现在客户端侧：协议错误，终止本代读循环。
			exitErr = &ProtocolError{cause: fmt.Errorf("client: 收到非法帧类型 %d", hdr.Type)}
		}
		if exitErr != nil {
			break
		}
	}
	// 本代死亡：回收 in-flight（幂等，Close 路径亦会调用）。
	c.failAllInflight(exitErr)
	// 协议级错误（版本不匹配/帧非法）重连无意义：终止整个 Client；
	// 网络类错误交由 supervisor 重连。
	if pe, ok := exitErr.(*ProtocolError); ok {
		c.terminate(pe)
	}
	if fn := c.onReadExit.Load(); fn != nil && exitErr != nil {
		go c.safeOnReadExit(*fn, exitErr)
	}
}

// terminate 协议级致命错误：终止整个 Client（不重连）。幂等；不等待 goroutine。
func (c *Client) terminate(cause error) {
	c.lifecycleMu.Lock()
	if !c.closed.Swap(true) {
		close(c.closeCh)
	}
	c.lifecycleMu.Unlock()
	_ = c.currentConn().Close()
	c.state.Store(int32(StateDisconnected))
}

// safeOnReadExit 带保护的读循环退出回调执行。
func (c *Client) safeOnReadExit(fn func(error), err error) {
	defer func() { _ = recover() }()
	fn(err)
}

// classifyReadError 将读循环错误归类为规范 §7 的错误类型。
func (c *Client) classifyReadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, frame.ErrProtocol) {
		// 帧协议非法（magic/version/type/seq/长度/包络非法）：不可重试。
		return &ProtocolError{cause: err}
	}
	return &NetworkError{cause: err}
}
