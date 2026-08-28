package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"time"
)

// invokeResult 是一次请求的回执（data 与 Status 二选一，或错误）。
type invokeResult struct {
	data []byte
	st   *frame.Status
	err  error
}

// InvokeOption 定制单次 Invoke 行为。
type InvokeOption func(*invokeOpts)

type invokeOpts struct{ failFast bool }

// WithFailFast 使本次 Invoke 在重连期间不排队、立即失败（默认排队等待重连成功后重发）。
func WithFailFast() InvokeOption {
	return func(o *invokeOpts) { o.failFast = true }
}

// Invoke 发送请求并等待响应：分配 (epoch, seq)、写帧、匹配响应、超时取消。
// req 为 nil 时不携带 payload（如心跳）；resp 非 nil 时反序列化业务数据。
// 重连期间默认排队（上限 WithReconnectQueueSize，满则失败），WithFailFast 可跳过排队。
// 业务拒绝返回 *BusinessError（按 Reason 分支）；超时/断连返回对应错误类型。
func (c *Client) Invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error {
	if c.closed.Load() {
		return NewNetworkError(errors.New("client: 连接已关闭"))
	}
	o := invokeOpts{}
	for _, opt := range opts {
		opt(&o)
	}
	// 重连期间排队（failFast 除外）；排队项在重连成功后按序重发。
	if c.state.Load() == int32(StateReconnecting) && !o.failFast {
		return c.enqueueAndWait(ctx, op, req, resp)
	}
	return c.invokeOnce(ctx, op, req, resp)
}

// enqueueAndWait 将请求排入重连队列并等待结果。
func (c *Client) enqueueAndWait(ctx context.Context, op string, req, resp any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q := &queuedInvoke{ctx: ctx, op: op, req: req, resp: resp, result: make(chan error, 1)}
	select {
	case c.queue <- q:
	case <-ctx.Done():
		return NewNetworkError(ctx.Err())
	case <-c.closeCh:
		return NewNetworkError(errors.New("client: 连接已关闭"))
	default:
		return NewNetworkError(fmt.Errorf("client: 重连排队已满（%d）", c.queueSize))
	}
	select {
	case err := <-q.result:
		return err
	case <-ctx.Done():
		return NewNetworkError(ctx.Err())
	case <-c.closeCh:
		return NewNetworkError(errors.New("client: 连接已关闭"))
	}
}

// invokeOnce 执行一次「写帧 + 等待响应」。
func (c *Client) invokeOnce(ctx context.Context, op string, req, resp any) error {
	// Reconnecting 期间（failFast 或重连前的旧调用）不写帧：
	// 死连接的写可能进内核缓冲后无响应，导致等待完整超时而非立即失败。
	if s := State(c.state.Load()); s == StateReconnecting {
		return NewNetworkError(fmt.Errorf("client: 正在重连（%s）", s))
	}
	var payload []byte
	if req != nil {
		var err error
		if payload, err = c.serial.Marshal(req); err != nil {
			return NewProtocolError(fmt.Errorf("client: 序列化请求失败: %w", err))
		}
	}
	body, err := frame.BuildRequestBody(op, payload)
	if err != nil {
		return NewProtocolError(err)
	}

	key := inflightKey{epoch: c.epochNum.Load(), seq: c.nextSeq()}
	ch := make(chan invokeResult, 1)
	c.inflight.Store(key, ch)
	defer c.inflight.Delete(key)

	c.writeMu.Lock()
	writeErr := frame.Write(c.currentConn(), frame.Header{Type: frame.MsgTypeRequest, Seq: key.seq}, body, c.maxBodySize)
	c.writeMu.Unlock()
	if writeErr != nil {
		return NewNetworkError(fmt.Errorf("client: 写帧失败: %w", writeErr))
	}
	return c.awaitResult(ctx, op, key, ch, resp)
}

// awaitResult 等待响应或超时（超时与响应竞态：先到者胜出，后者静默）。
func (c *Client) awaitResult(ctx context.Context, op string, key inflightKey, ch chan invokeResult, resp any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := c.invokeTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < timeout {
			timeout = d
		}
	}
	timer := time.AfterFunc(timeout, func() {
		// 原子认领后才发送：保证「恰好一次」结果投递。
		if _, loaded := c.inflight.LoadAndDelete(key); loaded {
			ch <- invokeResult{err: NewTimeoutError(fmt.Errorf("client: %s 超时（%s）", op, timeout))}
		}
	})
	defer timer.Stop()

	select {
	case <-ctx.Done():
		if _, loaded := c.inflight.LoadAndDelete(key); loaded {
			return NewNetworkError(ctx.Err())
		}
		// 已被 timer 或响应认领：等待那条结果（ch 容量 1，必不阻塞）。
		select {
		case r := <-ch:
			return c.resultToError(op, r, resp)
		case <-time.After(time.Second):
			return NewNetworkError(ctx.Err())
		}
	case r := <-ch:
		return c.resultToError(op, r, resp)
	}
}

// resultToError 将回执转换为 SDK 错误或反序列化响应。
func (c *Client) resultToError(op string, r invokeResult, resp any) error {
	switch {
	case r.err != nil:
		return r.err
	case r.st != nil:
		// 业务拒绝：还原为 SDK 级 BusinessError（规范 §7），Reason 为主键。
		return &BusinessError{
			Code:     r.st.Code,
			Reason:   r.st.Reason,
			Message:  r.st.Message,
			Metadata: r.st.Metadata,
		}
	default:
		if resp != nil && len(r.data) > 0 {
			if err := c.serial.Unmarshal(r.data, resp); err != nil {
				return NewProtocolError(fmt.Errorf("client: 反序列化响应失败: %w", err))
			}
		}
		return nil
	}
}

// dispatchResponse 按 (epoch, seq) 匹配响应；迟到响应（已超时移除）静默丢弃。
func (c *Client) dispatchResponse(hdr frame.Header, body []byte) {
	data, st, err := frame.DecodeReply(body)
	var r invokeResult
	switch {
	case err != nil:
		r = invokeResult{err: NewProtocolError(err)}
	case st != nil:
		r = invokeResult{st: st}
	default:
		r = invokeResult{data: data}
	}
	key := inflightKey{epoch: c.epochNum.Load(), seq: hdr.Seq}
	if ch, ok := c.inflight.LoadAndDelete(key); ok {
		ch.(chan invokeResult) <- r
	}
}

// failAllInflight 连接断开或客户端关闭时取消全部 in-flight。
func (c *Client) failAllInflight(cause error) {
	if cause == nil {
		cause = errors.New("client: 连接已关闭")
	}
	networkErr := NewNetworkError(cause)
	c.inflight.Range(func(key, value any) bool {
		if _, loaded := c.inflight.LoadAndDelete(key); loaded {
			if ch, ok := value.(chan invokeResult); ok {
				ch <- invokeResult{err: networkErr}
			}
		}
		return true
	})
}

// failAllQueued 客户端关闭时取消全部排队请求。
func (c *Client) failAllQueued() {
	for {
		select {
		case q := <-c.queue:
			q.result <- NewNetworkError(errors.New("client: 连接已关闭"))
		default:
			return
		}
	}
}

// drainQueue 重连成功后按序重发排队请求。
func (c *Client) drainQueue() {
	for {
		select {
		case q := <-c.queue:
			q.result <- c.invokeOnce(q.ctx, q.op, q.req, q.resp)
		default:
			return
		}
	}
}
