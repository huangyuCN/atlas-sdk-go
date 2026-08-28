package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// invokeResult 是一次请求的回执（data 与 Status 二选一，或错误）。
type invokeResult struct {
	data []byte
	st   *frame.Status
	err  error
}

// Invoke 发送请求并等待响应：分配 (epoch, seq)、写帧、匹配响应、超时取消。
// req 为 nil 时不携带 payload（如心跳）；resp 非 nil 时反序列化业务数据。
// 业务拒绝返回 *BusinessError（按 Reason 分支，见 IsBusinessError）；
// 超时/断连返回对应错误类型（规范 §7 四分类）。
func (c *Client) Invoke(ctx context.Context, op string, req, resp any) error {
	if c.closed.Load() {
		return NewNetworkError(errors.New("client: 连接已关闭"))
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

	key := inflightKey{epoch: c.epoch(), seq: c.nextSeq()}
	ch := make(chan invokeResult, 1)
	c.inflight.Store(key, ch)
	defer c.inflight.Delete(key)

	c.writeMu.Lock()
	writeErr := frame.Write(c.conn, frame.Header{Type: frame.MsgTypeRequest, Seq: key.seq}, body, c.maxBodySize)
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
		// 原子认领后才发送：保证「恰好一次」结果投递（评审 B2 修复）。
		if _, loaded := c.inflight.LoadAndDelete(key); loaded {
			ch <- invokeResult{err: NewTimeoutError(fmt.Errorf("client: %s 超时（%s）", op, timeout))}
		}
	})
	defer timer.Stop()

	select {
	case <-ctx.Done():
		// ctx 取消也走原子认领，与 timer/dispatch 三方竞争时同样保证单次投递。
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
	case <-c.closeCh:
		return NewNetworkError(errors.New("client: 连接已关闭"))
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
	// 原子认领后发送：迟到（已被 timer 认领）时静默丢弃，绝不阻塞读循环。
	if ch, ok := c.inflight.LoadAndDelete(inflightKey{epoch: c.epoch(), seq: hdr.Seq}); ok {
		ch.(chan invokeResult) <- r
	}
}

// failAllInflight 连接断开时取消全部 in-flight。
// 原子认领（LoadAndDelete）后才发送：与 timer/dispatchResponse 竞争时保证单次投递，
// 发送方永不阻塞（ch 容量 1 且所有权唯一，评审 B2 修复）。
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
