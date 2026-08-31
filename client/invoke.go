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

// InvokeOption 定制单次 Invoke 行为。
type InvokeOption func(*invokeOpts)

type invokeOpts struct{ failFast bool }

// WithFailFast 使本次 Invoke 在重连期间不排队、立即失败（默认排队等待重连成功后重发）。
func WithFailFast() InvokeOption {
	return func(o *invokeOpts) { o.failFast = true }
}

// invoke 发送请求并等待响应：分配 (epoch, seq)、写帧、匹配响应、超时取消。
// req 为 nil 时不携带 payload（如心跳）；resp 非 nil 时反序列化业务数据。
// 重连期间默认排队（上限 WithReconnectQueueSize，满则失败），WithFailFast 可跳过排队。
// 业务拒绝返回 *BusinessError（按 Reason 分支）；超时/断连返回对应错误类型。
func (ch *channel) invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error {
	if ch.closed.Load() {
		return NewNetworkError(errors.New("client: 连接已关闭"))
	}
	o := invokeOpts{}
	for _, opt := range opts {
		opt(&o)
	}
	// 排队判定与入队在 genMu 临界区内原子完成（评审 B4 修复）：
	// supervisor 的 drain 也在同一临界区，二者串行化——
	// 判定为 Reconnecting 后入队的请求，要么被本次 drain 消费，要么队列满立即失败，
	// 不存在「drain 空队列后请求才入队」的永久遗留窗口。
	if !o.failFast {
		ch.genMu.Lock()
		reconnecting := ch.state.Load() == int32(StateReconnecting)
		if !reconnecting {
			ch.genMu.Unlock()
			return ch.invokeOnce(ctx, op, req, resp)
		}
		// 排队（临界区内：与 supervisor 的 state 置位/drain 互斥）。
		q, err := ch.enqueueLocked(ctx, op, req, resp)
		ch.genMu.Unlock()
		if err != nil {
			return err
		}
		return ch.awaitQueued(ctx, q)
	}
	return ch.invokeOnce(ctx, op, req, resp)
}

// enqueueLocked 将请求排入重连队列（调用方必须持有 genMu）。
// 排队期限：单次超时（invokeTimeout）与 ctx deadline 取较早者——
// 排队阶段计入超时（评审 Important 修复：不再无限等待重连）。
func (ch *channel) enqueueLocked(ctx context.Context, op string, req, resp any) (*queuedInvoke, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	queueDeadline := time.Now().Add(ch.invokeTimeout)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(queueDeadline) {
		queueDeadline = deadline
	}
	q := &queuedInvoke{
		ctx:      ctx,
		op:       op,
		req:      req,
		resp:     resp,
		result:   make(chan error, 1),
		deadline: queueDeadline,
		ctxDone:  ctx.Done(),
		closeCh:  ch.closeCh,
	}
	select {
	case ch.queue <- q:
		// 排队超时看护：到点未 drain 则认领并超时失败（恰好一次语义）。
		go ch.queueDeadlineWatch(q)
		return q, nil
	default:
		return nil, NewNetworkError(fmt.Errorf("client: 重连排队已满（%d）", ch.queueSize))
	}
}

// queueDeadlineWatch 排队超时看护：到点后原子认领（LoadAndDelete 语义由
// queuedInvoke.claimed CAS 保证）并发送超时结果。
func (ch *channel) queueDeadlineWatch(q *queuedInvoke) {
	d := time.Until(q.deadline)
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-q.ctxDone:
			// ctx 先取消：仍等到 drain/关闭路径认领，此处仅标记加速失败。
		case <-q.closeCh:
			return
		}
	}
	// 认领失败 = 已被 drain 消费（drain 会忽略过期请求）或已失败。
	if !q.claimed.CompareAndSwap(false, true) {
		return
	}
	q.result <- NewTimeoutError(fmt.Errorf("client: %s 排队超时", q.op))
}

// awaitQueued 等待排队请求的结果（drain 重发 / 排队超时 / ctx 取消 / 关闭）。
func (ch *channel) awaitQueued(ctx context.Context, q *queuedInvoke) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case err := <-q.result:
		return err
	case <-ctx.Done():
		// ctx 取消：原子认领后失败（若已被 drain 认领则等它的结果）。
		if q.claimed.CompareAndSwap(false, true) {
			return NewNetworkError(ctx.Err())
		}
		select {
		case err := <-q.result:
			return err
		case <-time.After(time.Second):
			return NewNetworkError(ctx.Err())
		}
	case <-ch.closeCh:
		// 关闭路径 failAllQueued 已认领并发送结果。
		select {
		case err := <-q.result:
			return err
		default:
			return NewNetworkError(errors.New("client: 连接已关闭"))
		}
	}
}

// invokeOnce 执行一次「快照取代 → 写帧 → 等待响应」。
func (ch *channel) invokeOnce(ctx context.Context, op string, req, resp any) error {
	// Reconnecting 期间不写帧：死连接的写可能进内核缓冲后无响应，等待完整超时。
	// （failFast 路径与 drain 前的旧调用在此被拦截，立即失败。）
	if s := State(ch.state.Load()); s == StateReconnecting {
		return NewNetworkError(fmt.Errorf("client: 正在重连（%s）", s))
	}
	// 单次原子读取得 (epoch, conn, done)（评审 B3 修复：不再撕裂读）。
	g := ch.gen()
	if g == nil || ch.closed.Load() {
		return NewNetworkError(errors.New("client: 连接已关闭"))
	}
	var payload []byte
	if req != nil {
		var err error
		if payload, err = ch.serial.Marshal(req); err != nil {
			return NewProtocolError(fmt.Errorf("client: 序列化请求失败: %w", err))
		}
	}
	body, err := frame.BuildRequestBody(op, payload)
	if err != nil {
		return NewProtocolError(err)
	}

	key := inflightKey{epoch: g.epoch, seq: ch.nextSeq()}
	chRes := make(chan invokeResult, 1)
	ch.inflight.Store(key, chRes)
	defer ch.inflight.Delete(key)

	ch.writeMu.Lock()
	writeErr := g.tr.WriteFrame(frame.Header{Type: frame.MsgTypeRequest, Seq: key.seq}, body, ch.maxBodySize)
	ch.writeMu.Unlock()
	if writeErr != nil {
		return NewNetworkError(fmt.Errorf("client: 写帧失败: %w", writeErr))
	}
	return ch.awaitResult(ctx, op, key, chRes, resp)
}

// awaitResult 等待响应或超时（超时与响应竞态：先到者胜出，后者静默）。
func (ch *channel) awaitResult(ctx context.Context, op string, key inflightKey, chRes chan invokeResult, resp any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := ch.invokeTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < timeout {
			timeout = d
		}
	}
	timer := time.AfterFunc(timeout, func() {
		// 原子认领后才发送：保证「恰好一次」结果投递。
		if _, loaded := ch.inflight.LoadAndDelete(key); loaded {
			chRes <- invokeResult{err: NewTimeoutError(fmt.Errorf("client: %s 超时（%s）", op, timeout))}
		}
	})
	defer timer.Stop()

	select {
	case <-ctx.Done():
		if _, loaded := ch.inflight.LoadAndDelete(key); loaded {
			return NewNetworkError(ctx.Err())
		}
		// 已被 timer 或响应认领：等待那条结果（容量 1，必不阻塞）。
		select {
		case r := <-chRes:
			return ch.resultToError(op, r, resp)
		case <-time.After(time.Second):
			return NewNetworkError(ctx.Err())
		}
	case r := <-chRes:
		return ch.resultToError(op, r, resp)
	}
}

// resultToError 将回执转换为 SDK 错误或反序列化响应。
func (ch *channel) resultToError(op string, r invokeResult, resp any) error {
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
			if err := ch.serial.Unmarshal(r.data, resp); err != nil {
				return NewProtocolError(fmt.Errorf("client: 反序列化响应失败: %w", err))
			}
		}
		return nil
	}
}

// dispatchResponse 按 (epoch, seq) 匹配响应；迟到响应（已超时移除）静默丢弃。
// 包络非法为协议级致命错误（规范 §7：不可重试、连接已断）——上抛 readLoop 终止
// 本通道（评审 B5 修复：不再只回当前请求后继续用失步连接）。
func (ch *channel) dispatchResponse(hdr frame.Header, body []byte) (fatalErr error) {
	data, st, err := frame.DecodeReply(body)
	if err != nil {
		return NewProtocolError(err)
	}
	var r invokeResult
	if st != nil {
		r = invokeResult{st: st}
	} else {
		r = invokeResult{data: data}
	}
	if res, ok := ch.inflight.LoadAndDelete(inflightKey{epoch: ch.gen().epoch, seq: hdr.Seq}); ok {
		res.(chan invokeResult) <- r
	}
	return nil
}

// failAllInflight 连接断开或通道关闭时取消全部 in-flight。
func (ch *channel) failAllInflight(cause error) {
	if cause == nil {
		cause = errors.New("client: 连接已关闭")
	}
	networkErr := NewNetworkError(cause)
	ch.inflight.Range(func(key, value any) bool {
		if _, loaded := ch.inflight.LoadAndDelete(key); loaded {
			if res, ok := value.(chan invokeResult); ok {
				res <- invokeResult{err: networkErr}
			}
		}
		return true
	})
}

// failAllQueued 通道关闭时取消全部排队请求。
func (ch *channel) failAllQueued() {
	for {
		select {
		case q := <-ch.queue:
			if q.claimed.CompareAndSwap(false, true) {
				q.result <- NewNetworkError(errors.New("client: 连接已关闭"))
			}
		default:
			return
		}
	}
}

// drainQueue 重连成功后按序重发排队请求。
// 调用方持有 genMu（与 Invoke 的排队判定同临界区，评审 B4 修复）。
// 过期请求（排队超时已被看护认领 / ctx 已取消）跳过重发，避免执行
// 调用方已放弃的有副作用请求（评审 Important 修复）。
func (ch *channel) drainQueue() {
	for {
		select {
		case q := <-ch.queue:
			// 过期判定：ctx 已取消，或排队超时看护已认领 → 跳过。
			if q.ctx != nil && q.ctx.Err() != nil {
				if q.claimed.CompareAndSwap(false, true) {
					q.result <- NewNetworkError(q.ctx.Err())
				}
				continue
			}
			if !q.claimed.CompareAndSwap(false, true) {
				// 已被看护认领（排队超时）：跳过，结果已由看护投递。
				continue
			}
			// 认领成功：重发并投递结果。
			q.result <- ch.invokeOnce(q.ctx, q.op, q.req, q.resp)
		default:
			return
		}
	}
}
