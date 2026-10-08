package direct

import (
	"context"
	"errors"
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// invokeResult 是一次请求的回执（data 与 Status 二选一，或错误）。
type invokeResult struct {
	data []byte
	st   *frame.Status
	err  error
}

// JoinBattle 直连入局（重连时由会话自动重放；票据失效返回可判定的 ErrTicketExpired/ErrTicketInvalid）。
func (s *Session) JoinBattle(ctx context.Context, req *battlev1.JoinBattleReq) (*battlev1.JoinBattleReply, error) {
	var out battlev1.JoinBattleReply
	if err := s.Invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle, req, &out); err != nil {
		return nil, err
	}
	s.joined.Store(true)
	return &out, nil
}

// SendFrameInput 发送一帧输入（服务端回空成功包络；业务拒绝仍返回错误）。
func (s *Session) SendFrameInput(ctx context.Context, req *battlev1.FrameInputReq) error {
	return s.Invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SendFrameInput, req, nil)
}

// SyncFrames 从 last_seen_frame 补帧（重连后由会话自动重放补断点）。
func (s *Session) SyncFrames(ctx context.Context, req *battlev1.SyncFramesReq) (*battlev1.SyncFramesReply, error) {
	var out battlev1.SyncFramesReply
	if err := s.Invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Invoke 发一条战斗帧请求并等回执：请求帧逐帧置位会话槽（base64url 票密文），三面统一。
// 终态（对局已结束）返回 ErrBattleEnded 且不写线；未连接/重连中返回 *client.NetworkError
// （可重试语义），业务拒绝返回 *client.BusinessError。
func (s *Session) Invoke(ctx context.Context, op string, req, resp any) error {
	if s.Ended() {
		return s.endedErr(op)
	}
	if st := s.State(); st != StateConnected {
		return client.NewNetworkError(fmt.Errorf("direct: 当前状态 %s，拒绝发送 %s", st, op))
	}
	return s.invoke(ctx, op, req, resp)
}

// invoke 是请求-回执的共用实现（重放路径不校验连接状态：调用方已保证连接可用）。
func (s *Session) invoke(ctx context.Context, op string, req, resp any) error {
	g := s.currentGen()
	if g == nil {
		return client.NewNetworkError(fmt.Errorf("direct: 无可用连接（%s）", op))
	}
	payload, err := s.marshal(req)
	if err != nil {
		return err
	}
	seq, ch := s.newInflight()
	defer s.inflight.Delete(seq)
	if err := s.writeRequest(g, seq, op, payload); err != nil {
		s.inflight.Delete(seq)
		return err
	}
	return s.awaitReply(ctx, op, seq, ch, resp)
}

// marshal 序列化请求载荷（nil 请求 = 空载荷）。
func (s *Session) marshal(req any) ([]byte, error) {
	if req == nil {
		return nil, nil
	}
	payload, err := s.serial.Marshal(req)
	if err != nil {
		return nil, client.NewProtocolError(fmt.Errorf("direct: 序列化 %T 失败: %w", req, err))
	}
	return payload, nil
}

// newInflight 分配 seq 并登记等待通道（seq 回绕到 0 时跳过）。
func (s *Session) newInflight() (uint32, chan invokeResult) {
	for {
		seq := s.seq.Add(1)
		if seq == 0 {
			continue
		}
		ch := make(chan invokeResult, 1)
		s.inflight.Store(seq, ch)
		return seq, ch
	}
}

// writeRequest 组装并写出请求帧：body = op || 会话槽 || 载荷，帧头置位 FlagSession。
// 终态双检（进锁前 + 持写锁后）：终态置位后不再有新的字节上线——已在写锁内的那一次写不回滚。
func (s *Session) writeRequest(g *generation, seq uint32, op string, payload []byte) error {
	if s.Ended() {
		return s.endedErr(op)
	}
	body, err := frame.BuildRequestBodyWithSession(op, TicketSlot(s.plan.Ticket), "", payload)
	if err != nil {
		return client.NewProtocolError(err)
	}
	hdr := frame.Header{
		Type: frame.MsgTypeRequest, Version: frame.Version, Seq: seq, Flags: frame.FlagSession,
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.Ended() {
		return s.endedErr(op)
	}
	if err := g.tr.WriteFrame(hdr, body, frame.MaxBodySize); err != nil {
		return client.NewNetworkError(fmt.Errorf("direct: 写帧失败（%s）: %w", op, err))
	}
	return nil
}

// awaitReply 等回执/超时/ctx 取消/会话关闭（先到者胜出，迟到结果静默丢弃）。
func (s *Session) awaitReply(ctx context.Context, op string, seq uint32, ch chan invokeResult, resp any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := s.opt.invokeTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < timeout {
			timeout = d
		}
	}
	timer := time.AfterFunc(timeout, func() {
		if _, loaded := s.inflight.LoadAndDelete(seq); loaded {
			ch <- invokeResult{err: client.NewTimeoutError(fmt.Errorf("direct: %s 超时（%s）", op, timeout))}
		}
	})
	defer timer.Stop()

	select {
	case r := <-ch:
		return s.result(op, r, resp)
	case <-ctx.Done():
		return s.abort(op, seq, ch, resp, ctx)
	case <-s.closeCh:
		return ErrClosed
	}
}

// abort 处理 ctx 取消：未认领则立即失败，已被回执/超时认领则等那条结果。
func (s *Session) abort(op string, seq uint32, ch chan invokeResult, resp any, ctx context.Context) error {
	if _, loaded := s.inflight.LoadAndDelete(seq); loaded {
		return client.NewNetworkError(ctx.Err())
	}
	select {
	case r := <-ch:
		return s.result(op, r, resp)
	case <-time.After(time.Second):
		return client.NewNetworkError(ctx.Err())
	}
}

// result 把回执还原为错误或反序列化响应。
func (s *Session) result(op string, r invokeResult, resp any) error {
	switch {
	case r.err != nil:
		return r.err
	case r.st != nil:
		return s.wrapBusiness(op, r.st)
	default:
		if resp != nil && len(r.data) > 0 {
			if err := s.serial.Unmarshal(r.data, resp); err != nil {
				return client.NewProtocolError(fmt.Errorf("direct: 反序列化 %s 回执失败: %w", op, err))
			}
		}
		return nil
	}
}

// wrapBusiness 还原业务拒绝为 *client.BusinessError；票据类与结束类 reason 再包上可判定哨兵：
// 票据类供上层回业务链路重新匹配取新票（不重连），结束类让会话进终态并停止发送。
// 这里是**所有 op 业务拒绝的唯一收口**（业务帧、探针、重连重放都经 result 走到这里）。
func (s *Session) wrapBusiness(op string, st *frame.Status) error {
	err := &client.BusinessError{
		Code: st.Code, Reason: st.Reason, Message: st.Message, Metadata: st.Metadata, Class: st.Class,
	}
	switch st.Reason {
	case reasonTicketExpired:
		return fmt.Errorf("direct: %s: %w: %w", op, ErrTicketExpired, err)
	case reasonTicketInvalid:
		return fmt.Errorf("direct: %s: %w: %w", op, ErrTicketInvalid, err)
	case reasonBattleEnded:
		ended := fmt.Errorf("direct: %s: %w: %w", op, ErrBattleEnded, err)
		s.markEnded(ended) // 终态：停发、幂等，收尾窗口内仍收推送
		return ended
	default:
		return err
	}
}

// dispatchResponse 按 seq 匹配回执；迟到回执静默丢弃，版本不符/包络非法为协议级致命错误。
func (s *Session) dispatchResponse(hdr frame.Header, body []byte) error {
	if hdr.Version != frame.Version {
		return client.NewProtocolError(fmt.Errorf("direct: 响应帧 version %d, 期望 %d", hdr.Version, frame.Version))
	}
	data, st, err := frame.DecodeReply(body)
	if err != nil {
		return client.NewProtocolError(err)
	}
	r := invokeResult{data: data, st: st}
	if res, ok := s.inflight.LoadAndDelete(hdr.Seq); ok {
		res.(chan invokeResult) <- r
	}
	return nil
}

// failInflight 结算全部未决请求（连接中断/会话关闭/协议致命）。
func (s *Session) failInflight(cause error) {
	if cause == nil {
		cause = ErrClosed
	}
	s.inflight.Range(func(key, value any) bool {
		if _, loaded := s.inflight.LoadAndDelete(key); loaded {
			if ch, ok := value.(chan invokeResult); ok {
				ch <- invokeResult{err: cause}
			}
		}
		return true
	})
}

// retryable 报告错误是否可重试：网络/超时类可重试；被接入层拒绝与票据失效不可重试。
func retryable(err error) bool {
	return errors.Is(err, client.ErrNetwork) || errors.Is(err, client.ErrTimeout)
}
