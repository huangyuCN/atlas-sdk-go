package direct

import (
	"context"
	"errors"
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
)

// Reconnect 显式重连：重新 hello（含升级 query 票）、重放 JoinBattle 与 SyncFrames 补帧。
// 终态（ended/failed）返回对应终态族哨兵的错误（重连只会被稳定拒绝）；被接入层拒绝与票据失效
// 返回不可重试错误（会话随即终止，不再自动重试）。
func (s *Session) Reconnect(ctx context.Context) error {
	if s.Terminal() {
		return s.endedErr("Reconnect")
	}
	if err := s.Err(); err != nil {
		return err // 已因不可重试错误终止：重连无意义
	}
	res := make(chan error, 1)
	if err := s.enqueueReconnect(ctx, res); err != nil {
		return err
	}
	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return ErrClosed
	}
}

// enqueueReconnect 先登记等待者再投递重连信号（结果必达：完成/失败/关闭三路都会投递）。
func (s *Session) enqueueReconnect(ctx context.Context, res chan error) error {
	s.genMu.Lock()
	if s.closed {
		s.genMu.Unlock()
		return ErrClosed
	}
	s.waiters = append(s.waiters, res)
	s.genMu.Unlock()
	select {
	case s.manual <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return ErrClosed
	}
}

// supervise 监视本代连接：被动断线按自动重连策略处理，显式 Reconnect 则强制执行一轮。
func (s *Session) supervise(g *generation) {
	defer s.wg.Done()
	for {
		manual, ok := s.awaitEnd(g)
		if !ok {
			return
		}
		if s.Terminal() {
			return // 终态（ended/failed）：不重连（重连只会被稳定拒绝）
		}
		if !manual && !s.opt.autoReconnect {
			s.setState(StateDisconnected)
			if !s.waitManual() {
				return
			}
		}
		s.setState(StateReconnecting)
		if err := s.reconnect(); err != nil {
			if errors.Is(err, ErrClosed) {
				return
			}
			if s.Terminal() {
				s.finishWaiters(err) // 重连补帧期间进入终态：按终态收尾，不算异常终止
				return
			}
			s.setFatal(err)
			s.finishWaiters(err)
			return
		}
		s.setState(StateConnected)
		s.finishWaiters(nil)
		if g = s.currentGen(); g == nil {
			return
		}
	}
}

// awaitEnd 等本代连接结束；显式重连请求会主动断流并返回 manual=true；ok=false 表示已关闭。
func (s *Session) awaitEnd(g *generation) (manual bool, ok bool) {
	select {
	case <-s.closeCh:
		return false, false
	case <-g.done:
		return false, true
	case <-s.manual:
		_ = g.tr.Close()
		<-g.done
		return true, true
	}
}

// waitManual 在关闭自动重连时守候显式重连信号（会话关闭返回 false）。
func (s *Session) waitManual() bool {
	select {
	case <-s.closeCh:
		s.finishWaiters(ErrClosed)
		return false
	case <-s.manual:
		return true
	}
}

// reconnect 执行一轮「退避拨号 + 重放入局」直到成功或不可重试失败；轮次结果计入观测快照
// （ReconnectRounds/ReconnectFailures/LastReconnectErr，见 stats.go）。会话关闭不算失败。
func (s *Session) reconnect() error {
	err := s.reconnectRound()
	if err != nil && !errors.Is(err, ErrClosed) {
		s.stats.noteReconnectRound(err)
		return err
	}
	s.stats.noteReconnectRound(nil)
	return err
}

// reconnectRound 是重连主循环：退避 → 拨号 → 重放入局；可重试错误换下一档退避继续。
func (s *Session) reconnectRound() error {
	backoff := s.opt.backoffBase
	for {
		if err := sleepInterruptible(backoff, s.closeCh); err != nil {
			return ErrClosed
		}
		tr, err := s.dialOnce()
		if err != nil {
			if !retryable(err) {
				return err
			}
			backoff = nextBackoff(backoff, s.opt.backoffMax)
			continue
		}
		if !s.startGeneration(tr) {
			return ErrClosed
		}
		if err := s.restore(); err != nil {
			if !retryable(err) {
				return err
			}
			s.closeCurrent()
			backoff = nextBackoff(backoff, s.opt.backoffMax)
			continue
		}
		return nil
	}
}

// dialOnce 带握手上限拨号一次（重连路径用；错误按可重试性由调用方分流，计数见 dialTracked）。
func (s *Session) dialOnce() (conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.opt.handshakeTimeout)
	defer cancel()
	return s.dialTracked(ctx)
}

// restore 在新连接上重放入局：JoinBattle + SyncFrames(last_seen_frame) 补帧（仅入局过的会话）。
// 走内部 invoke（重连期间状态是 Reconnecting，公开 Invoke 会按未连接拒绝）。
func (s *Session) restore() error {
	if !s.joined.Load() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.opt.invokeTimeout)
	defer cancel()
	var join battlev1.JoinBattleReply
	if err := s.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: s.plan.BattleID}, &join); err != nil {
		return err
	}
	var sync battlev1.SyncFramesReply
	return s.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
		&battlev1.SyncFramesReq{BattleId: s.plan.BattleID, LastSeenFrame: s.LastSeenFrame()}, &sync)
}

// closeCurrent 关闭当前代连接（读循环随之退出，监管按策略处理）。
func (s *Session) closeCurrent() {
	if g := s.currentGen(); g != nil {
		_ = g.tr.Close()
	}
}

// finishWaiters 向本轮全部显式重连等待者投递结果（容量 1 的通道，不阻塞）。
func (s *Session) finishWaiters(err error) {
	s.genMu.Lock()
	waiters := s.waiters
	s.waiters = nil
	s.genMu.Unlock()
	for _, res := range waiters {
		res <- err
	}
}

// sleepInterruptible 可被会话关闭打断的退避睡眠。
func sleepInterruptible(d time.Duration, closeCh <-chan struct{}) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-closeCh:
		return fmt.Errorf("direct: 会话已关闭")
	}
}

// nextBackoff 计算下一档退避（×2 封顶）。
func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		return max
	}
	return next
}
