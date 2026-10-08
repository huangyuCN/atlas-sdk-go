package direct

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// 终态（对局结束）语义，四条边界一次说清：
//
//  1. 触发：任一 op（SendFrameInput/SyncFrames/Ping/JoinBattle）收到 reason
//     BATTLE_ENDED 的业务拒绝，或收到 BattleEndNotify 推送。两者同一收口——服务端对已结束
//     对局的迟到帧 op 一律稳定拒绝（identity 在投递前判「已留档」），客户端不必分辨来源。
//  2. 停发：终态下一切写线（发帧、补帧、探针）被拒并返回可判定的 ErrBattleEnded。判定位点
//     在写锁内（writeRequest 双检），故终态置位后不会有新的字节上线。
//  3. 仍可读：读循环不受终态影响，收尾窗口内继续分发推送——结算通知可能被有界补投
//     （首投 + CloseBattle 前重投 + 迟到/重连补投，服务端每局每人上限 5 次），客户端要收完。
//  4. 幂等：结束通知的首投唯一（按会话「已收到」标志去重），后续补投只计数、不重放回调；
//     载荷与首投不逐字一致属异常，处置是**首次为准**（不覆盖、不重放），另计数供排障。

// endedState 汇总会话的终态登记项：flag 可无锁读（快路径判定），其余在 mu 下读写。
type endedState struct {
	flag atomic.Bool // 终态标志（首个触发者置位，之后不可回退）

	mu      sync.Mutex
	cause   error // 触发原因（BATTLE_ENDED 业务拒绝原文，或结束通知合成的描述）
	notify  *battlev1.BattleEndNotify
	raw     []byte // 首条结束通知的原始载荷（逐字比对基准）
	noticed bool   // 结束通知是否已回调（幂等去重标志）
	replays uint64 // 首投之后的补投条数
	differs uint64 // 载荷与首投不逐字一致的条数（异常观测）
}

// Ended 报告会话是否处于终态（对局已结束：停发、收尾窗口内仍可读）。
func (s *Session) Ended() bool { return s.end.flag.Load() }

// EndCause 返回进入终态的原因：BATTLE_ENDED 业务拒绝的原文，或结束通知合成的描述；
// 未进入终态返回 nil。可用 errors.Is(err, ErrBattleEnded) 与 errors.As(*client.BusinessError) 判定。
func (s *Session) EndCause() error {
	s.end.mu.Lock()
	defer s.end.mu.Unlock()
	return s.end.cause
}

// EndNotify 返回首条战斗结束通知的副本（未收到通知返回 nil）。迟到的订阅者用它在注册回调
// 之前取结果：回调只随首投触发一次，补投不重放。
func (s *Session) EndNotify() *battlev1.BattleEndNotify {
	s.end.mu.Lock()
	defer s.end.mu.Unlock()
	if s.end.notify == nil {
		return nil
	}
	cloned, _ := proto.Clone(s.end.notify).(*battlev1.BattleEndNotify)
	return cloned
}

// EndStats 汇总结束通知的投递观测（首投一次 + 服务端有界补投）。
type EndStats struct {
	First      bool   // 是否已收到结束通知（首投）
	Replays    uint64 // 首投之后的补投条数（不重复触发回调）
	Mismatches uint64 // 载荷与首投不逐字一致的条数（异常观测；处置：首次为准）
}

// EndStats 返回结束通知的投递观测（未收到通知时为零值）。
func (s *Session) EndStats() EndStats {
	s.end.mu.Lock()
	defer s.end.mu.Unlock()
	return EndStats{First: s.end.noticed, Replays: s.end.replays, Mismatches: s.end.differs}
}

// EndLinger 返回终态的收尾窗口时长（0 = 进入终态即关连接，见 WithEndLinger 的取值依据）。
func (s *Session) EndLinger() time.Duration { return s.opt.endLinger }

// markEnded 进入终态（幂等：首个触发者胜）：置标志 → 记原因 → 置状态 → 起收尾窗口。
// 重复调用不覆盖原因、不重起窗口（补投的结束通知会反复走到这里）。
func (s *Session) markEnded(cause error) bool {
	if !s.end.flag.CompareAndSwap(false, true) {
		return false
	}
	s.end.mu.Lock()
	s.end.cause = cause
	s.end.mu.Unlock()
	s.setState(StateEnded)
	s.startLinger()
	return true
}

// noteEndNotify 登记一条结束通知并进入终态；返回 true 表示这是首投（应触发回调）。
// 补投只更新计数：回调不重放（幂等），载荷不一致时首次为准（不覆盖首条、不改写回调载荷）。
func (s *Session) noteEndNotify(payload []byte) bool {
	var n battlev1.BattleEndNotify
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload, &n); err != nil {
		return false // 载荷非法：不进终态、不回调（由协议层兜底，不臆造结束语义）
	}
	s.end.mu.Lock()
	first := !s.end.noticed
	if first {
		s.end.noticed, s.end.notify, s.end.raw = true, &n, append([]byte(nil), payload...)
	} else {
		s.end.replays++
		if !bytes.Equal(s.end.raw, payload) {
			s.end.differs++
		}
	}
	s.end.mu.Unlock()

	s.markEnded(fmt.Errorf("%w: battle=%s 胜者=%s", ErrBattleEnded, n.GetBattleId(), n.GetWinnerPlayerId()))
	return first
}

// endedErr 返回终态下拒绝发送的错误：包 ErrBattleEnded 哨兵（errors.Is 可判定），并带上首个
// 拒绝的原文（errors.As 仍可取到 *client.BusinessError）。
func (s *Session) endedErr(what string) error {
	if cause := s.EndCause(); cause != nil {
		return fmt.Errorf("%w：拒绝 %s（首个拒绝: %w）", ErrBattleEnded, what, cause)
	}
	return fmt.Errorf("%w：拒绝 %s", ErrBattleEnded, what)
}

// startLinger 起收尾窗口：窗口内保持连接可读（结算结果可能还在补投），到期由客户端关连接。
// 已关闭的会话不启动（避免与 Close 的 wg.Wait 竞争）；窗口为 0 表示不等，立即收连接。
func (s *Session) startLinger() {
	linger := s.opt.endLinger
	s.genMu.Lock()
	if s.closed {
		s.genMu.Unlock()
		return
	}
	if linger <= 0 {
		s.genMu.Unlock()
		s.closeCurrent()
		return
	}
	s.wg.Add(1)
	s.genMu.Unlock()
	go s.waitLinger(linger)
}

// waitLinger 守候收尾窗口：到期即关当前代连接（读循环随之退出、监管因终态不再重连）；
// 会话关闭则立即退出（资源由 Close 释放）。数据报面没有关闭握手，这一步是客户端侧的兜底。
func (s *Session) waitLinger(d time.Duration) {
	defer s.wg.Done()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		s.closeCurrent()
	case <-s.closeCh:
	}
}
