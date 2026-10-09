// 直连会话的观测快照（重连/握手/心跳的计数与最近错误）。零新依赖：计数用原子累加、
// 最近错误在统计锁下读写；`Stats()` 返回只读副本，调用方不需要加锁，也不影响会话。
//
// 与既有出口的分工（不重复也不互相取代）：
//   - `Stats()`：**累计计数 + 最近错误**，用于趋势与排障（重连抖动、握手失败率、探针质量）；
//   - `HeartbeatErr()`：探针**首个**被拒原因（只记一次，不随后续失败覆盖）；
//   - `EndCause()/EndReason()/EndStats()`：终态（对局结束等）的原因与结束通知投递观测。

package direct

import (
	"sync"
	"sync/atomic"
)

// Stats 是直连会话的只读观测快照。
//
// 口径：计数为会话生命周期内的累计值（单调递增，原子读）；最近错误是「最后一次发生」的原因
// （含网络类与业务/协议拒绝），与 `HeartbeatErr()` 的「首个被拒」分工不同。快照各字段不保证
// 同一时刻的值（观测只做趋势与排障，不做事务性判定）。
type Stats struct {
	// Connects 是成功建立的连接代次（首连 + 每次重连成功）。
	Connects uint64
	// ReconnectRounds 是重连轮次（每轮 = 退避拨号 → 重放入局，直到成功或不可重试失败）。
	ReconnectRounds uint64
	// ReconnectFailures 是以不可重试错误终止的重连轮次（终止后不再自动重试）。
	ReconnectFailures uint64
	// LastReconnectErr 是最近一次重连失败原因（nil = 尚未失败）。
	LastReconnectErr error
	// HandshakeAttempts 是握手尝试次数（首连与每次重连拨号各一次）。
	HandshakeAttempts uint64
	// HandshakeFailures 是握手失败次数（接入层拒绝 / 网络不可达 / hello 无回执）。
	HandshakeFailures uint64
	// LastHandshakeErr 是最近一次握手失败原因（nil = 尚未失败）。
	LastHandshakeErr error
	// HeartbeatSent 是成功送达的保活探针数（写出且收到帧引擎的空信封）。
	HeartbeatSent uint64
	// HeartbeatFailures 是网络类探针失败数（写失败 / 回执未达；只计数，不影响会话状态）。
	HeartbeatFailures uint64
	// HeartbeatRejects 是探针被业务/协议拒绝的次数（含终态类与票类；同样只计数）。
	HeartbeatRejects uint64
	// LastHeartbeatErr 是最近一次探针失败或被拒原因（nil = 尚未失败）。
	LastHeartbeatErr error
}

// sessionStats 是 Stats 的累加器（会话内唯一实例）：计数原子累加，最近错误加锁读写。
type sessionStats struct {
	connects          atomic.Uint64
	reconnectRounds   atomic.Uint64
	reconnectFailures atomic.Uint64
	handshakeAttempts atomic.Uint64
	handshakeFailures atomic.Uint64
	hbSent            atomic.Uint64
	hbFailures        atomic.Uint64
	hbRejects         atomic.Uint64

	mu               sync.Mutex
	lastReconnectErr error
	lastHandshakeErr error
	firstHBErr       error // 探针首个被拒原因（HeartbeatErr 出口；只写一次）
	lastHBErr        error // 探针最近一次失败/被拒原因（Stats 出口；每次覆盖）
}

// Stats 返回观测快照（只读；并发安全）。
func (s *Session) Stats() Stats {
	return Stats{
		Connects:          s.stats.connects.Load(),
		ReconnectRounds:   s.stats.reconnectRounds.Load(),
		ReconnectFailures: s.stats.reconnectFailures.Load(),
		LastReconnectErr:  s.stats.lastError(&s.stats.lastReconnectErr),
		HandshakeAttempts: s.stats.handshakeAttempts.Load(),
		HandshakeFailures: s.stats.handshakeFailures.Load(),
		LastHandshakeErr:  s.stats.lastError(&s.stats.lastHandshakeErr),
		HeartbeatSent:     s.stats.hbSent.Load(),
		HeartbeatFailures: s.stats.hbFailures.Load(),
		HeartbeatRejects:  s.stats.hbRejects.Load(),
		LastHeartbeatErr:  s.stats.lastError(&s.stats.lastHBErr),
	}
}

// lastError 在统计锁下读一个「最近错误」槽（nil 安全）。
func (st *sessionStats) lastError(slot *error) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return *slot
}

// noteConnect 记一次成功建立的连接代次。
func (st *sessionStats) noteConnect() { st.connects.Add(1) }

// noteHandshake 记一次握手尝试与结果（首连与每次重连拨号共用同一口径）。
func (st *sessionStats) noteHandshake(err error) {
	st.handshakeAttempts.Add(1)
	if err == nil {
		return
	}
	st.handshakeFailures.Add(1)
	st.mu.Lock()
	st.lastHandshakeErr = err
	st.mu.Unlock()
}

// noteReconnectRound 记一轮重连的开始与结果（err != nil 表示以不可重试错误终止）。
func (st *sessionStats) noteReconnectRound(err error) {
	st.reconnectRounds.Add(1)
	if err == nil {
		return
	}
	st.reconnectFailures.Add(1)
	st.mu.Lock()
	st.lastReconnectErr = err
	st.mu.Unlock()
}

// noteHeartbeatSent 记一次成功送达的探针。
func (st *sessionStats) noteHeartbeatSent() { st.hbSent.Add(1) }

// noteHeartbeatFailure 记一次网络类探针失败（写失败 / 回执未达）。
func (st *sessionStats) noteHeartbeatFailure(err error) {
	st.hbFailures.Add(1)
	st.setLastHeartbeatErr(err)
}

// noteHeartbeatReject 记一次探针被业务/协议拒绝：首个原因只写一次（HeartbeatErr 出口，
// 「日志只在状态首次变化时打一条」的等价口径），计数与最近原因每次都更新。
func (st *sessionStats) noteHeartbeatReject(err error) {
	st.hbRejects.Add(1)
	st.mu.Lock()
	if st.firstHBErr == nil {
		st.firstHBErr = err
	}
	st.lastHBErr = err
	st.mu.Unlock()
}

// setLastHeartbeatErr 只更新最近一次探针失败原因（不覆盖首个被拒原因）。
func (st *sessionStats) setLastHeartbeatErr(err error) {
	st.mu.Lock()
	st.lastHBErr = err
	st.mu.Unlock()
}

// firstHeartbeatErr 返回探针首个被拒原因（nil = 未被拒）。
func (st *sessionStats) firstHeartbeatErr() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.firstHBErr
}
