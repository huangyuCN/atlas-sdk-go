package direct

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// State 是直连会话的连接状态。
type State uint8

// 全部状态取值。
const (
	// StateConnected 表示当前连接可用（可发战斗 op）。
	StateConnected State = iota + 1
	// StateReconnecting 表示断线后正在退避重连（重连成功后会重放入局与补帧）。
	StateReconnecting
	// StateDisconnected 表示已断开：关闭、等待显式 Reconnect，或已因不可重试错误终止。
	StateDisconnected
	// StateEnded 表示**正常结束**终态：对局已结束（服务端拒 BATTLE_ENDED 或收到结束通知）——
	// 有结算可展示，停发（发帧/补帧/探针一律被拒），收尾窗口内仍可读推送，Close 之前不可回退。
	StateEnded
	// StateFailed 表示**终态失败**：无结算可展示的终态拒绝（对局不存在/已满/请求目标不符）——
	// 与 StateEnded 同样停发、不重连、不可回退，但连接立即回收（没有结算要等），
	// 上层据此回匹配链路重新开局，而不是去取一份不存在的结算。
	StateFailed
)

// String 返回状态文本。
func (s State) String() string {
	switch s {
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateDisconnected:
		return "disconnected"
	case StateEnded:
		return "ended"
	case StateFailed:
		return "failed"
	default:
		return fmt.Sprintf("state(%d)", uint8(s))
	}
}

// generation 是一条连接代际的不可变快照：换连接即整体替换，读循环按快照取连接。
type generation struct {
	tr   conn
	done chan struct{}
}

// Session 是一条战斗直连会话：按传输面直连接入层（或 battle 帧端口），逐帧在会话槽携带票
// 跑战斗 op（JoinBattle / SendFrameInput / SyncFrames），收帧广播/战斗结束推送；
// 断线后重新 hello 并重放 JoinBattle + SyncFrames(last_seen_frame) 补帧。
//
// 并发安全：Invoke/Reconnect/Close 可并发调用；推送回调在独立 goroutine 执行（panic 被 recover）。
type Session struct {
	plan Plan
	kind Transport
	addr string
	opt  options
	ver  uint8 // 出站载荷编码版本（由序列化器推导，白名单 {1,2}）

	genMu   sync.Mutex
	gen     *generation
	closed  bool
	fatal   error        // 不可重试的终止原因（被接入层拒绝/票据失效）
	waiters []chan error // 本轮重连的等待者

	writeMu  sync.Mutex
	seq      atomic.Uint32
	inflight sync.Map // uint32 → chan invokeResult
	state    atomic.Int32

	notifyMu sync.Mutex
	handlers map[string]map[string]pushHandler // op → 订阅键 → 订阅者（带载荷编码版本）

	joined   atomic.Bool
	lastSeen atomic.Uint64

	stats sessionStats // 观测快照累加器（重连/握手/心跳，见 stats.go）

	manual  chan struct{} // 显式 Reconnect 信号（结果经 waiters 投递）
	closeCh chan struct{}
	wg      sync.WaitGroup

	end endedState // 终态登记项（对局结束的判定/停发/幂等，见 ended.go）
}

// newSession 组装会话本体（不含拨号）：出站载荷编码版本由序列化器推导，白名单外的声明在此报错。
func newSession(plan Plan, kind Transport, addr string, o options) (*Session, error) {
	ver, err := payloadVersion(o.serial)
	if err != nil {
		return nil, err
	}
	s := &Session{
		plan:     plan,
		kind:     kind,
		addr:     addr,
		opt:      o,
		ver:      ver,
		handlers: make(map[string]map[string]pushHandler),
		manual:   make(chan struct{}, 8),
		closeCh:  make(chan struct{}),
	}
	s.state.Store(int32(StateDisconnected))
	return s, nil
}

// Open 按计划建立战斗直连会话：选面 → 建连（经接入层时先 hello）→ 启动读循环与重连监管。
//
// 计划缺票返回 ErrNotifyNoTicket，所指定的面未下发返回 ErrTransportNotFound（不猜端口）；
// 序列化器声明的载荷编码版本不在白名单 {1,2} 返回 ErrPayloadVersion（拨号前把关）；
// 被接入层拒绝返回 ErrRejected（无回执、连接被断，不重试），网络故障返回 *client.NetworkError。
func Open(ctx context.Context, plan Plan, opts ...Option) (*Session, error) {
	if len(plan.Ticket) == 0 {
		return nil, fmt.Errorf("%w: 票为空", ErrNotifyNoTicket)
	}
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	kind, addr, err := pickEndpoint(plan, o.transport)
	if err != nil {
		return nil, err
	}
	s, err := newSession(plan, kind, addr, o)
	if err != nil {
		return nil, err
	}
	tr, err := s.dialTracked(ctx)
	if err != nil {
		return nil, err
	}
	if !s.startGeneration(tr) {
		return nil, ErrClosed
	}
	s.setState(StateConnected)
	return s, nil
}

// dialTracked 记录一次握手尝试与结果（首连与每次重连拨号共用同一计数口径，见 stats.go）。
func (s *Session) dialTracked(ctx context.Context) (conn, error) {
	tr, err := dial(ctx, s.kind, s.addr, s.plan.Ticket, s.opt)
	s.stats.noteHandshake(err)
	return tr, err
}

// startGeneration 登记新一代连接并启动读循环、监管与**本代保活循环**（每代都重新起表，
// 首连与每次重连成功同一条路径）；会话已关闭返回 false（并关掉连接）。
func (s *Session) startGeneration(tr conn) bool {
	s.genMu.Lock()
	if s.closed {
		s.genMu.Unlock()
		_ = tr.Close()
		return false
	}
	g := &generation{tr: tr, done: make(chan struct{})}
	s.gen = g
	s.wg.Add(2)
	s.genMu.Unlock()
	s.stats.noteConnect() // 连接代次计数（首连 + 每次重连成功）
	go s.readLoop(g)
	go s.supervise(g)
	s.startHeartbeat(g) // 每代（含重连成功）重新起表：保活不因本代内的一次失败永久失效
	return true
}

// readLoop 读当前代连接的帧并分发，直到连接死亡（网络断开或协议错误）。
func (s *Session) readLoop(g *generation) {
	defer s.wg.Done()
	defer close(g.done)
	for {
		hdr, body, err := g.tr.ReadFrame(frame.MaxBodySize)
		if err != nil {
			s.failInflight(client.NewNetworkError(fmt.Errorf("direct: 连接中断: %w", err)))
			return
		}
		if err := s.dispatch(hdr, body); err != nil {
			s.failInflight(err)
			_ = g.tr.Close() // 协议级致命：断连（由监管按策略处理）
			return
		}
	}
}

// dispatch 分发一帧：响应按 seq 匹配未决请求，推送交订阅者，其余按协议错误终止本连接。
func (s *Session) dispatch(hdr frame.Header, body []byte) error {
	switch hdr.Type {
	case frame.MsgTypeResponse:
		return s.dispatchResponse(hdr, body)
	case frame.MsgTypeNotify:
		s.dispatchNotify(hdr.Version, body)
		return nil
	default:
		return client.NewProtocolError(fmt.Errorf("direct: 非法帧类型 %d", hdr.Type))
	}
}

// Close 关闭会话（幂等）：停止重连、断开当前连接、唤醒全部等待者与未决请求。
func (s *Session) Close() error {
	s.genMu.Lock()
	if s.closed {
		s.genMu.Unlock()
		s.wg.Wait()
		return nil
	}
	s.closed = true
	g := s.gen
	s.genMu.Unlock()

	close(s.closeCh)
	if g != nil {
		_ = g.tr.Close()
	}
	s.failInflight(ErrClosed)
	s.finishWaiters(ErrClosed)
	s.setState(StateDisconnected)
	s.wg.Wait()
	return nil
}

// Transport 返回本次实际使用的传输面。
func (s *Session) Transport() Transport { return s.kind }

// Endpoint 返回本次实际直连的接入层地址（来源是本局推送）。
func (s *Session) Endpoint() string { return s.addr }

// Ticket 返回本会话携带的票密文（上层据此判断是否需要回业务链路重新取票）。
func (s *Session) Ticket() []byte { return s.plan.Ticket }

// BattleID 返回本局的战斗标识。
func (s *Session) BattleID() string { return s.plan.BattleID }

// LastSeenFrame 返回最近一次收到的帧号（重连补帧的 last_seen_frame 基准）。
func (s *Session) LastSeenFrame() uint64 { return s.lastSeen.Load() }

// State 返回连接状态。
func (s *Session) State() State { return State(s.state.Load()) }

// Err 返回终止原因（被接入层拒绝/票据失效/已关闭；未终止为 nil）。
func (s *Session) Err() error {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	return s.fatal
}

// setState 更新连接状态：终态不可回退（含 Close 之后），关闭后不再改写成非断开态。
func (s *Session) setState(st State) {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	if s.end.flag.Load() && st != StateEnded && st != StateFailed {
		return // 终态不可回退（ended/failed 都不被降级）
	}
	if s.closed && st != StateDisconnected && st != StateEnded && st != StateFailed {
		return
	}
	s.state.Store(int32(st))
}

// setFatal 记录不可重试的终止原因并置断开态（终态优先，不被降级）。
func (s *Session) setFatal(err error) {
	s.genMu.Lock()
	s.fatal = err
	s.genMu.Unlock()
	s.setState(StateDisconnected)
}

// currentGen 返回当前代连接快照。
func (s *Session) currentGen() *generation {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	return s.gen
}
