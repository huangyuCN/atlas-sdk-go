package direct

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
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
	plan   Plan
	kind   Transport
	addr   string
	opt    options
	serial client.Serializer

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
	handlers map[string]map[string]func([]byte) // op → 订阅键 → handler

	joined   atomic.Bool
	lastSeen atomic.Uint64

	hbSent     atomic.Uint64 // 成功送达的保活探针数
	hbFailures atomic.Uint64 // 失败的保活探针数（只计数）
	hbErrMu    sync.Mutex
	hbErr      error // 探针被明确拒绝的原因（记录后探测停止；nil = 未被拒）

	manual  chan struct{} // 显式 Reconnect 信号（结果经 waiters 投递）
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// newSession 组装会话本体（不含拨号）。
func newSession(plan Plan, kind Transport, addr string, o options) *Session {
	s := &Session{
		plan:     plan,
		kind:     kind,
		addr:     addr,
		opt:      o,
		serial:   client.ProtoJSONSerializer{},
		handlers: make(map[string]map[string]func([]byte)),
		manual:   make(chan struct{}, 8),
		closeCh:  make(chan struct{}),
	}
	s.state.Store(int32(StateDisconnected))
	return s
}

// Open 按计划建立战斗直连会话：选面 → 建连（经接入层时先 hello）→ 启动读循环与重连监管。
//
// 计划缺票返回 ErrNotifyNoTicket，所指定的面未下发返回 ErrTransportNotFound（不猜端口）；
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
	s := newSession(plan, kind, addr, o)
	tr, err := dial(ctx, kind, addr, plan.Ticket, o)
	if err != nil {
		return nil, err
	}
	if !s.startGeneration(tr) {
		return nil, ErrClosed
	}
	s.setState(StateConnected)
	s.startHeartbeat()
	return s, nil
}

// startGeneration 登记新一代连接并启动读循环与监管 goroutine；会话已关闭返回 false（并关掉连接）。
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
	go s.readLoop(g)
	go s.supervise(g)
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
		s.dispatchNotify(body)
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

// setState 更新连接状态（关闭后不再改写）。
func (s *Session) setState(st State) {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	if s.closed && st != StateDisconnected {
		return
	}
	s.state.Store(int32(st))
}

// setFatal 记录不可重试的终止原因并置断开态。
func (s *Session) setFatal(err error) {
	s.genMu.Lock()
	s.fatal = err
	s.state.Store(int32(StateDisconnected))
	s.genMu.Unlock()
}

// currentGen 返回当前代连接快照。
func (s *Session) currentGen() *generation {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	return s.gen
}

// OnNotify 订阅指定 op 的推送（载荷为帧头编码的原始字节），返回退订函数。
func (s *Session) OnNotify(op string, fn func([]byte)) (off func()) {
	if fn == nil || op == "" {
		return func() {}
	}
	key := fmt.Sprintf("fn:%d", handlerSeq.Add(1))
	s.notifyMu.Lock()
	if s.handlers[op] == nil {
		s.handlers[op] = make(map[string]func([]byte))
	}
	s.handlers[op][key] = fn
	s.notifyMu.Unlock()
	return func() { s.unsubscribe(op, key) }
}

// OnFrame 订阅帧广播推送（解析为 *battlev1.FrameBroadcast 后回调），返回退订函数。
func (s *Session) OnFrame(fn func(*battlev1.FrameBroadcast)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnNotify(battlev1opclient.BattleServicePushOps.FrameBroadcast, func(payload []byte) {
		var fb battlev1.FrameBroadcast
		if err := protojson.Unmarshal(payload, &fb); err != nil {
			return
		}
		fn(&fb)
	})
}

// OnBattleEnd 订阅战斗结束推送（解析为 *battlev1.BattleEndNotify 后回调），返回退订函数。
func (s *Session) OnBattleEnd(fn func(*battlev1.BattleEndNotify)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnNotify(battlev1opclient.BattleServicePushOps.BattleEndNotify, func(payload []byte) {
		var n battlev1.BattleEndNotify
		if err := protojson.Unmarshal(payload, &n); err != nil {
			return
		}
		fn(&n)
	})
}

// OnPlayerOut 订阅出局推送（解析为 *battlev1.PlayerOutNotify 后回调），返回退订函数：
// 收到即表示该玩家被判出局并移出参战名单（掉线超时等），是保活验收的直接证据。
func (s *Session) OnPlayerOut(fn func(*battlev1.PlayerOutNotify)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnNotify(battlev1opclient.BattleServicePushOps.PlayerOutNotify, func(payload []byte) {
		var n battlev1.PlayerOutNotify
		if err := protojson.Unmarshal(payload, &n); err != nil {
			return
		}
		fn(&n)
	})
}

// handlerSeq 为推送订阅生成稳定去重键（同一函数重复订阅不叠加）。
var handlerSeq atomic.Uint64

// unsubscribe 退订并清理空表。
func (s *Session) unsubscribe(op, key string) {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	if set, ok := s.handlers[op]; ok {
		delete(set, key)
		if len(set) == 0 {
			delete(s.handlers, op)
		}
	}
}

// dispatchNotify 分发推送：先推进补帧基准（帧广播），再以独立 goroutine 交各订阅者。
func (s *Session) dispatchNotify(body []byte) {
	op, payload, err := frame.ParseRequestBody(body)
	if err != nil {
		return
	}
	if op == battlev1opclient.BattleServicePushOps.FrameBroadcast {
		s.trackFrame(payload)
	}
	for _, fn := range s.handlersOf(op) {
		go safeCall(fn, payload)
	}
}

// handlersOf 取某 op 的订阅者快照。
func (s *Session) handlersOf(op string) []func([]byte) {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	out := make([]func([]byte), 0, len(s.handlers[op]))
	for _, fn := range s.handlers[op] {
		out = append(out, fn)
	}
	return out
}

// safeCall 带 panic 保护的订阅者调用（单个回调异常不影响其他分发）。
func safeCall(fn func([]byte), payload []byte) {
	defer func() { _ = recover() }()
	fn(payload)
}

// trackFrame 解析帧广播并推进补帧基准（重连 SyncFrames 用它取 last_seen_frame）。
func (s *Session) trackFrame(payload []byte) {
	var fb battlev1.FrameBroadcast
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload, &fb); err != nil {
		return
	}
	if id := fb.GetFrame().GetFrameId(); id > s.lastSeen.Load() {
		s.lastSeen.Store(id)
	}
}
