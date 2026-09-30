package direct

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// frameConn 是测试用的帧连接接缝：三个传输面的服务端桩共用同一段 battle 逻辑
// （真实 battle 无论承载面如何都是同一份帧协议服务端）。
type frameConn interface {
	ReadFrame(maxBodySize int) (frame.Header, []byte, error)
	WriteFrame(h frame.Header, body []byte, maxBodySize int) error
	Close() error
}

// battle 桩的 op 名：与生成物 api/battle/v1 同源（测试里写字面量是为了让断言独立于实现）。
const (
	stubOpJoin  = "/battle.v1.BattleService/JoinBattle"
	stubOpInput = "/battle.v1.BattleService/SendFrameInput"
	stubOpSync  = "/battle.v1.BattleService/SyncFrames"
	stubOpFrame = "/battle.v1.FrameBroadcast"
	stubOpEnd   = "/battle.v1.BattleEndNotify"
)

// battleStub 是最小 battle 帧服务端桩：验帧槽 → 答 JoinBattle/SyncFrames → 按需推帧广播。
// 每个连接一个实例（serve 在连接读循环里串行执行，故索引字段无需加锁；跨连接的汇总字段加锁）。
type battleStub struct {
	ticket []byte

	mu       sync.Mutex
	conns    int      // 累计接入的连接数（重连断言用）
	slots    []string // 收到的全部帧槽（含空槽）
	flags    []uint8  // 收到的全部请求帧 flags
	ops      []string // 收到的全部 op（按到达顺序）
	problems []string // 服务端视角的违规记录（槽不符/未置位/协议非法）
	joinErrs []string // 每次 JoinBattle 的业务 reason（空 = 成功）
	syncLast []uint64 // 每次 SyncFrames 的 last_seen_frame
	pushed   []uint64 // 已推送的帧号
	pushSeq  uint32   // 推送帧 seq（服务端推送同样带非 0 seq，0 是协议非法值）
	closeOn  int      // 指定第几次接入的连接主动断开（0 = 不断开）
	closeIn  int      // 该连接处理多少个请求后断开（模拟拆流）
}

// newBattleStub 构造桩（ticket 为票密文，桩据此算出期望的帧槽取值）。
func newBattleStub(ticket []byte) *battleStub {
	return &battleStub{ticket: ticket}
}

// planFor 构造只含指定传输面的直连计划。
func planFor(t Transport, addr string, ticket []byte) Plan {
	return Plan{MatchID: "m-1", BattleID: "b-1", Ticket: ticket, Endpoints: map[Transport]string{t: addr}}
}

// setJoinError 让 JoinBattle 回指定 reason 的业务错误。
func (s *battleStub) setJoinError(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joinErrs = append(s.joinErrs, reason)
}

// setCloseAfter 让第 conn 次接入的连接处理 reqs 个请求后主动断开（模拟接入层/后端拆流）。
func (s *battleStub) setCloseAfter(conn, reqs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeOn, s.closeIn = conn, reqs
}

// snapshot 返回服务端观测快照（连接数、槽、flags、op、违规记录、补帧基准、已推帧号）。
func (s *battleStub) snapshot() (conns int, slots []string, flags []uint8, ops []string, problems []string, syncs []uint64, pushed []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := func(in []string) []string { return append([]string(nil), in...) }
	return s.conns, cp(s.slots), append([]uint8(nil), s.flags...), cp(s.ops), cp(s.problems), append([]uint64(nil), s.syncLast...), append([]uint64(nil), s.pushed...)
}

// addConn 记录一次新连接并返回其序号（1 起，桩按序号决定是否主动断开）。
func (s *battleStub) addConn() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns++
	return s.conns
}

// problem 记录一条服务端视角的违规。
func (s *battleStub) problem(format string, args ...any) {
	s.mu.Lock()
	s.problems = append(s.problems, fmt.Sprintf(format, args...))
	s.mu.Unlock()
}

// joinReason 取本次 JoinBattle 应回的业务 reason（空 = 成功），按顺序消费。
func (s *battleStub) joinReason() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.joinErrs) == 0 {
		return ""
	}
	r := s.joinErrs[0]
	s.joinErrs = s.joinErrs[1:]
	return r
}

// serve 在一条连接上跑 battle 桩直到读失败或按计划断开。
func (s *battleStub) serve(fc frameConn) {
	idx := s.addConn()
	for n := 0; ; n++ {
		hdr, body, err := fc.ReadFrame(frame.MaxBodySize)
		if err != nil {
			return
		}
		if !s.handle(fc, hdr, body) {
			return
		}
		if s.shouldClose(idx, n+1) {
			return
		}
	}
}

// shouldClose 报告第 idx 次接入的连接是否已处理够请求数、该主动断开。
func (s *battleStub) shouldClose(idx, done int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeOn == idx && s.closeIn > 0 && done >= s.closeIn
}

// handle 处理一条请求帧并回执；返回 false 表示读侧应终止（协议违规）。
func (s *battleStub) handle(fc frameConn, hdr frame.Header, body []byte) bool {
	if hdr.Type != frame.MsgTypeRequest {
		s.problem("收到非请求帧 type=%d", hdr.Type)
		return false
	}
	op, slot, payload, err := frame.ParseRequestBodyWithSession(body, hdr.Flags)
	if err != nil {
		s.problem("body 解析失败: %v", err)
		return false
	}
	s.record(hdr.Flags, slot, op)
	if err := s.reply(fc, hdr, op, payload); err != nil {
		s.problem("回执写失败: %v", err)
		return false
	}
	return true
}

// record 记录并校验一帧的会话槽（battle 侧 identity 的客户端侧等价断言）。
func (s *battleStub) record(flags uint8, slot, op string) {
	s.mu.Lock()
	s.flags = append(s.flags, flags)
	s.slots = append(s.slots, slot)
	s.ops = append(s.ops, op)
	s.mu.Unlock()
	if flags&frame.FlagSession == 0 {
		s.problem("请求帧未置位 FlagSession（op=%s）", op)
	}
	if want := TicketSlot(s.ticket); slot != want {
		s.problem("帧槽 = %q, 期望 %q（op=%s）", slot, want, op)
	}
}

// reply 按 op 回执：JoinBattle/SyncFrames 回具体消息，SendFrameInput 回空成功包络。
func (s *battleStub) reply(fc frameConn, hdr frame.Header, op string, payload []byte) error {
	switch op {
	case stubOpJoin:
		if reason := s.joinReason(); reason != "" {
			return s.write(fc, hdr, errorReply(reason))
		}
		return s.write(fc, hdr, successReply([]byte(`{"meta":{"sessionId":"b-1"},"currentFrame":3}`)))
	case stubOpSync:
		last := s.recordSync(payload)
		if err := s.write(fc, hdr, successReply([]byte(`{"currentFrame":3,"missed":[]}`))); err != nil {
			return err
		}
		s.pushFrame(fc, last+1)
		return nil
	case stubOpInput:
		return s.write(fc, hdr, successReply(nil))
	default:
		return s.write(fc, hdr, errorReply("UNIMPLEMENTED"))
	}
}

// recordSync 解析并记录 SyncFrames 的 last_seen_frame。
func (s *battleStub) recordSync(payload []byte) uint64 {
	last := lastSeenFromJSON(payload)
	s.mu.Lock()
	s.syncLast = append(s.syncLast, last)
	s.mu.Unlock()
	return last
}

// pushFrame 推一条帧广播（帧号非 0），并记录已推帧号。
func (s *battleStub) pushFrame(fc frameConn, frameID uint64) {
	payload := []byte(fmt.Sprintf(`{"battleId":"b-1","frame":{"frameId":%d,"inputs":[]}}`, frameID))
	body, err := frame.BuildRequestBody(stubOpFrame, payload)
	if err != nil {
		s.problem("帧广播 body 封装失败: %v", err)
		return
	}
	s.mu.Lock()
	s.pushSeq++
	seq := s.pushSeq
	s.mu.Unlock()
	hdr := frame.Header{Type: frame.MsgTypeNotify, Seq: seq}
	if err := fc.WriteFrame(hdr, body, frame.MaxBodySize); err != nil {
		s.problem("帧广播写失败: %v", err)
		return
	}
	s.mu.Lock()
	s.pushed = append(s.pushed, frameID)
	s.mu.Unlock()
}

// write 按请求 seq 回一条响应帧。
func (s *battleStub) write(fc frameConn, req frame.Header, body []byte) error {
	return fc.WriteFrame(frame.Header{Type: frame.MsgTypeResponse, Version: req.Version, Seq: req.Seq}, body, frame.MaxBodySize)
}

// successReply 构造成功回执包络：[0x00][dataLen u32][data]。
func successReply(data []byte) []byte {
	out := make([]byte, 5+len(data))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(len(data)))
	copy(out[5:], data)
	return out
}

// errorReply 构造业务错误回执包络：[0x01][statusLen u32][Status][dataLen u32][data]。
func errorReply(reason string) []byte {
	st := encodeStatus(1, reason, "桩拒绝")
	out := make([]byte, 1+4+len(st)+4)
	out[0] = 1
	binary.BigEndian.PutUint32(out[1:5], uint32(len(st)))
	copy(out[5:], st)
	return out
}

// encodeStatus 手写编码 atlas errors.Status（code=1 varint / reason=2 / message=3）。
func encodeStatus(code int32, reason, message string) []byte {
	var out []byte
	out = append(out, 0x08) // field 1, varint
	out = append(out, byte(code))
	out = append(out, 0x12) // field 2, bytes
	out = append(out, byte(len(reason)))
	out = append(out, reason...)
	out = append(out, 0x1a) // field 3, bytes
	out = append(out, byte(len(message)))
	out = append(out, message...)
	return out
}

// assertWireOK 断言服务端视角的直连链路一致性：无违规、接入连接数、op 序列，
// 且**每一帧**都置位会话槽、槽值等于 base64url 票密文（battle 侧 identity 的等价断言）。
func assertWireOK(t *testing.T, stub *battleStub, wantConns int, wantOps []string) {
	t.Helper()
	conns, slots, flags, ops, problems, _, _ := stub.snapshot()
	if len(problems) != 0 {
		t.Fatalf("服务端视角违规: %v", problems)
	}
	if conns != wantConns {
		t.Fatalf("连接数 = %d, 期望 %d", conns, wantConns)
	}
	if len(ops) != len(wantOps) {
		t.Fatalf("op 序列 = %v, 期望 %v", ops, wantOps)
	}
	for i, want := range wantOps {
		if ops[i] != want {
			t.Fatalf("第 %d 个 op = %q, 期望 %q（实际序列 %v）", i, ops[i], want, ops)
		}
	}
	assertSlotsCarryTicket(t, slots, flags)
}

// assertSlotsCarryTicket 断言逐帧会话槽：全部置位 FlagSession 且槽值 = base64url 票密文。
func assertSlotsCarryTicket(t *testing.T, slots []string, flags []uint8) {
	t.Helper()
	want := TicketSlot(testTicket)
	if len(slots) == 0 {
		t.Fatal("服务端未收到任何请求帧")
	}
	for i, slot := range slots {
		if slot != want {
			t.Fatalf("第 %d 帧槽 = %q, 期望 %q", i, slot, want)
		}
	}
	for i, f := range flags {
		if f&frame.FlagSession == 0 {
			t.Fatalf("第 %d 帧未置位会话槽（flags=%#x）", i, f)
		}
	}
}

// waitPushedFrame 等一条帧广播并校验帧号。
func waitPushedFrame(t *testing.T, ch <-chan uint64, want uint64) {
	t.Helper()
	select {
	case id := <-ch:
		if id != want {
			t.Fatalf("帧广播帧号 = %d, 期望 %d", id, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("未收到帧广播推送")
	}
}

// literalHello 是测试侧**手写**的 hello 段字节（独立裁判：不调用被测实现，避免自我印证）。
func literalHello(ticket []byte) []byte {
	out := []byte{'A', 'T', 'L', 'H', 0x01, byte(len(ticket) >> 8), byte(len(ticket))}
	return append(out, ticket...)
}

// literalFlowID 是测试侧手写的 8 字节大端 flow-id 前缀。
func literalFlowID(id uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, id)
	return out
}

// literalWrap 手写加 flow-id 前缀（发出去的方向）。
func literalWrap(id uint64, payload []byte) []byte { return append(literalFlowID(id), payload...) }

// literalStrip 手写剥 flow-id 前缀（收到的方向）；前缀缺失或失配返回 ok=false。
func literalStrip(data []byte, id uint64) ([]byte, bool) {
	if len(data) < 8 || binary.BigEndian.Uint64(data[:8]) != id {
		return nil, false
	}
	return data[8:], true
}
