// 保活心跳（Ping）单测：假传输（帧级接缝）上断言周期、载荷、失败容忍与 Close 收尾，
// 不经真实网络与接入层，故次数/op/载荷/帧槽的断言完全确定；真协议桩（WS）另有一条
// 「探针与业务帧互不干扰」的用例。

package direct

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas-sdk-go/api/lockstep"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
)

// testBattleID 是心跳单测的计划战斗标识（探针载荷断言用，与桩里的 b-1 区分开）。
const testBattleID = "b-heartbeat"

// fakeFrame 是假传输里排队待读的一帧。
type fakeFrame struct {
	hdr  frame.Header
	body []byte
}

// fakeConn 是保活单测用的假传输：逐帧记录请求，并按脚本回执/不答/写失败。
type fakeConn struct {
	mu       sync.Mutex
	ops      []string // 写出的请求 op（按到达顺序）
	slots    []string // 与 ops 同序的会话槽
	payloads [][]byte // 与 ops 同序的载荷副本
	failNext int      // > 0：接下来 n 次写失败（模拟发送失败）
	reason   string   // 非空：以该业务 reason 拒绝（模拟 op 未注册/被拒）
	script   []string // 按序消费的回执 reason（优先于 reason；空串 = 成功信封）
	silent   bool     // true：只收不回（模拟回执丢失）
	in       chan fakeFrame
	closed   chan struct{}
	once     sync.Once
}

// newFakeConn 构造假传输（回执队列带缓冲，回执不阻塞写侧）。
func newFakeConn() *fakeConn {
	return &fakeConn{in: make(chan fakeFrame, 64), closed: make(chan struct{})}
}

// WriteFrame 记录一帧请求并按脚本决定是否回执（回执复用桩里的包络编码）。
func (c *fakeConn) WriteFrame(h frame.Header, body []byte, _ int) error {
	op, slot, payload, err := frame.ParseRequestBodyWithSession(body, h.Flags)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.ops = append(c.ops, op)
	c.slots = append(c.slots, slot)
	c.payloads = append(c.payloads, append([]byte(nil), payload...))
	fail := c.failNext > 0
	if fail {
		c.failNext--
	}
	reason, silent := c.reason, c.silent
	if len(c.script) > 0 { // 回执脚本优先：按序为本次请求指定 reason（空串 = 成功信封）
		reason = c.script[0]
		c.script = c.script[1:]
	}
	c.mu.Unlock()
	if fail {
		return errors.New("假传输：写失败")
	}
	if silent {
		return nil
	}
	env := successReply(nil)
	if reason != "" {
		env = errorReply(reason)
	}
	return c.push(frame.Header{Type: frame.MsgTypeResponse, Version: h.Version, Seq: h.Seq}, env)
}

// push 把一帧排入待读队列（连接已关闭即失败）。
func (c *fakeConn) push(hdr frame.Header, body []byte) error {
	select {
	case c.in <- fakeFrame{hdr: hdr, body: body}:
		return nil
	case <-c.closed:
		return errors.New("假传输：已关闭")
	}
}

// ReadFrame 从队列取一帧（阻塞到有帧或连接关闭）。
func (c *fakeConn) ReadFrame(int) (frame.Header, []byte, error) {
	select {
	case f := <-c.in:
		return f.hdr, f.body, nil
	case <-c.closed:
		return frame.Header{}, nil, errors.New("假传输：已关闭")
	}
}

// Close 关闭假传输（幂等）并唤醒阻塞中的读。
func (c *fakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// replyNext 为紧接着的第 n 次写出指定回执 reason（按序消费；空串 = 成功信封）。
func (c *fakeConn) replyNext(reasons ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.script = append(c.script, reasons...)
}

// isClosed 报告假传输是否已被关闭（终态收尾窗口的断言用）。
func (c *fakeConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// count 返回已写出的指定 op 次数。
func (c *fakeConn) count(op string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, got := range c.ops {
		if got == op {
			n++
		}
	}
	return n
}

// snapshot 返回已写出请求的快照（op/槽/载荷，三者同序）。
func (c *fakeConn) snapshot() (ops, slots []string, payloads [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.payloads))
	for i, p := range c.payloads {
		out[i] = append([]byte(nil), p...)
	}
	return append([]string(nil), c.ops...), append([]string(nil), c.slots...), out
}

// newHeartbeatSession 用假传输组装一个已连接会话（绕过拨号与接入层，仅供心跳单测）。
func newHeartbeatSession(t *testing.T, fc *fakeConn, opts ...Option) *Session {
	t.Helper()
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	plan := Plan{
		BattleID: testBattleID, Ticket: testTicket,
		Endpoints: map[Transport]string{TransportWS: "127.0.0.1:1"},
	}
	s := newSession(plan, TransportWS, "127.0.0.1:1", o)
	if !s.startGeneration(fc) {
		t.Fatal("假传输登记失败：会话已关闭")
	}
	s.setState(StateConnected)
	s.startHeartbeat()
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// assertPingPayloads 断言全部探针载荷都是 PingReq 且 battle_id 正确、逐帧带票槽。
func assertPingPayloads(t *testing.T, fc *fakeConn) {
	t.Helper()
	ops, slots, payloads := fc.snapshot()
	if len(ops) == 0 {
		t.Fatal("未写出任何帧")
	}
	for i, op := range ops {
		if op != stubOpPing {
			t.Fatalf("第 %d 帧 op = %q, 期望 %q（静默期只该发探针）", i, op, stubOpPing)
		}
		var req battlev1.PingReq
		if err := protojson.Unmarshal(payloads[i], &req); err != nil {
			t.Fatalf("第 %d 帧探针载荷解析失败: %v", i, err)
		}
		if req.GetBattleId() != testBattleID {
			t.Fatalf("第 %d 帧探针 battle_id = %q, 期望 %q", i, req.GetBattleId(), testBattleID)
		}
		if slots[i] != TicketSlot(testTicket) {
			t.Fatalf("第 %d 帧槽 = %q, 期望 %q", i, slots[i], TicketSlot(testTicket))
		}
	}
}

// TestHeartbeatSendsPeriodicPing ①：周期到点即发 Ping（Tell 语义、无业务回执依赖），
// 载荷是 PingReq{battle_id}、逐帧携带票槽，且计数可观测。
func TestHeartbeatSendsPeriodicPing(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(30*time.Millisecond))

	waitFor(t, 3*time.Second, "累计 3 次探针", func() bool { return fc.count(stubOpPing) >= 3 })
	assertPingPayloads(t, fc)
	if got := sess.HeartbeatPeriod(); got != 30*time.Millisecond {
		t.Fatalf("探针周期 = %s, 期望 30ms", got)
	}
	if got, failures := sess.HeartbeatSent(), sess.HeartbeatFailures(); got < 3 || failures != 0 {
		t.Fatalf("探针计数 sent=%d failures=%d, 期望 sent≥3 且 failures=0", got, failures)
	}
	if err := sess.HeartbeatErr(); err != nil {
		t.Fatalf("探针未被拒却记录错误: %v", err)
	}
}

// TestHeartbeatPeriodConfigurable ④：周期配置生效（周期 → 频次），0 表示关闭，
// 缺省 2s 严格小于数据报面空闲读超时（offline_timeout/3 的缺省 5s）。
func TestHeartbeatPeriodConfigurable(t *testing.T) {
	fast := newHeartbeatSession(t, newFakeConn(), WithHeartbeat(20*time.Millisecond))
	if got := fast.HeartbeatPeriod(); got != 20*time.Millisecond {
		t.Fatalf("快周期 = %s, 期望 20ms", got)
	}
	if defaultHeartbeat >= 5*time.Second {
		t.Fatalf("缺省探针周期 %s 必须严格小于数据报面空闲读超时 5s", defaultHeartbeat)
	}

	off := newFakeConn()
	offSess := newHeartbeatSession(t, off, WithHeartbeat(0))
	if got := offSess.HeartbeatPeriod(); got != 0 {
		t.Fatalf("关闭心跳后周期 = %s, 期望 0", got)
	}
	def := newFakeConn()
	defSess := newHeartbeatSession(t, def)
	if got := defSess.HeartbeatPeriod(); got != defaultHeartbeat {
		t.Fatalf("缺省周期 = %s, 期望 %s", got, defaultHeartbeat)
	}
	time.Sleep(300 * time.Millisecond) // 300ms < 缺省 2s：两者都不该发出任何帧
	if n := off.count(stubOpPing); n != 0 {
		t.Fatalf("关闭心跳后仍发出 %d 次探针", n)
	}
	if n := def.count(stubOpPing); n != 0 {
		t.Fatalf("缺省周期 2s 在 300ms 内发出了 %d 次探针", n)
	}
}

// TestHeartbeatStopsOnClose ②：Close 后不再发探针，且 goroutine 回落到基线（不泄漏）。
func TestHeartbeatStopsOnClose(t *testing.T) {
	base := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		fc := newFakeConn()
		sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))
		waitFor(t, 2*time.Second, "探针起跑", func() bool { return fc.count(stubOpPing) > 0 })
		if err := sess.Close(); err != nil {
			t.Fatalf("Close 失败: %v", err)
		}
		sent := fc.count(stubOpPing)
		time.Sleep(100 * time.Millisecond) // 10 个周期：泄漏的话这里必然增长
		if got := fc.count(stubOpPing); got != sent {
			t.Fatalf("Close 后仍在发探针：%d → %d", sent, got)
		}
	}
	waitFor(t, 3*time.Second, "goroutine 回落到基线", func() bool { return runtime.NumGoroutine() <= base+2 })
}

// TestHeartbeatToleratesSendFailure ③：一次发送失败只计数（不终止会话、不打断后续探针），
// 业务帧照常可用。
func TestHeartbeatToleratesSendFailure(t *testing.T) {
	fc := newFakeConn()
	fc.failNext = 2
	sess := newHeartbeatSession(t, fc, WithHeartbeat(15*time.Millisecond))

	waitFor(t, 3*time.Second, "记到两次失败", func() bool { return sess.HeartbeatFailures() >= 2 })
	waitFor(t, 3*time.Second, "失败后继续发", func() bool { return sess.HeartbeatSent() >= 2 })
	if sess.State() != StateConnected {
		t.Fatalf("状态 = %s, 期望 connected（心跳失败不得让会话提前失败）", sess.State())
	}
	if err := sess.Err(); err != nil {
		t.Fatalf("会话因心跳失败终止: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := sess.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: testBattleID}); err != nil {
		t.Fatalf("心跳失败后业务帧不可用: %v", err)
	}
}

// TestHeartbeatToleratesMissingReply ③b：回执未达（探针是 Tell，帧引擎的空信封也可能丢）
// 只计失败、不终止会话，且下一周期照发（不因一次等待超时停摆）。
func TestHeartbeatToleratesMissingReply(t *testing.T) {
	fc := newFakeConn()
	fc.silent = true
	sess := newHeartbeatSession(t, fc, WithHeartbeat(20*time.Millisecond))

	waitFor(t, 3*time.Second, "记到两次失败", func() bool { return sess.HeartbeatFailures() >= 2 })
	waitFor(t, 3*time.Second, "失败后继续发", func() bool { return fc.count(stubOpPing) >= 4 })
	if n := sess.HeartbeatSent(); n != 0 {
		t.Fatalf("回执未达却记了成功 %d 次", n)
	}
	if sess.State() != StateConnected || sess.Err() != nil {
		t.Fatalf("回执未达不应终止会话：state=%s err=%v", sess.State(), sess.Err())
	}
}

// TestHeartbeatReportsDefiniteReject ⑤：服务端「op 未注册/被拒」这类明确失败按可判定错误
// 上报且停止探测（不静默重试），会话本身不受影响。
func TestHeartbeatReportsDefiniteReject(t *testing.T) {
	fc := newFakeConn()
	fc.reason = "TRANSPORT_NOT_FOUND"
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))

	waitFor(t, 2*time.Second, "记录可判定错误", func() bool { return sess.HeartbeatErr() != nil })
	var be *client.BusinessError
	if err := sess.HeartbeatErr(); !errors.As(err, &be) {
		t.Fatalf("探针错误 = %v, 期望 *client.BusinessError", err)
	} else if be.Reason != "TRANSPORT_NOT_FOUND" {
		t.Fatalf("探针错误 reason = %q, 期望 TRANSPORT_NOT_FOUND", be.Reason)
	}
	sent := fc.count(stubOpPing)
	time.Sleep(150 * time.Millisecond) // 15 个周期：明确拒绝后不得再重试
	if got := fc.count(stubOpPing); got != sent {
		t.Fatalf("被明确拒绝后仍在重试：%d → %d", sent, got)
	}
	if sess.State() != StateConnected || sess.Err() != nil {
		t.Fatalf("心跳被拒不应终止会话：state=%s err=%v", sess.State(), sess.Err())
	}
}

// TestHeartbeatOverWSStubWithInput ⑥（真帧协议桩）：探针经真实连接发出、逐帧带票槽，
// 且与业务帧互不干扰（入局/输入/补帧各成功一次）。
func TestHeartbeatOverWSStubWithInput(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)
	sess, err := Open(context.Background(), planFor(TransportWS, srv.addr, testTicket),
		WithHeartbeat(20*time.Millisecond))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer func() { _ = sess.Close() }()

	joinOnce(t, sess, "b-1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = sess.SendFrameInput(ctx, &battlev1.FrameInputReq{
		BattleId: "b-1", Input: &locksteppb.LockstepInput{FrameId: 1, Payload: []byte{1}},
	})
	if err != nil {
		t.Fatalf("SendFrameInput 失败: %v", err)
	}
	if _, err := sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: "b-1", LastSeenFrame: 0}); err != nil {
		t.Fatalf("SyncFrames 失败: %v", err)
	}

	waitFor(t, 3*time.Second, "服务端收到多次探针", func() bool { return len(stub.pingBattleIDs()) >= 3 })
	_, slots, flags, ops, problems, _, _ := stub.snapshot()
	if len(problems) != 0 {
		t.Fatalf("服务端视角违规: %v", problems)
	}
	assertSlotsCarryTicket(t, slots, flags)
	for _, op := range []string{stubOpJoin, stubOpInput, stubOpSync} {
		if n := countOp(ops, op); n != 1 {
			t.Fatalf("%s 次数 = %d, 期望 1（探针不得干扰业务帧）: %v", op, n, ops)
		}
	}
	for _, id := range stub.pingBattleIDs() {
		if id != "b-1" {
			t.Fatalf("探针 battle_id = %q, 期望 b-1", id)
		}
	}
	if sess.State() != StateConnected {
		t.Fatalf("状态 = %s, 期望 connected", sess.State())
	}
}
