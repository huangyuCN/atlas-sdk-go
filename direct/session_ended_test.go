// 对局结束（终态）单测：服务端以 reason BATTLE_ENDED 拒绝任一 op、以及结束通知的重复补投，
// 都必须让直连会话进入**可判定终态**并停发；断言全部落在假传输的写出记录上（零写入 = 停发的
// 直接证据），不依赖真实网络与接入层。

package direct

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
)

// assertNoWireWrites 断言假传输此后没有任何新的写出（终态「停发」的直接证据）。
func assertNoWireWrites(t *testing.T, fc *fakeConn, want int) {
	t.Helper()
	ops, _, _ := fc.snapshot()
	if len(ops) != want {
		t.Fatalf("终态后仍在写线：期望 %d 帧，实际 %d 帧（多出 %v）", want, len(ops), ops[want:])
	}
}

// assertEndedError 断言一次调用被终态明确拒绝（errors.Is 可判定为 ErrBattleEnded）。
func assertEndedError(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s 未被拒（终态下必须返回明确错误）", what)
	}
	if !errors.Is(err, ErrBattleEnded) {
		t.Fatalf("%s 错误 = %v, 期望 errors.Is(err, ErrBattleEnded)", what, err)
	}
}

// assertBusinessEnded 断言一次业务拒绝保留服务端原文（reason BATTLE_ENDED 的 *client.BusinessError）。
func assertBusinessEnded(t *testing.T, err error, what string) {
	t.Helper()
	assertEndedError(t, err, what)
	var be *client.BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("%s 错误 = %v, 期望保留 *client.BusinessError", what, err)
	}
	if be.Reason != reasonBattleEnded {
		t.Fatalf("%s 业务 reason = %q, 期望 %q", what, be.Reason, reasonBattleEnded)
	}
}

// TestEndedStopsWireOnBusinessReject ①：业务帧收到 BATTLE_ENDED 即进入终态——终态可判定
// （State/Ended/EndCause），且心跳与业务帧一律停发（假传输断言终态后零写入）。
func TestEndedStopsWireOnBusinessReject(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	joinOnce(t, sess, testBattleID)
	fc.replyStatusNext(fakeStatus{code: 409, reason: reasonBattleEnded, class: frame.ClassBusiness})
	assertBusinessEnded(t, sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}), stubOpInput)

	if cause := sess.EndCause(); cause == nil {
		t.Fatal("终态未记录触发原因")
	}
	// BATTLE_ENDED 是「正常结束」：ended（有结算可展示），不是终态失败。
	assertEndedState(t, sess, ErrBattleEnded, reasonBattleEnded, codeBattleEnded)
	ops, _, _ := fc.snapshot()
	time.Sleep(200 * time.Millisecond) // 20 个心跳周期：终态后必须一次都不再写线
	assertNoWireWrites(t, fc, len(ops))
}

// TestEndedStopsWireOnHeartbeatReject ①b：保活探针（Ping）被 BATTLE_ENDED 拒绝同样进入终态并
// 停发——「任一 op 被拒都停」不因 op 不同而分叉（三个 op 同一收口）。
func TestEndedStopsWireOnHeartbeatReject(t *testing.T) {
	fc := newFakeConn()
	fc.sticky = &fakeStatus{code: 409, reason: reasonBattleEnded, class: frame.ClassBusiness}
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))

	waitFor(t, 2*time.Second, "探针被拒后进入终态", sess.Ended)
	ops, _, _ := fc.snapshot()
	if countOp(ops, stubOpPing) == 0 {
		t.Fatal("终态前未发出任何探针（用例前提不成立）")
	}
	time.Sleep(200 * time.Millisecond)
	assertNoWireWrites(t, fc, len(ops))
	var be *client.BusinessError
	if err := sess.EndCause(); !errors.As(err, &be) || be.Reason != reasonBattleEnded {
		t.Fatalf("终态原因 = %v, 期望 *client.BusinessError(reason=%s)", err, reasonBattleEnded)
	}
}

// TestEndedRejectsFurtherSends ③：终态下一切发送与重连都返回明确错误（errors.Is 可判定）
// 且不写线——调用方据此区分「对局已结束」与「网络故障可重试」。
func TestEndedRejectsFurtherSends(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fc.replyNext(reasonBattleEnded)
	assertBusinessEnded(t, sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}), stubOpInput)
	ops, _, _ := fc.snapshot()

	calls := []struct {
		what string
		err  error
	}{
		{"SendFrameInput", sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID})},
		{"SyncFrames", errOfSync(sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: testBattleID}))},
		{"JoinBattle", errOfJoin(sess.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: testBattleID}))},
		{"Reconnect", sess.Reconnect(ctx)},
	}
	for _, c := range calls {
		assertEndedError(t, c.err, c.what)
	}
	assertNoWireWrites(t, fc, len(ops))
}

// errOfSync 把 SyncFrames 的两返回值收敛成一个错误（用例里只关心错误）。
func errOfSync(_ *battlev1.SyncFramesReply, err error) error { return err }

// errOfJoin 把 JoinBattle 的两返回值收敛成一个错误（用例里只关心错误）。
func errOfJoin(_ *battlev1.JoinBattleReply, err error) error { return err }

// pushFrameNotify 以 Notify 帧推一条帧广播（终态窗口内「仍可读」的探针）。
func pushFrameNotify(t *testing.T, fc *fakeConn, frameID uint64) {
	t.Helper()
	payload := []byte(fmt.Sprintf(`{"battleId":%q,"frame":{"frameId":%d,"inputs":[]}}`, testBattleID, frameID))
	body, err := frame.BuildRequestBody(stubOpFrame, payload)
	if err != nil {
		t.Fatalf("帧广播 body 封装失败: %v", err)
	}
	if err := fc.push(frame.Header{Type: frame.MsgTypeNotify, Seq: 2}, body); err != nil {
		t.Fatalf("帧广播推入失败: %v", err)
	}
}

// endNotifyPayload 生成一条结束通知的 protojson 载荷（battle_id 固定为本会话对局）。
func endNotifyPayload(t *testing.T, winner string) []byte {
	t.Helper()
	payload, err := protojson.Marshal(&battlev1.BattleEndNotify{BattleId: testBattleID, WinnerPlayerId: winner})
	if err != nil {
		t.Fatalf("结束通知载荷序列化失败: %v", err)
	}
	return payload
}

// pushEndNotifyPayload 以 Notify 帧把结束通知载荷推给会话（服务端补投 = 同一载荷多次投递）。
func pushEndNotifyPayload(t *testing.T, fc *fakeConn, payload []byte) {
	t.Helper()
	body, err := frame.BuildRequestBody(battlev1opclient.BattleServicePushOps.BattleEndNotify, payload)
	if err != nil {
		t.Fatalf("结束通知 body 封装失败: %v", err)
	}
	if err := fc.push(frame.Header{Type: frame.MsgTypeNotify, Seq: 1}, body); err != nil {
		t.Fatalf("结束通知推入失败: %v", err)
	}
}

// watchEnd 登记结束通知回调并返回「触发次数 + 最近一次胜者」的观测闭包。
func watchEnd(sess *Session) (count func() int64, winner func() string) {
	var calls atomic.Int64
	var last atomic.Value
	sess.OnBattleEnd(func(n *battlev1.BattleEndNotify) {
		calls.Add(1)
		last.Store(n.GetWinnerPlayerId())
	})
	return calls.Load, func() string {
		if v, ok := last.Load().(string); ok {
			return v
		}
		return ""
	}
}

// TestEndNotifyFiresCallbackOnce ②：同一局结束通知重复到达（服务端有界补投，载荷逐字一致）
// → 回调只触发一次；补投只计数、不重放，会话同时进入终态。
func TestEndNotifyFiresCallbackOnce(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))
	count, winner := watchEnd(sess)

	payload := endNotifyPayload(t, "p-1")
	for i := 0; i < 3; i++ {
		pushEndNotifyPayload(t, fc, payload) // 首投 + 2 次补投，逐字一致
	}
	waitFor(t, 2*time.Second, "结束通知回调", func() bool { return count() >= 1 })
	time.Sleep(200 * time.Millisecond) // 留出「重复回调」暴露窗口
	if got := count(); got != 1 {
		t.Fatalf("结束回调触发 %d 次, 期望 1 次（补投必须幂等）", got)
	}
	if got := winner(); got != "p-1" {
		t.Fatalf("回调胜者 = %q, 期望 p-1", got)
	}
	if st := sess.EndStats(); !st.First || st.Replays != 2 || st.Mismatches != 0 {
		t.Fatalf("结束通知统计 = %+v, 期望 首投=true 补投=2 不一致=0", st)
	}
	if !sess.Ended() || sess.State() != StateEnded {
		t.Fatalf("收到结束通知后 ended=%v 状态=%s, 期望 true/ended", sess.Ended(), sess.State())
	}
	if n := sess.EndNotify(); n == nil || n.GetWinnerPlayerId() != "p-1" {
		t.Fatalf("EndNotify = %v, 期望首条通知（胜者 p-1）", n)
	}
}

// TestEndNotifyMismatchKeepsFirst ②b：补投载荷与首投不逐字一致（异常）→ 首次为准：不覆盖首条、
// 不重放回调，只计入不一致计数供排障。
func TestEndNotifyMismatchKeepsFirst(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))
	count, winner := watchEnd(sess)

	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-1"))
	waitFor(t, 2*time.Second, "结束通知回调", func() bool { return count() >= 1 })
	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-2")) // 异常：与首投不一致

	waitFor(t, 2*time.Second, "不一致计数", func() bool { return sess.EndStats().Mismatches == 1 })
	time.Sleep(100 * time.Millisecond)
	if got := count(); got != 1 {
		t.Fatalf("载荷不一致时回调触发 %d 次, 期望仍为 1 次", got)
	}
	if got := winner(); got != "p-1" {
		t.Fatalf("回调胜者 = %q, 期望首投的 p-1（首次为准）", got)
	}
	if n := sess.EndNotify(); n.GetWinnerPlayerId() != "p-1" {
		t.Fatalf("EndNotify 胜者 = %q, 期望首投的 p-1（补投不覆盖）", n.GetWinnerPlayerId())
	}
}

// TestEndNotifyAfterRejectStillFires ②c：先被 BATTLE_ENDED 拒绝（终态已置），随后补投的结束
// 通知仍回调一次——「终态标志」与「通知已投递」是两件事：前者幂等，后者恰好一次。
func TestEndNotifyAfterRejectStillFires(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))
	count, winner := watchEnd(sess)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fc.replyNext(reasonBattleEnded)
	assertBusinessEnded(t, sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}), stubOpInput)
	if !sess.Ended() {
		t.Fatal("被 BATTLE_ENDED 拒绝后未进入终态")
	}
	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-9"))
	waitFor(t, 2*time.Second, "补投的结束通知回调", func() bool { return count() >= 1 })
	time.Sleep(100 * time.Millisecond)
	if got := count(); got != 1 {
		t.Fatalf("结束回调触发 %d 次, 期望 1 次", got)
	}
	if got := winner(); got != "p-9" {
		t.Fatalf("回调胜者 = %q, 期望 p-9", got)
	}
}

// TestEndLingerKeepsReadingThenReleases ③：终态收尾窗口内**仍可读**（尾帧/补投不丢），窗口到期由
// 客户端释放连接——数据报面（KCP/UDP）没有关闭握手，对端关闭不产生 EOF，只能客户端兜底；
// 到期后终态不回退、无需 Close 也不留 goroutine。
func TestEndLingerKeepsReadingThenReleases(t *testing.T) {
	base := runtime.NumGoroutine()
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithEndLinger(300*time.Millisecond))
	frames := atomic.Int64{}
	sess.OnFrame(func(*battlev1.FrameBroadcast) { frames.Add(1) })

	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-1"))
	waitFor(t, 2*time.Second, "进入终态", sess.Ended)
	if fc.isClosed() {
		t.Fatal("进入终态瞬间就关了连接（窗口内应保留可读）")
	}
	pushFrameNotify(t, fc, 7) // 终态之后仍应被读到并分发
	waitFor(t, 2*time.Second, "终态窗口内的帧广播", func() bool { return frames.Load() == 1 })
	if got := sess.LastSeenFrame(); got != 7 {
		t.Fatalf("补帧基准 = %d, 期望 7（终态窗口内仍推进）", got)
	}
	waitFor(t, 3*time.Second, "收尾窗口到期关连接", fc.isClosed)
	if !sess.Ended() || sess.State() != StateEnded {
		t.Fatalf("收尾后 ended=%v 状态=%s, 期望 true/ended（终态不回退）", sess.Ended(), sess.State())
	}
	waitFor(t, 3*time.Second, "goroutine 回落到基线", func() bool { return runtime.NumGoroutine() <= base+2 })
}

// TestEndLingerZeroClosesImmediately ③b：窗口配 0 = 不等窗口，进入终态即关连接（不需要收尾读
// 窗口的场景），终态照旧可判定。
func TestEndLingerZeroClosesImmediately(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithEndLinger(0))
	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-1"))

	waitFor(t, 2*time.Second, "进入终态", sess.Ended)
	waitFor(t, 2*time.Second, "进入终态即关连接", fc.isClosed)
	if got := sess.EndLinger(); got != 0 {
		t.Fatalf("收尾窗口 = %s, 期望 0", got)
	}
}

// TestEndLingerDefaultBounds ③c：缺省窗口的取值依据——覆盖结算后重投的约 1 个 RTT 并留抖动余量
// （见 WithEndLinger，与 TS/C# SDK 同口径），又远小于服务端结束留档 TTL（票据 + 掉线窗口 135s）。
func TestEndLingerDefaultBounds(t *testing.T) {
	sess := newHeartbeatSession(t, newFakeConn(), WithHeartbeat(0))
	if got := sess.EndLinger(); got != defaultEndLinger {
		t.Fatalf("缺省收尾窗口 = %s, 期望 %s", got, defaultEndLinger)
	}
	if defaultEndLinger < time.Second || defaultEndLinger > 5*time.Second {
		t.Fatalf("缺省收尾窗口 %s 超出依据区间 [1s, 5s]", defaultEndLinger)
	}
}

// TestEndedCloseNoLeak ④：终态会话 Close 幂等、终态不回退，且 goroutine 回落到基线（无泄漏）。
// 全仓 make test 与本次验收都以 -race 运行，计数断言在竞态检测下同样成立。
func TestEndedCloseNoLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		fc := newFakeConn()
		sess := newHeartbeatSession(t, fc, WithHeartbeat(5*time.Millisecond), WithEndLinger(30*time.Millisecond))
		pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-1"))
		waitFor(t, 2*time.Second, "进入终态", sess.Ended)
		if err := sess.Close(); err != nil {
			t.Fatalf("第 %d 轮 Close 失败: %v", i, err)
		}
		if err := sess.Close(); err != nil {
			t.Fatalf("第 %d 轮重复 Close 失败: %v", i, err)
		}
		if !sess.Ended() || sess.State() != StateEnded {
			t.Fatalf("第 %d 轮 Close 后 ended=%v 状态=%s, 期望 true/ended（终态不可回退）",
				i, sess.Ended(), sess.State())
		}
	}
	waitFor(t, 5*time.Second, "goroutine 回落到基线", func() bool { return runtime.NumGoroutine() <= base+2 })
}

// TestWriteRequestRefusesAfterEnded ①d：写线的唯一出口在**持写锁后**再查一次终态——终态置位后
// 任何一帧都不上线（进锁前已通过检查的那次写同样被拦下），这是「停发」的最后一层保证。
func TestWriteRequestRefusesAfterEnded(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))
	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-1"))
	waitFor(t, 2*time.Second, "进入终态", sess.Ended)

	g := sess.currentGen()
	if g == nil {
		t.Fatal("终态下无当前代连接（用例前提不成立）")
	}
	ops, _, _ := fc.snapshot()
	assertEndedError(t, sess.writeRequest(g, 99, stubOpInput, []byte(`{"battleId":"b-heartbeat"}`)), "终态写线")
	assertNoWireWrites(t, fc, len(ops))
}

// newSessionAt 组装一个已连接会话，地址指向 addr（当前连接仍由假传输顶替）：用于观察终态下
// 是否还会**拨号重连**——真拨号会打到达 addr 的监听器上，accept 计数即直接证据。
func newSessionAt(t *testing.T, fc *fakeConn, addr string, opts ...Option) *Session {
	t.Helper()
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	plan := Plan{
		BattleID: testBattleID, Ticket: testTicket,
		Endpoints: map[Transport]string{TransportWS: addr},
	}
	s := newSession(plan, TransportWS, addr, o)
	if !s.startGeneration(fc) {
		t.Fatal("假传输登记失败：会话已关闭")
	}
	s.setState(StateConnected)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestEndedDoesNotReconnect ①c：终态收尾（收尾窗口关连接）**不触发重连**——对局已结束，重连只会
// 被稳定拒绝，还会让服务端反复懒激活并结算已结束的对局实例（阶段 3 验收实测过每轮 9–13 次起停）。
func TestEndedDoesNotReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer func() { _ = ln.Close() }()
	var accepts atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			_ = c.Close()
		}
	}()

	fc := newFakeConn()
	sess := newSessionAt(t, fc, ln.Addr().String(), WithHeartbeat(0), WithEndLinger(50*time.Millisecond))
	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-1"))
	waitFor(t, 2*time.Second, "进入终态", sess.Ended)
	waitFor(t, 3*time.Second, "收尾窗口到期关连接", fc.isClosed)
	time.Sleep(time.Second) // 覆盖自动重连的首次退避（缺省 500ms 起步）：有重连这里必然拨号成功
	if n := accepts.Load(); n != 0 {
		t.Fatalf("终态后发生了 %d 次重连拨号, 期望 0 次（对局已结束，不许重连）", n)
	}
	ops, _, _ := fc.snapshot()
	assertNoWireWrites(t, fc, len(ops))
}
