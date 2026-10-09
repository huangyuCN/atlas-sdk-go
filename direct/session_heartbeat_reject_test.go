// 心跳被业务/协议拒绝的**分类处置**单测（与 TS/C# 同一契约，钉死口径见阶段 3 评审 P1-3②）：
//
//   - 终态类（BATTLE_ENDED / BATTLE_NOT_FOUND / BATTLE_FULL / FRAME_TARGET_MISMATCH）→ 入终态、
//     停探针、上报（不是只停探针：会话对上层立即失效，不再发送也不重连）；其中 BATTLE_ENDED 是
//     「正常结束」（ended，有结算可展示），其余三种是「终态失败」（failed，无结算可展示）；
//   - 票类（BATTLE_TICKET_EXPIRED / BATTLE_TICKET_INVALID）→ **不**终态：上报「需重新取票」
//     信号（errors.Is 哨兵）+ 计数，继续探测（票要重取，链路未必坏）；
//   - 其它业务拒绝 → 计数 + 经心跳出口暴露，继续探测（不重连）；
//   - 协议非法（回执版本/包络非法）→ 计数 + 上报，探针循环**不退出**：本代连接由读循环拆掉，
//     换代后自动恢复探测（Go 探针是会话级 goroutine，停了就永久失效 → 静默被判掉线）。
//
// 「日志只在状态首次变化时打一条」在 Go 侧的等价口径：首个拒绝原因只记一次（HeartbeatErr），
// 后续仅计数（Stats().HeartbeatRejects）——本包不内置日志，不做每拍输出。

package direct

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// TestHeartbeatTerminalRejectEntersFailed ②：探针被 BATTLE_NOT_FOUND 拒绝 → 入终态失败（而非仅
// 停探针；无结算可展示，故状态是 failed 而不是 ended）、停探针、上报；终态后零写入。
func TestHeartbeatTerminalRejectEntersFailed(t *testing.T) {
	fc := newFakeConn()
	fc.sticky = &fakeStatus{code: 404, reason: "BATTLE_NOT_FOUND", class: frame.ClassBusiness}
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))

	waitFor(t, 2*time.Second, "探针被拒后进入终态", sess.Terminal)
	assertFailedState(t, sess, ErrBattleNotFound, "BATTLE_NOT_FOUND", 404)
	ops, _, _ := fc.snapshot()
	time.Sleep(150 * time.Millisecond) // 15 个周期：终态后一次都不该再写线
	assertNoWireWrites(t, fc, len(ops))
}

// TestHeartbeatTicketRejectKeepsProbing ②b：探针被票类拒绝（BATTLE_TICKET_EXPIRED）→ 不终态、
// 上报可判定的「需重新取票」信号，且**继续探测**（票要重取，链路未必坏）。
func TestHeartbeatTicketRejectKeepsProbing(t *testing.T) {
	fc := newFakeConn()
	fc.sticky = &fakeStatus{code: 401, reason: "BATTLE_TICKET_EXPIRED", class: frame.ClassBusiness}
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))

	waitFor(t, 2*time.Second, "心跳出口暴露票类拒绝", func() bool { return sess.HeartbeatErr() != nil })
	if err := sess.HeartbeatErr(); !errors.Is(err, ErrTicketExpired) {
		t.Fatalf("探针错误 = %v, 期望 errors.Is(err, ErrTicketExpired)", err)
	}
	if sess.Terminal() {
		t.Fatal("票类拒绝不得进入终态（票要重取，对局未必结束）")
	}
	before := fc.count(stubOpPing)
	waitFor(t, 2*time.Second, "票类拒绝后继续探测", func() bool { return fc.count(stubOpPing) >= before+2 })
	if sess.State() != StateConnected || sess.Err() != nil {
		t.Fatalf("票类拒绝不应终止会话：state=%s err=%v", sess.State(), sess.Err())
	}
}

// TestHeartbeatReportsDefiniteReject ②c：其它业务拒绝（op 未注册等）→ 计数 + 经心跳出口暴露，
// **继续探测**（不重连、不终态）：链路还在，下一拍照发。
func TestHeartbeatReportsDefiniteReject(t *testing.T) {
	fc := newFakeConn()
	fc.sticky = &fakeStatus{code: 400, reason: "TRANSPORT_NOT_FOUND", class: frame.ClassRuntime}
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))

	waitFor(t, 2*time.Second, "记录可判定错误", func() bool { return sess.HeartbeatErr() != nil })
	var be *client.BusinessError
	if err := sess.HeartbeatErr(); !errors.As(err, &be) {
		t.Fatalf("探针错误 = %v, 期望 *client.BusinessError", err)
	} else if be.Reason != "TRANSPORT_NOT_FOUND" {
		t.Fatalf("探针错误 reason = %q, 期望 TRANSPORT_NOT_FOUND", be.Reason)
	}
	if sess.Terminal() {
		t.Fatal("非终态业务拒绝不得进入终态")
	}
	before := fc.count(stubOpPing)
	waitFor(t, 2*time.Second, "非终态拒绝后继续探测", func() bool { return fc.count(stubOpPing) >= before+2 })
	if sess.State() != StateConnected || sess.Err() != nil {
		t.Fatalf("心跳被拒不应终止会话：state=%s err=%v", sess.State(), sess.Err())
	}
}

// TestHeartbeatProtocolRejectKeepsProbingAfterReconnect ②d：回执版本/包络非法（协议级）→ 计数 +
// 上报，**探针循环不退出**：本代连接由读循环拆掉，重连成功后探测自动恢复。
//
// 为什么不能「停探针」（评审裁定）：Go 的探针是**会话级 goroutine**，一旦退出就再也不会重启，
// 于是「一次可恢复的协议抖动」会升级成「保活永久失效 → 静默 >5s 被服务端判掉线（玩家判负）」。
// 故协议非法只影响当拍（并计入 Stats.HeartbeatRejects），换代后照常探测。
func TestHeartbeatProtocolRejectKeepsProbingAfterReconnect(t *testing.T) {
	bad := newFakeConn()
	bad.sticky = &fakeStatus{raw: []byte{0x09}} // 包络 kind=9 非法：DecodeReply 必失败
	sess := newHeartbeatSession(t, bad, WithHeartbeat(10*time.Millisecond), WithAutoReconnect(false))

	waitFor(t, 2*time.Second, "协议非法被上报", func() bool { return sess.HeartbeatErr() != nil })
	if err := sess.HeartbeatErr(); !errors.Is(err, client.ErrProtocol) {
		t.Fatalf("探针错误 = %v, 期望 errors.Is(err, client.ErrProtocol)", err)
	}
	waitFor(t, 2*time.Second, "协议非法计数", func() bool { return sess.Stats().HeartbeatRejects >= 1 })

	// 换代：新连接回执正常 → 探针必须**自动恢复**（保活不得永久失效）。
	good := newFakeConn()
	if !sess.startGeneration(good) {
		t.Fatal("新一代连接登记失败（会话已关闭）")
	}
	sess.setState(StateConnected)
	waitFor(t, 2*time.Second, "换代后恢复探测", func() bool { return good.count(stubOpPing) >= 2 })
	if got := sess.Stats().HeartbeatSent; got == 0 {
		t.Fatal("换代后探针未计入成功（HeartbeatSent 仍为 0）")
	}
	if sess.Terminal() {
		t.Fatal("协议非法不得进入终态")
	}
}

// TestHeartbeatProtocolRejectRecoversAfterReconnect ②e（真 WS 桩 + 真自动重连）：探针回执协议非法
// 只影响当拍（计数可见），本代连接被读循环拆掉后自动重连，**换代后探测自动恢复**——保活不会因
// 一次协议抖动永久失效（否则静默 >5s 会被服务端按 idle=offline/3 判掉线）。
func TestHeartbeatProtocolRejectRecoversAfterReconnect(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)
	stub.setPoisonPing(true) // 服务端 framing 抖动：探针回执协议非法

	sess, err := Open(context.Background(), planFor(TransportWS, srv.addr, testTicket),
		WithHeartbeat(20*time.Millisecond),
		WithReconnectBackoff(20*time.Millisecond, 100*time.Millisecond))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer func() { _ = sess.Close() }()

	waitFor(t, 3*time.Second, "协议非法计数可见", func() bool { return sess.Stats().HeartbeatRejects >= 1 })
	if err := sess.HeartbeatErr(); !errors.Is(err, client.ErrProtocol) {
		t.Fatalf("探针错误 = %v, 期望 errors.Is(err, client.ErrProtocol)", err)
	}
	stub.setPoisonPing(false) // 服务端恢复正常 framing

	before := len(stub.pingBattleIDs())
	waitFor(t, 5*time.Second, "自动重连后探测恢复", func() bool {
		conns, _, _, _, _, _, _ := stub.snapshot()
		return conns >= 2 && len(stub.pingBattleIDs()) > before
	})
	if got := sess.Stats().HeartbeatSent; got == 0 {
		t.Fatal("换代后探针未计入成功（HeartbeatSent 仍为 0）：保活永久失效")
	}
	if sess.Terminal() {
		t.Fatal("协议非法不得进入终态")
	}
}
