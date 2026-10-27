package direct

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// testTicket 是测试用票密文（内容无关，服务端按字节比对）。
var testTicket = []byte{0x11, 0x22, 0x33, 0x44, 0x55}

// waitFor 轮询等待条件成立（异步断言：推送、重连、状态迁移）。
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", desc)
}

// openTest 建立测试会话，并补上「探针 + 重放 + 帧号钩子」三件套
// （生产上这三样都由游戏侧用协议包提供，见 testops_test.go）。
func openTest(t *testing.T, plan Plan, opts ...Option) *Session {
	t.Helper()
	base := []Option{withTestProbe(plan.BattleID), withTestReplay(plan.BattleID), WithFrameNumberFunc(jsonFrameNumber)}
	sess, err := Open(context.Background(), plan, append(base, opts...)...)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess
}

// TestOpenWSQueryAndFrameSlot 验证 WS 直连的成局握手与逐帧带票：
// 升级 query 带票（服务端逐字节比对）、每帧置位 FlagSession 且槽 = base64url 票密文。
func TestOpenWSQueryAndFrameSlot(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)

	sess := openTest(t, planFor(TransportWS, srv.addr, testTicket))
	if sess.Transport() != TransportWS {
		t.Fatalf("实际传输面 = %s, 期望 ws", sess.Transport())
	}

	var reply testJoinReply
	if err := json.Unmarshal(joinOnce(t, sess, "b-1"), &reply); err != nil || reply.Meta.SessionID != "b-1" {
		t.Fatalf("入局回执不符: %+v（err=%v）", reply, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sendFrameInput(ctx, sess, "b-1"); err != nil {
		t.Fatalf("发帧失败: %v", err)
	}
	if err := syncFrames(ctx, sess); err != nil {
		t.Fatalf("补帧失败: %v", err)
	}

	assertWireOK(t, stub, 1, []string{stubOpJoin, stubOpInput, stubOpSync})
}

// TestOpenWSRejectedByEdge 验证被接入层拒绝（无回执、连接被断）是明确的可判定错误，
// 且 Open 不重试；与网络断开（可重试）区分。
func TestOpenWSRejectedByEdge(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)
	srv.reject.Store(true)

	_, err := Open(context.Background(), planFor(TransportWS, srv.addr, testTicket),
		WithInvokeTimeout(500*time.Millisecond))
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("Open 错误 = %v, 期望 ErrRejected", err)
	}
	if srv.conns.Load() != 0 {
		t.Fatalf("被拒时不应完成升级，conns=%d", srv.conns.Load())
	}
}

// TestOpenWSJoinTicketExpired 验证 battle 侧 BATTLE_TICKET_EXPIRED 映射为可判定错误，供上层重新取票。
func TestOpenWSJoinTicketExpired(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)
	stub.setJoinError(reasonTicketExpired)

	sess := openTest(t, planFor(TransportWS, srv.addr, testTicket))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := invokeJoin(ctx, sess, "b-1"); !errors.Is(err, ErrTicketExpired) {
		t.Fatalf("入局错误 = %v, 期望 ErrTicketExpired", err)
	}
}

// TestWSFrameBroadcastPush 验证帧广播推送经回调下发，并推进补帧基准（重连补帧用它）。
func TestWSFrameBroadcastPush(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)

	sess := openTest(t, planFor(TransportWS, srv.addr, testTicket))

	got := make(chan uint64, 4)
	sess.OnPush(stubOpFrame, func(payload []byte, _ uint8) {
		if id, ok := jsonFrameNumber(payload); ok {
			got <- id
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := syncFrames(ctx, sess); err != nil {
		t.Fatalf("补帧失败: %v", err)
	}
	waitPushedFrame(t, got, 1)
	if last := sess.LastSeenFrame(); last != 1 {
		t.Fatalf("补帧基准 = %d, 期望 1", last)
	}
}

// TestReconnectReplaysHelloJoinAndSync 验证显式重连：重新 hello（新连接重新带票）、
// 按 WithReplay 重放入局与补帧（基准 = 已见帧号），逐帧仍带票。
func TestReconnectReplaysHelloJoinAndSync(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)

	sess := openTest(t, planFor(TransportWS, srv.addr, testTicket), WithAutoReconnect(false))

	seen := make(chan uint64, 4)
	sess.OnPush(stubOpFrame, func(payload []byte, _ uint8) {
		if id, ok := jsonFrameNumber(payload); ok {
			seen <- id
		}
	})
	joinOnce(t, sess, "b-1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := syncFrames(ctx, sess); err != nil {
		t.Fatalf("补帧失败: %v", err)
	}
	waitPushedFrame(t, seen, 1)

	stub.setCloseAfter(1, 2) // 触发一次拆流：本连接第 2 个请求回执后服务端断开
	if err := sess.Reconnect(ctx); err != nil {
		t.Fatalf("Reconnect 失败: %v", err)
	}
	conns, slots, flags, ops, problems, syncs, _ := stub.snapshot()
	if len(problems) != 0 {
		t.Fatalf("重连后服务端视角违规: %v", problems)
	}
	if conns != 2 {
		t.Fatalf("连接数 = %d, 期望 2（重连即重新 hello）", conns)
	}
	assertSlotsCarryTicket(t, slots, flags)
	if countOp(ops, stubOpJoin) != 2 {
		t.Fatalf("入局次数 = %d, 期望 2（重连重放）: %v", countOp(ops, stubOpJoin), ops)
	}
	if countOp(ops, stubOpSync) != 2 {
		t.Fatalf("补帧次数 = %d, 期望 2（重连补帧）: %v", countOp(ops, stubOpSync), ops)
	}
	if len(syncs) != 2 || syncs[1] != 1 {
		t.Fatalf("重连补帧基准 = %v, 期望第二次 last_seen_frame=1", syncs)
	}
}

// TestAutoReconnectReplaysOnDrop 验证断线自动重连（默认开启）同样按 WithReplay 重放入局与补帧。
func TestAutoReconnectReplaysOnDrop(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)
	stub.setCloseAfter(1, 2) // 入局 + 补帧之后断开

	sess := openTest(t, planFor(TransportWS, srv.addr, testTicket),
		WithReconnectBackoff(20*time.Millisecond, 100*time.Millisecond))

	joinOnce(t, sess, "b-1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := syncFrames(ctx, sess); err != nil {
		t.Fatalf("补帧失败: %v", err)
	}
	waitFor(t, 3*time.Second, "自动重连完成", func() bool {
		conns, _, _, ops, _, _, _ := stub.snapshot()
		return conns == 2 && countOp(ops, stubOpJoin) == 2
	})
	waitFor(t, 3*time.Second, "新连接状态回到已连接", func() bool { return sess.State() == StateConnected })
}

// TestRejectedDuringReconnectNotRetried 验证重连遇到「接入层拒绝」即终止（不重试），
// 与网络断开（继续退避重试）区分。
func TestRejectedDuringReconnectNotRetried(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)

	sess := openTest(t, planFor(TransportWS, srv.addr, testTicket),
		WithReconnectBackoff(20*time.Millisecond, 50*time.Millisecond))
	joinOnce(t, sess, "b-1")

	srv.reject.Store(true)
	stub.setCloseAfter(1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = syncFrames(ctx, sess)

	waitFor(t, 3*time.Second, "被拒后进入断开态", func() bool { return sess.State() == StateDisconnected })
	if err := sess.Err(); !errors.Is(err, ErrRejected) {
		t.Fatalf("终止原因 = %v, 期望 ErrRejected", err)
	}
	// 不重试：再等一段时间仍是断开态，且没有新连接完成升级。
	time.Sleep(200 * time.Millisecond)
	if n := srv.conns.Load(); n != 1 {
		t.Fatalf("被拒后不应再建连，升级成功数 = %d", n)
	}
}

// TestOpenRejectsMissingFace 验证计划里没有所要求的面即报错（不猜端口、不静默换面）。
func TestOpenRejectsMissingFace(t *testing.T) {
	plan := planFor(TransportWS, "127.0.0.1:1", testTicket)
	if _, err := Open(context.Background(), plan, WithTransport(TransportKCP)); !errors.Is(err, ErrTransportNotFound) {
		t.Fatalf("缺面错误 = %v, 期望 ErrTransportNotFound", err)
	}
}

// TestOpenDefaultPriorityPicksWS 验证未指定面时按 ws → kcp → udp 取第一个存在的面。
func TestOpenDefaultPriorityPicksWS(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)
	plan := Plan{
		BattleID: "b-1", Ticket: testTicket,
		Endpoints: map[Transport]string{TransportWS: srv.addr, TransportUDP: "127.0.0.1:1"},
	}
	sess := openTest(t, plan)
	if sess.Transport() != TransportWS {
		t.Fatalf("选中面 = %s, 期望 ws", sess.Transport())
	}
	joinOnce(t, sess, "b-1")
}

// countOp 统计服务端收到的指定 op 次数（重连重放断言用）。
func countOp(ops []string, want string) int {
	n := 0
	for _, op := range ops {
		if op == want {
			n++
		}
	}
	return n
}

// TestExpiredTicketFallback 验证票过期回退闭环：过期票入局被拒 → 返回可判定错误（不重试）→
// 上层回业务链路取新票后重建会话即成功（直连侧只负责把「需要新票」这件事说清楚）。
func TestExpiredTicketFallback(t *testing.T) {
	expiredTicket := []byte{0xee, 0x01}
	expiredStub := newBattleStub(expiredTicket)
	expiredStub.setJoinError(reasonTicketExpired)
	expiredSrv := startWSStub(t, expiredTicket, expiredStub)

	old := openTest(t, planFor(TransportWS, expiredSrv.addr, expiredTicket))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := invokeJoin(ctx, old, "b-1"); !errors.Is(err, ErrTicketExpired) {
		t.Fatalf("过期票入局错误 = %v, 期望 ErrTicketExpired", err)
	}

	// 回业务链路重新取票（新票）后重建会话：直接成功。
	freshStub := newBattleStub(testTicket)
	freshSrv := startWSStub(t, testTicket, freshStub)
	fresh := openTest(t, planFor(TransportWS, freshSrv.addr, testTicket))
	joinOnce(t, fresh, "b-1")
	assertWireOK(t, freshStub, 1, []string{stubOpJoin})
}
