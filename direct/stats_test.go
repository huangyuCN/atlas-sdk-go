// 观测快照（Stats）单测：重连/握手/心跳的计数与最近错误，覆盖「只读快照」的三条口径——
// 计数随事件增长、最近错误跟着最新一次失败走、首个被拒原因不被后续失败覆盖。
//
// 用例全部落在假传输与真实 WS 桩上（不引新依赖、不 sleep 猜时序：用 waitFor 等条件成立）。

package direct

import (
	"context"
	"errors"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// TestStatsHeartbeatCounters ④：心跳三类计数与最近错误；首个被拒原因只记一次
// （HeartbeatErr），后续被拒只增计数与最近错误（Stats().HeartbeatRejects/LastHeartbeatErr）。
func TestStatsHeartbeatCounters(t *testing.T) {
	fc := newFakeConn()
	fc.setSilent(true) // 先制造网络类失败（回执未达）
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))

	waitFor(t, 2*time.Second, "网络类失败计数", func() bool { return sess.Stats().HeartbeatFailures >= 2 })
	if st := sess.Stats(); st.HeartbeatSent != 0 || st.HeartbeatRejects != 0 || st.LastHeartbeatErr == nil {
		t.Fatalf("网络类失败期快照 = %+v, 期望 sent=0 rejects=0 lastErr≠nil", st)
	}

	fc.setSilent(false) // 恢复回执：计数应转向成功
	waitFor(t, 2*time.Second, "成功计数", func() bool { return sess.Stats().HeartbeatSent >= 1 })

	// 首次被拒：TRANSPORT_NOT_FOUND（非终态，会话继续探测）。
	fc.setSticky(&fakeStatus{code: 400, reason: "TRANSPORT_NOT_FOUND", class: frame.ClassRuntime})
	waitFor(t, 2*time.Second, "首次被拒计数", func() bool { return sess.Stats().HeartbeatRejects >= 1 })
	first := sess.HeartbeatErr()
	if !client.IsBusinessError(first, "TRANSPORT_NOT_FOUND") {
		t.Fatalf("首个被拒原因 = %v, 期望 TRANSPORT_NOT_FOUND", first)
	}

	// 再次被拒（换一个 reason）：首个原因不被覆盖，计数与最近错误更新。
	fc.setSticky(&fakeStatus{code: 404, reason: "PLAYER_NOT_FOUND", class: frame.ClassBusiness})
	waitFor(t, 2*time.Second, "第二次被拒计数", func() bool { return sess.Stats().HeartbeatRejects >= 2 })
	if got := sess.HeartbeatErr(); !client.IsBusinessError(got, "TRANSPORT_NOT_FOUND") {
		t.Fatalf("首个被拒原因被覆盖为 %v, 期望仍是 TRANSPORT_NOT_FOUND（只记首次）", got)
	}
	if last := sess.Stats().LastHeartbeatErr; !client.IsBusinessError(last, "PLAYER_NOT_FOUND") {
		t.Fatalf("最近错误 = %v, 期望 PLAYER_NOT_FOUND（跟最新一次走）", last)
	}
	if sess.Terminal() {
		t.Fatal("非终态业务拒绝不得进入终态")
	}
}

// TestStatsCountsSuccessfulReconnect ④b：成功重连的计数（连接代次、轮次、握手尝试）与
// 「失败计数保持 0」；显式重连同样走重放（JoinBattle/SyncFrames）路径。
func TestStatsCountsSuccessfulReconnect(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)

	sess, err := Open(context.Background(), planFor(TransportWS, srv.addr, testTicket), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer func() { _ = sess.Close() }()
	joinOnce(t, sess, "b-1")
	if st := sess.Stats(); st.Connects != 1 || st.HandshakeAttempts != 1 || st.HandshakeFailures != 0 {
		t.Fatalf("首连后快照 = %+v, 期望 connects=1 handshakes=1/0", st)
	}

	stub.setCloseAfter(1, 1) // 本连接第 1 个请求回执后服务端断开
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: "b-1"}) // 触发断流
	if err := sess.Reconnect(ctx); err != nil {
		t.Fatalf("Reconnect 失败: %v", err)
	}
	st := sess.Stats()
	if st.Connects != 2 || st.ReconnectRounds != 1 || st.ReconnectFailures != 0 {
		t.Fatalf("重连后快照 = %+v, 期望 connects=2 rounds=1 failures=0", st)
	}
	if st.HandshakeAttempts != 2 || st.HandshakeFailures != 0 || st.LastHandshakeErr != nil {
		t.Fatalf("握手快照 = %+v, 期望 attempts=2 failures=0 lastErr=nil", st)
	}
	if st.LastReconnectErr != nil {
		t.Fatalf("LastReconnectErr = %v, 期望 nil（本轮成功）", st.LastReconnectErr)
	}
}

// TestStatsCountsReconnectFailure ④c：重连遇到不可重试拒绝（接入层拒绝）→ 轮次失败计数 +
// 最近错误（握手失败同样计数），会话终止不再重试。
func TestStatsCountsReconnectFailure(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)

	sess, err := Open(context.Background(), planFor(TransportWS, srv.addr, testTicket),
		WithReconnectBackoff(20*time.Millisecond, 50*time.Millisecond))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer func() { _ = sess.Close() }()
	joinOnce(t, sess, "b-1")

	srv.reject.Store(true) // 之后一律拒绝升级（接入层拒绝语义：无回执直接断开）
	stub.setCloseAfter(1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: "b-1"})

	waitFor(t, 3*time.Second, "重连失败计数", func() bool { return sess.Stats().ReconnectFailures >= 1 })
	st := sess.Stats()
	if !errors.Is(st.LastReconnectErr, ErrRejected) {
		t.Fatalf("LastReconnectErr = %v, 期望 errors.Is(ErrRejected)", st.LastReconnectErr)
	}
	if st.HandshakeFailures == 0 || st.LastHandshakeErr == nil {
		t.Fatalf("握手失败未计数: %+v", st)
	}
	if st.Connects != 1 {
		t.Fatalf("Connects = %d, 期望 1（被拒后不得建立新代次）", st.Connects)
	}
	waitFor(t, 3*time.Second, "被拒后进入断开态", func() bool { return sess.State() == StateDisconnected })
}
