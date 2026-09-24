// Bind 订阅生命周期测试（S0.5 修订 1 · c③）：重复 Bind 不累积订阅、不同会话各自
// 持有订阅（去重按稳定键而非闭包指针）、Close 后不再收到推送。
package client_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// TestSessionTwoSessionsShareClient 断言两个会话绑同一 Client 时各自都收到推送：
// 订阅去重键是会话自身（稳定键），用闭包指针去重会让后绑定者顶掉先绑定者。
func TestSessionTwoSessionsShareClient(t *testing.T) {
	srv := startSessionServer(t)
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Login, []byte(`{"playerId":"42","token":"tok-42"}`))

	var aCalls, bCalls atomic.Int32
	a := client.NewSession(
		client.WithSessionProtocol(gatewayProtocol{}),
		client.WithSessionHeartbeatInterval(0),
		client.WithOnKicked(func(string) { aCalls.Add(1) }),
	)
	b := client.NewSession(
		client.WithSessionProtocol(gatewayProtocol{}),
		client.WithSessionHeartbeatInterval(0),
		client.WithOnKicked(func(string) { bCalls.Add(1) }),
	)
	cli, err := client.DialUDP(srv.addr(), append([]client.Option{client.WithHeartbeatInterval(0)}, a.ChannelOptions()...)...)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	if err := a.Bind(cli); err != nil {
		t.Fatalf("a.Bind: %v", err)
	}
	if err := b.Bind(cli); err != nil {
		t.Fatalf("b.Bind: %v", err)
	}
	if _, err := a.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
		t.Fatal(err)
	}
	if !srv.sendNotifyAt(gatewayv1opclient.SessionPushOps.KickedNotify, frame.Version,
		[]byte(`{"reason":"KICKED_REASON_LOGGED_IN_ELSEWHERE"}`)) {
		t.Fatal("推送发送失败")
	}
	waitSessionCleared(t, a)
	time.Sleep(100 * time.Millisecond) // 给重复投递留出窗口（订阅若累积即多次回调）
	if got := aCalls.Load(); got != 1 {
		t.Fatalf("会话 A 被挤下线回调 = %d, 期望 1（先绑定者被后绑定者顶掉即 0）", got)
	}
	if got := bCalls.Load(); got != 1 {
		t.Fatalf("会话 B 被挤下线回调 = %d, 期望 1", got)
	}
}

// TestSessionBindTwiceHandlesPushOnce 断言连续 Bind 两次后一次推送只处理一次
// （重复 Bind 先退订旧订阅，且订阅键稳定）。
func TestSessionBindTwiceHandlesPushOnce(t *testing.T) {
	srv := startSessionServer(t)
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Login, []byte(`{"playerId":"42","token":"tok-42"}`))
	var calls atomic.Int32
	sess, cli := dialSessionWith(t, srv, gatewayProtocol{}, []client.SessionOption{
		client.WithOnKicked(func(string) { calls.Add(1) }),
	})
	if _, err := sess.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := sess.Bind(cli); err != nil {
		t.Fatalf("重复 Bind: %v", err)
	}
	if !srv.sendNotify(gatewayv1opclient.SessionPushOps.KickedNotify,
		[]byte(`{"reason":"KICKED_REASON_LOGGED_IN_ELSEWHERE"}`)) {
		t.Fatal("推送发送失败")
	}
	waitSessionCleared(t, sess)
	time.Sleep(100 * time.Millisecond) // 订阅若累积，第二次投递会在此窗口内到达
	if got := calls.Load(); got != 1 {
		t.Fatalf("被挤下线回调 = %d, 期望 1（重复 Bind 不得累积订阅）", got)
	}
}

// TestSessionCloseStopsPushes 断言 Close 退订：Close 后推送不再进入本会话
// （凭据不被清空、原因不记录、回调不触发），且 Close 幂等。
func TestSessionCloseStopsPushes(t *testing.T) {
	srv := startSessionServer(t)
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Login, []byte(`{"playerId":"42","token":"tok-42"}`))
	var calls atomic.Int32
	sess, _ := dialSessionWith(t, srv, gatewayProtocol{}, []client.SessionOption{
		client.WithOnKicked(func(string) { calls.Add(1) }),
	})
	if _, err := sess.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("重复 Close: %v", err)
	}
	if !srv.sendNotify(gatewayv1opclient.SessionPushOps.KickedNotify,
		[]byte(`{"reason":"KICKED_REASON_LOGGED_IN_ELSEWHERE"}`)) {
		t.Fatal("推送发送失败")
	}
	time.Sleep(200 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("Close 后被挤下线回调 = %d, 期望 0", got)
	}
	if sess.KickedReason() != "" {
		t.Fatalf("Close 后不应记录被挤下线原因: %q", sess.KickedReason())
	}
	if sess.Token() != "tok-42" {
		t.Fatalf("Close 不应清空凭据（只解绑订阅）: token=%q", sess.Token())
	}
}

// waitSessionCleared 等待会话凭据被推送清空（超时即失败）。
func waitSessionCleared(t *testing.T, sess *client.Session) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sess.Token() == "" && sess.PlayerID() == "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待凭据清空超时: token=%q player=%q", sess.Token(), sess.PlayerID())
}
