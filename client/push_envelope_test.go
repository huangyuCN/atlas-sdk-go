// 推送信封测试（S0.5 修订 1 · c②）：帧头 version 必须随推送一起送到接缝——ver=1 是
// protojson JSON 字节、ver=2 是 protobuf wire 字节，接缝据此选解码器；版本丢失会让
// ver=2 的实现方按 protojson 解码失败，被挤下线原因静默丢失（ok=true、reason=""）。
package client_test

import (
	"context"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// TestSessionKickedReasonByFrameVersion 断言两种编码的推送都能提取被挤下线原因
// （接缝按 PushEnvelope.Version 选解码器），坏载荷不 panic、不误判原因。
// 未知 version 到不了接缝：帧头版本白名单在 Read 阶段即拒（协议级），故未知版本的
// 不 panic 语义由接缝单测覆盖（TestSessionProtocolKicked）。
func TestSessionKickedReasonByFrameVersion(t *testing.T) {
	pushOp := gatewayv1opclient.SessionPushOps.KickedNotify
	const reason = gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE
	cases := []struct {
		name    string
		version uint8
		body    []byte
		want    string
	}{
		{
			name:    "ver1-protojson",
			version: frame.Version,
			body:    []byte(`{"reason":"KICKED_REASON_LOGGED_IN_ELSEWHERE"}`),
			want:    reason.String(),
		},
		{
			name:    "ver2-protobuf",
			version: frame.Version2,
			body:    mustMarshal(t, &gatewayv1.KickedNotify{Reason: reason}),
			want:    reason.String(),
		},
		{
			name:    "undecodable-body",
			version: frame.Version, // 未解码：坏载荷不 panic，识别为被挤下线但取不到原因
			body:    []byte("{"),
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := startSessionServer(t)
			srv.setReply(gatewayv1opclient.SessionProtocolOps.Login, []byte(`{"playerId":"42","token":"tok-42"}`))
			kicked := make(chan string, 1)
			sess, _ := dialSessionWith(t, srv, gatewayProtocol{}, []client.SessionOption{
				client.WithOnKicked(func(r string) { kicked <- r }),
			})
			if _, err := sess.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
				t.Fatal(err)
			}
			if !srv.sendNotifyAt(pushOp, tc.version, tc.body) {
				t.Fatal("推送发送失败")
			}
			select {
			case got := <-kicked:
				if got != tc.want {
					t.Fatalf("被挤下线原因 = %q, 期望 %q（version=%d）", got, tc.want, tc.version)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("未收到被挤下线回调（version=%d）", tc.version)
			}
			if sess.Token() != "" || sess.PlayerID() != "" {
				t.Fatalf("被挤下线后凭据应清空: token=%q player=%q", sess.Token(), sess.PlayerID())
			}
		})
	}
}
