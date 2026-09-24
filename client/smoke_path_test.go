// 冒烟等价路径测试（A.5 本地替身）：本地无真机网关时，用假 gateway 覆盖
// `go run ./examples/smoke -transport tcp -serializer json|protojson|protobuf` 的会话闭环——
// 注册（生成 stub）→ 登录（会话状态机注入 client_version）→ 业务心跳 → 战斗绑定探针
// （业务拒绝）→ 全部 op 命中生成描述符。三编码走同一份生成 DTO 与同一条接缝。
//
// 需在服务器执行的命令（真机 e2e，本地不跑）：
//
//	go run ./examples/smoke -transport tcp -addr 10.10.9.36:9001 -serializer json|protojson|protobuf
package client_test

import (
	"context"
	"errors"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	pbserializer "github.com/huangyuCN/atlas-sdk-go/contrib/protobuf"
	"google.golang.org/protobuf/proto"
)

// smokeEncoding 是一种载荷编码的冒烟用例（protojson 两种命名语义相同，与冒烟一致）。
type smokeEncoding struct {
	name  string
	proto bool // true = ver=2 protobuf 二进制
}

// TestSmokePathThreeEncodings 三编码跑通冒烟等价会话闭环。
func TestSmokePathThreeEncodings(t *testing.T) {
	cases := []smokeEncoding{{name: "json"}, {name: "protojson"}, {name: "protobuf", proto: true}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runSmokePath(t, tc)
		})
	}
}

// runSmokePath 以指定编码跑一次注册 → 登录 → 心跳 → 战斗绑定探针。
func runSmokePath(t *testing.T, tc smokeEncoding) {
	t.Helper()
	srv := startSessionServer(t)
	seedSmokeReplies(t, srv, tc.proto)
	srv.setErrorReply(battlev1opclient.BattleServiceProtocolOps.JoinBattle, 7, 0, "NO_SESSION")

	var opts []client.Option
	if tc.proto {
		opts = append(opts, client.WithSerializer(pbserializer.Serializer{}))
	}
	sess, c := dialSession(t, srv, gatewayProtocol{}, opts...)
	ctx := context.Background()

	// 注册：生成的会话 stub（强类型方法，op 来自生成描述符）。
	rep, err := gatewayv1opclient.NewSession(c).Register(ctx, &gatewayv1.RegisterRequest{
		Account: "smoke-a", Password: "pw-123456",
	})
	if err != nil || rep.GetPlayerId() != "42" {
		t.Fatalf("注册失败: playerId=%q err=%v", rep.GetPlayerId(), err)
	}
	// 登录：会话状态机（SDK 注入 client_version，回执按生成 DTO 解码并保管凭据）。
	if err := loginViaSession(ctx, sess, "42"); err != nil {
		t.Fatalf("登录失败: %v", err)
	}
	if sess.Token() != "tok-42" || sess.PlayerID() != "42" {
		t.Fatalf("登录凭据 = %q/%q", sess.Token(), sess.PlayerID())
	}
	// 业务心跳：op 取自接缝，回执按生成 DTO 解码。
	if _, err := sess.Heartbeat(ctx); err != nil {
		t.Fatalf("业务心跳失败: %v", err)
	}
	// 战斗绑定探针：伪造目标 → 业务拒绝（Reason 可判，说明 payload 编解码正确）。
	_, err = battlev1opclient.NewBattleService(c).JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: "b1"})
	var be *client.BusinessError
	if !errors.As(err, &be) || be.Reason != "NO_SESSION" {
		t.Fatalf("战斗探针应业务拒绝（NO_SESSION），得到 %v", err)
	}
	assertSeenOpsGenerated(t, srv)
}

// loginViaSession 走会话状态机登录（冒烟 login 的测试侧等价实现）。
func loginViaSession(ctx context.Context, sess *client.Session, player string) error {
	reply, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: player, Password: "pw-123456"})
	if err != nil {
		return err
	}
	if _, ok := reply.(*gatewayv1.LoginReply); !ok {
		return errors.New("登录回执未按生成 DTO 解码")
	}
	return nil
}

// seedSmokeReplies 按编码预置各 op 的回执载荷（ver=1 protojson / ver=2 protobuf wire）。
func seedSmokeReplies(t *testing.T, srv *sessionTestServer, protobufEnc bool) {
	t.Helper()
	ops := gatewayv1opclient.SessionProtocolOps
	if !protobufEnc {
		srv.setReply(ops.Register, []byte(`{"playerId":"42"}`))
		srv.setReply(ops.Login, []byte(`{"playerId":"42","token":"tok-42"}`))
		srv.setReply(ops.Heartbeat, []byte(`{}`))
		return
	}
	srv.setReply(ops.Register, mustMarshal(t, &gatewayv1.RegisterReply{PlayerId: "42"}))
	srv.setReply(ops.Login, mustMarshal(t, &gatewayv1.LoginReply{PlayerId: "42", Token: "tok-42"}))
	srv.setReply(ops.Heartbeat, mustMarshal(t, &gatewayv1.HeartbeatReply{ServerTimeUnixMs: 1}))
}

// mustMarshal 以 protobuf 二进制编码生成 DTO（ver=2 用例）。
func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return data
}

// assertSeenOpsGenerated 断言服务端收到的每个 op 都命中生成描述符（冒烟零字面量契约）。
func assertSeenOpsGenerated(t *testing.T, srv *sessionTestServer) {
	t.Helper()
	known := map[string]bool{
		gatewayv1opclient.SessionProtocolOps.Register:        true,
		gatewayv1opclient.SessionProtocolOps.Login:           true,
		gatewayv1opclient.SessionProtocolOps.Heartbeat:       true,
		battlev1opclient.BattleServiceProtocolOps.JoinBattle: true,
	}
	seen := srv.seenOps()
	if len(seen) < 3 {
		t.Fatalf("服务端收到的 op 过少: %v", seen)
	}
	for _, op := range seen {
		if !known[op] {
			t.Fatalf("冒烟发出字面量 op %q（不在生成描述符内）", op)
		}
	}
}
