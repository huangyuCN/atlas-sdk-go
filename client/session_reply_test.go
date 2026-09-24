// 会话回执解析与凭据校验测试（S0.5 修订 1 · c①）：会话 op 的回执必须解析成已注册的
// 生成 DTO，解析失败或关键凭据为空都必须显式报错——不回退通用载体 map，否则
// token/playerId 取到空串却报「登录成功」，直到 Resume/Heartbeat 才暴露。
package client_test

import (
	"context"
	"errors"
	"testing"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
)

// unregisteredProtocol 是「op 不在 protobuf 全局注册表内」的接缝：会话回执无法解析成
// 生成 DTO（项目二进制未链接生成 DTO 包，或 op 与生成 stub 不同源）。钩子复用
// fakeProtocol 的常量返回——回执解析在钩子之前就失败。
type unregisteredProtocol struct{ fakeProtocol }

// Ops 返回未注册的会话 op 名（/fake/* 不在任何生成描述符内）。
func (unregisteredProtocol) Ops() client.SessionOps {
	return client.SessionOps{
		Register:  "/fake/Register",
		Login:     "/fake/Login",
		Resume:    "/fake/Resume",
		Logout:    "/fake/Logout",
		Heartbeat: "/fake/Heartbeat",
	}
}

// TestSessionReplyUnresolved 断言未注册 DTO 的会话 op 回执即报 ErrSessionReplyUnresolved：
// unregisteredProtocol 的 /fake/* op 不在 protobuf 全局注册表内（对应「项目二进制未链接
// 生成的会话 DTO 包」这一部署缺陷），回执无法解析成生成 DTO。
func TestSessionReplyUnresolved(t *testing.T) {
	srv := startSessionServer(t)
	srv.setReply("/fake/Login", []byte(`{"token":"tok","playerId":"p"}`))
	srv.setReply("/fake/Register", []byte(`{"playerId":"p"}`))
	srv.setReply("/fake/Resume", []byte(`{"playerId":"p"}`))
	sess, _ := dialSession(t, srv, unregisteredProtocol{})
	ctx := context.Background()

	if _, err := sess.Login(ctx, map[string]any{"playerId": "p"}); !errors.Is(err, client.ErrSessionReplyUnresolved) {
		t.Fatalf("未注册回执的 Login 应报 ErrSessionReplyUnresolved，得到 %v", err)
	}
	if sess.Token() != "" || sess.PlayerID() != "" {
		t.Fatalf("解析失败不应保管任何凭据: token=%q player=%q", sess.Token(), sess.PlayerID())
	}
	if _, err := sess.Register(ctx, map[string]any{"account": "a"}); !errors.Is(err, client.ErrSessionReplyUnresolved) {
		t.Fatalf("未注册回执的 Register 应报 ErrSessionReplyUnresolved，得到 %v", err)
	}
	if _, err := sess.Restore(ctx, "tok-x", "42"); !errors.Is(err, client.ErrSessionReplyUnresolved) {
		t.Fatalf("未注册回执的 Restore 应报 ErrSessionReplyUnresolved，得到 %v", err)
	}
}

// TestSessionCredentialsEmpty 断言关键凭据为空即失败：Login/Register 至少要有 token 或
// playerId，Resume/Restore 的恢复回执必须有 playerId。
func TestSessionCredentialsEmpty(t *testing.T) {
	ops := gatewayv1opclient.SessionProtocolOps
	ctx := context.Background()

	t.Run("login", func(t *testing.T) {
		srv := startSessionServer(t)
		srv.setReply(ops.Login, []byte(`{}`)) // 生成 DTO 解析成功但凭据为空
		sess, _ := dialSession(t, srv, gatewayProtocol{})
		if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); !errors.Is(err, client.ErrSessionCredentialsEmpty) {
			t.Fatalf("空凭据登录应报 ErrSessionCredentialsEmpty，得到 %v", err)
		}
		if sess.Token() != "" || sess.PlayerID() != "" {
			t.Fatalf("空凭据不应保管: token=%q player=%q", sess.Token(), sess.PlayerID())
		}
	})

	t.Run("register", func(t *testing.T) {
		srv := startSessionServer(t)
		srv.setReply(ops.Register, []byte(`{}`))
		sess, _ := dialSession(t, srv, gatewayProtocol{})
		if _, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: "a", Password: "pw-123456"}); !errors.Is(err, client.ErrSessionCredentialsEmpty) {
			t.Fatalf("空 playerId 注册应报 ErrSessionCredentialsEmpty，得到 %v", err)
		}
	})

	t.Run("resume", func(t *testing.T) {
		srv := startSessionServer(t)
		srv.setReply(ops.Login, []byte(`{"token":"tok-42"}`)) // 仅 token：playerId 仍缺
		srv.setReply(ops.Resume, []byte(`{}`))
		sess, _ := dialSession(t, srv, gatewayProtocol{})
		if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
			t.Fatal(err)
		}
		if _, err := sess.Resume(ctx); !errors.Is(err, client.ErrSessionCredentialsEmpty) {
			t.Fatalf("恢复回执无 playerId 应报 ErrSessionCredentialsEmpty，得到 %v", err)
		}
	})

	t.Run("restore", func(t *testing.T) {
		srv := startSessionServer(t)
		srv.setReply(ops.Resume, []byte(`{}`))
		sess, _ := dialSession(t, srv, gatewayProtocol{})
		if _, err := sess.Restore(ctx, "tok-x", "42"); !errors.Is(err, client.ErrSessionCredentialsEmpty) {
			t.Fatalf("Restore 回执无 playerId 应报 ErrSessionCredentialsEmpty，得到 %v", err)
		}
	})
}
