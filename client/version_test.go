// 客户端版本上报测试（A.7）：登录/恢复请求带 client_version，值取自 client.Version
// 单一来源；ver=1（protojson）与 ver=2（protobuf 二进制）两条编码路径都断言。
package client_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	pbserializer "github.com/huangyuCN/atlas-sdk-go/contrib/protobuf"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// semverRe 是 client.Version 的形态约束（模板 client_version 是 semver 字符串）。
var semverRe = regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

// TestClientVersionSingleSource 断言版本单一来源：client.Version 形态为 semver
// （模板 client_version 字段类型），SDK 内不另设函数形态副本。
func TestClientVersionSingleSource(t *testing.T) {
	if !semverRe.MatchString(client.Version) {
		t.Fatalf("client.Version = %q, 期望 semver 字符串", client.Version)
	}
}

// TestLoginRequestCarriesClientVersion 断言登录请求（ver=1 protojson）带 client_version：
// SDK 就地注入版本，调用方字段不受影响，线上字段名是 protojson 的 clientVersion。
func TestLoginRequestCarriesClientVersion(t *testing.T) {
	op := gatewayv1opclient.SessionProtocolOps.Login
	srv := startSessionServer(t)
	srv.setReply(op, []byte(`{"playerId":"42","token":"tok-42"}`))
	sess, _ := dialSession(t, srv, gatewayProtocol{})

	req := &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}
	if _, err := sess.Login(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if req.GetClientVersion() != client.Version {
		t.Fatalf("请求 DTO 未注入 client_version: %q", req.GetClientVersion())
	}
	if req.GetPlayerId() != "42" || req.GetPassword() != "x" {
		t.Fatalf("注入版本不应破坏调用方字段: %+v", req)
	}
	var sent gatewayv1.LoginRequest
	if err := protojson.Unmarshal(srv.lastPayload(op), &sent); err != nil {
		t.Fatalf("解登录请求: %v", err)
	}
	if sent.GetClientVersion() != client.Version {
		t.Fatalf("线上请求 client_version = %q, 期望 %q", sent.GetClientVersion(), client.Version)
	}
	if !strings.Contains(string(srv.lastPayload(op)), `"clientVersion":"`+client.Version+`"`) {
		t.Fatalf("请求载荷缺 clientVersion 字段: %s", srv.lastPayload(op))
	}
}

// TestResumeRequestCarriesClientVersionProtobuf 断言 ver=2（protobuf 二进制）下 Resume
// 请求仍带 client_version（与 Login 复述同一版本），回执按生成 DTO 解码且凭据沿用。
func TestResumeRequestCarriesClientVersionProtobuf(t *testing.T) {
	loginOp := gatewayv1opclient.SessionProtocolOps.Login
	resumeOp := gatewayv1opclient.SessionProtocolOps.Resume
	srv := startSessionServer(t)
	loginReply, err := proto.Marshal(&gatewayv1.LoginReply{PlayerId: "42", Token: "tok-42"})
	if err != nil {
		t.Fatal(err)
	}
	resumeReply, err := proto.Marshal(&gatewayv1.ResumeReply{PlayerId: "42"})
	if err != nil {
		t.Fatal(err)
	}
	srv.setReply(loginOp, loginReply)
	srv.setReply(resumeOp, resumeReply)
	sess, _ := dialSession(t, srv, gatewayProtocol{}, client.WithSerializer(pbserializer.Serializer{}))

	if _, err := sess.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
		t.Fatal(err)
	}
	if sess.Token() != "tok-42" {
		t.Fatalf("ver=2 登录凭据未保管: %q", sess.Token())
	}
	if _, err := sess.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	var req gatewayv1.ResumeRequest
	if err := proto.Unmarshal(srv.lastPayload(resumeOp), &req); err != nil {
		t.Fatalf("解 ver=2 Resume 请求: %v", err)
	}
	if req.GetClientVersion() != client.Version || req.GetToken() != "tok-42" || req.GetPlayerId() != "42" {
		t.Fatalf("Resume 请求 = version:%q token:%q player:%q", req.GetClientVersion(), req.GetToken(), req.GetPlayerId())
	}
	if sess.Token() != "tok-42" || sess.PlayerID() != "42" {
		t.Fatalf("ver=2 Resume 后凭据 = %q/%q", sess.Token(), sess.PlayerID())
	}
}
