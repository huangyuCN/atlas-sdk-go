// SessionProtocol 接缝契约测试（S0.5 冻结形状：5 个 op + 3 个解码钩子 + 1 个推送识别）。
//
// 接缝是「会话状态机」与「会话消息类型」之间的唯一缝：注入 fake 接缝即可驱动
// 登录/心跳/被踢，无需任何真实会话 DTO；生成物侧素材（本仓生成的会话 stub）另有
// 用例锁定 op 名、提取器与推送 op。与 atlas-sdk-ts test/sessionProtocol.test.ts、
// atlas-sdk-csharp SessionProtocolTest 同构（三语言同职责，命名按各语言惯用）。
package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/proto"
)

// 编译期冻结：接缝成员集合固定（各方多一个成员即编译失败）。
var (
	_ client.SessionProtocol = fakeProtocol{}
	_ client.SessionProtocol = gatewayProtocol{}
)

// fakeProtocol 是纯 fake 接缝：op 名取自生成描述符（S0.5 修订 1 要求会话回执解析成
// 生成 DTO，故 op 必须可解析），凭据与原因返回常量、不读任何回执字段——证明状态机
// 只依赖接缝，而不是靠解析真实 DTO 内容。
type fakeProtocol struct{}

// Ops 返回生成描述符声明的 5 个会话 op 名。
func (fakeProtocol) Ops() client.SessionOps { return gatewayProtocol{}.Ops() }

// Token 返回常量 token（不依赖回执内容）。
func (fakeProtocol) Token(any) string { return "fake-token" }

// PlayerID 返回常量 playerId。
func (fakeProtocol) PlayerID(any) string { return "fake-player" }

// ExpiresAt 返回常量过期时间（本轮无消费方：模板无 expiry 字段，不启用续期）。
func (fakeProtocol) ExpiresAt(any) int64 { return 1_700_000_000_000 }

// Kicked 只识别生成物声明的被挤下线推送 op，原因返回常量（不读载荷）。
func (fakeProtocol) Kicked(op string, msg any) (string, bool) {
	if op != gatewayv1opclient.SessionPushOps.KickedNotify {
		return "", false
	}
	return "LOGGED_IN_ELSEWHERE", true
}

// TestSessionProtocolShape 断言接缝形状与生成物素材：5 op 一一对应、成员数冻结、
// 3 提取器命中生成 DTO、推送 op 来自生成物。
func TestSessionProtocolShape(t *testing.T) {
	seam := gatewayProtocol{}
	ops := seam.Ops()
	gen := gatewayv1opclient.SessionProtocolOps
	if ops.Register != gen.Register || ops.Login != gen.Login || ops.Resume != gen.Resume ||
		ops.Logout != gen.Logout || ops.Heartbeat != gen.Heartbeat {
		t.Fatalf("接缝 op 与生成物不一致: %+v vs %+v", ops, gen)
	}
	if n := reflect.TypeOf(gen).NumField(); n != 5 {
		t.Fatalf("生成物 op 成员数 = %d, 期望 5（S0.5 冻结）", n)
	}
	if n := reflect.TypeOf(ops).NumField(); n != 5 {
		t.Fatalf("SessionOps 成员数 = %d, 期望 5（S0.5 冻结）", n)
	}
	if !strings.HasSuffix(gatewayv1opclient.SessionPushOps.KickedNotify, "KickedNotify") {
		t.Fatalf("推送 op = %q, 期望被挤下线推送名", gatewayv1opclient.SessionPushOps.KickedNotify)
	}
	// 生成物提取器：命中字段取值，未命中/未知载荷返回零值不抛错。
	if got := seam.Token(&gatewayv1.LoginReply{Token: "tok-1"}); got != "tok-1" {
		t.Fatalf("Token(LoginReply) = %q", got)
	}
	if got := seam.Token(&gatewayv1.ResumeRequest{Token: "tok-2"}); got != "tok-2" {
		t.Fatalf("Token(ResumeRequest) = %q", got)
	}
	if got := seam.PlayerID(&gatewayv1.LoginReply{PlayerId: "p-1"}); got != "p-1" {
		t.Fatalf("PlayerID(LoginReply) = %q", got)
	}
	for _, empty := range []any{nil, "", 0, []byte{}} {
		if seam.Token(empty) != "" || seam.PlayerID(empty) != "" {
			t.Fatalf("未知载荷 %v 应返回零值", empty)
		}
	}
	if got := seam.ExpiresAt(nil); got != 0 {
		t.Fatalf("ExpiresAt = %d, 期望 0（模板无该字段）", got)
	}
}

// TestSessionProtocolKicked 断言推送识别与原因提取：信封携带帧头版本，接缝按版本选
// 解码器（ver=1 protojson / ver=2 protobuf wire）；未知版本不 panic、不误判原因。
func TestSessionProtocolKicked(t *testing.T) {
	seam := gatewayProtocol{}
	pushOp := gatewayv1opclient.SessionPushOps.KickedNotify
	jsonBody := []byte(`{"reason":"KICKED_REASON_LOGGED_IN_ELSEWHERE"}`)
	wireBody, err := proto.Marshal(&gatewayv1.KickedNotify{Reason: gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE})
	if err != nil {
		t.Fatal(err)
	}
	good := []struct {
		name string
		env  client.PushEnvelope
	}{
		{"ver1-protojson", client.PushEnvelope{Op: pushOp, Version: frame.Version, Body: jsonBody}},
		{"ver2-protobuf", client.PushEnvelope{Op: pushOp, Version: frame.Version2, Body: wireBody}},
	}
	for _, tc := range good {
		if reason, ok := seam.Kicked(pushOp, tc.env); !ok || reason != "KICKED_REASON_LOGGED_IN_ELSEWHERE" {
			t.Fatalf("%s 推送原因 = %q/%v", tc.name, reason, ok)
		}
	}
	// 未知 version：识别为被挤下线但无法解码（不 panic、不猜编码）。
	if reason, ok := seam.Kicked(pushOp, client.PushEnvelope{Op: pushOp, Version: 9, Body: jsonBody}); !ok || reason != "" {
		t.Fatalf("未知 version: reason=%q ok=%v", reason, ok)
	}
	// 识别为被挤下线但载荷取不到原因（空/坏载荷）：ok=true、reason 空。
	for _, bad := range [][]byte{nil, {}, []byte("{")} {
		env := client.PushEnvelope{Op: pushOp, Version: frame.Version, Body: bad}
		if reason, ok := seam.Kicked(pushOp, env); !ok || reason != "" {
			t.Fatalf("坏载荷 %q: reason=%q ok=%v", bad, reason, ok)
		}
	}
	// 非会话推送 op：不识别。
	if _, ok := seam.Kicked("/game.v1.PlayerService/Notify", client.PushEnvelope{}); ok {
		t.Fatal("非推送 op 不应识别为被挤下线")
	}
}

// TestSessionProtocolMissing 断言未注入接缝即显式报错（不留默认 gateway.v1 副本）。
func TestSessionProtocolMissing(t *testing.T) {
	srv := startSessionServer(t)
	sess := client.NewSession(client.WithSessionHeartbeatInterval(0))
	cli, err := client.DialUDP(srv.addr(), client.WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = cli.Close() }()

	ctx := context.Background()
	if err := sess.Bind(cli); !errors.Is(err, client.ErrNoSessionProtocol) {
		t.Fatalf("Bind 应报未接入会话协议，得到 %v", err)
	}
	if _, err := sess.Login(ctx, nil); !errors.Is(err, client.ErrNoSessionProtocol) {
		t.Fatalf("Login 应报未接入会话协议，得到 %v", err)
	}
	if _, err := sess.Heartbeat(ctx); !errors.Is(err, client.ErrNoSessionProtocol) {
		t.Fatalf("Heartbeat 应报未接入会话协议，得到 %v", err)
	}
	if _, err := sess.Resume(ctx); !errors.Is(err, client.ErrNoSessionProtocol) {
		t.Fatalf("Resume 应报未接入会话协议，得到 %v", err)
	}
	if err := sess.Logout(ctx); !errors.Is(err, client.ErrNoSessionProtocol) {
		t.Fatalf("Logout 应报未接入会话协议，得到 %v", err)
	}
}

// TestSessionFakeProtocolDrivesStateMachine 断言注入 fake 接缝即可驱动登录/心跳/被踢：
// op 名取自生成描述符（回执类型可解析），凭据与原因全部来自接缝的常量返回——状态机
// 不读回执字段，也不认识任何会话消息类型。
func TestSessionFakeProtocolDrivesStateMachine(t *testing.T) {
	srv := startSessionServer(t)
	sess := client.NewSession(
		client.WithSessionProtocol(fakeProtocol{}),
		client.WithSessionHeartbeatInterval(15*time.Millisecond),
	)
	cli, err := client.DialUDP(srv.addr(), append([]client.Option{client.WithHeartbeatInterval(0)}, sess.ChannelOptions()...)...)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = cli.Close() }()
	if err := sess.Bind(cli); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if _, err := sess.Login(context.Background(), map[string]any{"playerId": "p-1"}); err != nil {
		t.Fatal(err)
	}
	if sess.Token() != "fake-token" || sess.PlayerID() != "fake-player" {
		t.Fatalf("凭据应来自接缝钩子: token=%q player=%q", sess.Token(), sess.PlayerID())
	}
	ops := gatewayProtocol{}.Ops()
	waitSeenOp(t, srv, ops.Login)
	waitSeenOp(t, srv, ops.Heartbeat) // 周期会话心跳的 op 取自接缝

	// 非会话推送：接缝 ok=false → 凭据不动、原因不记。
	srv.sendNotify("/game.v1.Other/Notify", []byte("xx"))
	time.Sleep(50 * time.Millisecond)
	if sess.Token() != "fake-token" || sess.KickedReason() != "" {
		t.Fatalf("非会话推送不应清空凭据: token=%q reason=%q", sess.Token(), sess.KickedReason())
	}
	// 被挤下线推送：接缝提取原因 → 状态机清空凭据并记录原因。
	srv.sendNotify(gatewayv1opclient.SessionPushOps.KickedNotify, []byte("LOGGED_IN_ELSEWHERE"))
	deadline := time.Now().Add(2 * time.Second)
	for sess.Token() != "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sess.Token() != "" || sess.PlayerID() != "" {
		t.Fatalf("被挤下线后凭据应清空: token=%q player=%q", sess.Token(), sess.PlayerID())
	}
	if sess.KickedReason() != "LOGGED_IN_ELSEWHERE" {
		t.Fatalf("被挤下线原因 = %q", sess.KickedReason())
	}
}

// waitSeenOp 等待服务端收到指定 op（会话心跳等异步路径）。
func waitSeenOp(t *testing.T, srv *sessionTestServer, op string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, got := range srv.seenOps() {
			if got == op {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("未等到 op %q（已收到 %v）", op, srv.seenOps())
}

// TestStateMachineHasNoSessionTypes 断言会话状态机（client 包非测试源码）既不引用
// 会话消息类型（不 import 生成协议包），也不含会话 op 字面量——协议事实只允许出现在
// 接缝实现与生成 stub 内（S0.5：状态机零会话消息类型；验收的 grep 断言在此固化）。
func TestStateMachineHasNoSessionTypes(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("读取包目录: %v", err)
	}
	const protoPkgPrefix = "github.com/huangyuCN/atlas-sdk-go/api/"
	scanned := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		scanned++
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatalf("解析 %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			if p := strings.Trim(imp.Path.Value, `"`); strings.HasPrefix(p, protoPkgPrefix) {
				t.Errorf("%s 引用了生成协议包 %s（状态机不得依赖会话消息类型）", name, p)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if ok && lit.Kind == token.STRING && strings.Contains(lit.Value, "/gateway.v1.") {
				t.Errorf("%s 含会话 op 字面量 %s（应取自接缝）", name, lit.Value)
			}
			return true
		})
	}
	if scanned == 0 {
		t.Fatal("未扫描到任何源码文件（断言失效）")
	}
}

// TestGoldenSessionOperationFromGenerated 断言 golden 会话用例声明的 op 命中生成 stub
// 的会话 op（A.8 · 会话 op 解析：字节用例的 op 解析在 frame golden 测试，本用例锁定
// 「golden 的 op 与生成描述符同源」）。
func TestGoldenSessionOperationFromGenerated(t *testing.T) {
	dir := goldenDir(t)
	ops := gatewayProtocol{}.Ops()
	known := map[string]string{
		ops.Register: "Register", ops.Login: "Login", ops.Resume: "Resume",
		ops.Logout: "Logout", ops.Heartbeat: "Heartbeat",
	}
	manifest := readGoldenManifest(t, dir)
	matched := 0
	for id, kind := range manifest {
		if kind != "frame" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, "cases", id, "expected.json"))
		if err != nil {
			t.Fatalf("读取 %s/expected.json: %v", id, err)
		}
		var want map[string]any
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatalf("解析 %s/expected.json: %v", id, err)
		}
		op, _ := want["operation"].(string)
		if !strings.HasPrefix(op, "/gateway.v1.Session/") {
			continue
		}
		method, ok := known[op]
		if !ok {
			t.Errorf("golden 用例 %s 的会话 op %q 不在生成物声明内", id, op)
			continue
		}
		matched++
		if method == "Login" && op != ops.Login {
			t.Errorf("用例 %s 的 op %q ≠ 生成物 Login op %q", id, op, ops.Login)
		}
	}
	if matched == 0 {
		t.Fatal("golden 未包含会话 op 用例（期望 frame-request-session-login）")
	}
}

// readGoldenManifest 返回用例 id → kind（manifest 缺失即失败：向量源不可静默跳过）。
func readGoldenManifest(t *testing.T, dir string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("读取 golden manifest（%s）: %v", dir, err)
	}
	var m struct {
		Cases map[string]struct {
			Kind string `json:"kind"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("解析 manifest: %v", err)
	}
	out := make(map[string]string, len(m.Cases))
	for id, meta := range m.Cases {
		out[id] = meta.Kind
	}
	return out
}

// goldenDir 解析向量目录：env 优先，缺省为本仓同级的 atlas 主仓。
func goldenDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("ATLAS_GOLDEN_DIR"); dir != "" {
		return dir
	}
	abs, err := filepath.Abs(filepath.Join("..", "..", "atlas", "testdata", "golden"))
	if err != nil {
		t.Fatalf("解析默认向量目录: %v", err)
	}
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		t.Fatalf("golden 向量目录不存在: %s（请检出 atlas 主仓到本仓同级目录，或设置 ATLAS_GOLDEN_DIR）", abs)
	}
	return abs
}
