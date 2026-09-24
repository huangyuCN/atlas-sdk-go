// Session 集成测试（外部测试包：会话状态机只经导出 API 驱动，接缝素材取自本仓
// 生成的会话 stub）：凭据流转语义——登录保管凭据 / 无凭据 Resume 报错且不发网络
// 请求 / UDP 业务请求帧携带会话槽（真实 UDP 假服务端，本地回环无外部依赖）。
package client_test

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// sessionTestServer 是会话测试服务端（UDP）：按 op 回预置载荷（原始字节，编码无关），
// 记录每次请求的 op/会话槽/载荷与来源地址——供凭据流转、版本上报与推送用例断言。
type sessionTestServer struct {
	conn *net.UDPConn

	mu       sync.Mutex
	replies  map[string][]byte
	errors   map[string][]byte
	seen     []string
	payloads map[string][]byte
	lastAddr *net.UDPAddr
}

// startSessionServer 起一个会话测试服务端（UDP，本地回环）。
func startSessionServer(t *testing.T) *sessionTestServer {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	s := &sessionTestServer{
		conn:     conn,
		replies:  map[string][]byte{},
		errors:   map[string][]byte{},
		payloads: map[string][]byte{},
	}
	go s.serve()
	t.Cleanup(func() { _ = conn.Close() })
	return s
}

// addr 返回服务端地址。
func (s *sessionTestServer) addr() string { return s.conn.LocalAddr().String() }

// setReply 预置某 op 的回执载荷（原始字节；protojson 或 protobuf wire 由用例决定）。
func (s *sessionTestServer) setReply(op string, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies[op] = payload
}

// setErrorReply 预置某 op 的业务拒绝回执（code/class/reason 手写编码 Status，编码无关包络）。
func (s *sessionTestServer) setErrorReply(op string, code, class int, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors[op] = errorReply(code, class, reason)
}

// statusBytes 手写编码 atlas errors.Status 最小字段（code=1 varint、reason=2 string、
// class=5 varint；varint 支持多字节，如 code=404）。
func statusBytes(code, class int, reason string) []byte {
	out := []byte{0x08}
	out = binary.AppendUvarint(out, uint64(code))
	out = append(out, 0x12, byte(len(reason)))
	out = append(out, reason...)
	if class != 0 {
		out = append(out, 0x28)
		out = binary.AppendUvarint(out, uint64(class))
	}
	return out
}

// errorReply 组装错误回执包络：[hasError=1][statusLen:u32][Status][dataLen:u32][data]。
func errorReply(code, class int, reason string) []byte {
	st := statusBytes(code, class, reason)
	env := make([]byte, 1+4+len(st)+4)
	env[0] = 1
	binary.BigEndian.PutUint32(env[1:5], uint32(len(st)))
	copy(env[5:], st)
	return env
}

// serve 收帧 → 记录 → 回执（回执帧 ver 回显请求 ver，包络 [hasError=0][len][data]）。
func (s *sessionTestServer) serve() {
	buf := make([]byte, 64*1024)
	for {
		n, raddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		hdr, body, err := frame.Decode(buf[:n], frame.MaxBodySize)
		if err != nil {
			continue
		}
		op, session, payload, err := frame.ParseRequestBodyWithSession(body, hdr.Flags)
		if err != nil {
			continue
		}
		s.mu.Lock()
		s.seen = append(s.seen, op+"|"+session)
		s.payloads[op] = payload
		s.lastAddr = raddr
		reply, isErr := s.replies[op], s.errors[op]
		s.mu.Unlock()
		env := successReply(reply)
		if isErr != nil {
			env = isErr // 业务拒绝：整条错误包络已预置（编码无关）
		}
		out, err := frame.Encode(frame.Header{
			Type: frame.MsgTypeResponse, Version: hdr.Version, Seq: hdr.Seq,
		}, env, frame.MaxBodySize)
		if err != nil {
			continue
		}
		_, _ = s.conn.WriteToUDP(out, raddr)
	}
}

// successReply 组装成功回执包络（[hasError=0][dataLen:u32][data]）。
func successReply(data []byte) []byte {
	env := make([]byte, 5+len(data))
	binary.BigEndian.PutUint32(env[1:5], uint32(len(data)))
	copy(env[5:], data)
	return env
}

// sendNotify 向最近一次请求来源回推一条 ver=1（protojson）的 Notify 帧（被挤下线推送用例）。
func (s *sessionTestServer) sendNotify(op string, payload []byte) bool {
	return s.sendNotifyAt(op, frame.Version, payload)
}

// sendNotifyAt 按指定帧头 version 回推 Notify 帧：ver=1 传 JSON 字节、ver=2 传 protobuf
// wire 字节（S0.5 修订 1：接缝按 version 选解码器，推送必须带版本）。
func (s *sessionTestServer) sendNotifyAt(op string, version uint8, payload []byte) bool {
	s.mu.Lock()
	raddr := s.lastAddr
	s.mu.Unlock()
	if raddr == nil {
		return false
	}
	body, err := frame.BuildRequestBody(op, payload)
	if err != nil {
		return false
	}
	out, err := frame.Encode(frame.Header{Type: frame.MsgTypeNotify, Version: version, Seq: 1}, body, frame.MaxBodySize)
	if err != nil {
		return false
	}
	_, err = s.conn.WriteToUDP(out, raddr)
	return err == nil
}

// lastSeen 返回最近一条 "op|session" 记录（无请求为空串）。
func (s *sessionTestServer) lastSeen() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) == 0 {
		return ""
	}
	return s.seen[len(s.seen)-1]
}

// lastPayload 返回某 op 最近一次请求载荷（无记录为 nil）。
func (s *sessionTestServer) lastPayload(op string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.payloads[op]
}

// seenOps 返回服务端收到的全部 op（按到达序；会话心跳等异步路径断言用）。
func (s *sessionTestServer) seenOps() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ops := make([]string, 0, len(s.seen))
	for _, rec := range s.seen {
		if i := strings.Index(rec, "|"); i >= 0 {
			ops = append(ops, rec[:i])
		}
	}
	return ops
}

// gatewayProtocol 是本仓生成会话 stub 素材组装的接缝（项目侧真实接入即此形态）：
// 5 个 op 名与 3 个解码钩子全部来自生成物，SDK 内核零会话消息类型。
type gatewayProtocol struct{}

// Ops 返回生成物声明的 5 个会话 op 名。
func (gatewayProtocol) Ops() client.SessionOps {
	ops := gatewayv1opclient.SessionProtocolOps
	return client.SessionOps{
		Register:  ops.Register,
		Login:     ops.Login,
		Resume:    ops.Resume,
		Logout:    ops.Logout,
		Heartbeat: ops.Heartbeat,
	}
}

// Token 经生成物提取器取 token。
func (gatewayProtocol) Token(msg any) string { return gatewayv1opclient.SessionToken(msg) }

// PlayerID 经生成物提取器取 playerId。
func (gatewayProtocol) PlayerID(msg any) string { return gatewayv1opclient.SessionPlayerID(msg) }

// ExpiresAt 经生成物提取器取过期时间（模板当前无该字段，恒 0）。
func (gatewayProtocol) ExpiresAt(msg any) int64 { return gatewayv1opclient.SessionExpiresAt(msg) }

// Kicked 按生成物推送 op 判定被挤下线并解出原因枚举名：msg 是推送信封
// （client.PushEnvelope），按帧头 version 选解码器——ver=1 protojson、ver=2 protobuf
// wire、未知版本不解码（识别为被挤下线但取不到原因，不 panic）。
func (gatewayProtocol) Kicked(op string, msg any) (string, bool) {
	if op != gatewayv1opclient.SessionPushOps.KickedNotify {
		return "", false
	}
	env, ok := msg.(client.PushEnvelope)
	if !ok || len(env.Body) == 0 {
		return "", true
	}
	var n gatewayv1.KickedNotify
	switch env.Version {
	case frame.Version:
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(env.Body, &n); err != nil {
			return "", true
		}
	case frame.Version2:
		if err := proto.Unmarshal(env.Body, &n); err != nil {
			return "", true
		}
	default:
		return "", true
	}
	return n.GetReason().String(), true
}

// dialSession 拨号 UDP 通道并装配接缝会话（传输心跳关闭；会话心跳由会话配置决定）。
func dialSession(t *testing.T, srv *sessionTestServer, proto client.SessionProtocol, opts ...client.Option) (*client.Session, *client.Client) {
	t.Helper()
	return dialSessionWith(t, srv, proto, nil, opts...)
}

// dialSessionWith 在 dialSession 基础上追加会话级选项（如 WithOnKicked 被挤下线回调）。
func dialSessionWith(t *testing.T, srv *sessionTestServer, proto client.SessionProtocol, sessOpts []client.SessionOption, opts ...client.Option) (*client.Session, *client.Client) {
	t.Helper()
	sessionOpts := append([]client.SessionOption{
		client.WithSessionProtocol(proto),
		client.WithSessionHeartbeatInterval(0),
	}, sessOpts...)
	sess := client.NewSession(sessionOpts...)
	dialOpts := append([]client.Option{client.WithHeartbeatInterval(0)}, sess.ChannelOptions()...)
	dialOpts = append(dialOpts, opts...)
	cli, err := client.DialUDP(srv.addr(), dialOpts...)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	if err := sess.Bind(cli); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return sess, cli
}

// TestSessionLoginStoresToken 验证登录后凭据被保管（凭据经接缝提取器从生成 DTO
// 回执取出）、Logout 后清空。
func TestSessionLoginStoresToken(t *testing.T) {
	srv := startSessionServer(t)
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Login, []byte(`{"playerId":"42","token":"tok-42"}`))
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Logout, nil)
	sess, _ := dialSession(t, srv, gatewayProtocol{})

	reply, err := sess.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reply.(*gatewayv1.LoginReply); !ok {
		t.Fatalf("登录回执应按生成 DTO 解码，得到 %T", reply)
	}
	if sess.Token() != "tok-42" || sess.PlayerID() != "42" {
		t.Fatalf("登录后凭据未保管: token=%q player=%q", sess.Token(), sess.PlayerID())
	}
	if err := sess.Logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sess.Token() != "" || sess.PlayerID() != "" {
		t.Fatalf("登出后凭据应清空: token=%q player=%q", sess.Token(), sess.PlayerID())
	}
}

// TestSessionResumeRequiresToken 验证无凭据时 Resume 报错且不发网络请求。
func TestSessionResumeRequiresToken(t *testing.T) {
	srv := startSessionServer(t)
	sess, _ := dialSession(t, srv, gatewayProtocol{})
	if _, err := sess.Resume(context.Background()); err == nil {
		t.Fatal("无凭据 Resume 应报错")
	}
	if got := srv.lastSeen(); got != "" {
		t.Fatalf("无凭据 Resume 不应发请求，服务端收到 %q", got)
	}
}

// TestSessionResumeUsesGeneratedRequest 验证 Resume 请求按生成 DTO 构造（token/playerId
// 与客户端版本同载），且回执未携带凭据时本地 token 不被清空。
func TestSessionResumeUsesGeneratedRequest(t *testing.T) {
	srv := startSessionServer(t)
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Login, []byte(`{"playerId":"42","token":"tok-42"}`))
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Resume, []byte(`{"playerId":"42"}`))
	sess, _ := dialSession(t, srv, gatewayProtocol{})
	if _, err := sess.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	var req gatewayv1.ResumeRequest
	if err := protojson.Unmarshal(srv.lastPayload(gatewayv1opclient.SessionProtocolOps.Resume), &req); err != nil {
		t.Fatalf("解 Resume 请求: %v", err)
	}
	if req.GetToken() != "tok-42" || req.GetPlayerId() != "42" {
		t.Fatalf("Resume 请求字段 = %q/%q", req.GetToken(), req.GetPlayerId())
	}
	if sess.Token() != "tok-42" {
		t.Fatalf("回执无 token 时不应清空本地凭据: %q", sess.Token())
	}
}

// TestUDPSessionSlotCarriedOnInvoke 验证无连接传输的请求帧携带会话槽：
// 登录后凭据被保管，随后的 Invoke（无身份字段的消息）经帧槽携带凭据。
func TestUDPSessionSlotCarriedOnInvoke(t *testing.T) {
	srv := startSessionServer(t)
	srv.setReply(gatewayv1opclient.SessionProtocolOps.Login, []byte(`{"playerId":"42","token":"tok-42"}`))
	sess, cli := dialSession(t, srv, gatewayProtocol{})

	if _, err := sess.Login(context.Background(), &gatewayv1.LoginRequest{PlayerId: "42", Password: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Invoke(context.Background(), "/game.v1.PlayerService/GetPlayer", nil, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if strings.HasPrefix(srv.lastSeen(), "/game.v1.PlayerService/GetPlayer|tok-42") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("业务请求未携带会话槽, last=%q", srv.lastSeen())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
