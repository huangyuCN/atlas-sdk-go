package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// sessionTestServer 是会话测试服务端（UDP）：按 op 回预置 protojson 回执，
// 并记录每次请求的 (op, 会话槽)——供会话槽与凭据流转断言。
type sessionTestServer struct {
	conn    *net.UDPConn
	replies map[string]map[string]any

	mu       sync.Mutex
	seen     []string // "op|session" 记录
	seenChan chan string
}

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
		conn: conn,
		replies: map[string]map[string]any{
			OpSessionLogin:  {"playerId": "42", "token": "tok-42"},
			OpSessionResume: {"playerId": "42", "token": "tok-42"},
			OpSessionLogout: {},
		},
		seenChan: make(chan string, 16),
	}
	go s.serve()
	t.Cleanup(func() { _ = conn.Close() })
	return s
}

func (s *sessionTestServer) addr() string { return s.conn.LocalAddr().String() }

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
		op, session, _, err := frame.ParseRequestBodyWithSession(body, hdr.Flags)
		if err != nil {
			continue
		}
		rec := op + "|" + session
		s.mu.Lock()
		s.seen = append(s.seen, rec)
		s.mu.Unlock()
		select {
		case s.seenChan <- rec:
		default:
		}
		resp, ok := s.replies[op]
		if !ok {
			resp = map[string]any{} // 业务 op 统一空回执（测试只验证链路与会话槽）
		}
		if op == OpSessionHeartbeat {
			resp = map[string]any{}
		}
		data, _ := json.Marshal(resp)
		// 响应包络：[hasError=0][dataLen:u32][data]（frame.DecodeReply 对应格式）。
		env := make([]byte, 5+len(data))
		binary.BigEndian.PutUint32(env[1:5], uint32(len(data)))
		copy(env[5:], data)
		out, err := frame.Encode(frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq}, env, frame.MaxBodySize)
		if err != nil {
			continue
		}
		_, _ = s.conn.WriteToUDP(out, raddr)
	}
}

// lastSeen 返回最近一条记录（测试断言用）。
func (s *sessionTestServer) lastSeen() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) == 0 {
		return ""
	}
	return s.seen[len(s.seen)-1]
}

// TestSessionLoginStoresToken 验证登录后凭据被保管、Logout 后清空。
func TestSessionLoginStoresToken(t *testing.T) {
	srv := startSessionServer(t)
	s := NewSession(WithSessionHeartbeatInterval(0))
	cli, err := DialUDP(srv.addr(), s.ChannelOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	s.Bind(cli)

	reply, err := s.Login(context.Background(), map[string]any{"playerId": "42", "password": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Token != "tok-42" || s.Token() != "tok-42" || s.PlayerID() != "42" {
		t.Fatalf("登录后凭据未保管: reply=%v token=%q player=%q", reply, s.Token(), s.PlayerID())
	}
	if err := s.Logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Token() != "" || s.PlayerID() != "" {
		t.Fatalf("登出后凭据应清空: token=%q player=%q", s.Token(), s.PlayerID())
	}
}

// TestSessionResumeRequiresToken 验证无凭据时 Resume 报错且不发网络请求。
func TestSessionResumeRequiresToken(t *testing.T) {
	srv := startSessionServer(t)
	s := NewSession(WithSessionHeartbeatInterval(0))
	cli, err := DialUDP(srv.addr(), s.ChannelOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	s.Bind(cli)
	if _, err := s.Resume(context.Background()); err == nil {
		t.Fatal("无凭据 Resume 应报错")
	}
}

// TestUDPSessionSlotCarriedOnInvoke 验证无连接传输的请求帧携带会话槽：
// 登录后凭据被保管，随后的 Invoke（无身份字段的消息）经帧槽携带凭据。
func TestUDPSessionSlotCarriedOnInvoke(t *testing.T) {
	srv := startSessionServer(t)
	s := NewSession(WithSessionHeartbeatInterval(0))
	cli, err := DialUDP(srv.addr(), s.ChannelOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	s.Bind(cli)

	if _, err := s.Login(context.Background(), nil); err != nil {
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
