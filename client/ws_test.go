package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// wsTestServer 是 WebSocket 测试服务端：一条二进制消息 = 一个完整帧，
// operation 语义经 echoService 与 TCP 替身保持一致；仅接受 /ws 路径。
type wsTestServer struct {
	ln    net.Listener
	echo  echoService
	connN atomic.Int32 // 升级成功的连接数（重连观测用）
}

func startWSServer(t *testing.T) *wsTestServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	s := &wsTestServer{ln: ln, echo: echoService{pingSeen: make(chan struct{}, 1)}}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *wsTestServer) addr() string { return s.ln.Addr().String() }

// handleWS 升级连接后按帧处理：读一条消息 → Decode → echoService → Encode → 单条消息回写。
func (s *wsTestServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.connN.Add(1)
	defer func() { _ = conn.Close() }()
	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if mt != websocket.BinaryMessage {
			return // 非二进制消息：与服务端语义一致，断开
		}
		hdr, body, err := frame.Decode(msg, frame.MaxBodySize)
		if err != nil {
			return // 协议非法：断开
		}
		op, payload, err := frame.ParseRequestBody(body)
		if err != nil {
			return
		}
		resp, notifyBody, closeConn := s.echo.handleRequest(op, payload)
		if notifyBody != nil {
			if err := s.send(conn, frame.Header{Type: frame.MsgTypeNotify, Seq: 999}, notifyBody); err != nil {
				return
			}
		}
		if err := s.send(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq}, resp); err != nil {
			return
		}
		if closeConn {
			return // kick：回包后断开（defer 关闭连接）
		}
	}
}

func (s *wsTestServer) send(conn *websocket.Conn, h frame.Header, body []byte) error {
	buf, err := frame.Encode(h, body, frame.MaxBodySize)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, buf)
}

// TestWSInvokeEcho 验证 WS 通道请求-响应闭环（一条消息 = 一个完整帧）。
func TestWSInvokeEcho(t *testing.T) {
	s := startWSServer(t)
	c, err := DialWS(s.addr(), "/ws", WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "hi"}, &resp); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.Msg != "hi" {
		t.Fatalf("回显不一致: %+v", resp)
	}
}

// TestWSBusinessError 验证 WS 通道业务拒绝还原为 *BusinessError。
func TestWSBusinessError(t *testing.T) {
	s := startWSServer(t)
	c, err := DialWS(s.addr(), "/ws", WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "boom", nil, nil)
	var be *BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("期望 *BusinessError, 实际 %T: %v", err, err)
	}
	if be.Reason != "NOT_ENOUGH_GOLD" || be.Code != 400 {
		t.Fatalf("BusinessError 还原不一致: %+v", be)
	}
}

// TestWSNotifyDispatch 验证 WS 通道 Notify 按 operation 分发。
func TestWSNotifyDispatch(t *testing.T) {
	s := startWSServer(t)
	c, err := DialWS(s.addr(), "/ws", WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	got := make(chan string, 1)
	off := c.On("/push.test", func(_ string, payload []byte) {
		var m struct {
			K string `json:"k"`
		}
		_ = json.Unmarshal(payload, &m)
		got <- m.K
	})
	defer off()

	if err := c.Invoke(context.Background(), "notify-op", nil, nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	select {
	case k := <-got:
		if k != "v" {
			t.Fatalf("Notify payload 不一致: %q", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待 Notify 超时")
	}
}

// TestWSHeartbeatKeepAlive 验证 WS 通道传输保活心跳按周期发送。
func TestWSHeartbeatKeepAlive(t *testing.T) {
	s := startWSServer(t)
	c, err := DialWS(s.addr(), "/ws", WithHeartbeatInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case <-s.echo.pingSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("心跳未按周期发送")
	}
}

// TestWSReconnect 验证 WS 通道断线自动重连（kick → Reconnecting → Connected → 请求恢复）。
func TestWSReconnect(t *testing.T) {
	s := startWSServer(t)
	c, err := DialWS(s.addr(), "/ws",
		WithHeartbeatInterval(0),
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("kick: %v", err)
	}
	waitReconnecting(t, c)
	waitConnected(t, c)

	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "r2"}, &resp); err != nil {
		t.Fatalf("重连后 Invoke: %v", err)
	}
	if resp.Msg != "r2" {
		t.Fatalf("重连后回显不一致: %+v", resp)
	}
	if got := s.connN.Load(); got < 2 {
		t.Fatalf("服务端升级连接数 = %d, 期望 ≥ 2（发生过重连）", got)
	}
}

// TestWSDialWrongPath 验证路径包装：服务端仅接受 /ws，错误路径拨号失败。
func TestWSDialWrongPath(t *testing.T) {
	s := startWSServer(t)
	if _, err := DialWS(s.addr(), "/wrong", WithHeartbeatInterval(0)); err == nil {
		t.Fatal("错误路径拨号应失败")
	}
}

// TestWSURLForms 验证拨号地址的两种形态等价（host:port+path 与完整 ws:// URL）。
func TestWSURLForms(t *testing.T) {
	s := startWSServer(t)

	// 完整 URL 形态（path 忽略）。
	c1, err := DialWS("ws://"+s.addr()+"/ws", "", WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("完整 URL 拨号: %v", err)
	}
	defer func() { _ = c1.Close() }()
	if err := c1.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("完整 URL Invoke: %v", err)
	}

	// 空路径默认 /ws（对齐模板网关约定）。
	c2, err := DialWS(s.addr(), "", WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("默认路径拨号: %v", err)
	}
	defer func() { _ = c2.Close() }()
	if err := c2.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("默认路径 Invoke: %v", err)
	}
}

// TestDualWSBattle 是 v0.3 验收①的编排形态：dual 编排 + WS 战斗通道。
// kick 战斗（WS）不影响业务（TCP）；战斗独立重连恢复。
func TestDualWSBattle(t *testing.T) {
	biz := startFakeServer(t)
	bat := startWSServer(t)

	c, err := DialDual(
		ChannelConfig{Transport: TransportTCP, Addr: biz.addr()},
		ChannelConfig{Transport: TransportWS, Addr: bat.addr(), Path: "/ws"},
		WithHeartbeatInterval(0),
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 业务 Invoke（TCP）与战斗视图 Invoke（WS）各自成功。
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "biz"}, &resp); err != nil {
		t.Fatalf("业务 Invoke: %v", err)
	}
	if err := c.Channel(KindBattle).Invoke(context.Background(), "echo", map[string]string{"msg": "bat"}, &resp); err != nil {
		t.Fatalf("战斗 Invoke: %v", err)
	}

	// kick 战斗（WS）：战斗重连、业务不受影响、聚合状态向下降级。
	if err := c.Channel(KindBattle).Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("kick: %v", err)
	}
	waitState(t, "战斗通道", func() State { return c.Channel(KindBattle).State() }, StateReconnecting)
	if got := c.Channel(KindBusiness).State(); got != StateConnected {
		t.Fatalf("业务通道状态 = %s, 期望 connected", got)
	}
	if got := c.State(); got != StateReconnecting {
		t.Fatalf("聚合状态 = %s, 期望 reconnecting", got)
	}

	// 战斗恢复后可用。
	waitState(t, "战斗通道重连", func() State { return c.Channel(KindBattle).State() }, StateConnected)
	if err := c.Channel(KindBattle).Invoke(context.Background(), "echo", map[string]string{"msg": "r2"}, &resp); err != nil {
		t.Fatalf("战斗重连后 Invoke: %v", err)
	}
	if resp.Msg != "r2" {
		t.Fatalf("战斗重连后回显不一致: %+v", resp)
	}
}
