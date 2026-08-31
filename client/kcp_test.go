package client

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
	kcpgo "github.com/xtaci/kcp-go/v5"
)

// kcpTestServer 是 KCP 测试服务端：kcp-go 监听 + frame.Read/Write 流式分帧，
// operation 语义经 echoService 与 TCP 替身保持一致（服务端基线：明文、无 FEC）。
type kcpTestServer struct {
	ln    *kcpgo.Listener
	echo  echoService
	sessN atomic.Int32 // 接受的会话数（重连观测用）
}

func startKCPServer(t *testing.T) *kcpTestServer {
	t.Helper()
	ln, err := kcpgo.ListenWithOptions("127.0.0.1:0", nil, 0, 0) // 与服务端基线一致：明文、无 FEC
	if err != nil {
		t.Fatalf("KCP 监听失败: %v", err)
	}
	s := &kcpTestServer{ln: ln, echo: echoService{pingSeen: make(chan struct{}, 1)}}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *kcpTestServer) addr() string { return s.ln.Addr().String() }

func (s *kcpTestServer) serve() {
	for {
		sess, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.sessN.Add(1)
		go s.handle(sess)
	}
}

func (s *kcpTestServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	for {
		hdr, body, err := frame.Read(conn, frame.MaxBodySize)
		if err != nil {
			return
		}
		if hdr.Type != frame.MsgTypeRequest {
			continue
		}
		op, payload, err := frame.ParseRequestBody(body)
		if err != nil {
			return
		}
		resp, notifyBody, closeConn := s.echo.handleRequest(op, payload)
		if notifyBody != nil {
			_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeNotify, Seq: 999}, notifyBody, frame.MaxBodySize)
		}
		_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq}, resp, frame.MaxBodySize)
		if closeConn {
			return // kick：回包后断开（客户端读侧 EOF → 自动重连）
		}
	}
}

// TestKCPDialInvokeEcho 验证 KCP 通道请求-响应闭环（流式分帧与 TCP 同构）。
func TestKCPDialInvokeEcho(t *testing.T) {
	s := startKCPServer(t)
	c, err := DialKCP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialKCP: %v", err)
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

// TestKCPBusinessError 验证 KCP 通道业务拒绝还原为 *BusinessError。
func TestKCPBusinessError(t *testing.T) {
	s := startKCPServer(t)
	c, err := DialKCP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialKCP: %v", err)
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

// TestKCPHeartbeatKeepAlive 验证 KCP 通道传输保活心跳按周期发送。
func TestKCPHeartbeatKeepAlive(t *testing.T) {
	s := startKCPServer(t)
	c, err := DialKCP(s.addr(), WithHeartbeatInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("DialKCP: %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case <-s.echo.pingSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("心跳未按周期发送")
	}
}

// TestKCPNotifyDispatch 验证 KCP 通道 Notify 按 operation 分发。
func TestKCPNotifyDispatch(t *testing.T) {
	s := startKCPServer(t)
	c, err := DialKCP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialKCP: %v", err)
	}
	defer func() { _ = c.Close() }()

	got := make(chan string, 1)
	off := c.On("/push.test", func(_ string, payload []byte) { got <- string(payload) })
	defer off()

	if err := c.Invoke(context.Background(), "notify-op", nil, nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	select {
	case p := <-got:
		if p != `{"k":"v"}` {
			t.Fatalf("Notify payload 不一致: %q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("等待 Notify 超时")
	}
}

// TestKCPKickReconnect 验证 KCP 会话断开后自动重连。
// KCP 无连接关闭通知（服务端 Close 不告知对端），且回包可能未冲刷即丢——
// 死链只能由传输心跳连续失败发现（规范 §5.2），故本用例必须开启心跳。
func TestKCPKickReconnect(t *testing.T) {
	s := startKCPServer(t)
	c, err := DialKCP(s.addr(),
		WithHeartbeatInterval(50*time.Millisecond),
		WithInvokeTimeout(200*time.Millisecond), // 覆盖 KCP 40ms flush 间隔的回包窗口
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialKCP: %v", err)
	}
	defer func() { _ = c.Close() }()

	// kick：回包可能丢失，Invoke 结果不判定（超时属合法结果）。
	_ = c.Invoke(context.Background(), "kick", nil, nil)
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
	if got := s.sessN.Load(); got < 2 {
		t.Fatalf("服务端会话数 = %d, 期望 ≥ 2（发生过重连）", got)
	}
}

// TestDualTCPBusinessKCPBattle 验证 dual 编排的模板主形态：业务 TCP + 战斗 KCP。
func TestDualTCPBusinessKCPBattle(t *testing.T) {
	biz := startFakeServer(t)
	bat := startKCPServer(t)

	c, err := DialDual(
		ChannelConfig{Transport: TransportTCP, Addr: biz.addr()},
		ChannelConfig{
			Transport: TransportKCP,
			Addr:      bat.addr(),
			// KCP 死链只能由传输心跳发现（无关闭通知），战斗通道开启快节奏心跳。
			Opts: []Option{
				WithHeartbeatInterval(50 * time.Millisecond),
				WithInvokeTimeout(200 * time.Millisecond),
			},
		},
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}
	defer func() { _ = c.Close() }()

	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "biz"}, &resp); err != nil {
		t.Fatalf("业务 Invoke: %v", err)
	}
	if err := c.Channel(KindBattle).Invoke(context.Background(), "echo", map[string]string{"msg": "bat"}, &resp); err != nil {
		t.Fatalf("战斗 Invoke: %v", err)
	}

	// kick 战斗（KCP）：回包可能丢失，结果不判定；死链由战斗通道心跳发现。
	_ = c.Channel(KindBattle).Invoke(context.Background(), "kick", nil, nil)
	waitState(t, "战斗通道", func() State { return c.Channel(KindBattle).State() }, StateReconnecting)
	if got := c.Channel(KindBusiness).State(); got != StateConnected {
		t.Fatalf("业务通道状态 = %s, 期望 connected", got)
	}
	if got := c.State(); got != StateReconnecting {
		t.Fatalf("聚合状态 = %s, 期望 reconnecting", got)
	}

	waitState(t, "战斗通道重连", func() State { return c.Channel(KindBattle).State() }, StateConnected)
	if err := c.Channel(KindBattle).Invoke(context.Background(), "echo", map[string]string{"msg": "r2"}, &resp); err != nil {
		t.Fatalf("战斗重连后 Invoke: %v", err)
	}
}

// TestKCPLargeFrameFragmentation 验证评审指出的关键缺口：超过 MTU(1400) 的
// 大帧经 KCP 分片重组后完整往返（「流式分帧与 TCP 同构」的实测证据）。
func TestKCPLargeFrameFragmentation(t *testing.T) {
	s := startKCPServer(t)
	c, err := DialKCP(s.addr())
	if err != nil {
		t.Fatalf("DialKCP: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 3.5×MTU 的 payload：KCP 内部按 MSS 分片、接收侧重组，
	// 帧层应完整透明——双向各一次。
	big := make([]byte, kcpMTU*3+kcpMTU/2)
	for i := range big {
		big[i] = byte(i % 251)
	}
	var got []byte
	if err := c.Invoke(context.Background(), "echo", &big, &got); err != nil {
		t.Fatalf("大帧往返: %v", err)
	}
	if len(got) != len(big) {
		t.Fatalf("大帧长度不一致: got %d, want %d（KCP 分片重组失效）", len(got), len(big))
	}
	for i := range big {
		if got[i] != big[i] {
			t.Fatalf("大帧内容不一致 at %d（分片错位）", i)
		}
	}
}

// TestKCPProtocolErrorTerminates 验证 KCP 通道的协议级错误终止 Client（评审缺口：
// 原仅覆盖 TCP）。服务端回非法 version 的响应帧 → Client terminate 不重连。
func TestKCPProtocolErrorTerminates(t *testing.T) {
	ln, err := kcpgo.ListenWithOptions("127.0.0.1:0", nil, 0, 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		sess, err := ln.AcceptKCP()
		if err != nil {
			return
		}
		sess.SetNoDelay(kcpNoDelay, kcpInterval, kcpResend, kcpNC)
		hdr, _, err := frame.Read(sess, frame.MaxBodySize)
		if err != nil {
			return
		}
		// 非法 version 响应帧
		_ = frame.Write(sess, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq, Version: 99}, nil, frame.MaxBodySize)
		time.Sleep(2 * time.Second) // 保持会话（验证 Client 主动终止而非等 EOF）
	}()

	c, err := DialKCP(ln.Addr().String(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialKCP: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "echo", nil, nil)
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("期望 ProtocolError, 实际 %T: %v", err, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.State() == StateDisconnected {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("KCP 协议错误后 state=%s（应 disconnected 且不重连）", c.State())
}
