package client

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// reconnServer 是重连测试的服务端替身：同一地址持续接受多代连接，
// 支持 "kick" 操作（回包后主动断开当前连接，模拟服务端踢线/重启）。
type reconnServer struct {
	ln       net.Listener
	conns    chan net.Conn // 每接受一个新连接投递一次（带缓冲）
	connCnt  atomic.Int32
	kickSelf chan struct{}
}

func startReconnServer(t *testing.T) *reconnServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &reconnServer{ln: ln, conns: make(chan net.Conn, 8)}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *reconnServer) addr() string { return s.ln.Addr().String() }

func (s *reconnServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.connCnt.Add(1)
		select {
		case s.conns <- conn:
		default:
		}
		go s.handle(conn)
	}
}

// handle 回显协议：echo 回显；kick 回空包后立即断开当前连接。
func (s *reconnServer) handle(conn net.Conn) {
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
		switch op {
		case "kick":
			_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq},
				replyBytes(nil, nil), frame.MaxBodySize)
			return // defer 关闭连接 → 客户端侧读循环退出
		case "notify-op":
			// 先推一条 Notify 再回响应（与 client_test 的 fakeServer 同款语义），
			// 支撑「订阅重放」真实验证（评审 B5）。
			nb, _ := frame.BuildRequestBody("/push.test", []byte(`{"k":"v"}`))
			_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeNotify, Seq: 999}, nb, frame.MaxBodySize)
			_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq},
				replyBytes(nil, nil), frame.MaxBodySize)
		default:
			_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq},
				replyBytes(nil, payload), frame.MaxBodySize)
		}
	}
}

// waitConnected 等待客户端进入 Connected（重连成功）。
func waitConnected(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.State() == StateConnected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 失败现场：dump 全部 goroutine 栈定位卡点（临时诊断）。
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Fatalf("等待重连成功超时，当前状态 %s\n%s", c.State(), buf[:n])
}

// TestAutoReconnect 验证：服务端断开后自动重连、状态机流转、订阅在新连接继续生效。
func TestAutoReconnect(t *testing.T) {
	s := startReconnServer(t)
	c, err := Dial(s.addr(), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	notified := make(chan string, 1)
	off := c.On("/push.test", func(_ string, payload []byte) {
		notified <- string(payload)
	})
	defer off()

	if got := c.State(); got != StateConnected {
		t.Fatalf("初始状态 = %s, 期望 connected", got)
	}

	// 第一代连接：kick 触发服务端断开。
	if err := c.Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("kick: %v", err)
	}

	// 等断连被感知（Reconnecting）与自动重连成功（Connected）。
	waitReconnecting(t, c)
	waitConnected(t, c)

	// 自动重连成功后，Invoke 与 Notify 均在新连接上工作。
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "r2"}, &resp); err != nil {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("重连后 Invoke: %v\n%s", err, buf[:n])
	}
	if resp.Msg != "r2" {
		t.Fatalf("回显不一致: %+v", resp)
	}

	// 订阅重放（评审 B5 修复：真实验证而非空引用）：
	// notify-op 会先推一条 Notify 再回响应——重连后发送它，
	// 断言订阅表（Client 级保留、跨连接重放）把 Notify 送达了 handler。
	if err := c.Invoke(context.Background(), "notify-op", nil, nil); err != nil {
		t.Fatalf("重连后 notify-op: %v", err)
	}
	select {
	case got := <-notified:
		if got == "" {
			t.Fatal("重连后 Notify 送达但 payload 为空")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("订阅重放失效：重连后 2s 内未收到 Notify（评审 B5）")
	}
}

// waitReconnecting 等待客户端进入 Reconnecting（断连已被感知）。
func waitReconnecting(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.State() == StateReconnecting {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待 Reconnecting 超时，当前状态 %s", c.State())
}

// TestReconnectQueue 验证断线期间 Invoke 排队、重连成功后按序重发成功。
func TestReconnectQueue(t *testing.T) {
	s := startReconnServer(t)
	c, err := Dial(s.addr(), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// kick：回包后服务端断开；等断连被感知后再发起排队请求（消除窗口竞态）。
	if err := c.Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("kick: %v", err)
	}
	waitReconnecting(t, c)

	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "queued"}, nil); err != nil {
		t.Fatalf("排队请求应在重连后成功: %v", err)
	}
	waitConnected(t, c)
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "after"}, &resp); err != nil {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("重连后 Invoke: %v\n状态=%s\n%s", err, c.State(), buf[:n])
	}
	if resp.Msg != "after" {
		t.Fatalf("回显不一致: %+v", resp)
	}
}

// TestFailFastDuringReconnect 验证 failFast 在断线期间立即失败而非排队。
func TestFailFastDuringReconnect(t *testing.T) {
	s := startReconnServer(t)
	c, err := Dial(s.addr(), WithBackoff(200*time.Millisecond, 500*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("kick: %v", err)
	}
	waitReconnecting(t, c)

	start := time.Now()
	err = c.Invoke(context.Background(), "echo", nil, nil, WithFailFast())
	if err == nil {
		t.Fatal("Reconnecting 期间 failFast 应立即失败")
	}
	var ne *NetworkError
	if !errors.As(err, &ne) {
		t.Fatalf("期望 NetworkError, 实际 %T: %v", err, err)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("failFast 应立即失败, 实际耗时 %s", elapsed)
	}
}

// TestCloseStopsReconnect 验证关闭后不再重连（supervisor 退出）。
func TestCloseStopsReconnect(t *testing.T) {
	s := startReconnServer(t)
	c, err := Dial(s.addr(), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("kick: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	before := s.connCnt.Load()
	time.Sleep(300 * time.Millisecond) // 若仍在重连，服务端会看到新连接
	if after := s.connCnt.Load(); after != before {
		t.Fatalf("Close 后不应继续重连: 连接数 %d → %d", before, after)
	}
	if got := c.State(); got != StateDisconnected {
		t.Fatalf("关闭后状态 = %s, 期望 disconnected", got)
	}
}
