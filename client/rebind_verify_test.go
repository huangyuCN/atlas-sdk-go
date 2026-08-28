package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// startRestartableServer 可断开/恢复的 echo 服务端（重连验证共用）。
type restartableServer struct {
	ln      net.Listener
	mu      sync.Mutex
	conns   []net.Conn
	healthy atomic.Bool
	acceptN atomic.Int32
}

func startRestartableServer(t *testing.T) *restartableServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &restartableServer{ln: ln}
	s.healthy.Store(true)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			if !s.healthy.Load() {
				_ = conn.Close()
				continue
			}
			s.acceptN.Add(1)
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			go s.handle(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *restartableServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	for {
		hdr, body, err := frame.Read(conn, frame.MaxBodySize)
		if err != nil {
			return
		}
		if hdr.Type != frame.MsgTypeRequest {
			continue
		}
		_, payload, err := frame.ParseRequestBody(body)
		if err != nil {
			return
		}
		reply := make([]byte, 5+len(payload))
		reply[0] = 0
		reply[1] = byte(len(payload) >> 24)
		reply[2] = byte(len(payload) >> 16)
		reply[3] = byte(len(payload) >> 8)
		reply[4] = byte(len(payload))
		copy(reply[5:], payload)
		_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq}, reply, frame.MaxBodySize)
	}
}

func (s *restartableServer) acceptCount() int32 { return s.acceptN.Load() }

func (s *restartableServer) dropAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
}

func (s *restartableServer) restore() { s.healthy.Store(true) }
func (s *restartableServer) down() {
	s.healthy.Store(false)
	s.dropAll()
}

// TestRebindHookInvoke 验证评审 B1 修复：钩子内 Invoke 正常完成（状态已 Connected）。
func TestRebindHookInvoke(t *testing.T) {
	s := startRestartableServer(t)
	var clientPtr atomic.Pointer[Client]
	relogged := atomic.Bool{}

	c, err := Dial(s.ln.Addr().String(),
		WithHeartbeatInterval(0),
		WithInvokeTimeout(500*time.Millisecond),
		WithBackoff(50*time.Millisecond, 200*time.Millisecond),
		WithOnReconnected(func() error {
			relogged.Store(true)
			return clientPtr.Load().Invoke(context.Background(), "login", nil, nil)
		}),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	clientPtr.Store(c)
	defer func() { _ = c.Close() }()

	if err := c.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("首请求: %v", err)
	}
	s.down()
	time.Sleep(100 * time.Millisecond)
	s.restore()
	waitAcceptN(t, s, 2)

	// 钩子内 Invoke 应已完成（重登成功），且后续请求正常
	if !relogged.Load() {
		t.Fatal("钩子未执行")
	}
	done := make(chan error, 1)
	go func() { done <- c.Invoke(context.Background(), "echo", nil, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("重连后请求失败: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B1 未修复：重连后 Invoke 3s 未返回")
	}
}

// TestCloseInsideRebindHook 验证评审 B2 修复：钩子内 Close 不死锁。
func TestCloseInsideRebindHook(t *testing.T) {
	s := startRestartableServer(t)
	var clientPtr atomic.Pointer[Client]
	hookRan := atomic.Bool{}
	closeReturned := atomic.Bool{}

	c, err := Dial(s.ln.Addr().String(),
		WithHeartbeatInterval(0),
		WithBackoff(50*time.Millisecond, 200*time.Millisecond),
		WithOnReconnected(func() error {
			hookRan.Store(true)
			_ = clientPtr.Load().Close()
			closeReturned.Store(true)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	clientPtr.Store(c)

	if err := c.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("首请求: %v", err)
	}
	s.down()
	// 等待钩子执行（重连可能在 healthy=false 期间就成功——服务端 accept 后立即关闭，
	// 客户端视角连接已建立，钩子随即调度）
	hookDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(hookDeadline) {
		if hookRan.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !hookRan.Load() {
		t.Fatal("钩子未执行")
	}
	// 钩子内 Close：异步执行器监听 closeCh，不再自等待（评审 B2 修复验证点）
	closeDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(closeDeadline) {
		if closeReturned.Load() {
			return // 修复确认
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("B2 未修复：钩子内 Close 3s 未返回")
}

func waitAcceptN(t *testing.T, s *restartableServer, n int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if s.acceptN.Load() >= n {
			time.Sleep(200 * time.Millisecond) // 留出钩子执行时间
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("服务端未收到第 %d 次连接", n)
}

// TestFatalReplyTerminatesClient 验证评审 B5 修复：非法 Reply 包络终止整个 Client（规范 §7）。
func TestFatalReplyTerminatesClient(t *testing.T) {
	// 服务端回一个损坏的 Reply 包络（hasError 后 dataLen 声称大于实际）
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		hdr, _, err := frame.Read(conn, frame.MaxBodySize)
		if err != nil {
			return
		}
		bad := []byte{0, 0xFF, 0xFF, 0xFF, 0xFF} // dataLen=4G 但无数据：截断 → ProtocolError
		_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq}, bad, frame.MaxBodySize)
		block := make(chan struct{})
		<-block // 保持连接打开（验证 Client 主动终止而非等 EOF）
	}()

	c, err := Dial(ln.Addr().String(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "echo", nil, nil)
	if err == nil {
		t.Fatal("非法包络应返回错误")
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("期望 ProtocolError, 实际 %T: %v", err, err)
	}
	// Client 应被终止（不重连）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.State() == StateDisconnected {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("B5 未修复：包络非法后 state=%s（应 disconnected 且不重连）", c.State())
}

// TestRebindHookFailureRetries 验证评审 Important 修复：钩子返回错误继续退避重连。
func TestRebindHookFailureRetries(t *testing.T) {
	s := startRestartableServer(t)
	var clientPtr atomic.Pointer[Client]
	hookCount := atomic.Int32{}

	c, err := Dial(s.ln.Addr().String(),
		WithHeartbeatInterval(0),
		WithInvokeTimeout(500*time.Millisecond),
		WithBackoff(50*time.Millisecond, 200*time.Millisecond),
		WithOnReconnected(func() error {
			// 前两次失败，第三次成功
			if hookCount.Add(1) < 3 {
				return errors.New("重登失败（模拟）")
			}
			return clientPtr.Load().Invoke(context.Background(), "login", nil, nil)
		}),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	clientPtr.Store(c)
	defer func() { _ = c.Close() }()

	if err := c.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("首请求: %v", err)
	}
	s.down()
	time.Sleep(100 * time.Millisecond)
	s.restore() // 恢复服务端：让钩子重试纯粹对抗「钩子自身失败」而非服务端不可达

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if hookCount.Load() >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := hookCount.Load(); got < 3 {
		t.Fatalf("钩子失败后应继续退避重试: 实际执行 %d 次", got)
	}
	// 第三次成功后客户端应可用
	done := make(chan error, 1)
	go func() { done <- c.Invoke(context.Background(), "echo", nil, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("重登成功后请求失败: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("重登成功后 Invoke 3s 未返回")
	}
}

// TestQueuedInvokeTimeout 验证排队请求计入单次超时（评审 Important 修复：不再无限等待重连）。
func TestQueuedInvokeTimeout(t *testing.T) {
	s := startRestartableServer(t)
	c, err := Dial(s.ln.Addr().String(),
		WithHeartbeatInterval(0),
		WithInvokeTimeout(300*time.Millisecond),     // 排队期限 = 单次超时
		WithBackoff(10*time.Second, 20*time.Second), // 重连极慢，强制排队超时
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("首请求: %v", err)
	}
	s.down()
	time.Sleep(100 * time.Millisecond) // 进入 Reconnecting

	start := time.Now()
	err = c.Invoke(context.Background(), "echo", nil, nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("排队超时应返回错误")
	}
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("期望 TimeoutError, 实际 %T: %v", err, err)
	}
	// 排队期限 = invokeTimeout(300ms)，不应等到重连（10s+）
	if elapsed > 2*time.Second {
		t.Fatalf("排队超时应约 300ms, 实际 %s（未计入排队期限）", elapsed)
	}
}
