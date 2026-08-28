package client

import (
	"net"
	"testing"
	"time"
)

// startSilentServer 起一个只 Accept 不读不回的服务端：客户端写帧不会失败，
// 心跳必然超时（用于复现心跳死锁）。
func startSilentServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() }) // 仅持有连接，不读写
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// TestReproHeartbeatCloseDeadlock 复现评审 Blocker 1：
// 心跳连续失败后 heartbeatLoop 在 wg 内调用 Close()，Close() 的 wg.Wait()
// 等待心跳 goroutine 自身退出 → 确定性死锁。
// 修复后本测试应在远小于 10s 的时间内通过。
func TestReproHeartbeatCloseDeadlock(t *testing.T) {
	ln := startSilentServer(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// 心跳 50ms 一次，连续 3 次失败 ≈ 150ms 后触发死链 Close
		c, err := Dial(ln.Addr().String(),
			WithHeartbeatInterval(50*time.Millisecond),
			WithInvokeTimeout(30*time.Millisecond), // 心跳 Invoke 快速超时
		)
		if err != nil {
			t.Errorf("dial: %v", err)
			return
		}
		_ = c.Close() // 若死锁发生，Close 永远不返回
	}()

	select {
	case <-done:
		// Close 返回了；再给一点时间确认 heartbeatLoop 也正常退出
	case <-time.After(10 * time.Second):
		t.Fatal("死锁复现：Close() 在 10s 内未返回（heartbeatLoop 在 wg 内自等待）")
	}
}

// TestReproFailAllInflightRace 复现评审 Blocker 2/3：
// failAllInflight 的 Range+发送+Delete 非原子——
// 断连瞬间大量 in-flight 与各自超时 timer 竞争同一容量 1 的 channel。
// 若发送方阻塞，读循环/关闭路径卡死，本测试 10s 超时失败。
func TestReproFailAllInflightRace(t *testing.T) {
	ln := startSilentServer(t)
	c, err := Dial(ln.Addr().String(),
		WithHeartbeatInterval(0), // 关闭心跳，专注 in-flight 竞态
		WithInvokeTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	const n = 64
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			// 请求发到 silent server：永远无响应，全部挂在 in-flight 上
			results <- c.Invoke(nil, "silent", nil, nil)
		}()
	}
	// 等 64 个请求全部写入 in-flight（略大于写盘所需时间）
	time.Sleep(100 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		_ = c.Close() // 触发 failAllInflight 与 64 个 timer 的竞态
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("死锁复现：Close() 未返回（failAllInflight 与 timer 双写 channel 阻塞）")
	}

	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err == nil {
				t.Error("断连后 Invoke 应返回错误，got nil")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("死锁复现：第 %d 个 Invoke 的结果 2s 内未送达（发送方阻塞）", i)
		}
	}
}
