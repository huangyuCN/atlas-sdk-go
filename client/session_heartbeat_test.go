package client

import (
	"sync/atomic"
	"testing"
	"time"
)

// 会话心跳测试（评审 v0.3-B3 配套）：WithSessionHeartbeat 的调度、
// 业务错误触发重登钩子、网络错误静默、opFactory 未就绪跳过、仅业务通道生效。

// TestSessionHeartbeatInvokesBusinessOp 验证会话心跳按周期在业务通道发起请求。
func TestSessionHeartbeatInvokesBusinessOp(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr(),
		WithHeartbeatInterval(0), // 关闭传输心跳，隔离会话心跳观测
		WithSessionHeartbeat(20*time.Millisecond, func() (string, any) {
			return "echo", map[string]string{"msg": "hb"}
		}),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.requestCount() >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("会话心跳未按周期发起请求（服务端收到 %d 条）", s.requestCount())
}

// TestSessionHeartbeatBusinessErrorTriggersHook 验证会话心跳收到业务拒绝
// （如会话过期）时触发重登钩子（规范 §5.2 会话失效语义）。
func TestSessionHeartbeatBusinessErrorTriggersHook(t *testing.T) {
	s := startFakeServer(t)
	var hookN atomic.Int32
	c, err := Dial(s.addr(),
		WithHeartbeatInterval(0),
		WithInvokeTimeout(500*time.Millisecond),
		WithSessionHeartbeat(20*time.Millisecond, func() (string, any) {
			return "boom", nil // 服务端对 boom 回业务错误 Status
		}),
		WithOnReconnected(func() error { hookN.Add(1); return nil }),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hookN.Load() >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("会话心跳业务错误未触发重登钩子（执行 %d 次）", hookN.Load())
}

// TestSessionHeartbeatNetworkErrorSilent 验证网络类失败（超时）静默：
// 不触发重登钩子（连接存续由重连机制与传输心跳负责）。
func TestSessionHeartbeatNetworkErrorSilent(t *testing.T) {
	ln := startSilentServer(t) // 只收不回：Invoke 必然超时
	var hookN atomic.Int32
	c, err := Dial(ln.Addr().String(),
		WithHeartbeatInterval(0),
		WithInvokeTimeout(30*time.Millisecond),
		WithSessionHeartbeat(50*time.Millisecond, func() (string, any) {
			return "echo", nil
		}),
		WithOnReconnected(func() error { hookN.Add(1); return nil }),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	time.Sleep(300 * time.Millisecond)
	if got := hookN.Load(); got != 0 {
		t.Fatalf("网络类失败不应触发重登钩子（执行 %d 次）", got)
	}
}

// TestSessionHeartbeatSkipsEmptyOp 验证 opFactory 返回空 operation 时跳过本轮
// （业务侧尚未登录、无 token 可带的场景）。
func TestSessionHeartbeatSkipsEmptyOp(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr(),
		WithHeartbeatInterval(0),
		WithSessionHeartbeat(20*time.Millisecond, func() (string, any) {
			return "", nil // 未就绪
		}),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	time.Sleep(200 * time.Millisecond)
	if got := s.requestCount(); got != 0 {
		t.Fatalf("opFactory 未就绪不应发请求（服务端收到 %d 条）", got)
	}
}

// TestSessionHeartbeatBusinessChannelOnly 验证会话心跳仅业务通道生效
// （规范 §5.2：会话绑定业务通道；顶层配置时战斗通道不得运行会话心跳）。
func TestSessionHeartbeatBusinessChannelOnly(t *testing.T) {
	biz := startFakeServer(t)
	bat := startFakeServer(t)
	c, err := DialDual(
		ChannelConfig{Addr: biz.addr()},
		ChannelConfig{Addr: bat.addr()},
		WithHeartbeatInterval(0),
		WithSessionHeartbeat(20*time.Millisecond, func() (string, any) {
			return "echo", nil
		}),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}
	defer func() { _ = c.Close() }()

	time.Sleep(300 * time.Millisecond)
	if got := biz.requestCount(); got < 1 {
		t.Fatalf("业务通道未收到会话心跳（%d 条）", got)
	}
	if got := bat.requestCount(); got != 0 {
		t.Fatalf("战斗通道不应运行会话心跳（收到 %d 条）", got)
	}
}
