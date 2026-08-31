package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// dual 双通道编排测试（v0.3）：Client 拆为「编排器 + Channel 连接本体」后——
//   - Invoke/On 默认走业务通道，现有调用方零改动；
//   - 每条通道独立连接、心跳、重连与请求排队；
//   - Client.State() 聚合向下降级，细粒度走 Channel(kind).State()（规范 §5.1/§5.2）；
//   - 断线重连对每条通道独立成立：kick 通道 A 不影响通道 B 的 in-flight（v0.3 验收②）。

// waitState 轮询等待任意状态源流转到位（消除「断连与回包」的窗口竞态，
// 与 reconnect_test.go 的 waitReconnecting 同一纪律）。
func waitState(t *testing.T, what string, fn func() State, want State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := fn(); got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待 %s → %s 超时，当前 %s", what, want, fn())
}

// TestDialDualConnectAndInvoke 验证 dual 基础编排：双通道各自 Connected、
// Client.Invoke/On 默认走业务通道、战斗视图 Invoke/On 独立工作、未知 kind 返回 nil。
func TestDialDualConnectAndInvoke(t *testing.T) {
	biz := startFakeServer(t)
	bat := startFakeServer(t)

	c, err := DialDual(
		ChannelConfig{Transport: TransportTCP, Addr: biz.addr()},
		ChannelConfig{Transport: TransportTCP, Addr: bat.addr()},
		WithHeartbeatInterval(0),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}
	defer func() { _ = c.Close() }()

	if got := c.State(); got != StateConnected {
		t.Fatalf("Client.State() = %s, 期望 connected", got)
	}
	if got := c.Channel(KindBusiness).State(); got != StateConnected {
		t.Fatalf("业务通道状态 = %s, 期望 connected", got)
	}
	if got := c.Channel(KindBattle).State(); got != StateConnected {
		t.Fatalf("战斗通道状态 = %s, 期望 connected", got)
	}

	// Client.Invoke 默认业务通道：仅业务服务端收到请求。
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "hi"}, &resp); err != nil {
		t.Fatalf("Client.Invoke: %v", err)
	}
	if resp.Msg != "hi" {
		t.Fatalf("业务回显不一致: %+v", resp)
	}
	if n := bat.requestCount(); n != 0 {
		t.Fatalf("战斗服务端不应收到业务请求，实际 %d 条", n)
	}

	// 战斗视图 Invoke 独立工作（请求只到达战斗服务端）。
	var resp2 struct {
		Msg string `json:"msg"`
	}
	if err := c.Channel(KindBattle).Invoke(context.Background(), "echo", map[string]string{"msg": "go"}, &resp2); err != nil {
		t.Fatalf("战斗视图 Invoke: %v", err)
	}
	if resp2.Msg != "go" {
		t.Fatalf("战斗回显不一致: %+v", resp2)
	}
	if n := biz.requestCount(); n != 1 {
		t.Fatalf("业务服务端请求数 = %d, 期望 1", n)
	}

	// Client.On 默认业务通道；战斗视图 On 独立订阅，互不串扰。
	bizGot := make(chan string, 1)
	batGot := make(chan string, 1)
	offBiz := c.On("/push.test", func(_ string, payload []byte) { bizGot <- string(payload) })
	defer offBiz()
	offBat := c.Channel(KindBattle).On("/push.test", func(_ string, payload []byte) { batGot <- string(payload) })
	defer offBat()

	if err := c.Channel(KindBattle).Invoke(context.Background(), "notify-op", nil, nil); err != nil {
		t.Fatalf("战斗视图 notify-op: %v", err)
	}
	select {
	case p := <-batGot:
		if p != `{"k":"v"}` {
			t.Fatalf("战斗推送 payload 不一致: %q", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("战斗视图未收到推送")
	}
	select {
	case p := <-bizGot:
		t.Fatalf("战斗通道推送不应串扰业务订阅: %q", p)
	case <-time.After(300 * time.Millisecond):
	}

	// 未知 kind：返回 nil 视图（方法 nil 安全）。
	if v := c.Channel(Kind(9)); v != nil {
		t.Fatal("未知 kind 应返回 nil 视图")
	}
	if err := c.Channel(Kind(9)).Invoke(context.Background(), "echo", nil, nil); err == nil {
		t.Fatal("nil 视图 Invoke 应返回错误")
	}
	if got := c.Channel(Kind(9)).State(); got != StateDisconnected {
		t.Fatalf("nil 视图状态 = %s, 期望 disconnected", got)
	}
}

// TestDualKickBattleKeepsBusinessInflight 是 v0.3 验收②：
// kick 战斗通道不影响业务通道的 in-flight；两通道状态独立、聚合状态向下降级。
func TestDualKickBattleKeepsBusinessInflight(t *testing.T) {
	biz := startFakeServer(t)
	bat := startReconnServer(t)

	c, err := DialDual(
		ChannelConfig{Addr: biz.addr()},
		ChannelConfig{Addr: bat.addr()},
		WithHeartbeatInterval(0),
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 业务通道挂起一个 in-flight（服务端等待放行信号；holdSeen/release 由
	// startFakeServer 构造期初始化，避免与服务端 goroutine 产生数据竞争）。
	result := make(chan error, 1)
	go func() { result <- c.Invoke(context.Background(), "hold", nil, nil) }()
	select {
	case <-biz.holdSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("hold 请求未到达业务服务端")
	}

	// kick 战斗通道：战斗进入重连，业务不受影响。
	if err := c.Channel(KindBattle).Invoke(context.Background(), "kick", nil, nil); err != nil {
		t.Fatalf("kick: %v", err)
	}
	waitState(t, "战斗通道", func() State { return c.Channel(KindBattle).State() }, StateReconnecting)
	if got := c.State(); got != StateReconnecting {
		t.Fatalf("聚合状态 = %s, 期望 reconnecting（任一通道非 Connected 向下降级）", got)
	}
	if got := c.Channel(KindBusiness).State(); got != StateConnected {
		t.Fatalf("业务通道状态 = %s, 期望 connected（战斗 kick 不应影响业务）", got)
	}

	// 业务 in-flight 正常完成。
	close(biz.release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("战斗 kick 不应影响业务 in-flight: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("业务 in-flight 未返回")
	}

	// 战斗通道自动重连成功并恢复可用。
	waitState(t, "战斗通道重连", func() State { return c.Channel(KindBattle).State() }, StateConnected)
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Channel(KindBattle).Invoke(context.Background(), "echo", map[string]string{"msg": "r2"}, &resp); err != nil {
		t.Fatalf("战斗重连后 Invoke: %v", err)
	}
	if resp.Msg != "r2" {
		t.Fatalf("战斗重连后回显不一致: %+v", resp)
	}
	if got := c.State(); got != StateConnected {
		t.Fatalf("恢复后聚合状态 = %s, 期望 connected", got)
	}
}

// TestDualPerChannelHooks 验证每通道独立重连钩子（业务通道重登 / 战斗通道 Join 重绑定，
// 规范 §5.2 dual 双通道独立重连）：kick 战斗只触发战斗钩子。
func TestDualPerChannelHooks(t *testing.T) {
	biz := startRestartableServer(t)
	bat := startRestartableServer(t)

	var reloginN, joinN atomic.Int32
	c, err := DialDual(
		ChannelConfig{
			Addr: biz.ln.Addr().String(),
			Opts: []Option{WithOnReconnected(func() error { reloginN.Add(1); return nil })},
		},
		ChannelConfig{
			Addr: bat.ln.Addr().String(),
			Opts: []Option{WithOnReconnected(func() error { joinN.Add(1); return nil })},
		},
		WithHeartbeatInterval(0),
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 预热：两通道各做一次往返，确保服务端已完成健康 accept
	//（消除「Dial 与 down() 竞争 accept 循环」的测试窗口竞态，与 reconnect 系测试纪律一致）。
	var warm struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "b"}, &warm); err != nil {
		t.Fatalf("业务通道预热: %v", err)
	}
	if err := c.Channel(KindBattle).Invoke(context.Background(), "echo", map[string]string{"msg": "j"}, &warm); err != nil {
		t.Fatalf("战斗通道预热: %v", err)
	}

	// 只断开战斗通道：join 钩子触发，重登钩子不动。
	bat.down()
	time.Sleep(100 * time.Millisecond)
	bat.restore()
	waitAcceptN(t, bat, 2)
	waitState(t, "战斗通道重连", func() State { return c.Channel(KindBattle).State() }, StateConnected)

	// 钩子按「每次连接建立」触发：服务端不可用窗口内每次瞬态连接都会执行一次
	// （钩子失败→弃用该连接→继续退避，v0.2 既有语义），故计数断言用下界。
	if got := joinN.Load(); got < 1 {
		t.Fatalf("战斗通道 Join 重绑定钩子未执行（%d 次）", got)
	}
	if got := reloginN.Load(); got != 0 {
		t.Fatalf("战斗通道断连不应触发业务重登钩子（执行 %d 次）", got)
	}
	joinAfterBat := joinN.Load()

	// 再断开业务通道：重登钩子触发，且业务重登成功后自动链式触发战斗重绑
	// （评审 v0.3-B2 修复：规范 §5.2「业务通道重连成功后由 SDK 自动对战斗通道
	// 执行重新绑定」——业务钩子成功后链式调用战斗钩子）。
	biz.down()
	time.Sleep(100 * time.Millisecond)
	biz.restore()
	waitAcceptN(t, biz, 2)
	waitState(t, "业务通道重连", func() State { return c.Channel(KindBusiness).State() }, StateConnected)

	if got := reloginN.Load(); got < 1 {
		t.Fatalf("业务通道重登钩子未执行（%d 次）", got)
	}
	// 链式编排：业务重登成功 → 战斗钩子被自动触发（joinN 递增）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if joinN.Load() > joinAfterBat {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := joinN.Load(); got <= joinAfterBat {
		t.Fatalf("业务重登成功后应自动触发战斗重绑（评审 v0.3-B2）：join %d → %d", joinAfterBat, got)
	}
}

// TestDualPerChannelTimeoutOverride 验证通道级 Option 覆盖：
// 战斗通道经 ChannelConfig.Opts 配置短超时，业务通道保持默认。
func TestDualPerChannelTimeoutOverride(t *testing.T) {
	biz := startFakeServer(t)
	batLn := startSilentServer(t) // 战斗通道：只收不回，必然超时

	c, err := DialDual(
		ChannelConfig{Addr: biz.addr()},
		ChannelConfig{
			Addr: batLn.Addr().String(),
			Opts: []Option{WithInvokeTimeout(100 * time.Millisecond)}, // 战斗高频短超时
		},
		WithHeartbeatInterval(0),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	err = c.Channel(KindBattle).Invoke(context.Background(), "never", nil, nil)
	elapsed := time.Since(start)
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("期望 TimeoutError, 实际 %T: %v", err, err)
	}
	if elapsed > time.Second {
		t.Fatalf("战斗通道短超时未生效: 耗时 %s", elapsed)
	}

	// 业务通道默认配置不受覆盖影响。
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "ok"}, &resp); err != nil {
		t.Fatalf("业务通道应不受战斗通道配置影响: %v", err)
	}
}

// TestDualCloseClosesAllChannels 验证 Client.Close 关闭全部通道且幂等。
func TestDualCloseClosesAllChannels(t *testing.T) {
	biz := startFakeServer(t)
	bat := startFakeServer(t)

	c, err := DialDual(
		ChannelConfig{Addr: biz.addr()},
		ChannelConfig{Addr: bat.addr()},
		WithHeartbeatInterval(0),
	)
	if err != nil {
		t.Fatalf("DialDual: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil { // 幂等
		t.Fatalf("重复 Close: %v", err)
	}
	if got := c.State(); got != StateDisconnected {
		t.Fatalf("关闭后聚合状态 = %s, 期望 disconnected", got)
	}
	if got := c.Channel(KindBusiness).State(); got != StateDisconnected {
		t.Fatalf("关闭后业务通道状态 = %s, 期望 disconnected", got)
	}
	if got := c.Channel(KindBattle).State(); got != StateDisconnected {
		t.Fatalf("关闭后战斗通道状态 = %s, 期望 disconnected", got)
	}
}

// TestDialDualRollbackOnFailure 验证第二通道拨号失败：整体失败且已建通道被回滚关闭。
func TestDialDualRollbackOnFailure(t *testing.T) {
	biz := startFakeServer(t)

	_, err := DialDual(
		ChannelConfig{Addr: biz.addr()},
		ChannelConfig{Addr: "127.0.0.1:1"}, // 端口 1：连接拒绝
		WithHeartbeatInterval(0),
	)
	if err == nil {
		t.Fatal("战斗通道拨号失败应整体失败")
	}

	// 已建业务通道应被关闭（服务端侧观测到连接断开）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if biz.closedCount() >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("拨号失败后已建业务通道未被回滚关闭")
}

// TestDialDualDuplicateKind 验证通道角色冲突被拒绝。
func TestDialDualDuplicateKind(t *testing.T) {
	biz := startFakeServer(t)
	_, err := DialDual(
		ChannelConfig{Kind: KindBattle, Addr: biz.addr()},
		ChannelConfig{Kind: KindBattle, Addr: biz.addr()},
		WithHeartbeatInterval(0),
	)
	if err == nil {
		t.Fatal("通道角色重复应返回错误")
	}
}
