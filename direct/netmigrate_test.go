// KCP 换网胶水（flowPacketConn）的用例：切 socket 会话不重建、窗口内写失败不毒化、
// 吞错预算与无收包两条兜底、代次不匹配静默改读、并发切换合并、关闭收口。
//
// 换网模拟全部落在本机：假接入层（fakeedge_test.go）+ 本地双 UDP socket + 可注入拨号器，
// 不依赖 10.10.9.36 等外部环境，CI 可直接跑。

package direct

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// faultSocket 是测试用 socket：fail 置位后写返回 ENETUNREACH（读不受影响，模拟只有上行断）。
type faultSocket struct {
	*net.UDPConn
	fail atomic.Bool
}

// Write 在 fail 置位时返回「网络不可达」错误。
func (s *faultSocket) Write(p []byte) (int, error) {
	if s.fail.Load() {
		return 0, &net.OpError{Op: "write", Net: "udp", Err: errNetworkUnreachable}
	}
	return s.UDPConn.Write(p)
}

// errNetworkUnreachable 是测试用的网络不可达错误（不引入 syscall 平台差异）。
var errNetworkUnreachable = errors.New("network is unreachable")

// withSwitchSocketDial 注入换网拨号器（返回可写故障的 socket 包装）。
func withSwitchSocketDial(fn func(context.Context) (datagramSocket, error)) Option {
	return func(o *options) { o.switchDial = fn }
}

// openFlowTestConn 直接构造一条换网胶水（不建 KCP 会话：用例只测胶水行为）。
func openFlowTestConn(t *testing.T, edge *fakeEdge, opts ...Option) *flowPacketConn {
	t.Helper()
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	pc, err := newKCPFlowConn(edge.addr(), testTicket, o)
	if err != nil {
		t.Fatalf("构造换网胶水失败: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

// kcpFlowPC 取出会话当前代 KCP 承载的换网胶水（同包测试专用）。
func kcpFlowPC(t *testing.T, sess *Session) *flowPacketConn {
	t.Helper()
	g := sess.currentGen()
	kc, ok := g.tr.(*kcpConn)
	if !ok || kc.pc == nil {
		t.Fatalf("当前代连接不是带换网胶水的 KCP 连接: %T", g.tr)
	}
	return kc.pc
}

// fixedWatcher 是测试用网络变化事件源（通道由用例控制）。
type fixedWatcher chan struct{}

// Changes 返回用例注入的通道。
func (w fixedWatcher) Changes() <-chan struct{} { return w }

// TestFlowPacketConn_DefaultsFromOptions ①：缺省阈值与非法装配期报错。
func TestFlowPacketConn_DefaultsFromOptions(t *testing.T) {
	o := defaultOptions()
	if o.switchWindow != 500*time.Millisecond || o.idleSwitch != 3*time.Second {
		t.Fatalf("缺省窗口/T1 = %s/%s, 期望 500ms/3s", o.switchWindow, o.idleSwitch)
	}
	if o.idleDead != 15*time.Second || o.swallowMax != 256 {
		t.Fatalf("缺省 T2/吞错上限 = %s/%d, 期望 15s/256", o.idleDead, o.swallowMax)
	}
	plan := planFor(TransportKCP, "127.0.0.1:1", testTicket)
	if _, err := Open(context.Background(), plan, WithSwitchWindow(0)); err == nil {
		t.Fatal("WithSwitchWindow(0) 未在装配期报错")
	}
}

// TestFlowPacketConn_MigrateKeepsSessionAlive ①：换 socket 后同一 KCP 会话继续收发。
func TestFlowPacketConn_MigrateKeepsSessionAlive(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	watcher := make(chan struct{}, 1)
	sess := openTest(t, planFor(TransportKCP, edge.addr(), testTicket),
		WithHeartbeat(0), WithSwitchWatcher(fixedWatcher(watcher)))
	joinOnce(t, sess, "b-1")

	pc := kcpFlowPC(t, sess)
	oldGen := pc.snapshot().gen
	watcher <- struct{}{} // 系统网络事件（第三路信号）
	waitFor(t, 2*time.Second, "切网完成", func() bool { return sess.Stats().Flow.Switches == 1 })
	if got := pc.snapshot().gen; got != oldGen+1 {
		t.Fatalf("切换后代次 = %d, 期望 %d", got, oldGen+1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := syncFrames(ctx, sess); err != nil {
		t.Fatalf("切网后同一会话补帧失败: %v", err)
	}
	if st := sess.Stats(); st.Connects != 1 || st.ReconnectRounds != 0 {
		t.Fatalf("切网触发了会话重建: connects=%d rounds=%d", st.Connects, st.ReconnectRounds)
	}
}

// TestFlowPacketConn_SwallowWriteErrorInWindow ②：窗口内写失败被吞掉，KCP 不被毒化。
func TestFlowPacketConn_SwallowWriteErrorInWindow(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	var mu sync.Mutex
	var socks []*faultSocket
	dial := func(context.Context) (datagramSocket, error) {
		u, err := edge.dialUDP()
		if err != nil {
			return nil, err
		}
		fs := &faultSocket{UDPConn: u}
		mu.Lock()
		socks = append(socks, fs)
		mu.Unlock()
		return fs, nil
	}
	sess := openTest(t, planFor(TransportKCP, edge.addr(), testTicket),
		WithHeartbeat(0), withSwitchSocketDial(dial))
	joinOnce(t, sess, "b-1")

	mu.Lock()
	socks[0].fail.Store(true) // 模拟上行断（读仍正常）
	mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sendFrameInput(ctx, sess, "b-1"); err != nil {
		t.Fatalf("窗口内写失败被交给 kcp-go（会话毒化）: %v", err)
	}
	waitFor(t, 2*time.Second, "吞错并完成切网", func() bool {
		st := sess.Stats()
		return st.Flow.SwallowedWrites >= 1 && st.Flow.Switches >= 1
	})
	if err := sendFrameInput(ctx, sess, "b-1"); err != nil {
		t.Fatalf("切网后写仍失败（会话被毒化）: %v", err)
	}
	if st := sess.Stats(); st.Connects != 1 || st.ReconnectRounds != 0 {
		t.Fatalf("吞错路径触发了 KCP 会话重建: connects=%d rounds=%d", st.Connects, st.ReconnectRounds)
	}
}

// TestFlowPacketConn_MigrateBusyBackoff ⑥：hello 回 1 字节 0x00（背压）退避重试，不判死。
func TestFlowPacketConn_MigrateBusyBackoff(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	pc := openFlowTestConn(t, edge)
	edge.busy.Store(2)
	pc.requestSwitch(SwitchNetworkEvent)
	waitFor(t, 2*time.Second, "背压后切网成功", func() bool { return pc.flowStats().Switches == 1 })
	if got := edge.hellos.Load(); got < 3 {
		t.Fatalf("hello 次数 = %d, 期望至少 3（背压 2 次 + 成功 1 次）", got)
	}
	if st := pc.flowStats(); st.SwitchFailures != 0 || st.LastSwitchErr != nil {
		t.Fatalf("背压被计入切网失败: failures=%d err=%v", st.SwitchFailures, st.LastSwitchErr)
	}
}

// TestFlowPacketConn_SwallowBudgetExceeded ③：吞错超阈值 ⇒ ReadFrom 返回坏死读错误。
func TestFlowPacketConn_SwallowBudgetExceeded(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	var calls atomic.Int32
	dial := func(context.Context) (datagramSocket, error) {
		if calls.Add(1) == 1 {
			return edge.dialUDP()
		}
		return nil, errNetworkUnreachable
	}
	pc := openFlowTestConn(t, edge, WithSwitchWindow(5*time.Second),
		WithSwallowBudget(3), withSwitchSocketDial(dial))
	if err := pc.snapshot().sock.Close(); err != nil {
		t.Fatalf("关闭当前 socket 失败: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := pc.WriteTo([]byte("x"), nil); err != nil {
			t.Fatalf("第 %d 次窗口内写失败未吞掉: %v", i+1, err)
		}
	}
	_, _, err := pc.ReadFrom(make([]byte, 16))
	if !errors.Is(err, ErrFlowDead) {
		t.Fatalf("ReadFrom 错误 = %v, 期望 ErrFlowDead", err)
	}
	if reason, ok := FlowDeadReason(err); !ok || reason != DeadSwallow {
		t.Fatalf("坏死原因 = %v/%v, 期望 DeadSwallow", reason, ok)
	}
	if st := pc.flowStats(); st.DeadRebuilds != 1 || st.LastDeadReason != DeadSwallow {
		t.Fatalf("兜底计数 = %+v, 期望 deadRebuilds=1 reason=DeadSwallow", st)
	}
}

// TestFlowPacketConn_IdleDeadline ④：T2 无收包 ⇒ ReadFrom 返回坏死读错误（DeadIdle）。
func TestFlowPacketConn_IdleDeadline(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	pc := openFlowTestConn(t, edge, WithIdleSwitch(50*time.Millisecond), WithIdleDeadline(150*time.Millisecond))
	edge.silent.Store(true)

	done := make(chan error, 1)
	go func() {
		_, _, err := pc.ReadFrom(make([]byte, 16))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrFlowDead) {
			t.Fatalf("ReadFrom 错误 = %v, 期望 ErrFlowDead", err)
		}
		if reason, ok := FlowDeadReason(err); !ok || reason != DeadIdle {
			t.Fatalf("坏死原因 = %v/%v, 期望 DeadIdle", reason, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("T2 到期后 ReadFrom 未返回坏死读错误")
	}
	if st := pc.flowStats(); st.DeadRebuilds != 1 {
		t.Fatalf("兜底计数 = %d, 期望 1", st.DeadRebuilds)
	}
}

// TestFlowPacketConn_StaleGenReadRetried ⑧：旧 socket 关闭产生的读错误被静默改读新 socket。
func TestFlowPacketConn_StaleGenReadRetried(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	pc := openFlowTestConn(t, edge)
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 64)
		n, _, err := pc.ReadFrom(buf)
		if err == nil {
			got <- append([]byte(nil), buf[:n]...)
		}
	}()
	time.Sleep(20 * time.Millisecond) // 确保读协程已阻塞在旧 socket
	pc.requestSwitch(SwitchNetworkEvent)
	waitFor(t, 2*time.Second, "切换完成", func() bool { return pc.flowStats().Switches == 1 })
	edge.sendToClient([]byte("after-migrate"))
	select {
	case data := <-got:
		if string(data) != "after-migrate" {
			t.Fatalf("读到 %q, 期望 after-migrate", data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("旧 socket 读错误未被静默重试到新 socket")
	}
}

// TestFlowPacketConn_WriteErrorOutsideWindow ⑦：窗口外的写错误照旧上抛。
func TestFlowPacketConn_WriteErrorOutsideWindow(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	var calls atomic.Int32
	dial := func(ctx context.Context) (datagramSocket, error) {
		if calls.Add(1) == 1 {
			return edge.dialUDP()
		}
		<-ctx.Done() // 迁移卡住（窗口很快过期）
		return nil, ctx.Err()
	}
	pc := openFlowTestConn(t, edge, WithSwitchWindow(50*time.Millisecond), withSwitchSocketDial(dial))
	if err := pc.snapshot().sock.Close(); err != nil {
		t.Fatalf("关闭当前 socket 失败: %v", err)
	}
	if _, err := pc.WriteTo([]byte("x"), nil); err != nil {
		t.Fatalf("切换起点内的第一次写失败应被吞掉: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := pc.WriteTo([]byte("x"), nil); err == nil {
		t.Fatal("窗口外的写错误被吞掉（应照旧上抛）")
	}
}

// TestFlowPacketConn_ConcurrentSwitchSerialized ⑨：并发信号合并为一次切换。
func TestFlowPacketConn_ConcurrentSwitchSerialized(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	pc := openFlowTestConn(t, edge)
	oldGen := pc.snapshot().gen
	var wg sync.WaitGroup
	for _, reason := range []SwitchReason{SwitchWriteError, SwitchReadTimeout} {
		wg.Add(1)
		go func(r SwitchReason) {
			defer wg.Done()
			pc.requestSwitch(r)
		}(reason)
	}
	wg.Wait()
	waitFor(t, 2*time.Second, "切换完成", func() bool { return pc.flowStats().Switches == 1 })
	time.Sleep(20 * time.Millisecond)
	if st := pc.flowStats(); st.Switches != 1 {
		t.Fatalf("并发信号触发 %d 次切换, 期望合并为 1 次", st.Switches)
	}
	if got := pc.snapshot().gen; got != oldGen+1 {
		t.Fatalf("代次 = %d, 期望只 +1（%d）", got, oldGen+1)
	}
}

// TestFlowPacketConn_CloseStopsMigration ⑩：Close 后迁移退出、不再 dial。
func TestFlowPacketConn_CloseStopsMigration(t *testing.T) {
	stub := newBattleStub(testTicket)
	edge := startFakeEdge(t, testTicket, stub)
	var calls atomic.Int32
	dialing := make(chan struct{}, 2)
	dial := func(ctx context.Context) (datagramSocket, error) {
		if calls.Add(1) == 1 {
			return edge.dialUDP()
		}
		dialing <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	pc := openFlowTestConn(t, edge, withSwitchSocketDial(dial))
	before := countGoroutines()
	pc.requestSwitch(SwitchNetworkEvent)
	select {
	case <-dialing:
	case <-time.After(time.Second):
		t.Fatal("迁移拨号未被调用")
	}
	_ = pc.Close()
	time.Sleep(50 * time.Millisecond)
	if got := pc.snapshot(); got.gen != 1 {
		t.Fatalf("Close 后仍安装了新 socket（gen=%d）", got.gen)
	}
	if after := countGoroutines(); after > before+2 {
		t.Fatalf("Close 后 goroutine 泄漏：%d → %d", before, after)
	}
}

// countGoroutines 返回当前 goroutine 数（泄漏断言用）。
func countGoroutines() int { return runtime.NumGoroutine() }
