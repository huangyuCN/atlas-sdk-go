// KCP 换网胶水：kcp-go 只看到一层 PacketConn，本文件把它做成「带锁 + 可热替换 socket」的
// 指针类型——切网只换底层 UDP socket 与接入层 flow-id（代次 +1），KCP 的 conv/窗口/重传队列
// 全部保留。读侧按代次路由（旧 socket 的读错误静默改读新 socket），写侧在切换窗口内吞掉
// 写错误（kcp-go 对 PacketConn 的写错误只记一次就永久坏死，见 F5）。
//
// 本文件只放收发与代次；迁移流程见 netmigrate_switch.go，坏死兜底见 netmigrate_dead.go，
// 网络变化轮询见 netwatch.go。

package direct

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/client"
)

// 换网胶水的缺省阈值（全部集中在这一处，测试与 options 同源）。
const (
	// defaultSwitchWindow 是吞写错误的时间窗：窗口只覆盖切换前后，与 RTO 同量级。
	defaultSwitchWindow = 500 * time.Millisecond
	// defaultIdleSwitch 是 T1：有写出但无收包多久尝试一次迁移。
	defaultIdleSwitch = 3 * time.Second
	// defaultIdleDead 是 T2：无收包多久判定坏死（必须大于 2×心跳周期）。
	defaultIdleDead = 15 * time.Second
	// defaultSwallowMax 是吞错条数上限（超限即返回读错误交给既有重连）。
	defaultSwallowMax uint64 = 256
	// defaultSwitchHelloTimeout 是迁移时重发 hello 的超时。
	defaultSwitchHelloTimeout = time.Second
	// defaultSwitchRetryBase 是 hello 背压与切换防抖的退避起步时长。
	defaultSwitchRetryBase = 200 * time.Millisecond
	// defaultSwitchRetries 是 hello 背压的最大重试次数（之后计数失败，交由 T2 收敛）。
	defaultSwitchRetries = 3
	// defaultSwitchPollInterval 是网络变化轮询周期（Go 无系统网络事件）。
	defaultSwitchPollInterval = time.Second
	// maxReadTick 是读侧定时 tick 的上限（用于检查 T1/T2，不靠数据到达驱动）。
	maxReadTick = time.Second
	// minReadTick 是读侧定时 tick 的下限（避免阈值很小时忙等）。
	minReadTick = 10 * time.Millisecond
)

// SwitchReason 是触发换网切换的原因（枚举，禁魔法字符串）。
type SwitchReason string

// 全部切换原因。
const (
	// SwitchNetworkEvent 是系统网络变化事件 / 本地地址集合变化（缺省由轮询发现）。
	SwitchNetworkEvent SwitchReason = "network_event"
	// SwitchWriteError 是 WriteTo 写失败（窗口内会被吞掉并计数）。
	SwitchWriteError SwitchReason = "write_error"
	// SwitchReadTimeout 是写后 T1 无收包。
	SwitchReadTimeout SwitchReason = "read_timeout"
)

// flowConnConfig 是重建 socket 与重发 hello 所需的全部信息（换网期间不变量）。
type flowConnConfig struct {
	// EdgeAddr 是接入层地址（与 dialKCP 入参一致）。
	EdgeAddr string
	// Ticket 是同一张票（过期则迁移失败，交由上层重取）。
	Ticket []byte
	// HelloTimeout 是迁移重发 hello 的单次超时。
	HelloTimeout time.Duration
	// InitialHelloTimeout 是首连 hello 的超时（与握手段一致）。
	InitialHelloTimeout time.Duration
	// EdgeHello 表示是否经接入层（false = 直连 battle 帧端口，不带 flow-id 前缀）。
	EdgeHello bool
	// Dial 是测试/自定义注入点（nil 走 DialContext）。
	Dial func(ctx context.Context) (*net.UDPConn, error)
	// DialSocket 是内部测试注入点（优先于 Dial，允许故障 socket 包装）。
	DialSocket func(ctx context.Context) (datagramSocket, error)
	// Now 是时钟注入点（nil 走 time.Now）。
	Now func() time.Time
	// Stats 是会话级累加器（nil 时本条连接自建）。
	Stats *flowCounters
	// Watcher 是网络变化事件源（nil 时用地址集合轮询）。
	Watcher NetworkWatcher
	// PollInterval 是缺省轮询周期。
	PollInterval time.Duration
}

// switchLimits 是切换与兜底的阈值（全部来自 options，缺省见文件头常量）。
type switchLimits struct {
	// Window 是吞写错误的时间窗。
	Window time.Duration
	// IdleSwitch 是 T1：写后无收包多久尝试一次迁移。
	IdleSwitch time.Duration
	// IdleDead 是 T2：无收包多久判定坏死。
	IdleDead time.Duration
	// SwallowMax 是吞错条数上限。
	SwallowMax uint64
	// RetryBase 是迁移 hello 背压/切换防抖的退避起步。
	RetryBase time.Duration
}

// datagramSocket 是换网胶水持有的最小 socket 接缝：缺省是 *net.UDPConn，测试可包装注入
// 写故障（只影响写，读仍是真实 socket），从而在不依赖真机的情况下覆盖「窗口内写失败」。
type datagramSocket interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	LocalAddr() net.Addr
	SetReadDeadline(t time.Time) error
}

// flowEndpoint 是一代底层通路（切换即整体替换，读侧按 gen 判断是否 stale）。
type flowEndpoint struct {
	sock   datagramSocket
	flowID uint64
	gen    uint64
}

// flowPacketConn 是 KCP 会话看到的 PacketConn：写侧加 flow-id 前缀、读侧剥前缀；
// socket 与 flow-id 可在会话存续期间整体替换（带锁 + 代次）。
type flowPacketConn struct {
	cfg    flowConnConfig
	limits switchLimits
	remote *net.UDPAddr // 逻辑远端（恒为接入层地址，不随换网变化）

	mu     sync.RWMutex
	cur    *flowEndpoint
	closed atomic.Bool

	closeOnce sync.Once
	closeCh   chan struct{}

	// 切换状态（switchMu 保护；不嵌套 c.mu，避免锁序问题）。
	switchMu     sync.Mutex
	switching    bool
	pending      bool
	pendingCause SwitchReason
	windowStart  time.Time
	swallowSince time.Time
	lastSwitchAt time.Time
	switchCancel context.CancelFunc

	lastRecvNs  atomic.Int64  // 最近一次收到数据报（unixnano）
	lastWriteNs atomic.Int64  // 最近一次写出成功（unixnano）
	swallowed   atomic.Uint64 // 本条连接窗口内吞掉的写错误条数
	dead        atomic.Pointer[flowDeadError]

	stats *flowCounters
	now   func() time.Time

	watcher    NetworkWatcher
	ownedWatch *pollWatcher
}

// newFlowPacketConn 拨首个 socket、完成 hello 并装配换网胶水。
func newFlowPacketConn(cfg flowConnConfig, limits switchLimits) (*flowPacketConn, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.HelloTimeout <= 0 {
		cfg.HelloTimeout = defaultSwitchHelloTimeout
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultSwitchPollInterval
	}
	raddr, err := net.ResolveUDPAddr("udp", cfg.EdgeAddr)
	if err != nil {
		return nil, client.NewNetworkError(fmt.Errorf("direct: 解析地址 %s 失败: %w", cfg.EdgeAddr, err))
	}
	if cfg.Dial == nil && cfg.DialSocket == nil {
		cfg.Dial = func(ctx context.Context) (*net.UDPConn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "udp", cfg.EdgeAddr)
			if err != nil {
				return nil, err
			}
			udp, ok := conn.(*net.UDPConn)
			if !ok {
				_ = conn.Close()
				return nil, fmt.Errorf("direct: 拨号返回非 UDP 连接 %T", conn)
			}
			return udp, nil
		}
	}
	stats := cfg.Stats
	if stats == nil {
		stats = &flowCounters{}
	}
	c := &flowPacketConn{cfg: cfg, limits: limits, remote: raddr, stats: stats, now: cfg.Now,
		closeCh: make(chan struct{})}
	sock, flowID, err := c.dialEndpoint(context.Background(), cfg.InitialHelloTimeout)
	if err != nil {
		return nil, err
	}
	c.cur = &flowEndpoint{sock: sock, flowID: flowID, gen: 1}
	c.touchRecv()
	c.touchWrite()
	c.startWatcher()
	return c, nil
}

// validate 校验切换阈值（非法值在装配期报错，不静默回退）。
func (l switchLimits) validate() error {
	if l.Window <= 0 || l.IdleSwitch <= 0 || l.IdleDead <= 0 || l.RetryBase <= 0 {
		return fmt.Errorf("direct: 换网阈值非法（window=%s T1=%s T2=%s retry=%s）",
			l.Window, l.IdleSwitch, l.IdleDead, l.RetryBase)
	}
	if l.IdleDead <= l.IdleSwitch {
		return fmt.Errorf("direct: 无收包判定阈值 T2=%s 必须大于 T1=%s", l.IdleDead, l.IdleSwitch)
	}
	if l.SwallowMax == 0 {
		return fmt.Errorf("direct: 吞错上限必须大于 0")
	}
	return nil
}

// readTick 返回读侧定时检查 T1/T2 的间隔。
func (l switchLimits) readTick() time.Duration {
	tick := l.IdleDead / 4
	if tick < minReadTick {
		return minReadTick
	}
	if tick > maxReadTick {
		return maxReadTick
	}
	return tick
}

// snapshot 返回当前代快照（读侧每轮取一次，用于判定 stale）。
func (c *flowPacketConn) snapshot() *flowEndpoint {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cur
}

// flowStats 返回本条胶水所属会话的换网观测快照。
func (c *flowPacketConn) flowStats() FlowStats { return c.stats.snapshot() }

// isClosed 报告胶水是否已关闭。
func (c *flowPacketConn) isClosed() bool { return c.closed.Load() }

// nowNs 返回注入时钟的纳秒值。
func (c *flowPacketConn) nowNs() int64 { return c.now().UnixNano() }

// touchRecv 刷新最近收包时间。
func (c *flowPacketConn) touchRecv() { c.lastRecvNs.Store(c.nowNs()) }

// touchWrite 刷新最近写出时间。
func (c *flowPacketConn) touchWrite() { c.lastWriteNs.Store(c.nowNs()) }

// ReadFrom 读一个数据报并剥掉本流前缀：失配的包丢弃后继续读；旧代次上的错误静默改读
// 新 socket（关闭旧 socket 是切换的读切换点，不是会话故障）。
func (c *flowPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	if err := c.deadErr(); err != nil {
		return 0, nil, err
	}
	buf := make([]byte, max(len(p), 2048))
	for {
		if err := c.deadErr(); err != nil {
			return 0, nil, err
		}
		ep := c.snapshot()
		if ep == nil || c.isClosed() {
			return 0, nil, ErrClosed
		}
		_ = ep.sock.SetReadDeadline(c.now().Add(c.limits.readTick()))
		n, err := ep.sock.Read(buf)
		if err != nil {
			if c.isClosed() {
				return 0, nil, ErrClosed
			}
			if c.snapshot() != ep {
				continue // 旧代次：静默改读新 socket
			}
			if isTimeout(err) {
				if derr := c.checkTimers(); derr != nil {
					return 0, nil, derr
				}
				continue
			}
			return 0, nil, err
		}
		c.touchRecv()
		if c.isClosed() {
			return 0, nil, ErrClosed
		}
		if ep.flowID == 0 {
			return copy(p, buf[:n]), c.remote, nil
		}
		payload, serr := StripFlowID(buf[:n], ep.flowID)
		if serr != nil {
			continue // 换网窗口内的旧前缀包：丢弃（KCP 会重传）
		}
		return copy(p, payload), c.remote, nil
	}
}

// WriteTo 给数据报加 flow-id 前缀后写出；写失败时按切换窗口决定吞掉还是上抛。
func (c *flowPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	if err := c.deadErr(); err != nil {
		return 0, err
	}
	ep := c.snapshot()
	if ep == nil || c.isClosed() {
		return 0, ErrClosed
	}
	payload := p
	if ep.flowID != 0 {
		payload = WrapFlowID(ep.flowID, p)
	}
	if _, err := ep.sock.Write(payload); err != nil {
		if c.isClosed() {
			return 0, ErrClosed
		}
		if !c.onWriteError(err) {
			return 0, err
		}
		return len(p), nil
	}
	c.touchWrite()
	return len(p), nil
}

// onWriteError 处理写失败：先请求切换（窗口内吞掉并计数），窗口外返回 false 让错误上抛。
func (c *flowPacketConn) onWriteError(err error) bool {
	c.requestSwitch(SwitchWriteError)
	now := c.now()
	c.switchMu.Lock()
	inWindow := !c.windowStart.IsZero() && now.Sub(c.windowStart) <= c.limits.Window
	c.switchMu.Unlock()
	if !inWindow {
		return false
	}
	c.noteSwallowed(err)
	return true
}

// LocalAddr 返回当前 socket 的本地地址。
func (c *flowPacketConn) LocalAddr() net.Addr {
	ep := c.snapshot()
	if ep == nil {
		return nil
	}
	return ep.sock.LocalAddr()
}

// SetDeadline 设置当前 socket 的读写截止时间。
func (c *flowPacketConn) SetDeadline(t time.Time) error { return c.SetReadDeadline(t) }

// SetReadDeadline 设置当前 socket 的读截止时间。
func (c *flowPacketConn) SetReadDeadline(t time.Time) error {
	ep := c.snapshot()
	if ep == nil {
		return ErrClosed
	}
	return ep.sock.SetReadDeadline(t)
}

// SetWriteDeadline 返回 nil（已连接 UDP socket 的写不会阻塞，写超时由 KCP 层控制）。
func (c *flowPacketConn) SetWriteDeadline(time.Time) error { return nil }

// Close 关闭胶水（幂等）：停轮询、取消迁移、关当前 socket；之后的迁移不得再装新 socket。
func (c *flowPacketConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.mu.Lock()
		ep := c.cur
		c.mu.Unlock()
		close(c.closeCh)
		c.switchMu.Lock()
		cancel := c.switchCancel
		c.switchMu.Unlock()
		if cancel != nil {
			cancel()
		}
		if ep != nil {
			_ = ep.sock.Close()
		}
		if c.ownedWatch != nil {
			_ = c.ownedWatch.Close()
		}
	})
	return nil
}

// dialEndpoint 拨一条新 socket 并按需完成 hello（busy 回执走退避重试）。
func (c *flowPacketConn) dialEndpoint(ctx context.Context, timeout time.Duration) (datagramSocket, uint64, error) {
	sock, err := c.dialSocket(ctx)
	if err != nil {
		return nil, 0, client.NewNetworkError(fmt.Errorf("direct: 换网拨号 %s 失败: %w", c.cfg.EdgeAddr, err))
	}
	if !c.cfg.EdgeHello {
		return sock, 0, nil
	}
	id, err := c.helloFlow(ctx, sock, timeout)
	if err != nil {
		_ = sock.Close()
		return nil, 0, err
	}
	return sock, id, nil
}

// dialSocket 调用注入拨号器或缺省 UDP 拨号。
func (c *flowPacketConn) dialSocket(ctx context.Context) (datagramSocket, error) {
	if c.cfg.DialSocket != nil {
		return c.cfg.DialSocket(ctx)
	}
	conn, err := c.cfg.Dial(ctx)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// helloFlow 发 hello 并等 flow-id；收到 1 字节 0x00（背压）按 RetryBase 退避重试。
func (c *flowPacketConn) helloFlow(ctx context.Context, sock datagramSocket, timeout time.Duration) (uint64, error) {
	if timeout <= 0 {
		timeout = c.cfg.HelloTimeout
	}
	backoff := c.limits.RetryBase
	for attempt := 0; ; attempt++ {
		id, busy, err := exchangeHello(sock, c.cfg.Ticket, timeout)
		if err == nil && !busy {
			return id, nil
		}
		if err != nil {
			return 0, err
		}
		if attempt >= defaultSwitchRetries {
			return 0, fmt.Errorf("%w: hello 背压重试耗尽", ErrRejected)
		}
		if serr := sleepCtx(ctx, backoff); serr != nil {
			return 0, serr
		}
		backoff *= 2
	}
}

// exchangeHello 在给定 socket 上完成一次 hello 往返：8 字节 flow-id、1 字节 0x00 表示背压。
func exchangeHello(sock datagramSocket, ticket []byte, timeout time.Duration) (uint64, bool, error) {
	if _, err := sock.Write(EncodeHello(ticket)); err != nil {
		return 0, false, client.NewNetworkError(fmt.Errorf("direct: 写 hello 段失败: %w", err))
	}
	_ = sock.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, flowIDLen)
	n, err := sock.Read(buf)
	_ = sock.SetReadDeadline(time.Time{})
	if err != nil {
		if isTimeout(err) {
			return 0, false, fmt.Errorf("%w: hello 无回执（%s 内未收到 flow-id）", ErrRejected, timeout)
		}
		return 0, false, client.NewNetworkError(fmt.Errorf("direct: 读 flow-id 失败: %w", err))
	}
	if n == 1 && buf[0] == 0x00 {
		return 0, true, nil
	}
	if n != flowIDLen {
		return 0, false, fmt.Errorf("%w: flow-id 长度 %d, 期望 %d", ErrRejected, n, flowIDLen)
	}
	return binary.BigEndian.Uint64(buf), false, nil
}

// sleepCtx 可被 ctx 打断的退避睡眠。
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
