// Package client 是 Atlas 帧协议的客户端运行时内核：
// 通道（TCP）、Invoke 请求-响应匹配、心跳保活、Notify 订阅分发、自动重连。
// 规范见 atlas 仓库 docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md；
// dual 双通道编排/KCP·UDP 通道/DTO 生成器按规范路线后续批次交付。
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// HeartbeatOperation 是传输保活心跳 operation（服务端 StreamEngine 内置空响应 handler）。
const HeartbeatOperation = "/atlas.internal.Heartbeat/Ping"

// 默认参数（与规范 §5.2 对齐）。
const (
	defaultHeartbeatInterval = 30 * time.Second
	defaultInvokeTimeout     = 10 * time.Second
	defaultHeartbeatFailures = 3
	defaultBackoffBase       = 500 * time.Millisecond
	defaultBackoffMax        = 30 * time.Second
	defaultQueueSize         = 64
	defaultHookTimeout       = 10 * time.Second
)

// State 是客户端连接状态（规范 §5.1 状态机）。
type State int32

// 状态：Disconnected（初始/关闭）→ Connected；断线进入 Reconnecting 直至重连成功或关闭。
const (
	StateDisconnected State = iota
	StateConnected
	StateReconnecting
)

// String 实现字符串化。
func (s State) String() string {
	switch s {
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	default:
		return "disconnected"
	}
}

// generation 是一次连接代际的完整快照（评审 B3 修复）：
// epoch / 连接 / 死亡信号绑定在同一个不可变结构里，Invoke 通过单次
// atomic Load 取齐三项，杜绝「旧 epoch + 新连接」的撕裂读。
// 代际切换 = 整体替换该指针（setConn），旧的 generation 永不修改。
type generation struct {
	epoch uint64
	conn  net.Conn
	done  chan struct{} // 读循环退出时关闭
}

// Client 是到单个服务端的长连接客户端（内建自动重连；dual 多通道编排后续批次）。
type Client struct {
	addr string

	// 当前代快照：Invoke/读循环/心跳统一经 gen() 获取（评审 B3 修复）。
	genPtr atomic.Pointer[generation]

	writeMu sync.Mutex // 帧级写锁：并发 Invoke/Notify 回调下整帧不交错

	serial Serializer
	// inflight 表按 (epoch, seq) 匹配（规范 §5.2）：值使用单一所有权语义——
	// 认领（LoadAndDelete）后发送，timer/ctx/断连/迟到响应四方竞争恰好一次投递。
	inflight sync.Map // inflightKey → chan invokeResult
	seq      atomic.Uint32

	notifyMu   sync.Mutex
	notifies   map[string]map[uintptr]notifyEntry // op → handler 集合（函数值指针幂等去重）
	onReadExit atomic.Pointer[func(error)]

	// 重连配置与状态。
	autoReconnect     bool
	backoffBase       time.Duration
	backoffMax        time.Duration
	queueSize         int
	queue             chan *queuedInvoke
	onReconnected     func() error    // 重连成功后的会话重登钩子
	onReconnectedF    asyncHookRunner // 钩子的异步执行器（评审 B1/B2 修复）
	hookTimeout       time.Duration   // 钩子执行超时上限
	state             atomic.Int32    // State
	closed            atomic.Bool     // 关闭标记（幂等）
	closeCh           chan struct{}   // 关闭信号（所有 goroutine 的退出源）
	lifecycleMu       sync.Mutex      // 序列化 wg.Add 与 Close 的 wg.Wait（WaitGroup 并发纪律）
	wg                sync.WaitGroup  // supervisor + 每代读循环/心跳 + 钩子执行
	genMu             sync.Mutex      // setConn 的代际切换互斥（切换与 wg.Add 原子）
	fatal             atomic.Bool     // 协议级致命错误已终止 Client（不重连）
	dialer            *net.Dialer     // 可取消拨号（评审 Important：Close 不被系统拨号阻塞）
	heartbeatInterval time.Duration
	invokeTimeout     time.Duration
	maxBodySize       int
}

// asyncHookRunner 是重登钩子的执行形态：异步、带超时、不阻塞 supervisor。
type asyncHookRunner func(c *Client, done chan struct{}, hookTimeout time.Duration, wg *sync.WaitGroup, closeCh chan struct{}, invokeTimeout time.Duration)

// inflightKey 是 in-flight 表的复合键：(epoch, seq)。
type inflightKey struct {
	epoch uint64
	seq   uint32
}

// notifyEntry 是订阅表条目：以函数值本身判等（配合 reflect 型指针比较实现幂等）。
type notifyEntry struct {
	fn NotifyHandler
}

// NotifyHandler 是 Notify 帧回调：收到原始 payload（SDK 不做 DTO 解码，业务侧自行处理）。
// handler 在独立 goroutine 执行，异常被 recover，不影响其他分发。
type NotifyHandler func(op string, payload []byte)

// Option 配置 Client。
type Option func(*Client)

// WithHeartbeatInterval 设置传输保活心跳周期（默认 30s；≤0 关闭心跳）。
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *Client) { c.heartbeatInterval = d }
}

// WithInvokeTimeout 设置默认请求超时（默认 10s；可被 Invoke 的 ctx 覆盖）。
func WithInvokeTimeout(d time.Duration) Option {
	return func(c *Client) { c.invokeTimeout = d }
}

// WithMaxBodySize 设置单帧 body 上限（默认/上限均为 frame.MaxBodySize）。
// 注意：必须与服务端配置对齐，单端调小即有断连风险（规范 §2）。
func WithMaxBodySize(n int) Option {
	return func(c *Client) {
		if n > 0 && n <= frame.MaxBodySize {
			c.maxBodySize = n
		}
	}
}

// WithSerializer 设置序列化器（默认 JSON）。
func WithSerializer(s Serializer) Option {
	return func(c *Client) { c.serial = s }
}

// WithAutoReconnect 设置断线自动重连开关（默认开启）。
func WithAutoReconnect(enabled bool) Option {
	return func(c *Client) { c.autoReconnect = enabled }
}

// WithBackoff 设置重连退避参数（base 起步 ×2 递增、封顶 max，默认 500ms/30s，带抖动）。
func WithBackoff(base, max time.Duration) Option {
	return func(c *Client) {
		if base > 0 {
			c.backoffBase = base
		}
		if max >= c.backoffBase {
			c.backoffMax = max
		}
	}
}

// WithReconnectQueueSize 设置断线重连期间的请求排队上限（默认 64；满后立即失败）。
func WithReconnectQueueSize(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.queueSize = n
		}
	}
}

// WithOnReconnected 注册重连成功后的会话钩子（业务侧在此重登/重绑定；
// 返回错误视为本次重连未完成，SDK 继续退避重试）。
// 钩子在独立 goroutine 执行（评审 B1/B2 修复）：不阻塞 supervisor 状态机，
// 钩子内可安全调用 Invoke（此时状态已置 Connected）与 Close（不会自等待死锁）；
// 执行超过 hookTimeout 视为失败，继续退避。默认 10s。
func WithOnReconnected(fn func() error) Option {
	return func(c *Client) { c.onReconnected = fn }
}

// OnReadExit 注册读循环退出回调（每次连接断开触发一次；如需自定义重连建议改用
// WithAutoReconnect/WithOnReconnected）。
func (c *Client) OnReadExit(fn func(error)) {
	if fn == nil {
		return
	}
	c.onReadExit.Store(&fn)
}

// Dial 建立 TCP 长连接并启动读循环、心跳与重连监管。
func Dial(addr string, opts ...Option) (*Client, error) {
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr:              addr,
		serial:            JSONSerializer{},
		notifies:          make(map[string]map[uintptr]notifyEntry),
		heartbeatInterval: defaultHeartbeatInterval,
		invokeTimeout:     defaultInvokeTimeout,
		maxBodySize:       frame.MaxBodySize,
		autoReconnect:     true,
		backoffBase:       defaultBackoffBase,
		backoffMax:        defaultBackoffMax,
		queueSize:         defaultQueueSize,
		hookTimeout:       defaultHookTimeout,
		closeCh:           make(chan struct{}),
		dialer:            &net.Dialer{},
	}
	c.onReconnectedF = runHookAsync
	for _, opt := range opts {
		opt(c)
	}
	c.queue = make(chan *queuedInvoke, c.queueSize)

	// 首代连接：generation 在本 goroutine 构造（代际切换只在 supervisor/Dial 串行发生）。
	c.setConn(conn)
	c.state.Store(int32(StateConnected)) // 首连已在手，置位先于 supervisor 调度
	c.wg.Add(1)
	go c.supervisor()
	return c, nil
}

// setConn 登记新一代连接：构造不可变 generation 整体替换 genPtr。
// 必须在 Dial/supervisor 主线程调用（代际切换串行化；epoch 单调递增）。
func (c *Client) setConn(conn net.Conn) {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	done := make(chan struct{})
	prev := c.genPtr.Load()
	epoch := uint64(1)
	if prev != nil {
		epoch = prev.epoch + 1
	}
	c.genPtr.Store(&generation{epoch: epoch, conn: conn, done: done})
}

// gen 返回当前代快照（单次原子读，评审 B3 修复：杜绝撕裂读）。
func (c *Client) gen() *generation { return c.genPtr.Load() }

// closeConn 关闭当前代连接（触发读循环退出 → supervisor 重连）。
func (c *Client) closeConn() {
	if g := c.gen(); g != nil {
		_ = g.conn.Close()
	}
}

// supervisor 是连接生命周期监管：监视连接死亡并驱动自动重连。
func (c *Client) supervisor() {
	defer c.wg.Done()
	g := c.gen()
	c.startConnLoops(g)

	for {
		select {
		case <-c.closeCh:
			return
		case <-g.done:
			if c.closed.Load() {
				return
			}
			// 连接死亡（对端断开/读错误/心跳死链）：进入重连。
			// （协议级致命错误由 readLoop 直接 terminate Client，不经此路径。）
			if !c.autoReconnect {
				c.state.Store(int32(StateDisconnected))
				return
			}
			c.state.Store(int32(StateReconnecting))
			ng, ok := c.reconnectOnce()
			if !ok {
				c.state.Store(int32(StateDisconnected))
				return
			}
			// 评审 B6 修复：新连接读循环可能已在钩子/置位前遇到协议错误并 terminate
			// （closed=true, state=Disconnected）——不得覆盖回 Connected。
			if c.closed.Load() {
				return
			}
			// 重登钩子异步执行（评审 B1/B2 修复）：不阻塞 supervisor。
			// 钩子失败会主动关闭新连接（g.done 关闭）→ 下一轮循环再次进入重连。
			if c.onReconnected != nil {
				c.onReconnectedF(c, ng.done, c.hookTimeout, &c.wg, c.closeCh, c.invokeTimeout)
			}
			// 状态置 Connected + drain 队列：与代际切换的先后关系由 genMu 序列化，
			// drainQueue 经 gen() 获取的就是新代（评审 B4 修复：入队方在同一临界区
			// 判定 Connected 后必然能被本次 drain 消费，或排队满立即失败）。
			c.genMu.Lock()
			if c.closed.Load() {
				// 双重检查：genMu 等待期间可能发生 terminate（评审 B6）。
				c.genMu.Unlock()
				return
			}
			c.state.Store(int32(StateConnected))
			c.drainQueue()
			c.genMu.Unlock()
			g = ng // 进入下一代监视
		}
	}
}

// startConnLoops 为当前代连接启动读循环与心跳（每代各一对）。
func (c *Client) startConnLoops(g *generation) {
	c.wg.Add(2)
	go c.readLoop(g)
	if c.heartbeatInterval > 0 {
		go c.heartbeatLoop(g)
	} else {
		c.wg.Done()
	}
}

// reconnectOnce 执行一轮「退避重连 + 新连接登记」；返回新一代与是否继续。
// 重登钩子不在本函数执行（评审 B1/B2：钩子异步化，见 supervisor）。
func (c *Client) reconnectOnce() (*generation, bool) {
	backoff := c.backoffBase
	for {
		// 退避等待（带抖动抖开重连风暴）；期间关闭则立即退出。
		if err := sleepInterruptible(jitter(backoff), c.closeCh); err != nil {
			return nil, false
		}
		// 可取消拨号（评审 Important 修复）：Close 不被系统拨号阻塞。
		conn, err := c.dialer.DialContext(context.Background(), "tcp", c.addr)
		if err != nil {
			backoff = c.nextBackoff(backoff)
			continue
		}
		c.lifecycleMu.Lock()
		if c.closed.Load() {
			c.lifecycleMu.Unlock()
			_ = conn.Close()
			return nil, false
		}
		// startConnLoops（含 wg.Add）必须在锁内完成：
		// 保证 Close 的 wg.Wait 开始后不可能再有 Add（WaitGroup 并发纪律）。
		c.setConn(conn)
		ng := c.gen()
		c.startConnLoops(ng)
		c.lifecycleMu.Unlock()
		return ng, true
	}
}

// nextBackoff 计算下一轮退避时长（×2 封顶；抖动只在睡眠时应用一次，
// 评审 Important 修复：不再在保存下一档时重复抖动）。
func (c *Client) nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > c.backoffMax {
		next = c.backoffMax
	}
	return next
}

// State 返回当前连接状态。
func (c *Client) State() State { return State(c.state.Load()) }

// nextSeq 分配下一个请求 seq；回绕到 0 时跳过（0 为协议非法值）。
// 2^32-1 次请求后才可能触发，防御性处理。
func (c *Client) nextSeq() uint32 {
	for {
		s := c.seq.Add(1)
		if s != 0 {
			return s
		}
	}
}

// Close 优雅关闭：停止重连与心跳、断开当前连接、取消全部 in-flight 与排队请求。
// 幂等；任何时刻调用都不会死锁（钩子异步化后 supervisor 不再被业务回调阻塞；
// wg.Wait 的每一项都能在 closeCh 关闭后独立退出）。
func (c *Client) Close() error {
	c.lifecycleMu.Lock()
	if c.closed.Swap(true) {
		c.lifecycleMu.Unlock()
		c.wg.Wait()
		return nil
	}
	close(c.closeCh)
	if g := c.gen(); g != nil {
		_ = g.conn.Close()
	}
	c.lifecycleMu.Unlock()

	c.wg.Wait() // supervisor/读循环/心跳/钩子执行器均监听 closeCh 或连接关闭
	c.failAllInflight(errors.New("client: 连接已关闭"))
	c.failAllQueued()
	c.state.Store(int32(StateDisconnected))
	return nil
}

// connOfDone 按代信号通道精确查找对应代的连接（钩子失败只弃用自己那一代）。
// 找不到（已被换代且 old done 不再是当前代）返回 nil，调用方跳过关闭。
func (c *Client) connOfDone(done chan struct{}) net.Conn {
	g := c.genPtr.Load()
	if g != nil && g.done == done {
		return g.conn
	}
	return nil
}

// runHookAsync 是默认的钩子异步执行器：独立 goroutine + 超时 + panic 保护。
// 钩子执行时状态已是 Connected（supervisor 在调度本执行器前已置位），
// 因此钩子内调用 Invoke 走正常写帧路径（评审 B1 修复）。
// 钩子在独立 goroutine，Close 的 wg.Wait 等待的是本执行器（监听 closeCh 可退出），
// 不可能等待业务回调自身（评审 B2 修复）。
func runHookAsync(c *Client, done chan struct{}, hookTimeout time.Duration, wg *sync.WaitGroup, closeCh chan struct{}, invokeTimeout time.Duration) {
	// 注意：本执行器 goroutine 不加入 wg（评审 B2 修复的关键）——
	// 若加入，钩子内调用 Close 时 Close 的 wg.Wait 会等待本执行器，
	// 而本执行器又阻塞在钩子里，形成自等待死锁。执行器监听 closeCh
	// 或钩子完成即退出；Close 返回后钩子可能仍在运行（文档明示语义）。
	go func() {
		// 等待本代读循环就绪（连接可用）后再执行钩子，避免钩子 Invoke 撞上
		// 尚未完成的代际初始化。读循环 panic 由自身 recover 语义覆盖。
		timer := time.NewTimer(hookTimeout)
		defer timer.Stop()
		result := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					result <- fmt.Errorf("client: 重连钩子 panic: %v", r)
				}
			}()
			result <- c.onReconnected()
		}()
		select {
		case err := <-result:
			if err != nil {
				// 钩子失败：弃用「本次重连的那一代」连接（精确匹配 done），
				// 不得用 c.gen()（supervisor 可能已换代，误杀新连接）。
				_ = c.connOfDone(done).Close()
			}
		case <-timer.C:
			// 钩子超时：同失败路径（业务回调可能仍在运行，但不再阻塞状态机；
			// 其后续 Invoke 会因连接已被关闭而快速失败）。
			_ = c.connOfDone(done).Close()
		case <-closeCh:
		}
	}()
}
