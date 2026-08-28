// Package client 是 Atlas 帧协议的客户端运行时内核：
// 通道（TCP）、Invoke 请求-响应匹配、心跳保活、Notify 订阅分发、自动重连。
// 规范见 atlas 仓库 docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md；
// dual 双通道编排/KCP·UDP 通道/DTO 生成器按规范路线后续批次交付。
package client

import (
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

// Client 是到单个服务端的长连接客户端（内建自动重连；dual 多通道编排后续批次）。
type Client struct {
	addr string

	// 当前代连接（重连后更换；atomic 保证 Invoke/读循环跨 goroutine 一致视图）。
	connPtr  atomic.Pointer[net.Conn]
	writeMu  sync.Mutex                    // 帧级写锁：并发 Invoke/Notify 回调下整帧不交错
	connDone atomic.Pointer[chan struct{}] // 当前代的死亡信号（读循环退出时关闭）
	epochNum atomic.Uint64                 // 连接世代：每次启动新连接循环前 +1（在 Dial/supervisor 主线程）

	serial Serializer
	// inflight 表按 (epoch, seq) 匹配（规范 §5.2）：值使用单一所有权语义——
	// 认领（LoadAndDelete）后发送，timer/ctx/断连/迟到响应四方竞争恰好一次投递。
	inflight sync.Map // inflightKey → chan invokeResult
	seq      atomic.Uint32

	notifyMu   sync.Mutex
	notifies   map[string]map[uintptr]notifyEntry // op → handler 集合（函数值指针幂等去重）
	onReadExit atomic.Pointer[func(error)]

	// 重连配置与状态。
	autoReconnect bool
	backoffBase   time.Duration
	backoffMax    time.Duration
	queueSize     int
	queue         chan *queuedInvoke
	onReconnected func() error // 重连成功后的会话重登钩子（返回错误则继续退避）

	heartbeatInterval time.Duration
	invokeTimeout     time.Duration
	maxBodySize       int

	state       atomic.Int32 // State
	lifecycleMu sync.Mutex   // 序列化 closed 标记与 wg.Add（规避 Wait/Add 竞态）
	closed      atomic.Bool
	closeCh     chan struct{}
	connCloseMu sync.Mutex
	wg          sync.WaitGroup // supervisor + 每代读循环/心跳
}

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
	conn, err := net.Dial("tcp", addr)
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
		closeCh:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	c.queue = make(chan *queuedInvoke, c.queueSize)

	// 首代连接：epoch 在本 goroutine 分配（禁止放进读循环 goroutine，见 supervisor 注释）。
	c.setConn(conn)
	c.state.Store(int32(StateConnected)) // 首连已在手，置位先于 supervisor 调度
	c.wg.Add(1)
	go c.supervisor()
	return c, nil
}

// setConn 更换当前代连接：epoch+1、登记 conn 与死亡信号通道。
// 必须在 Dial/supervisor 主线程调用（epoch 分配与启动顺序在此串行化）。
func (c *Client) setConn(conn net.Conn) chan struct{} {
	done := make(chan struct{})
	c.connPtr.Store(&conn)
	c.connDone.Store(&done)
	c.epochNum.Add(1)
	return done
}

// currentConn 返回当前代连接。
func (c *Client) currentConn() net.Conn { return *c.connPtr.Load() }

// currentConnDone 返回当前代死亡信号通道。
func (c *Client) currentConnDone() chan struct{} { return *c.connDone.Load() }

// closeConn 关闭当前代连接（触发读循环退出 → supervisor 重连）。
func (c *Client) closeConn() {
	_ = c.currentConn().Close()
}

// supervisor 是连接生命周期监管：首代读循环启动后，监视连接死亡并驱动自动重连。
func (c *Client) supervisor() {
	defer c.wg.Done()
	c.startConnLoops(c.currentConnDone())
	c.state.Store(int32(StateConnected))

	for {
		select {
		case <-c.closeCh:
			return
		case <-c.currentConnDone():
			if c.closed.Load() {
				return
			}
			// 连接死亡（对端断开/读错误/心跳死链）：进入重连。
			// （协议级致命错误由 readLoop 直接 terminate Client，不经此路径。）
			c.state.Store(int32(StateReconnecting))
			if !c.autoReconnect {
				c.state.Store(int32(StateDisconnected))
				return
			}
			if !c.reconnectOnce() {
				c.state.Store(int32(StateDisconnected))
				return
			}
			c.state.Store(int32(StateConnected))
			c.drainQueue()
		}
	}
}

// startConnLoops 为当前代连接启动读循环与心跳（每代各一对）。
func (c *Client) startConnLoops(done chan struct{}) {
	c.wg.Add(2)
	go c.readLoop(done)
	if c.heartbeatInterval > 0 {
		go c.heartbeatLoop(done)
	} else {
		c.wg.Done()
	}
}

// reconnectOnce 执行一轮「退避重连 + 重登钩子」；返回 false 表示应停止（Client 已关闭）。
func (c *Client) reconnectOnce() bool {
	backoff := c.backoffBase
	for {
		// 退避等待（带抖动抖开重连风暴）；期间关闭则立即退出。
		if err := sleepInterruptible(jitter(backoff), c.closeCh); err != nil {
			return false
		}
		conn, err := net.Dial("tcp", c.addr)
		if err == nil {
			// 新连接登记（epoch 在本线程分配）；业务重登钩子失败则弃用并继续退避。
			// lifecycleMu 保护「检查关闭 + wg.Add」的原子性（规避 Close 的 Wait/Add 竞态），
			// goroutine 计数由 startConnLoops 内部统一 Add。
			var done chan struct{}
			c.lifecycleMu.Lock()
			if c.closed.Load() {
				c.lifecycleMu.Unlock()
				_ = conn.Close()
				return false
			}
			// startConnLoops（含 wg.Add）必须在锁内完成：
			// 保证 Close 的 wg.Wait 开始后不可能再有 Add（WaitGroup 并发纪律）。
			done = c.setConn(conn)
			c.startConnLoops(done)
			c.lifecycleMu.Unlock()
			if c.onReconnected != nil {
				if err := c.safeRebind(); err != nil {
					c.closeConn()
					backoff = c.nextBackoff(backoff)
					continue
				}
			}
			return true
		}
		backoff = c.nextBackoff(backoff)
	}
}

// safeRebind 带保护的会话重登钩子执行。
func (c *Client) safeRebind() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("client: 重连钩子 panic: %v", r)
		}
	}()
	return c.onReconnected()
}

// nextBackoff 计算下一轮退避时长（×2 封顶 + 20% 抖动）。
func (c *Client) nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > c.backoffMax {
		next = c.backoffMax
	}
	return jitter(next)
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

// epoch 返回当前连接世代。
func (c *Client) epoch() uint64 { return c.epochNum.Load() }

// Close 优雅关闭：停止重连与心跳、断开当前连接、取消全部 in-flight 与排队请求。
// 幂等；任何时刻调用都不会死锁。
func (c *Client) Close() error {
	c.lifecycleMu.Lock()
	if c.closed.Swap(true) {
		c.lifecycleMu.Unlock()
		<-c.closeCh
		c.wg.Wait()
		return nil
	}
	close(c.closeCh)
	err := c.currentConn().Close()
	c.lifecycleMu.Unlock()

	c.wg.Wait() // supervisor 收到 closeCh 退出；各代读循环因 conn 关闭退出
	c.failAllInflight(errors.New("client: 连接已关闭"))
	c.failAllQueued()
	c.state.Store(int32(StateDisconnected))
	return err
}
