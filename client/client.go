// Package client 是 Atlas 帧协议的客户端运行时内核（最小集）：
// 通道（TCP）、Invoke 请求-响应匹配、心跳保活、Notify 订阅分发。
// 规范见 atlas 仓库 docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md；
// 重连/多通道编排/KCP·UDP 通道按规范路线后续批次交付。
package client

import (
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
)

// Client 是到单个服务端的长连接客户端（v0.1：单 TCP 通道）。
type Client struct {
	conn    net.Conn
	writeMu sync.Mutex // 帧级写锁：并发 Invoke/Notify 回调下整帧不交错
	serial  Serializer

	seq atomic.Uint32 // 请求 seq，从 1 单调递增（0 为非法帧 seq，见 nextSeq 回绕守卫）

	// inflight 表按 (epoch, seq) 匹配（规范 §5.2）：epoch 在每次读循环启动时递增，
	// 隔离旧连接的迟到响应；值使用单一所有权语义——认领（LoadAndDelete）后发送。
	inflight sync.Map      // inflightKey → chan invokeResult
	epochNum atomic.Uint64 // 连接世代：读循环启动时 +1（v0.1 单连接不变，重连批次后每连一代）

	notifyMu   sync.Mutex
	notifies   map[string]map[uintptr]notifyEntry // op → handler 集合（幂等去重，见 On）
	onReadExit atomic.Pointer[func(error)]        // 读循环退出回调（重连钩子的接入点，后续批次）

	heartbeatInterval time.Duration
	invokeTimeout     time.Duration
	maxBodySize       int

	closed  atomic.Bool
	closeCh chan struct{}
	closeMu sync.Mutex // 串行化 Close：避免心跳路径与外部 Close 并发各自执行收尾
	wg      sync.WaitGroup
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

// OnReadExit 注册读循环退出回调（连接断开时触发；重连编排的接入点，后续批次）。
// 回调在独立 goroutine 执行且带 panic recovery，不阻塞 Close。
func (c *Client) OnReadExit(fn func(error)) {
	if fn == nil {
		return
	}
	c.onReadExit.Store(&fn)
}

// Dial 建立 TCP 长连接并启动读循环与心跳。
func Dial(addr string, opts ...Option) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:              conn,
		serial:            JSONSerializer{},
		notifies:          make(map[string]map[uintptr]notifyEntry),
		heartbeatInterval: defaultHeartbeatInterval,
		invokeTimeout:     defaultInvokeTimeout,
		maxBodySize:       frame.MaxBodySize,
		closeCh:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	c.wg.Add(2)
	go c.readLoop()
	if c.heartbeatInterval > 0 {
		go c.heartbeatLoop()
	} else {
		c.wg.Done()
	}
	return c, nil
}

// shutdown 收尾（Stop 心跳/读循环并回收 in-flight），供 Close 与死链路径共用。
// 幂等：closed 原子标记保证只执行一次；调用方不得持有会被 wg.Wait 等待的 goroutine。
// 返回连接关闭错误（如有）。
func (c *Client) shutdown() error {
	if !c.closed.Swap(true) {
		close(c.closeCh)
		return c.conn.Close()
	}
	return nil
}

// Close 优雅关闭：停读写循环、取消全部 in-flight（NetworkError）。
// 心跳死链路径使用 shutdown 而非本方法，避免 goroutine 在 wg 内自等待死锁。
func (c *Client) Close() error {
	err := c.shutdown()
	c.wg.Wait()
	return err
}

// nextSeq 分配下一个请求 seq；回绕到 0 时跳过（0 为协议非法值，规范 §5.2）。
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

// readLoop 循环读帧并分发；退出时取消全部 in-flight 并回调 OnReadExit。
func (c *Client) readLoop() {
	defer c.wg.Done()
	var exitErr error
	for {
		hdr, body, err := frame.Read(c.conn, c.maxBodySize)
		if err != nil {
			if !c.closed.Load() {
				// 分类：帧协议非法 → ProtocolError；其余（EOF/超时/重置）→ NetworkError
				exitErr = c.classifyReadError(err)
			}
			break
		}
		switch hdr.Type {
		case frame.MsgTypeResponse:
			c.dispatchResponse(hdr, body)
		case frame.MsgTypeNotify:
			c.dispatchNotify(hdr, body)
		default:
			// Request 帧不应出现在客户端侧：协议错误，终止读循环。
			exitErr = fmt.Errorf("client: 收到非法帧类型 %d: %w", hdr.Type, frame.ErrProtocol)
		}
	}
	// 连接已死：停止心跳、失败回收全部 in-flight。
	_ = c.shutdown()
	c.failAllInflight(exitErr)
	if fn := c.onReadExit.Load(); fn != nil && exitErr != nil {
		go c.safeOnReadExit(*fn, exitErr)
	}
}

// safeOnReadExit 带保护的读循环退出回调执行。
func (c *Client) safeOnReadExit(fn func(error), err error) {
	defer func() { _ = recover() }()
	fn(err)
}

// classifyReadError 将读循环错误归类为规范 §7 的错误类型。
func (c *Client) classifyReadError(err error) error {
	if err == nil {
		return nil
	}
	if errorsIs(err, frame.ErrProtocol) {
		// 帧协议非法（magic/version/type/seq/长度/包络非法）：不可重试。
		return &ProtocolError{cause: err}
	}
	return &NetworkError{cause: err}
}
