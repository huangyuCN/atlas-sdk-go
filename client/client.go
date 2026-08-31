// Package client 是 Atlas 帧协议的客户端运行时内核：
// 多通道编排（dual 形态：业务 + 战斗）、通道传输（TCP / WebSocket / KCP / UDP）、
// Invoke 请求-响应匹配、双层心跳语义、Notify 订阅分发、断线自动重连。
// 规范见 atlas 仓库 docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md；
// DTO 生成器按规范路线后续批次交付。
package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
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

// State 是通道连接状态（规范 §5.1 状态机）。
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

// Kind 是通道角色（dual 形态区分业务/战斗通道，规范 §5.1）。
type Kind int

const (
	// KindBusiness 是业务通道：登录/会话/常规业务请求；
	// Client 级 Invoke/On 未指定通道时默认走此通道。
	KindBusiness Kind = iota
	// KindBattle 是战斗通道：高频帧输入（dual 形态第二通道）。
	KindBattle
)

// String 实现字符串化。
func (k Kind) String() string {
	switch k {
	case KindBusiness:
		return "business"
	case KindBattle:
		return "battle"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

// Transport 是通道传输类型。
type Transport int

const (
	// TransportTCP 是 TCP 流式传输（默认；粘包由帧头 bodyLen 切分）。
	TransportTCP Transport = iota
	// TransportWS 是 WebSocket 消息边界传输（一条消息 = 一个完整帧）。
	TransportWS
	// TransportKCP 是 KCP 可靠 UDP 传输（kcp-go；流式分帧与 TCP 同构，
	// 会话参数与 atlas 服务端基线对齐：明文、无 FEC）。
	TransportKCP
	// TransportUDP 是 UDP 数据报传输（一报一帧；单数据报上限 64KiB 含帧头；
	// 坏数据报静默丢弃；死链由传输心跳发现后自动重拨）。
	TransportUDP
)

// String 实现字符串化。
func (t Transport) String() string {
	switch t {
	case TransportWS:
		return "ws"
	case TransportKCP:
		return "kcp"
	case TransportUDP:
		return "udp"
	default:
		return fmt.Sprintf("transport(%d)", int(t))
	}
}

// ChannelConfig 描述一条通道的拨号配置（dual 形态逐通道定制）。
type ChannelConfig struct {
	// Kind 是通道角色；DialDual 的战斗参数零值按参数位置推断为 KindBattle。
	Kind Kind
	// Transport 是传输类型（零值 TCP）。
	Transport Transport
	// Addr 是服务端地址 host:port；WS 通道亦接受完整 ws:// 或 wss:// URL（此时 Path 忽略）。
	Addr string
	// Path 是 WS 服务端包装路径（仅 WS 通道；空则 "/ws"，对齐模板网关约定）。
	Path string
	// Opts 是本通道覆盖项：在顶层 Option 之后应用
	//（如战斗通道短超时、Join 重绑定钩子，规范 §5.2）。
	Opts []Option
}

// Client 是多通道编排器（门面）：管理一条或多条通道，Invoke/On 默认走业务通道；
// dual 形态用 Channel(kind) 取得通道视图做独立请求/订阅/状态观测。
// 通道生命周期归 Client：视图不单独关闭（规范 §5.1）。
type Client struct {
	// channels 在构造完成后不可变（并发读安全）；不变式：业务通道恒存在。
	channels map[Kind]*channel
	business *channel
}

// ChannelView 是通道视图：独立 Invoke/On/State/OnReadExit。
// 由 Client.Channel(kind) 创建，生命周期归 Client（无 Close）。
type ChannelView struct{ ch *channel }

// Invoke 发送请求并等待响应（默认业务通道；dual 形态指定通道走 Channel(kind).Invoke）。
// req 为 nil 时不携带 payload（如心跳）；resp 非 nil 时反序列化业务数据。
// 重连期间默认排队（上限 WithReconnectQueueSize，满则失败），WithFailFast 可跳过排队。
// 业务拒绝返回 *BusinessError（按 Reason 分支）；超时/断连返回对应错误类型。
func (c *Client) Invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error {
	return c.business.invoke(ctx, op, req, resp, opts...)
}

// On 订阅 Notify 帧（按 operation 分发，默认业务通道），返回退订函数。
// 幂等语义：同一 (op, handler) 重复注册只保留一份，重复退订安全（规范 §5.1）；
// 重连后订阅自动重放。handler 在独立 goroutine 执行，panic 隔离。
func (c *Client) On(op string, h NotifyHandler) (off func()) {
	return c.business.on(op, h)
}

// OnReadExit 注册默认业务通道的读循环退出回调（每次连接断开触发一次；如需自定义
// 重连建议改用 WithAutoReconnect/WithOnReconnected；每通道版本见 ChannelView）。
func (c *Client) OnReadExit(fn func(error)) {
	c.business.onReadExit(fn)
}

// State 聚合全部通道状态：任一通道非 Connected 即向下降级（规范 §5.1）；
// 细粒度状态用 Channel(kind).State()。
func (c *Client) State() State {
	worst := StateConnected
	for _, ch := range c.channels {
		if s := ch.State(); stateSeverity(s) > stateSeverity(worst) {
			worst = s
		}
	}
	return worst
}

// stateSeverity 状态劣化度：Connected(0) < Reconnecting(1) < Disconnected(2)。
func stateSeverity(s State) int {
	switch s {
	case StateConnected:
		return 0
	case StateReconnecting:
		return 1
	default:
		return 2
	}
}

// Channel 返回通道视图（独立 Invoke/On/State；生命周期归 Client，不单独 Close）。
// 未知 kind 返回 nil（视图方法 nil 安全）。
func (c *Client) Channel(kind Kind) *ChannelView {
	ch, ok := c.channels[kind]
	if !ok {
		return nil
	}
	return &ChannelView{ch: ch}
}

// Close 优雅关闭全部通道：停心跳/重连、断开连接、取消全部 in-flight 与排队请求。
// 幂等；任何时刻调用都不会死锁。
func (c *Client) Close() error {
	for _, ch := range c.channels {
		_ = ch.Close()
	}
	return nil
}

// Invoke 在本通道上发送请求并等待响应（语义同 Client.Invoke）。
func (v *ChannelView) Invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error {
	if v == nil || v.ch == nil {
		return NewNetworkError(errors.New("client: 未知通道"))
	}
	return v.ch.invoke(ctx, op, req, resp, opts...)
}

// On 订阅本通道的 Notify 分发（重连后自动重放；语义同 Client.On）。
func (v *ChannelView) On(op string, h NotifyHandler) (off func()) {
	if v == nil || v.ch == nil {
		return func() {}
	}
	return v.ch.on(op, h)
}

// State 返回本通道独立状态（dual 双通道独立重连的细粒度观测，规范 §5.2）。
func (v *ChannelView) State() State {
	if v == nil || v.ch == nil {
		return StateDisconnected
	}
	return v.ch.State()
}

// Kind 返回通道角色。
func (v *ChannelView) Kind() Kind {
	if v == nil || v.ch == nil {
		return KindBusiness
	}
	return v.ch.kind
}

// OnReadExit 注册本通道读循环退出回调（每次连接断开触发一次）。
func (v *ChannelView) OnReadExit(fn func(error)) {
	if v == nil || v.ch == nil {
		return
	}
	v.ch.onReadExit(fn)
}

// Dial 建立 TCP 长连接（单通道 = 业务通道）并启动读循环、心跳与重连监管。
func Dial(addr string, opts ...Option) (*Client, error) {
	return dialChannels([]ChannelConfig{{Kind: KindBusiness, Transport: TransportTCP, Addr: addr}}, opts)
}

// DialWS 建立 WebSocket 长连接（单通道 = 业务通道；一条 WS 消息 = 一个完整帧）。
// path 为服务端包装路径（空则 "/ws"，对齐模板网关）；addr 亦可为完整 ws:// 或 wss:// URL。
func DialWS(addr, path string, opts ...Option) (*Client, error) {
	return dialChannels([]ChannelConfig{{Kind: KindBusiness, Transport: TransportWS, Addr: addr, Path: path}}, opts)
}

// DialKCP 建立 KCP 长连接（单通道 = 业务通道；kcp-go 消息模式与服务端一致，
// 会话参数与 atlas 服务端基线对齐：明文、无 FEC、NoDelay(0,40,0,0)、窗口 128、MTU 1400；
// 帧写带写超时兜底，死链窗口满时不再永久阻塞）。
func DialKCP(addr string, opts ...Option) (*Client, error) {
	return dialChannels([]ChannelConfig{{Kind: KindBusiness, Transport: TransportKCP, Addr: addr}}, opts)
}

// DialUDP 建立 UDP 长连接（单通道 = 业务通道；一报一帧，单数据报上限 64KiB 含帧头，
// 坏数据报静默丢弃；UDP 无连接——死链由传输心跳连续失败判定后自动重拨）。
func DialUDP(addr string, opts ...Option) (*Client, error) {
	return dialChannels([]ChannelConfig{{Kind: KindBusiness, Transport: TransportUDP, Addr: addr}}, opts)
}

// DialDual 建立 dual 双通道客户端（规范 §5.2 dual 双通道独立重连）：业务 + 战斗通道
// 各自独立连接、心跳、重连与请求排队；断线重连按通道独立成立，每通道独立会话钩子
// （业务通道重登 / 战斗通道 Join 重绑定，经 ChannelConfig.Opts 配置）。
// 任一通道拨号失败即整体失败（已建通道被回滚关闭）。
func DialDual(business, battle ChannelConfig, opts ...Option) (*Client, error) {
	if business.Kind != KindBusiness && business.Kind != KindBattle {
		return nil, fmt.Errorf("client: 非法业务通道角色 %d", int(business.Kind))
	}
	if battle.Kind == KindBusiness {
		// 零值视为未设置，按参数位置推断为战斗通道。
		battle.Kind = KindBattle
	}
	if business.Kind == battle.Kind {
		return nil, fmt.Errorf("client: dual 通道角色重复（%s）", business.Kind)
	}
	// 评审 v0.3-B2 修复：业务重登成功后自动触发战斗通道重绑（Join），
	// 规范 §5.2「业务通道重连成功后由 SDK 自动对战斗通道执行重新绑定」。
	// 实现为链式包装（追加到业务通道）：业务钩子 = 业务重登 → 成功后调用战斗钩子。
	// 注意：链式编排要求两个钩子都配置在 ChannelConfig.Opts——
	// 顶层 Option 配置的钩子按「全部通道默认值」生效，不参与链式编排。
	var clientSlot atomic.Pointer[Client]
	// 战斗钩子提取（链式调用的第二环）。
	batProbe := defaultSettings()
	for _, o := range battle.Opts {
		o(&batProbe)
	}
	batHook := batProbe.onReconnected
	business.Opts = append(business.Opts, func(s *channelSettings) {
		prev := s.onReconnected
		s.onReconnected = func() error {
			if prev != nil {
				if err := prev(); err != nil {
					return err
				}
			}
			if batHook == nil {
				return nil
			}
			// 评审 v0.4 修复：战斗通道可能还在自身重连中（两通道同时断线、战斗
			// 恢复较慢）。此时跳过本次重绑——战斗通道自身重连成功后会执行自己的
			// Join 钩子，避免业务钩子因「战斗未就绪」失败而拖累业务通道反复重连。
			if cc := clientSlot.Load(); cc != nil {
				if bat := cc.Channel(KindBattle); bat != nil && bat.State() != StateConnected {
					return nil
				}
			}
			return batHook()
		}
	})
	c, err := dialChannels([]ChannelConfig{business, battle}, opts)
	if err == nil {
		clientSlot.Store(c)
	}
	return c, err
}

// wrapBattleRebind 保留通道钩子原样（链式「业务重登 → 战斗重绑」编排已由
// DialDual 的 clientSlot 方案实现：业务钩子执行后按战斗通道实时状态决定是否
// 立即触发战斗重绑；战斗通道未就绪时跳过，由其自身重连钩子兜底）。
func wrapBattleRebind(bizOpts, _ []Option) []Option {
	return bizOpts
}

// dialChannels 是全部构造器的公共路径：逐通道构建连接本体并启动监管；
// 任一通道失败即回滚关闭已建通道。所有构造器保证业务通道存在
// （Dial/DialWS 显式指定；DialDual 校验角色互异）。
func dialChannels(cfgs []ChannelConfig, defaults []Option) (*Client, error) {
	c := &Client{channels: make(map[Kind]*channel, len(cfgs))}
	for _, cfg := range cfgs {
		ch, err := newChannel(cfg, defaults)
		if err != nil {
			for _, built := range c.channels {
				_ = built.Close()
			}
			return nil, err
		}
		if _, dup := c.channels[ch.kind]; dup {
			_ = ch.Close()
			for _, built := range c.channels {
				_ = built.Close()
			}
			return nil, fmt.Errorf("client: 通道角色重复（%s）", ch.kind)
		}
		c.channels[ch.kind] = ch
		if ch.kind == KindBusiness {
			c.business = ch
		}
	}
	return c, nil
}
