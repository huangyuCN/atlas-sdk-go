package client

import (
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// Option 配置通道内核参数：构造器传入的顶层 Option 作为全部通道的默认值；
// dual 形态下 ChannelConfig.Opts 在其后应用，实现按通道覆盖
// （如战斗通道高频短超时、独立重连钩子）。
type Option func(*channelSettings)

// channelSettings 是单条通道的内核参数集（连接本体 channel 的构建输入）。
type channelSettings struct {
	transport Transport
	addr      string
	path      string // 仅 WS：服务端包装路径

	serial            Serializer
	heartbeatInterval time.Duration
	invokeTimeout     time.Duration
	maxBodySize       int
	autoReconnect     bool
	backoffBase       time.Duration
	backoffMax        time.Duration
	queueSize         int
	hookTimeout       time.Duration
	onReconnected     func() error // 重连成功后的会话钩子（业务重登 / Join 重绑定）

	sessionHeartbeatOp       func() (string, any) // 会话心跳请求工厂（返回 operation 与 req；nil 跳过本轮）
	sessionHeartbeatInterval time.Duration        // 会话心跳周期
}

// defaultSettings 返回内核默认参数（与规范 §5.2 对齐）。
func defaultSettings() channelSettings {
	return channelSettings{
		serial:            JSONSerializer{},
		heartbeatInterval: defaultHeartbeatInterval,
		invokeTimeout:     defaultInvokeTimeout,
		maxBodySize:       frame.MaxBodySize,
		autoReconnect:     true,
		backoffBase:       defaultBackoffBase,
		backoffMax:        defaultBackoffMax,
		queueSize:         defaultQueueSize,
		hookTimeout:       defaultHookTimeout,
	}
}

// apply 按序应用配置项。
func (s *channelSettings) apply(opts []Option) {
	for _, opt := range opts {
		opt(s)
	}
}

// WithHeartbeatInterval 设置传输保活心跳周期（默认 30s；≤0 关闭心跳）。
func WithHeartbeatInterval(d time.Duration) Option {
	return func(s *channelSettings) { s.heartbeatInterval = d }
}

// WithInvokeTimeout 设置默认请求超时（默认 10s；可被 Invoke 的 ctx 覆盖）。
func WithInvokeTimeout(d time.Duration) Option {
	return func(s *channelSettings) { s.invokeTimeout = d }
}

// WithMaxBodySize 设置单帧 body 上限（默认/上限均为 frame.MaxBodySize）。
// 注意：必须与服务端配置对齐，单端调小即有断连风险（规范 §2）。
func WithMaxBodySize(n int) Option {
	return func(s *channelSettings) {
		if n > 0 && n <= frame.MaxBodySize {
			s.maxBodySize = n
		}
	}
}

// WithSerializer 设置序列化器（默认 JSON）。
func WithSerializer(s Serializer) Option {
	return func(c *channelSettings) { c.serial = s }
}

// WithAutoReconnect 设置断线自动重连开关（默认开启）。
func WithAutoReconnect(enabled bool) Option {
	return func(s *channelSettings) { s.autoReconnect = enabled }
}

// WithBackoff 设置重连退避参数（base 起步 ×2 递增、封顶 max，默认 500ms/30s，带抖动）。
func WithBackoff(base, max time.Duration) Option {
	return func(s *channelSettings) {
		if base > 0 {
			s.backoffBase = base
		}
		if max >= s.backoffBase {
			s.backoffMax = max
		}
	}
}

// WithReconnectQueueSize 设置断线重连期间的请求排队上限（默认 64；满后立即失败）。
func WithReconnectQueueSize(n int) Option {
	return func(s *channelSettings) {
		if n > 0 {
			s.queueSize = n
		}
	}
}

// WithSessionHeartbeat 注册 SDK 内置的会话心跳调度（规范 §5.2 双层心跳）：
// opFactory 每轮被调用返回 (operation, req)——业务侧从闭包捕获最新 token/player_id，
// req 为 nil 时不携带 payload；SDK 在业务通道 Connected 期间按 interval 周期 Invoke，
// 业务错误（*BusinessError）单飞触发 onReconnected 重登回调（并发去重），
// 网络错误静默（由重连机制处理）。
// 仅对业务通道生效（实现按通道角色强制；规范 §5.2：会话绑定业务通道，
// 战斗通道不续租）；interval 必须小于服务端会话租期（建议 租期/2）。
func WithSessionHeartbeat(interval time.Duration, opFactory func() (string, any)) Option {
	return func(s *channelSettings) {
		if interval > 0 && opFactory != nil {
			s.sessionHeartbeatInterval = interval
			s.sessionHeartbeatOp = opFactory
		}
	}
}

// WithOnReconnected 注册本通道重连成功后的会话钩子（业务侧在此重登/重绑定；
// 返回错误视为本次重连未完成，SDK 继续退避重试）。
// dual 形态下每通道独立：业务通道配置重登、战斗通道配置 Join 重绑定（规范 §5.2）。
// 钩子在 supervisor 内同步执行（评审 v0.3-B1）：执行期间本通道状态保持
// Reconnecting（外部请求继续排队），钩子内 Invoke 经内部直通路径送达当前代连接
// ——可安全调用 Invoke（含本通道重登）与 Close（不会自等待死锁）；
// 执行超过 hookTimeout 视为失败：连接被弃用、请求保留排队，SDK 退避重连后重试。
// 默认 10s。dual 形态下经 DialDual 配置的业务+战斗钩子自动链式编排
// （业务重登成功 → 战斗重绑），要求两个钩子都配置在 ChannelConfig.Opts。
func WithOnReconnected(fn func() error) Option {
	return func(s *channelSettings) { s.onReconnected = fn }
}
