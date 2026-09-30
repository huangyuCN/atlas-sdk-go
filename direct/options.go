package direct

import "time"

// 直连会话缺省参数。
const (
	// defaultInvokeTimeout 是单次请求超时（JoinBattle/SendFrameInput/SyncFrames）。
	defaultInvokeTimeout = 10 * time.Second
	// defaultHandshakeTimeout 是握手段超时（WS 升级 / hello 段等 flow-id）。
	defaultHandshakeTimeout = 3 * time.Second
	// defaultBackoffBase 是重连退避起步时长。
	defaultBackoffBase = 500 * time.Millisecond
	// defaultBackoffMax 是重连退避封顶时长。
	defaultBackoffMax = 10 * time.Second
	// defaultPath 是 WS 升级路径（接入层原样转发升级头，路径须与 battle 帧面一致）。
	defaultPath = "/"
)

// Option 配置直连会话（Open 的可选项）。
type Option func(*options)

// options 是一次直连会话的生效配置。
type options struct {
	transport        Transport     // 显式指定的面（0 = 按 ws → kcp → udp 取计划里第一个存在的面）
	path             string        // WS 升级路径
	invokeTimeout    time.Duration // 单次请求超时
	handshakeTimeout time.Duration // 握手段超时
	backoffBase      time.Duration // 重连退避起步
	backoffMax       time.Duration // 重连退避封顶
	autoReconnect    bool          // 断线自动重连（被接入层拒绝/票问题一律不重试）
	edgeHello        bool          // 是否走接入层 hello 握手段
}

// defaultOptions 返回缺省配置。
func defaultOptions() options {
	return options{
		path:             defaultPath,
		invokeTimeout:    defaultInvokeTimeout,
		handshakeTimeout: defaultHandshakeTimeout,
		backoffBase:      defaultBackoffBase,
		backoffMax:       defaultBackoffMax,
		autoReconnect:    true,
		edgeHello:        true,
	}
}

// WithTransport 指定直连传输面；计划里没有该面的地址即报错（不静默换面）。
// 不指定时按 ws → kcp → udp 取计划里第一个存在的面。
func WithTransport(t Transport) Option {
	return func(o *options) { o.transport = t }
}

// WithPath 设置 WS 升级路径（缺省 "/"；接入层按原样转发升级头，路径须与 battle 帧面一致）。
func WithPath(path string) Option {
	return func(o *options) {
		if path != "" {
			o.path = path
		}
	}
}

// WithInvokeTimeout 设置单次请求超时（缺省 10s）。
func WithInvokeTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.invokeTimeout = d
		}
	}
}

// WithHandshakeTimeout 设置握手段超时（WS 升级 / hello 等 flow-id，缺省 3s）。
func WithHandshakeTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.handshakeTimeout = d
		}
	}
}

// WithReconnectBackoff 设置重连退避（base 起步 ×2 递增、封顶 max，缺省 500ms/10s）。
func WithReconnectBackoff(base, max time.Duration) Option {
	return func(o *options) {
		if base > 0 {
			o.backoffBase = base
		}
		if max >= o.backoffBase {
			o.backoffMax = max
		}
	}
}

// WithAutoReconnect 开关断线自动重连（缺省开启）：重连即重新 hello 并重放
// JoinBattle/SyncFrames 补帧；被接入层拒绝与票据失效属不可重试错误，一律终止且不重试。
func WithAutoReconnect(enabled bool) Option {
	return func(o *options) { o.autoReconnect = enabled }
}

// WithoutEdgeHello 关闭接入层 hello 握手段，直接对 battle 帧端口建连（联调/闭环验证用）：
// WS 不再带升级 query 票，KCP/UDP 不再发 hello 段、不带 flow-id 前缀。
// 生产路径必须经接入层，故缺省开启 hello；地址来源仍须是本局推送。
func WithoutEdgeHello() Option {
	return func(o *options) { o.edgeHello = false }
}
