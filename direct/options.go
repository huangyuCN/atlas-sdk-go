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
	// defaultHeartbeat 是缺省保活探针周期（2s）：必须**严格小于**数据报面空闲读超时
	//（battle 侧 offline_timeout/3，缺省 15s/3 = 5s），否则静默期仍会被判掉线。
	defaultHeartbeat = 2 * time.Second
	// defaultEndLinger 是缺省的终态收尾窗口（2s，取值依据见 WithEndLinger）。
	defaultEndLinger = 2 * time.Second
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
	heartbeat        time.Duration // 保活探针周期（<= 0 = 关闭）
	endLinger        time.Duration // 终态收尾窗口（<= 0 = 进入终态即关连接）
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
		heartbeat:        defaultHeartbeat,
		endLinger:        defaultEndLinger,
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

// WithHeartbeat 设置直连保活探针周期（battle.v1.BattleService/Ping，Tell 无业务回执）：
// 无输入期间由会话周期发送，维持帧面活跃（数据报面靠收包刷新空闲读超时，顺带续 NAT 映射）。
// 缺省 2s，必须严格小于 battle 侧 offline_timeout/3（缺省 15s/3 = 5s）；period <= 0 关闭探针
// （自管心跳或对照实验用，关闭后静默期会被判掉线）。
func WithHeartbeat(period time.Duration) Option {
	return func(o *options) { o.heartbeat = period }
}

// WithEndLinger 设置终态收尾窗口：进入终态（收到结束通知，或任一 op 被 BATTLE_ENDED 拒绝）后，
// 会话在窗口内**仍然可读**——结算结果会被服务端有界补投（首投 + 关闭前 2 次重投 + 迟到/重连
// 补投，每局每人上限 5 次），尾帧也可能还在路上；窗口到期由**客户端**关闭连接，因为数据报面
// （KCP/UDP）没有关闭握手，对端关闭不产生 EOF，只能客户端兜底收连接。
//
// 缺省 2s 的依据（与 TS/C# SDK 同一口径）：首投与 CloseBattle 前的 2 次重投都发生在结算后
// 约 1 个 RTT 内，2s 已覆盖这段尺度并留出抖动余量；又远小于服务端结束留档 TTL（票据有效期 +
// 掉线窗口，缺省 120s + 15s = 135s——窗口只需覆盖「结果送到手」这一段，不必让 socket 与
// goroutine 多挂）。d <= 0 表示不等窗口：进入终态即关连接（负值按 0 处理）。
func WithEndLinger(d time.Duration) Option {
	return func(o *options) { o.endLinger = max(d, 0) }
}
