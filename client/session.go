package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Session 是会话生命周期管理器：凭据保管、断线自动恢复、内置会话心跳与帧会话槽
// 装配。业务请求经 Client.Invoke 发送（消息体不含身份字段），无连接传输（UDP/KCP）
// 按帧会话槽携带凭据、长连接按连接绑定。
//
// 协议事实（op 名、凭据字段、被挤下线推送）全部经 SessionProtocol 接缝取得，本状态机
// 不引用任何会话消息类型；未注入接缝时所有会话方法返回 ErrNoSessionProtocol。
// 会话回执必须解析成已注册的生成 DTO（项目二进制须链接生成的会话 DTO 包），解析失败
// 返回 ErrSessionReplyUnresolved；关键凭据为空返回 ErrSessionCredentialsEmpty。
type Session struct {
	cli   *Client
	proto SessionProtocol

	mu           sync.RWMutex
	token        string
	playerID     string
	kickedReason string
	unbind       func() // Bind 的推送退订句柄（重复 Bind / Close 先退订）

	// 配置。
	heartbeatInterval time.Duration
	autoResume        bool
	onKicked          func(reason string)
	extraOnReconnect  func() error // 用户自定义重连钩子（自动恢复之后链式执行）
}

// SessionOption 配置 Session。
type SessionOption func(*Session)

// NewSession 构造会话管理器（经 Bind 绑定 Client、WithSessionProtocol 注入接缝后使用）。
func NewSession(opts ...SessionOption) *Session {
	s := &Session{
		heartbeatInterval: 30 * time.Second,
		autoResume:        true,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// WithSessionHeartbeatInterval 设置内置会话心跳周期（默认 30s；≤0 关闭）。
// 心跳请求不携带 payload：服务端按连接（长连接）或帧会话槽（无连接）定位会话续租。
func WithSessionHeartbeatInterval(interval time.Duration) SessionOption {
	return func(s *Session) { s.heartbeatInterval = interval }
}

// WithAutoResume 设置断线重连后自动恢复会话（默认开启）：
// 业务通道重连成功后以保存的凭据调 Resume；失败时 SDK 继续退避重连。
func WithAutoResume(enabled bool) SessionOption {
	return func(s *Session) { s.autoResume = enabled }
}

// WithResumeHook 设置自动恢复成功后的附加钩子（dual 形态战斗通道的 Join 重绑定
// 由用户在战斗通道 Option 里另行配置，与本钩子无关）。
func WithResumeHook(fn func() error) SessionOption {
	return func(s *Session) { s.extraOnReconnect = fn }
}

// WithOnKicked 设置被挤下线回调：接缝识别出该推送并清空凭据后调用，
// 参数是接缝提取的原因标识（枚举名，如 KICKED_REASON_LOGGED_IN_ELSEWHERE）。
func WithOnKicked(fn func(reason string)) SessionOption {
	return func(s *Session) { s.onKicked = fn }
}

// Bind 绑定 Client（必须先于 Login/Resume/Logout 调用），并自动订阅全部推送：接缝按
// 推送信封（op + 帧头编码版本 + 原始字节）判定「被挤下线」，命中即清空本地凭据、记录
// 原因并回调 onKicked（配置时）。重复 Bind 先退订旧订阅（订阅键 = 会话自身，稳定去重，
// 不按闭包指针），Close 时退订。未注入接缝返回 ErrNoSessionProtocol。
func (s *Session) Bind(cli *Client) error {
	proto, err := s.sessionProtocol()
	if err != nil {
		return err
	}
	s.unsubscribe()
	off := cli.business.onAnyKeyed(s.pushKey(), func(env PushEnvelope) {
		s.handlePush(proto, env)
	})
	s.mu.Lock()
	s.cli, s.unbind = cli, off
	s.mu.Unlock()
	return nil
}

// Close 解除会话绑定（幂等）：退订推送订阅并解绑 Client。Close 后不再有新推送进入本
// 会话（已在途的回调可能仍执行一次，分发已取订阅快照）；凭据保留供调用方读取或迁移。
func (s *Session) Close() error {
	s.unsubscribe()
	s.mu.Lock()
	s.cli = nil
	s.mu.Unlock()
	return nil
}

// handlePush 处理一条推送：接缝判定为「被挤下线」即清空凭据、记录原因并回调 onKicked。
func (s *Session) handlePush(proto SessionProtocol, env PushEnvelope) {
	reason, ok := proto.Kicked(env.Op, env)
	if !ok {
		return
	}
	s.clear()
	s.mu.Lock()
	s.kickedReason = reason
	s.mu.Unlock()
	if s.onKicked != nil {
		s.onKicked(reason)
	}
}

// unsubscribe 退订当前推送订阅（无订阅时安全）。
func (s *Session) unsubscribe() {
	s.mu.Lock()
	off := s.unbind
	s.unbind = nil
	s.mu.Unlock()
	if off != nil {
		off()
	}
}

// pushKey 返回本会话推送订阅的稳定去重键（会话自身；重复 Bind 退化成同一订阅）。
func (s *Session) pushKey() string { return fmt.Sprintf("session:%p", s) }

// ChannelOptions 返回装配到业务通道的选项：会话凭据提供者（帧会话槽）、
// 内置会话心跳与自动恢复钩子。在 Client 构造时传入。
func (s *Session) ChannelOptions() []Option {
	opts := []Option{WithSessionTokenProvider(func() string { return s.Token() })}
	if s.heartbeatInterval > 0 {
		opts = append(opts, WithSessionHeartbeat(s.heartbeatInterval, s.heartbeatRequest))
	}
	if s.autoResume {
		opts = append(opts, WithOnReconnected(s.resumeHook))
	}
	return opts
}

// heartbeatRequest 是会话心跳请求工厂：未登录或未注入接缝时跳过本轮（不发请求）。
func (s *Session) heartbeatRequest() (string, any) {
	if s.proto == nil || s.Token() == "" {
		return "", nil
	}
	return s.proto.Ops().Heartbeat, nil
}

// Login 调用会话登录接口并保管回执凭据；req 为业务登录请求（生成的会话 DTO，
// 如 *gatewayv1.LoginRequest，SDK 会就地补上 client_version）。
// 回执按接缝 op 解析成已注册的生成 DTO（如 *gatewayv1.LoginReply）：解析失败返回
// ErrSessionReplyUnresolved，回执未取到 token 与 playerId 返回 ErrSessionCredentialsEmpty
// ——空凭据不算登录成功（否则失败会推迟到 Resume/Heartbeat 才暴露）。
func (s *Session) Login(ctx context.Context, req any, opts ...InvokeOption) (any, error) {
	proto, err := s.ready()
	if err != nil {
		return nil, err
	}
	applyClientVersion(req)
	reply, err := s.invokeSession(ctx, proto.Ops().Login, req, opts...)
	if err != nil {
		return nil, err
	}
	token, playerID := proto.Token(reply), proto.PlayerID(reply)
	if token == "" && playerID == "" {
		return nil, fmt.Errorf("session: 登录回执未取到 token/playerId: %w", ErrSessionCredentialsEmpty)
	}
	s.mu.Lock()
	s.token, s.playerID, s.kickedReason = token, playerID, ""
	s.mu.Unlock()
	return reply, nil
}

// Register 调用注册接口（回执含 playerId；不建立会话、不保管凭据）：回执解析失败返回
// ErrSessionReplyUnresolved，未取到 playerId/token 返回 ErrSessionCredentialsEmpty。
func (s *Session) Register(ctx context.Context, req any, opts ...InvokeOption) (any, error) {
	proto, err := s.ready()
	if err != nil {
		return nil, err
	}
	reply, err := s.invokeSession(ctx, proto.Ops().Register, req, opts...)
	if err != nil {
		return nil, err
	}
	if proto.PlayerID(reply) == "" && proto.Token(reply) == "" {
		return nil, fmt.Errorf("session: 注册回执未取到 playerId: %w", ErrSessionCredentialsEmpty)
	}
	return reply, nil
}

// Resume 用保管中的凭据免密恢复会话（断线重连场景）；无凭据返回错误。
func (s *Session) Resume(ctx context.Context, opts ...InvokeOption) (any, error) {
	if _, err := s.ready(); err != nil {
		return nil, err
	}
	token := s.Token()
	if token == "" {
		return nil, errors.New("session: 无会话凭据（未登录）")
	}
	return s.restoreWith(ctx, token, s.PlayerID(), opts...)
}

// Restore 用外部凭据恢复会话（成功后凭据由 Session 保管）：断线重连/接管恢复场景——
// 凭据来自上一代连接（如 prev.token），区别于 Resume（用保管中的凭据）。
func (s *Session) Restore(ctx context.Context, token, playerID string, opts ...InvokeOption) (any, error) {
	if token == "" || playerID == "" {
		return nil, errors.New("session: 恢复凭据与玩家 ID 不能为空")
	}
	return s.restoreWith(ctx, token, playerID, opts...)
}

// restoreWith 以给定凭据构造恢复请求（生成 DTO：token/player_id/client_version）并调用
// 恢复 op；恢复回执必须取到 playerId（缺失返回 ErrSessionCredentialsEmpty），token 未
// 回带时沿用本地凭据。
func (s *Session) restoreWith(ctx context.Context, token, playerID string, opts ...InvokeOption) (any, error) {
	proto, err := s.ready()
	if err != nil {
		return nil, err
	}
	op := proto.Ops().Resume
	req := newSessionRequest(op, map[string]string{
		sessionFieldToken:         token,
		sessionFieldPlayerID:      playerID,
		sessionFieldClientVersion: Version,
	})
	reply, err := s.invokeSession(ctx, op, req, opts...)
	if err != nil {
		return nil, err
	}
	got := proto.PlayerID(reply)
	if got == "" {
		return nil, fmt.Errorf("session: 恢复回执未取到 playerId: %w", ErrSessionCredentialsEmpty)
	}
	s.mu.Lock()
	if t := proto.Token(reply); t != "" {
		s.token = t
	}
	s.playerID = got
	s.mu.Unlock()
	return reply, nil
}

// Logout 登出并清空本地凭据（无论请求成败都清空）。
func (s *Session) Logout(ctx context.Context, opts ...InvokeOption) error {
	proto, err := s.ready()
	if err != nil {
		return err
	}
	op := proto.Ops().Logout
	err = s.invoke(ctx, op, newSessionRequest(op, nil), nil, opts...)
	s.clear()
	return err
}

// Heartbeat 手动触发一次会话心跳并返回回执（未登录显式报错；内置定时器为静默跳过）。
// 无载荷：服务端按连接/帧槽定位会话续租。
func (s *Session) Heartbeat(ctx context.Context, opts ...InvokeOption) (any, error) {
	proto, err := s.ready()
	if err != nil {
		return nil, err
	}
	if s.Token() == "" {
		return nil, errors.New("session: 无会话凭据（未登录）")
	}
	return s.invokeSession(ctx, proto.Ops().Heartbeat, nil, opts...)
}

// Invoke 发送业务请求（会话凭据已按传输形态自动携带，消息体不含身份字段）。
func (s *Session) Invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error {
	return s.invoke(ctx, op, req, resp, opts...)
}

// Token 返回当前会话凭据（未登录为空串）。
func (s *Session) Token() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.token
}

// PlayerID 返回当前会话的玩家 ID（未登录为空串）。
func (s *Session) PlayerID() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.playerID
}

// KickedReason 返回最近一次「被挤下线」的原因标识（接缝提取；未被踢为空串）。
func (s *Session) KickedReason() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.kickedReason
}

// sessionProtocol 返回已注入的接缝；未注入返回 ErrNoSessionProtocol。
func (s *Session) sessionProtocol() (SessionProtocol, error) {
	if s.proto == nil {
		return nil, ErrNoSessionProtocol
	}
	return s.proto, nil
}

// ready 校验会话可用：接缝已注入且 Client 已绑定。
func (s *Session) ready() (SessionProtocol, error) {
	proto, err := s.sessionProtocol()
	if err != nil {
		return nil, err
	}
	if s.client() == nil {
		return nil, errors.New("session: 未绑定 Client")
	}
	return proto, nil
}

// client 返回已绑定的 Client（未绑定或已 Close 返回 nil）；与 Bind/Close 的写入互斥。
func (s *Session) client() *Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cli
}

// resumeHook 是断线重连后的自动恢复钩子：无凭据（未登录即断连）不算失败，
// 登录由业务层重新发起；有凭据则调 Resume，失败返回错误（SDK 继续退避重连后再试）。
func (s *Session) resumeHook() error {
	if s.Token() == "" {
		return nil
	}
	if _, err := s.Resume(context.Background()); err != nil {
		return err
	}
	if s.extraOnReconnect != nil {
		return s.extraOnReconnect()
	}
	return nil
}

// clear 清空凭据（登出或会话失效）；被踢原因不在此清空（供业务读取）。
func (s *Session) clear() {
	s.mu.Lock()
	s.token = ""
	s.playerID = ""
	s.mu.Unlock()
}

// invoke 委托业务通道 Invoke；Client 未绑定返回错误。
func (s *Session) invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error {
	cli := s.client()
	if cli == nil {
		return errors.New("session: 未绑定 Client")
	}
	return cli.Invoke(ctx, op, req, resp, opts...)
}

// invokeSession 发送会话请求并把回执解析成**已注册的生成 DTO**：op 在 protobuf 全局
// 注册表中查不到回执类型（项目二进制未链接生成的会话 DTO 包，或 op 与生成 stub 不同源）
// 即返回 ErrSessionReplyUnresolved——不回退通用载体 map：map 下接缝提取器取不到字段，
// token/playerId 会是空串，形成「空凭据却登录成功」的静默降级（S0.5 修订 1 · c①）。
func (s *Session) invokeSession(ctx context.Context, op string, req any, opts ...InvokeOption) (any, error) {
	target, ok := sessionMessageForOp(op, true)
	if !ok {
		return nil, fmt.Errorf("%w: op=%s", ErrSessionReplyUnresolved, op)
	}
	if err := s.invoke(ctx, op, req, target, opts...); err != nil {
		return nil, err
	}
	return target, nil
}
