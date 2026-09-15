package client

import (
	"context"
	"errors"
	"sync"
	"time"
)

// 会话生命周期接口的默认 operation（gateway.v1 收敛后的 Gateway 自留接口；
// 服务端 proto 重写后以此为准，可经 WithSessionOps 覆盖）。
const (
	// OpSessionLogin 登录：凭账号信息建立会话，回执下发会话凭据。
	OpSessionLogin = "/gateway.v1.Session/Login"
	// OpSessionRegister 注册（建立会话前的账号创建）。
	OpSessionRegister = "/gateway.v1.Session/Register"
	// OpSessionResume 断线重连免密恢复会话（凭据从帧会话槽或请求体携带）。
	OpSessionResume = "/gateway.v1.Session/Resume"
	// OpSessionLogout 登出（服务端清理会话）。
	OpSessionLogout = "/gateway.v1.Session/Logout"
	// OpSessionHeartbeat 会话心跳（无 payload；服务端按连接/会话槽续租）。
	OpSessionHeartbeat = "/gateway.v1.Session/Heartbeat"
	// OpSessionKickedNotify 是被挤下线推送的 operation（服务端 Notify 帧按消息名寻址）。
	OpSessionKickedNotify = "/gateway.v1.KickedNotify"
)

// SessionReply 是会话生命周期接口的统一回执形状（协议约定：gateway.v1 会话消息）。
// protojson 语义下字段名为 lowerCamel；业务也可用自身 DTO 经 protojson 解析。
type SessionReply struct {
	PlayerID string `json:"playerId,omitempty"`
	Token    string `json:"token,omitempty"`
}

// ResumeReq 是会话恢复请求（凭据与玩家 ID 放请求体；服务端按路由表校验后重绑连接）。
type ResumeReq struct {
	Token    string `json:"token,omitempty"`
	PlayerID string `json:"playerId,omitempty"`
}

// LogoutReq 是登出请求。
type LogoutReq struct {
	Token string `json:"token,omitempty"`
}

// Session 是会话生命周期管理器：凭据保管、断线自动恢复、内置会话心跳
// 与帧会话槽装配。业务请求经 Client.Invoke 发送（消息体不含身份字段），
// 无连接传输（UDP/KCP）按帧会话槽携带凭据、长连接按连接绑定。
type Session struct {
	cli *Client
	ops SessionOps

	mu        sync.RWMutex
	token     string
	playerID  string
	closeCh   chan struct{}
	closeOnce sync.Once

	// 配置。
	heartbeatInterval time.Duration
	autoResume        bool
	onKicked          func(reason string)
	extraOnReconnect  func() error // 用户自定义重连钩子（自动恢复之后链式执行）
}

// SessionOption 配置 Session。
type SessionOption func(*Session)

// SessionOps 是会话生命周期 op 名集合（默认见 OpSession* 常量；可整体覆盖）。
type SessionOps struct {
	Login     string
	Resume    string
	Logout    string
	Heartbeat string
}

// DefaultSessionOps 返回默认会话 op 集。
func DefaultSessionOps() SessionOps {
	return SessionOps{
		Login:     OpSessionLogin,
		Resume:    OpSessionResume,
		Logout:    OpSessionLogout,
		Heartbeat: OpSessionHeartbeat,
	}
}

// NewSession 构造会话管理器（经 Bind 绑定 Client 后使用）。
func NewSession(opts ...SessionOption) *Session {
	s := &Session{
		ops:               DefaultSessionOps(),
		heartbeatInterval: 30 * time.Second,
		autoResume:        true,
		closeCh:           make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// WithSessionOps 覆盖会话生命周期 op 名（服务端 op 约定不一致时使用）。
func WithSessionOps(ops SessionOps) SessionOption {
	return func(s *Session) { s.ops = ops }
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

// Bind 绑定 Client（必须先于 Login/Resume/Logout 调用），并自动订阅被挤下线
// 推送：收到即清空本地凭据（会话已失效），onKicked 回调（配置时）收到原因。
func (s *Session) Bind(cli *Client) {
	s.cli = cli
	cli.On(OpSessionKickedNotify, func(_ string, _ []byte) {
		s.clear()
	})
}

// ChannelOptions 返回装配到业务通道的选项：会话凭据提供者（帧会话槽）、
// 内置会话心跳与自动恢复钩子。在 Client 构造时传入。
func (s *Session) ChannelOptions() []Option {
	opts := []Option{WithSessionTokenProvider(func() string { return s.Token() })}
	if s.heartbeatInterval > 0 {
		ops := s.ops
		opts = append(opts, WithSessionHeartbeat(s.heartbeatInterval, func() (string, any) {
			if s.Token() == "" {
				return "", nil // 未登录：跳过本轮
			}
			return ops.Heartbeat, nil
		}))
	}
	if s.autoResume {
		opts = append(opts, WithOnReconnected(s.resumeHook))
	}
	return opts
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

// Login 调用会话登录接口并保管回执凭据。
// req 为业务登录请求（protojson 序列化；如账号密码）。
func (s *Session) Login(ctx context.Context, req any) (SessionReply, error) {
	return s.call(ctx, s.ops.Login, req)
}

// Register 调用注册接口（回执含 playerId；不建立会话）。
func (s *Session) Register(ctx context.Context, req any) (SessionReply, error) {
	var reply SessionReply
	if err := s.invoke(ctx, OpSessionRegister, req, &reply); err != nil {
		return SessionReply{}, err
	}
	return reply, nil
}

// Resume 用保管中的凭据免密恢复会话（断线重连场景）；无凭据返回错误。
func (s *Session) Resume(ctx context.Context) (SessionReply, error) {
	token := s.Token()
	if token == "" {
		return SessionReply{}, errors.New("session: 无会话凭据（未登录）")
	}
	return s.call(ctx, s.ops.Resume, ResumeReq{Token: token, PlayerID: s.PlayerID()})
}

// Logout 登出并清空本地凭据。
func (s *Session) Logout(ctx context.Context) error {
	err := s.invoke(ctx, s.ops.Logout, LogoutReq{Token: s.Token()}, nil)
	s.clear()
	return err
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

// clear 清空凭据（登出或会话失效）。
func (s *Session) clear() {
	s.mu.Lock()
	s.token = ""
	s.playerID = ""
	s.mu.Unlock()
}

// invoke 委托业务通道 Invoke；cli 未绑定返回错误。
func (s *Session) invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error {
	if s.cli == nil {
		return errors.New("session: 未绑定 Client")
	}
	return s.cli.Invoke(ctx, op, req, resp, opts...)
}

// call 调用会话接口并在成功后保管回执凭据。
func (s *Session) call(ctx context.Context, op string, req any) (SessionReply, error) {
	var reply SessionReply
	if err := s.invoke(ctx, op, req, &reply); err != nil {
		return SessionReply{}, err
	}
	s.mu.Lock()
	s.token = reply.Token
	s.playerID = reply.PlayerID
	s.mu.Unlock()
	return reply, nil
}
