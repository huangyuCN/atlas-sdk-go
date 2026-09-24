package client

import "errors"

// ErrNoSessionProtocol 表示会话状态机未注入 SessionProtocol 接缝。SDK 不留任何
// gateway.v1 默认副本：未接入接缝即显式报错，避免静默沿用已漂移的旧契约。
var ErrNoSessionProtocol = errors.New("session: 未接入会话协议（client.WithSessionProtocol）")

// ErrSessionReplyUnresolved 表示会话 op 的回执无法解析成**已注册的生成 DTO**：项目
// 二进制没有链接生成的会话 DTO 包，或 op 与生成 stub 不同源。会话凭据只能从生成 DTO
// 提取，故此处必须显式失败——回退到通用载体 map 会让 token/playerId 取到空串却报
// 「登录成功」，直到 Resume/Heartbeat 才暴露（S0.5 修订 1 · c①）。
var ErrSessionReplyUnresolved = errors.New("session: 回执未解析成已注册的生成 DTO（项目须链接生成的会话 DTO 包）")

// ErrSessionCredentialsEmpty 表示会话回执解析成功但关键凭据为空：Login/Register 至少
// 要有 token 或 playerId，Resume/Restore 必须有 playerId。空凭据不算成功——否则失败
// 会推迟到后续业务请求才暴露。
var ErrSessionCredentialsEmpty = errors.New("session: 关键凭据为空（token/playerId）")

// SessionOps 是会话生命周期 op 名集合（register / login / resume / logout / heartbeat）。
// 唯一来源是 SessionProtocol 实现——典型为模板仓生成的会话 stub 描述符
// （<Pkg>ProtocolOps）；SDK 内核不留任何 op 字面量副本。
type SessionOps struct {
	Register  string
	Login     string
	Resume    string
	Logout    string
	Heartbeat string
}

// SessionProtocol 是会话协议接缝（三语言同名同职责；S0.5 冻结形状：5 个 op +
// 3 个解码钩子 + 1 个推送识别，各语言不得自行加成员）。
//
// 会话状态机（登录/注册/恢复/登出/心跳 + 重连 + 被踢）只依赖本接缝：op 名、凭据提取
// 与推送识别全部经它取得，SDK 内核不引用任何会话消息类型。实现由项目侧基于模板仓
// 生成的会话 stub/描述符提供（生成物只给素材：<Pkg>ProtocolOps 五个 op、Token/
// PlayerID/ExpiresAt 三个提取器、推送 op 常量；适配器形态见 examples/smoke/protocol.go）：
//
//	client.NewSession(client.WithSessionProtocol(sessionProtocol{}))
type SessionProtocol interface {
	// Ops 返回 5 个会话 op 名（接缝是唯一来源，状态机不写 op 字面量）。
	Ops() SessionOps
	// Token 从会话请求/回执取 token；无该字段返回空串（可选钩子语义）。
	Token(msg any) string
	// PlayerID 从会话请求/回执取玩家 ID；无该字段返回空串（可选钩子语义）。
	PlayerID(msg any) string
	// ExpiresAt 从会话回执取过期时间（毫秒；无该字段返回 0）。模板当前无 expiry 字段，
	// 故本轮不启用续期——钩子先备好，协议补字段后由状态机直接消费。
	ExpiresAt(msg any) int64
	// Kicked 判定推送是否为「被挤下线」：op 命中推送 op 时返回 ok=true 与原因标识
	// （枚举名，如 KICKED_REASON_LOGGED_IN_ELSEWHERE；载荷取不到原因时 reason 为空串）；
	// 非本会话推送返回 ok=false。msg 的具象类型是 PushEnvelope（op + 帧头编码版本 +
	// 原始字节）——实现方必须按 Version 选解码器（ver=1 protojson / ver=2 protobuf wire），
	// 假定单一编码会让 ver=2 的原因静默丢失（S0.5 修订 1 · c②）。
	Kicked(op string, msg any) (reason string, ok bool)
}

// WithSessionProtocol 注入会话协议接缝（会话状态机的唯一协议来源）。
func WithSessionProtocol(p SessionProtocol) SessionOption {
	return func(s *Session) { s.proto = p }
}
