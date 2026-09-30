// protocol.go 是闭环脚本的会话协议接缝实现：op 名、凭据提取、推送识别全部取自本仓生成物
// （api/gateway/v1/opclient）。SDK 内核不内置任何会话消息类型，项目侧以此形态接入。
package main

import (
	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
)

// sessionProtocol 是零状态接缝实现（闭环里只用到登录链路，被挤下线识别退化为不识别）。
type sessionProtocol struct{}

// Ops 返回生成物声明的 5 个会话 op 名。
func (sessionProtocol) Ops() client.SessionOps {
	ops := gatewayv1opclient.SessionProtocolOps
	return client.SessionOps{
		Register:  ops.Register,
		Login:     ops.Login,
		Resume:    ops.Resume,
		Logout:    ops.Logout,
		Heartbeat: ops.Heartbeat,
	}
}

// Token 经生成物提取器取 token（无该字段的消息返回空串）。
func (sessionProtocol) Token(msg any) string { return gatewayv1opclient.SessionToken(msg) }

// PlayerID 经生成物提取器取 playerId（无该字段的消息返回空串）。
func (sessionProtocol) PlayerID(msg any) string { return gatewayv1opclient.SessionPlayerID(msg) }

// ExpiresAt 返回会话过期时刻（模板当前无该字段，恒 0）。
func (sessionProtocol) ExpiresAt(msg any) int64 { return gatewayv1opclient.SessionExpiresAt(msg) }

// Kicked 识别「被挤下线」推送：闭环不模拟顶号，故一律不识别。
func (sessionProtocol) Kicked(string, any) (string, bool) { return "", false }

// 保证生成物 import 被使用（凭据提取器类型断言需要生成 DTO 类型在全局注册表中）。
var _ = gatewayv1.LoginReply{}
