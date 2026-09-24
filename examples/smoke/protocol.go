// sessionProtocol 是会话协议接缝的项目侧实现：5 个 op 名、3 个解码钩子与推送识别
// 全部取自模板仓生成物（api/gateway/v1/opclient），SDK 内核零会话消息类型。
// 真实项目里的接入即此形态——把生成 stub 的描述符接到 client.WithSessionProtocol
// （生成物只给素材，接缝实现由项目侧提供）。
package main

import (
	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// sessionProtocol 是零状态的接缝实现（方法全部直接转发生成物素材）。
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

// Token 经生成物提取器取 token（会话请求/回执；无该字段返回空串）。
func (sessionProtocol) Token(msg any) string { return gatewayv1opclient.SessionToken(msg) }

// PlayerID 经生成物提取器取 playerId（无该字段返回空串）。
func (sessionProtocol) PlayerID(msg any) string { return gatewayv1opclient.SessionPlayerID(msg) }

// ExpiresAt 经生成物提取器取过期时间（模板当前无该字段，恒 0，本轮不启用续期）。
func (sessionProtocol) ExpiresAt(msg any) int64 { return gatewayv1opclient.SessionExpiresAt(msg) }

// Kicked 按生成物推送 op 判定被挤下线并解出原因枚举名。msg 是推送信封
// （client.PushEnvelope：op + 帧头编码版本 + 原始字节），本实现按 Version 选解码器——
// ver=1 走 protojson、ver=2 走 protobuf wire；未知版本不解码（识别为被挤下线但取不到
// 原因，不 panic、不猜编码）。
func (sessionProtocol) Kicked(op string, msg any) (string, bool) {
	if op != gatewayv1opclient.SessionPushOps.KickedNotify {
		return "", false
	}
	env, ok := msg.(client.PushEnvelope)
	if !ok || len(env.Body) == 0 {
		return "", true // 识别为被挤下线，但载荷取不到原因
	}
	var notify gatewayv1.KickedNotify
	var err error
	switch env.Version {
	case frame.Version:
		err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(env.Body, &notify)
	case frame.Version2:
		err = proto.Unmarshal(env.Body, &notify)
	default:
		return "", true
	}
	if err != nil {
		return "", true
	}
	return notify.GetReason().String(), true
}
