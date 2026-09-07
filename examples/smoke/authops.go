// authOps 抽象注册/登录/心跳的 DTO 构造与响应解析——三编码统一用 gatewayv1
// proto message（与模板 api/gateway/v1/auth.proto 同源，examples/smoke/gatewayv1
// 子集）：json/protojson 走默认双通道 ProtoJSONSerializer（protojson 语义、零值
// 省略），protobuf 走 contrib/protobuf（ver=2 二进制）。
package main

import (
	"context"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/examples/smoke/gatewayv1"
)

// authOps 提供三编码共用的 op 调用（req/resp 统一为 proto message）。
type authOps interface {
	// register 注册账号，返回 playerId。
	register(ctx context.Context, c *client.Client, account string) (string, error)
	// login 登录，返回 playerId/token。
	login(ctx context.Context, c *client.Client, player, password string) (string, string, error)
	// heartbeat 业务心跳往返。
	heartbeat(ctx context.Context, c *client.Client, player, token string) error
	// joinBattle 战斗绑定探针（KCP/UDP 战斗通道；伪造 token 验证业务 payload
	// 编解码——服务端因会话无效回业务拒绝即证明解码成功）。
	joinBattle(ctx context.Context, c *client.Client) error
}

// protoAuthOps 是统一 DTO 实现（gatewayv1 proto message；编码由 serializer 决定）。
type protoAuthOps struct{}

func newAuthOps(_ smokeMode) authOps {
	return protoAuthOps{}
}

func (protoAuthOps) register(ctx context.Context, c *client.Client, account string) (string, error) {
	var rep gatewayv1.RegisterReply
	if err := c.Invoke(ctx, opRegister, &gatewayv1.RegisterRequest{
		Account: account, Password: smokePassword, Nickname: "冒烟玩家",
	}, &rep); err != nil {
		return "", err
	}
	return rep.GetPlayerId(), nil
}

func (protoAuthOps) login(ctx context.Context, c *client.Client, player, password string) (string, string, error) {
	var rep gatewayv1.LoginReply
	if err := c.Invoke(ctx, opLogin, &gatewayv1.LoginRequest{
		PlayerId: player, Password: password,
	}, &rep); err != nil {
		return "", "", err
	}
	return rep.GetPlayerId(), rep.GetToken(), nil
}

func (protoAuthOps) joinBattle(ctx context.Context, c *client.Client) error {
	var rep gatewayv1.JoinBattleReply
	return c.Invoke(ctx, opJoinBattle, &gatewayv1.JoinBattleRequest{
		Token: "no-token", PlayerId: "none", BattleId: "b1",
	}, &rep)
}

func (protoAuthOps) heartbeat(ctx context.Context, c *client.Client, player, token string) error {
	var hb gatewayv1.HeartbeatReply
	return c.Invoke(ctx, opHeartbeat, &gatewayv1.HeartbeatRequest{
		Token: token, PlayerId: player, Ts: time.Now().UnixMilli(),
	}, &hb)
}
