// authOps 抽象注册/登录/心跳的 DTO 构造与响应解析——按编码模式分派：
// json 用 plain struct；protojson/protobuf 用 gatewayv1 proto message
// （与模板 api/gateway/v1/auth.proto 同源，examples/smoke/gatewayv1 子集）。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/examples/smoke/gatewayv1"
)

// authOps 提供三编码共用的 op 调用（req/resp 形态由实现决定）。
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

// jsonAuthOps 是 json 编码的 plain struct DTO（与既有 smoke 一致）。
type jsonAuthOps struct{}

// protoAuthOps 是 protojson/protobuf 编码的 proto message DTO。
type protoAuthOps struct{}

func newAuthOps(m smokeMode) authOps {
	if m == modeJSON {
		return jsonAuthOps{}
	}
	return protoAuthOps{}
}

// ---- json 实现（plain struct，字段 camelCase tag） ----

type jsonRegisterReq struct {
	Account  string `json:"account"`
	Password string `json:"password"`
	Nickname string `json:"nickname"`
}
type jsonRegisterReply struct {
	PlayerId string `json:"playerId"`
}
type jsonLoginReq struct {
	PlayerId string `json:"playerId"`
	Password string `json:"password"`
}
type jsonLoginReply struct {
	PlayerId string `json:"playerId"`
	Token    string `json:"token"`
}
type jsonHeartbeatReq struct {
	Token    string `json:"token"`
	PlayerId string `json:"playerId"`
	Ts       string `json:"ts"` // int64 → protojson 字符串形态
}
type jsonHeartbeatReply struct {
	Ok bool `json:"ok"`
}
type jsonJoinBattleReq struct {
	Token    string `json:"token"`
	PlayerId string `json:"playerId"`
	BattleId string `json:"battleId"`
}
type jsonJoinBattleReply struct {
	Ok      bool   `json:"ok"`
	Message string `json:"message"`
}

func (jsonAuthOps) register(ctx context.Context, c *client.Client, account string) (string, error) {
	var rep jsonRegisterReply
	if err := c.Invoke(ctx, opRegister, jsonRegisterReq{
		Account: account, Password: smokePassword, Nickname: "冒烟玩家",
	}, &rep); err != nil {
		return "", err
	}
	return rep.PlayerId, nil
}

func (jsonAuthOps) login(ctx context.Context, c *client.Client, player, password string) (string, string, error) {
	var rep jsonLoginReply
	if err := c.Invoke(ctx, opLogin, jsonLoginReq{PlayerId: player, Password: password}, &rep); err != nil {
		return "", "", err
	}
	return rep.PlayerId, rep.Token, nil
}

func (jsonAuthOps) joinBattle(ctx context.Context, c *client.Client) error {
	var rep jsonJoinBattleReply
	return c.Invoke(ctx, opJoinBattle, jsonJoinBattleReq{Token: "no-token", PlayerId: "none", BattleId: "b1"}, &rep)
}

func (jsonAuthOps) heartbeat(ctx context.Context, c *client.Client, player, token string) error {
	var hb jsonHeartbeatReply
	return c.Invoke(ctx, opHeartbeat, jsonHeartbeatReq{
		Token: token, PlayerId: player, Ts: fmt.Sprintf("%d", time.Now().UnixMilli()),
	}, &hb)
}

// ---- proto 实现（proto message，json 名由 protojson/protobuf 决定） ----

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

// 编译期断言：两实现均满足接口。
var (
	_ authOps = jsonAuthOps{}
	_ authOps = protoAuthOps{}
)
