// authOps 抽象注册/登录/心跳/战斗绑定的调用形态——DTO 与 op 全部取自本仓生成物
// （api/gateway/v1、api/battle/v1 及其 opclient stub），冒烟内不写字面量契约：
//   - 注册/战斗绑定走生成 stub（强类型方法，op 由生成描述符决定）；
//   - 登录/会话心跳走 client.Session（凭据由状态机保管，op 由接缝提供）。
//
// json/protojson 走默认双通道 ProtoJSONSerializer（protojson 语义、零值省略），
// protobuf 走 contrib/protobuf（ver=2 二进制）。
package main

import (
	"context"
	"fmt"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
)

// smokePassword 是冒烟账号口令（每次运行随机账号）。
const smokePassword = "pw-123456"

// register 用生成的会话 stub 注册账号，返回 playerId（注册不建立会话）。
func register(ctx context.Context, c *client.Client, account string) (string, error) {
	rep, err := gatewayv1opclient.NewSession(c).Register(ctx, &gatewayv1.RegisterRequest{
		Account: account, Password: smokePassword, Nickname: "冒烟玩家",
	})
	if err != nil {
		return "", err
	}
	return rep.GetPlayerId(), nil
}

// login 经会话状态机登录：客户端版本由 SDK 注入 client_version，回执按生成 DTO 解码
// （api/gateway/v1.LoginReply 已登记全局注册表），凭据由 Session 保管。
func login(ctx context.Context, sess *client.Session, player string) error {
	reply, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: player, Password: smokePassword})
	if err != nil {
		return err
	}
	if _, ok := reply.(*gatewayv1.LoginReply); !ok {
		return fmt.Errorf("登录回执类型 %T（期望 *gatewayv1.LoginReply）", reply)
	}
	return nil
}

// sessionHeartbeat 会话心跳往返（业务续租层；传输心跳由 SDK 周期自动发送）：
// op 取自会话接缝；未登录时显式报错（内置定时心跳为静默跳过）。
func sessionHeartbeat(ctx context.Context, sess *client.Session) error {
	_, err := sess.Heartbeat(ctx)
	return err
}

// registerAndLogin 注册并登录，返回 playerId（登录凭据由 Session 保管）。
func registerAndLogin(c *client.Client, sess *client.Session, account string) (string, error) {
	ctx := context.Background()
	player, err := register(ctx, c, account)
	if err != nil {
		return "", fmt.Errorf("注册失败: %w", err)
	}
	fmt.Printf("[冒烟] 注册成功 playerId=%s\n", player)
	if err := login(ctx, sess, player); err != nil {
		return "", fmt.Errorf("登录失败: %w", err)
	}
	return player, nil
}
