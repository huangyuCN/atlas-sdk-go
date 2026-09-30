package main

import (
	"context"
	"fmt"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-sdk-go/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/direct"
)

// password 是闭环账号口令（每次运行随机账号，互不干扰）。
const password = "pw-directloop"

// enterMatchQueueOp 是入队 op（game.v1.PlayerService 的 CLIENT 方法）；SDK 未内置 game.v1
// 生成物，故以命名常量收敛字面量（与模板生成物的 op 名逐字一致）。
const enterMatchQueueOp = "/game.v1.PlayerService/EnterMatchQueue"

// player 是一名业务侧玩家：SDK 客户端 + 会话状态机 + 成局推送通道。
type player struct {
	id      string
	cli     *client.Client
	sess    *client.Session
	started chan direct.Plan
}

// openPlayers 建立 count 名玩家（拨号 → 注册 → 登录 → 订阅成局推送）；任一失败即回收已建连接。
func openPlayers(ctx context.Context, gateway string, count int) ([]*player, error) {
	out := make([]*player, 0, count)
	for i := 0; i < count; i++ {
		p, err := newPlayer(ctx, gateway, i)
		if err != nil {
			closePlayers(out)
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// newPlayer 建立单人业务连接并完成注册登录，订阅成局推送（op 用 direct 包的命名常量）。
func newPlayer(ctx context.Context, gateway string, idx int) (*player, error) {
	sess := client.NewSession(client.WithSessionProtocol(sessionProtocol{}))
	opts := append([]client.Option{
		client.WithHeartbeatInterval(0), client.WithInvokeTimeout(10 * time.Second),
	}, sess.ChannelOptions()...)
	cli, err := client.Dial(gateway, opts...)
	if err != nil {
		return nil, fmt.Errorf("拨号网关 %s 失败: %w", gateway, err)
	}
	if err := sess.Bind(cli); err != nil {
		_ = cli.Close()
		return nil, err
	}
	account := fmt.Sprintf("directloop-%d-%d", time.Now().UnixNano(), idx)
	reg, err := gatewayv1opclient.NewSession(cli).Register(ctx, &gatewayv1.RegisterRequest{
		Account: account, Password: password, Nickname: "直连闭环",
	})
	if err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("注册 %s 失败: %w", account, err)
	}
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: password}); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("登录 %s 失败: %w", reg.GetPlayerId(), err)
	}
	p := &player{id: reg.GetPlayerId(), cli: cli, sess: sess, started: make(chan direct.Plan, 1)}
	cli.On(direct.PushOpMatchStarted, func(op string, payload []byte) {
		plan, err := direct.PlanFromPush(op, payload)
		if err != nil {
			fmt.Printf("[玩家] %s 成局通知解析失败: %v\n", p.id, err)
			return
		}
		select {
		case p.started <- plan:
		default:
		}
	})
	fmt.Printf("[玩家] %s 注册登录完成\n", p.id)
	return p, nil
}

// queueAll 让全部玩家入队（入队载荷是 game.v1 的 protojson 字段：ruleset）。
func queueAll(ctx context.Context, players []*player, ruleset string) error {
	for _, p := range players {
		req := map[string]string{"ruleset": ruleset}
		if err := p.cli.Invoke(ctx, enterMatchQueueOp, req, nil); err != nil {
			return fmt.Errorf("%s 入队失败: %w", p.id, err)
		}
	}
	fmt.Printf("[匹配] %d 名玩家已入队\n", len(players))
	return nil
}

// awaitPlans 等全部玩家的成局推送并取回直连计划（核对同一局、票据非空）。
func awaitPlans(ctx context.Context, players []*player) ([]direct.Plan, error) {
	plans := make([]direct.Plan, 0, len(players))
	for _, p := range players {
		select {
		case plan := <-p.started:
			if plan.BattleID == "" {
				return nil, fmt.Errorf("%s 成局通知缺 battle_id", p.id)
			}
			if len(plan.Ticket) == 0 {
				return nil, fmt.Errorf("%s 成局通知缺 battle_ticket", p.id)
			}
			plans = append(plans, plan)
		case <-ctx.Done():
			return nil, fmt.Errorf("%s 未收到成局通知: %w", p.id, ctx.Err())
		}
	}
	if plans[0].BattleID != plans[1].BattleID {
		return nil, fmt.Errorf("双方 battle_id 不一致: %q vs %q", plans[0].BattleID, plans[1].BattleID)
	}
	fmt.Printf("[成局] battle=%s 双方票据已到手（面数各 %d）\n", plans[0].BattleID, len(plans[0].Endpoints))
	return plans, nil
}

// closePlayers 回收全部业务连接（幂等）。
func closePlayers(players []*player) {
	for _, p := range players {
		if p == nil {
			continue
		}
		if p.sess != nil {
			_ = p.sess.Close()
		}
		if p.cli != nil {
			_ = p.cli.Close()
		}
	}
}
