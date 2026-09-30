package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas-sdk-go/api/lockstep"
	"github.com/huangyuCN/atlas-sdk-go/direct"
)

// battleRound 跑一局直连闭环：建连 → 逐人入局/输入 → 补帧（可选重连）→ 等帧广播。
func battleRound(ctx context.Context, o roundOpts, players []*player, plans []direct.Plan) error {
	sessions, err := openBattleSessions(ctx, o, players, plans)
	if err != nil {
		return err
	}
	defer closeBattleSessions(sessions)
	counters := watchFrames(sessions)
	for i, s := range sessions {
		if err := joinAndInput(ctx, s, o.frames, byte(i)); err != nil {
			return fmt.Errorf("%s: %w", players[i].id, err)
		}
	}
	if err := syncAll(ctx, sessions); err != nil {
		return err
	}
	if err := maybeReconnect(ctx, o, sessions); err != nil {
		return err
	}
	return waitFrames(ctx, sessions, counters)
}

// openBattleSessions 建立双方直连会话：地址按面取本局推送的 endpoints（生产路径），
// 仅在显式给 -kcp/-udp/-ws 覆盖时改用命令行地址（联调）；握手方式由 -without-edge-hello 决定。
func openBattleSessions(ctx context.Context, o roundOpts, players []*player, plans []direct.Plan) ([]*direct.Session, error) {
	out := make([]*direct.Session, 0, len(plans))
	for i, plan := range plans {
		addr, err := resolveAddr(plan, o)
		if err != nil {
			closeBattleSessions(out)
			return nil, fmt.Errorf("%s: %w", players[i].id, err)
		}
		plan.Endpoints[o.kind] = addr
		sess, err := direct.Open(ctx, plan, openOptions(o)...)
		if err != nil {
			closeBattleSessions(out)
			return nil, fmt.Errorf("%s 直连（%s 面 %s）失败: %w", players[i].id, o.kind, addr, err)
		}
		fmt.Printf("[直连] %s 面=%s 地址=%s\n", players[i].id, sess.Transport(), sess.Endpoint())
		out = append(out, sess)
	}
	return out, nil
}

// resolveAddr 解析本面要连的地址：缺省取本局推送的 endpoints（生产路径，缺面即报错、
// 不猜端口）；只有显式覆盖（联调）才用命令行地址——覆盖不影响握手方式。
func resolveAddr(plan direct.Plan, o roundOpts) (string, error) {
	if o.addr != "" {
		fmt.Printf("[直连] 地址覆盖（联调）：%s 面 %q → %q\n", o.kind, plan.Endpoints[o.kind], o.addr)
		return o.addr, nil
	}
	addr, err := plan.Endpoint(o.kind)
	if err != nil {
		return "", fmt.Errorf("本局推送未下发 %s 面接入层地址: %w", o.kind, err)
	}
	return addr, nil
}

// openOptions 组装建连选项：默认走接入层 hello 握手段（生产路径），-without-edge-hello 才关闭。
func openOptions(o roundOpts) []direct.Option {
	opts := []direct.Option{direct.WithTransport(o.kind)}
	if !o.edgeHello {
		opts = append(opts, direct.WithoutEdgeHello())
	}
	return opts
}

// joinAndInput 入局并发送若干帧输入（step 区分双方输入内容，服务端按帧聚合）。
func joinAndInput(ctx context.Context, s *direct.Session, frames int, step byte) error {
	join, err := s.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: s.BattleID()})
	if err != nil {
		return fmt.Errorf("JoinBattle 失败: %w", err)
	}
	for i := 1; i <= frames; i++ {
		req := &battlev1.FrameInputReq{
			BattleId: s.BattleID(),
			Input:    &locksteppb.LockstepInput{FrameId: uint64(i), Payload: []byte{step}},
		}
		if err := s.SendFrameInput(ctx, req); err != nil {
			return fmt.Errorf("第 %d 帧输入失败: %w", i, err)
		}
	}
	fmt.Printf("[直连] JoinBattle ok（session=%s 当前帧=%d）+ %d 帧输入已发\n",
		join.GetMeta().GetSessionId(), join.GetCurrentFrame(), frames)
	return nil
}

// syncAll 双方各补一次帧（基准取各自已见帧号）。
func syncAll(ctx context.Context, sessions []*direct.Session) error {
	for _, s := range sessions {
		reply, err := s.SyncFrames(ctx, &battlev1.SyncFramesReq{
			BattleId: s.BattleID(), LastSeenFrame: s.LastSeenFrame(),
		})
		if err != nil {
			return fmt.Errorf("SyncFrames 失败: %w", err)
		}
		fmt.Printf("[直连] SyncFrames ok（当前帧=%d 补帧组=%d 基准=%d）\n",
			reply.GetCurrentFrame(), len(reply.GetMissed()), s.LastSeenFrame())
	}
	return nil
}

// maybeReconnect 在 -reconnect 打开时对第一条会话做一次显式重连：重新 hello（含票）+ 重放
// JoinBattle/SyncFrames 补帧，验证真实路径上的重连语义。
func maybeReconnect(ctx context.Context, o roundOpts, sessions []*direct.Session) error {
	if !o.reconnect {
		return nil
	}
	before := sessions[0].LastSeenFrame()
	if err := sessions[0].Reconnect(ctx); err != nil {
		return fmt.Errorf("重连失败: %w", err)
	}
	fmt.Printf("[直连] Reconnect ok（重连前基准=%d 重连后基准=%d 状态=%s）\n",
		before, sessions[0].LastSeenFrame(), sessions[0].State())
	return nil
}

// watchFrames 为每条会话登记帧广播/战斗结束回调，返回帧计数（各会话独立计数）。
func watchFrames(sessions []*direct.Session) []*atomic.Int64 {
	counters := make([]*atomic.Int64, len(sessions))
	for i, s := range sessions {
		counters[i] = &atomic.Int64{}
		s.OnFrame(func(*battlev1.FrameBroadcast) { counters[i].Add(1) })
		s.OnBattleEnd(func(n *battlev1.BattleEndNotify) {
			fmt.Printf("[直连] 战斗结束通知：battle=%s 胜者=%s\n", n.GetBattleId(), n.GetWinnerPlayerId())
		})
	}
	return counters
}

// waitFrames 等双方各收到至少一条帧广播（帧广播经直连通道下发，不再经网关）。
func waitFrames(ctx context.Context, sessions []*direct.Session, counters []*atomic.Int64) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if allPositive(counters) {
			fmt.Printf("[直连] 帧广播 A=%d B=%d\n", counters[0].Load(), counters[1].Load())
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("未收到帧广播（A=%d B=%d，状态 %s/%s）",
		counters[0].Load(), counters[1].Load(), sessions[0].State(), sessions[1].State())
}

// allPositive 报告全部计数是否都已大于 0。
func allPositive(counters []*atomic.Int64) bool {
	for _, c := range counters {
		if c.Load() == 0 {
			return false
		}
	}
	return true
}

// closeBattleSessions 关闭全部直连会话（幂等）。
func closeBattleSessions(sessions []*direct.Session) {
	for _, s := range sessions {
		if s != nil {
			_ = s.Close()
		}
	}
}
