// 结算收尾验收（-await-end）：让对局跑到自然结束，断言 SDK 的终态语义在真实链路上成立——
// 结束后不再发帧/心跳（发送被 ErrBattleEnded 拒绝、心跳计数冻结），且结束结果只回调一次。
//
// 与 -idle-hold 的分工：那条验收「心跳把连接保活」，本文件验收「结算把会话收敛」；
// 两者都只在显式开关下生效，缺省闭环行为不变。

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas-sdk-go/api/lockstep"
	"github.com/huangyuCN/atlas-sdk-go/direct"
)

// awaitEnd 等双方对局自然结束，再按收尾语义逐条断言（见文件头）。
func awaitEnd(ctx context.Context, o roundOpts, sessions []*direct.Session, watch []*sessionWatch) error {
	if err := waitEnded(ctx, o, sessions); err != nil {
		return err
	}
	printEndState(sessions, watch)
	if err := assertEndOnce(watch); err != nil {
		return err
	}
	if err := assertStopSending(ctx, sessions); err != nil {
		return err
	}
	return assertHeartbeatFrozen(sessions)
}

// waitEnded 轮询等双方进入终态（收到结束通知即 Ended()），超时即报错并附现场状态。
func waitEnded(ctx context.Context, o roundOpts, sessions []*direct.Session) error {
	deadline := time.Now().Add(o.awaitEnd)
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sessions[0].Ended() && sessions[1].Ended() {
			return nil
		}
		if time.Since(lastLog) >= time.Second {
			lastLog = time.Now()
			fmt.Printf("[收尾] 等待自然结束：A=%s B=%s\n", sessions[0].State(), sessions[1].State())
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("等 %s 仍未结束（A=%s B=%s）", o.awaitEnd, sessions[0].State(), sessions[1].State())
}

// printEndState 打印终态现场：状态、首投/补投统计、胜者与触发原因。
func printEndState(sessions []*direct.Session, watch []*sessionWatch) {
	for i, s := range sessions {
		st := s.EndStats()
		fmt.Printf("[收尾] 玩家%d 状态=%s 首投=%v 补投=%d 载荷不一致=%d 胜者=%q 原因=%v\n",
			i, s.State(), st.First, st.Replays, st.Mismatches, watch[i].winnerText(), s.EndCause())
	}
}

// assertEndOnce 断言结束结果**只回调一次**（服务端有界补投多次，回调恰好一次）。
func assertEndOnce(watch []*sessionWatch) error {
	for i, w := range watch {
		if n := w.ended.Load(); n != 1 {
			return fmt.Errorf("玩家%d 结束通知回调 %d 次, 期望恰好 1 次（补投必须幂等）", i, n)
		}
	}
	fmt.Printf("[收尾] 断言①通过：结束通知回调恰好 1 次（A=%d B=%d）\n", watch[0].ended.Load(), watch[1].ended.Load())
	return nil
}

// assertStopSending 断言结束后不再发帧：终态可判定，且发送被 ErrBattleEnded 明确拒绝（不再写线）。
func assertStopSending(ctx context.Context, sessions []*direct.Session) error {
	for i, s := range sessions {
		if s.State() != direct.StateEnded {
			return fmt.Errorf("玩家%d 状态 = %s, 期望 ended", i, s.State())
		}
		req := &battlev1.FrameInputReq{
			BattleId: s.BattleID(),
			Input:    &locksteppb.LockstepInput{FrameId: s.LastSeenFrame() + 1},
		}
		if err := s.SendFrameInput(ctx, req); !errors.Is(err, direct.ErrBattleEnded) {
			return fmt.Errorf("玩家%d 终态下发帧未被拒: err=%v（期望 direct.ErrBattleEnded）", i, err)
		}
		fmt.Printf("[收尾] 玩家%d 终态发帧被拒（零写线）：%v\n", i, s.EndCause())
	}
	fmt.Printf("[收尾] 断言②通过：终态下 SendFrameInput 被 ErrBattleEnded 拒绝\n")
	return nil
}

// assertHeartbeatFrozen 断言心跳已停：观察窗（≥3 个探针周期）内探针计数不增长。
func assertHeartbeatFrozen(sessions []*direct.Session) error {
	window := heartbeatWindow(sessions)
	before := [2]uint64{sessions[0].HeartbeatSent(), sessions[1].HeartbeatSent()}
	fmt.Printf("[收尾] 观察 %s（≥3 个探针周期）内心跳计数是否冻结……\n", window)
	time.Sleep(window)
	for i, s := range sessions {
		if got := s.HeartbeatSent(); got != before[i] {
			return fmt.Errorf("玩家%d 终态后仍在发心跳：%d → %d", i, before[i], got)
		}
	}
	fmt.Printf("[收尾] 断言③通过：心跳计数冻结（A=%d B=%d；周期 A=%s B=%s；收尾窗口 A=%s）\n",
		before[0], before[1], sessions[0].HeartbeatPeriod(), sessions[1].HeartbeatPeriod(), sessions[0].EndLinger())
	return nil
}

// heartbeatWindow 取心跳冻结的观察窗：至少 3 个探针周期（探针关闭时用 3s 兜底）。
func heartbeatWindow(sessions []*direct.Session) time.Duration {
	period := sessions[0].HeartbeatPeriod()
	if p := sessions[1].HeartbeatPeriod(); p > period {
		period = p
	}
	if period <= 0 {
		return 3 * time.Second
	}
	return 3 * period
}
