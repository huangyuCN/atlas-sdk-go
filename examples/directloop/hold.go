// 保活验收（-idle-hold）：让 A 在局中只发心跳、不发任何输入，验证「探针确实在保活」。
//
// 依据：数据报面（KCP/UDP）没有关闭握手，battle 侧只能靠帧面空闲读超时（缺省 offline_timeout/3
// = 5s）判掉线；探针每 2s 一帧，收包即刷新活跃，故静默期不会被驱逐。对照组（-no-heartbeat
// 配 -idle-drop）只关 A 的探针、B 保留缺省探针：同一局里 B 仍在收帧而 A 掉队，就是
// 「保活的是心跳、不是运气」的反证（两边一起关就分不清差异来自哪里）。

package main

import (
	"context"
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas-sdk-go/api/lockstep"
	"github.com/huangyuCN/atlas-sdk-go/direct"
)

// holdStep 是静默期的采样步长（状态采样 + 每秒打一行现场证据）。
const holdStep = 100 * time.Millisecond

// holdReport 是静默期的观测汇总（A 是静默方，B 是对照观察方）。
type holdReport struct {
	elapsed time.Duration // 实际静默时长
	offline int           // A 状态离开 connected 的采样次数（结束通知之前）
	framesA int64         // A 期间新增帧广播
	framesB int64         // B 期间新增帧广播
	endedAt time.Duration // 首次收到结束通知时的静默时长（0 = 未结束）
}

// holdIdle 跑静默保活时段并按开关断言：缺省断言「心跳保住了 A」，-idle-drop 断言相反。
func holdIdle(ctx context.Context, o roundOpts, sessions []*direct.Session, watch []*sessionWatch, players []*player) error {
	base := []int64{watch[0].frames.Load(), watch[1].frames.Load()}
	fmt.Printf("[保活] A(%s) 静默 %s 开始：不发任何输入/其它 op；探针 A=%s B=%s（B 是同一局的参照）\n",
		players[0].id, o.idleHold, heartbeatLabel(sessions[0]), heartbeatLabel(sessions[1]))
	rep := sampleHold(ctx, o, sessions, watch)
	rep.framesA, rep.framesB = watch[0].frames.Load()-base[0], watch[1].frames.Load()-base[1]
	printHoldReport(sessions, watch, rep)
	if o.expectDrop {
		return assertDropped(watch, rep)
	}
	return assertAlive(ctx, sessions, watch, rep)
}

// heartbeatLabel 返回探针状态的展示文本（关闭时点明对照组）。
func heartbeatLabel(s *direct.Session) string {
	if p := s.HeartbeatPeriod(); p > 0 {
		return "开启（周期 " + p.String() + "）"
	}
	return "关闭（-no-heartbeat：对照组）"
}

// sampleHold 按步长采样静默期，每秒打一行现场证据；A 被判出局即提前结束。
func sampleHold(ctx context.Context, o roundOpts, sessions []*direct.Session, watch []*sessionWatch) holdReport {
	start := time.Now()
	var rep holdReport
	lastLog := 0
	for ctx.Err() == nil && time.Since(start) < o.idleHold && watch[0].outSelf.Load() == 0 {
		time.Sleep(holdStep)
		rep.elapsed = time.Since(start)
		rep.note(sessions, watch)
		if sec := int(rep.elapsed.Seconds()); sec > lastLog {
			lastLog = sec
			printHoldTick(sessions, watch, rep)
		}
	}
	return rep
}

// note 记录一次采样：结束通知之前 A 状态离开 connected 才算掉线（之后的连接回收属结算收尾）。
func (r *holdReport) note(sessions []*direct.Session, watch []*sessionWatch) {
	if watch[0].ended.Load() > 0 {
		if r.endedAt == 0 {
			r.endedAt = r.elapsed
		}
		return
	}
	if sessions[0].State() != direct.StateConnected {
		r.offline++
	}
}

// printHoldTick 打印一行静默期采样（现场证据：状态、帧广播、心跳计数、出局/结束通知）。
func printHoldTick(sessions []*direct.Session, watch []*sessionWatch, rep holdReport) {
	fmt.Printf("[保活] t=%s A 状态=%s 帧广播 A=%d B=%d 心跳 sent=%d fail=%d 出局通知(A/B)=%d/%d 结束=%d\n",
		rep.elapsed.Round(time.Second), sessions[0].State(), watch[0].frames.Load(), watch[1].frames.Load(),
		sessions[0].HeartbeatSent(), sessions[0].HeartbeatFailures(),
		watch[0].outSelf.Load(), watch[1].outSelf.Load(), watch[0].ended.Load())
}

// printHoldReport 打印静默期汇总（含探针错误与两侧状态）。
func printHoldReport(sessions []*direct.Session, watch []*sessionWatch, rep holdReport) {
	fmt.Printf("[保活] 静默 %s 结束：A 状态=%s 帧广播 +%d/%d 心跳 sent=%d fail=%d err=%v；B 状态=%s 帧广播 +%d/%d；出局通知 A=%d B=%d；非 connected 采样=%d；结束=%v\n",
		rep.elapsed.Round(time.Second), sessions[0].State(), rep.framesA, watch[0].frames.Load(),
		sessions[0].HeartbeatSent(), sessions[0].HeartbeatFailures(), sessions[0].HeartbeatErr(),
		sessions[1].State(), rep.framesB, watch[1].frames.Load(),
		watch[0].outSelf.Load(), watch[1].outSelf.Load(), rep.offline, watch[0].describeEnd())
}

// assertAlive 断言保活组：A 未被判出局、静默期仍在下行、状态未离开 connected，且静默后仍能
// 继续发输入与补帧（SyncFrames 被受理 = 仍在参战名单——被判出局者会被 battle 明确拒绝）。
func assertAlive(ctx context.Context, sessions []*direct.Session, watch []*sessionWatch, rep holdReport) error {
	if n := watch[0].outSelf.Load(); n > 0 {
		return fmt.Errorf("保活断言失败：A 被判出局（通知 %d 条：%s）", n, watch[0].describeOut())
	}
	if rep.framesA == 0 {
		return fmt.Errorf("保活断言失败：A 在静默期一条帧广播都没收到（下行已断）")
	}
	fmt.Printf("[保活] 断言①通过：A 未被判出局（出局通知 A=%d B=%d）\n",
		watch[0].outSelf.Load(), watch[1].outSelf.Load())
	fmt.Printf("[保活] 断言②通过：A 静默期持续下行（帧广播 A+%d B+%d），A 非 connected 采样=%d\n",
		rep.framesA, rep.framesB, rep.offline)
	if rep.offline > 0 {
		return fmt.Errorf("保活断言失败：A 静默期有 %d 次采样离开 connected", rep.offline)
	}
	if rep.endedAt > 0 {
		fmt.Printf("[保活] 说明：静默第 %s 收到战斗结束通知（%s），其后的连接回收不计入断言\n",
			rep.endedAt.Round(time.Second), watch[0].describeEnd())
		return nil
	}
	return postHoldResume(ctx, sessions[0])
}

// assertDropped 断言对照组：无心跳时 A 在静默期掉队——状态离开 connected，或（数据报面无 EOF
// 感知）帧广播明显落后于 B（对端空闲驱逐后下行断流）。
func assertDropped(watch []*sessionWatch, rep holdReport) error {
	switch {
	case rep.offline > 0:
		fmt.Printf("[保活] 对照组断言通过：A 静默期 %d 次采样离开 connected（被判掉线）\n", rep.offline)
		return nil
	case rep.framesA+2 <= rep.framesB:
		fmt.Printf("[保活] 对照组断言通过：A 帧广播 +%d 明显落后 B +%d（空闲驱逐后下行断流）\n",
			rep.framesA, rep.framesB)
		return nil
	default:
		return fmt.Errorf("对照组断言失败：关掉心跳后 A 仍未被判掉线（帧广播 A+%d B+%d，非 connected 采样=%d）——本面可能没有空闲读超时（如 WS）",
			rep.framesA, rep.framesB, rep.offline)
	}
}

// postHoldResume 让 A 在静默期结束后继续发输入并补帧：SyncFrames 被受理即证明 A 仍在参战名单。
func postHoldResume(ctx context.Context, a *direct.Session) error {
	if a.State() != direct.StateConnected {
		return fmt.Errorf("保活断言失败：A 静默后状态 %s，无法继续发送", a.State())
	}
	frame := a.LastSeenFrame() + 1
	req := &battlev1.FrameInputReq{BattleId: a.BattleID(), Input: &locksteppb.LockstepInput{FrameId: frame}}
	if err := a.SendFrameInput(ctx, req); err != nil {
		return fmt.Errorf("保活断言失败：A 静默后发输入失败: %w", err)
	}
	reply, err := a.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: a.BattleID(), LastSeenFrame: a.LastSeenFrame()})
	if err != nil {
		return fmt.Errorf("保活断言失败：A 静默后补帧被拒（可能已被移出名单）: %w", err)
	}
	fmt.Printf("[保活] 断言③通过：A 静默后继续发输入（帧=%d）+ SyncFrames ok（当前帧=%d 补帧组=%d）\n",
		frame, reply.GetCurrentFrame(), len(reply.GetMissed()))
	return nil
}
