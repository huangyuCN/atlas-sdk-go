// 终态族（对局结束 / 对局不存在 / 入局被拒 / 请求目标不符）的统一口径：可判定哨兵 +
// 业务分类 + 终态化 + 上报。与 TS/C# 同一契约（钉死口径见阶段 3 评审 P1-3）。
//
// 四条 reason 同族处理，理由一致——它们都表示**这张票/这条会话已经不可用**，重试只会
// 反复撞同一拒绝（甚至让服务端反复懒激活已结束的对局）：
//
//   - BATTLE_ENDED（3003）：对局已结束，已结束对局的迟到帧 op 一律稳定拒绝；
//   - BATTLE_NOT_FOUND（3001）：对局不存在（未建/已被清理）；
//   - BATTLE_FULL（3002）：入局被拒（对局已满），本会话进不去；
//   - FRAME_TARGET_MISMATCH（403）：票面对局与请求正文目标不一致（框架 frameops 的目标
//     一致性校验）——正常 SDK 不会产生，出现即说明计划/票与请求不匹配，重试无用。
//
// 票据类 reason（BATTLE_TICKET_*）**不属于**终态族：票要重取，会话本身未必打完，故
// 只上报「需重新取票」并计数（见 heartbeat.go），不置终态。
//
// 上报形态统一：错误链上同时有终态哨兵（errors.Is 分支）与 *client.BusinessError
// （errors.As 取 code/reason/metadata），Class 恒为 business——服务端未显式标注时由
// SDK 归一（见 businessClass），避免同一 reason 在不同服务端版本下分类漂移。

package direct

import (
	"errors"
	"fmt"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// 终态类 reason（与服务端 api/error/v1 及框架 frameops 的枚举名逐字一致）。
const (
	// reasonBattleEnded 是「该对局已结束」（biz_code 3003）。
	reasonBattleEnded = "BATTLE_ENDED"
	// reasonBattleNotFound 是「对局不存在」（biz_code 3001）。
	reasonBattleNotFound = "BATTLE_NOT_FOUND"
	// reasonBattleFull 是「对局已满、入局被拒」（biz_code 3002）。
	reasonBattleFull = "BATTLE_FULL"
	// reasonFrameTargetMismatch 是「票面对局与请求正文目标不一致」（框架 frameops 403）。
	reasonFrameTargetMismatch = "FRAME_TARGET_MISMATCH"
)

// 终态类状态码（与服务端 Err* / frameops 的 code 同口径；本地结算合成 Status 时照抄，
// 让「远端真的拒了」与「本地已终态不再发」对上层是同一个判定）。
const (
	codeBattleEnded         int32 = 409
	codeBattleNotFound      int32 = 404
	codeBattleFull          int32 = 409
	codeFrameTargetMismatch int32 = 403
)

// localSettledKey 是本地终态结算的 metadata 标记键：排障据此区分「服务端回的终态拒绝」与
// 「SDK 因终态本地结算」；业务分支只看 reason，不依赖本键。
//
// **三 SDK 统一键名，勿各写一套**：本轮审计发现三仓曾各不相同（Go `x-atlas-sdk-local-ended`、
// TS `x-atlas-sdk-local-settled` + `x-atlas-sdk-local-ended`、C# `x-atlas-sdk-local-settlement`），
// 已统一为 `x-atlas-sdk-local-settled`（Go/TS/C# 同步）。
const localSettledKey = "x-atlas-sdk-local-settled"

// 终态族哨兵（errors.Is 判定）：一条错误链上同时保留 *client.BusinessError 原文
// （errors.As 取 code/reason/metadata），故调用方一处判定即可分支处置。
var (
	// ErrBattleEnded 表示本局已结束（reason BATTLE_ENDED 的业务拒绝，或收到结束通知）：
	// 会话进入终态并停止发送，继续重试没有意义；结算结果见 EndNotify/OnBattleEnd。
	ErrBattleEnded = errors.New("direct: 对局已结束")
	// ErrBattleNotFound 表示对局不存在（reason BATTLE_NOT_FOUND，biz_code 3001）：
	// 不可重试、终态化并上报。
	ErrBattleNotFound = errors.New("direct: 对局不存在")
	// ErrBattleFull 表示对局已满、入局被拒（reason BATTLE_FULL，biz_code 3002）：
	// 不可重试、终态化并上报。
	ErrBattleFull = errors.New("direct: 对局已满（入局被拒）")
	// ErrFrameTargetMismatch 表示票面对局与请求正文目标不一致（reason FRAME_TARGET_MISMATCH，
	// 403）：请求与凭据不匹配属客户端/协议侧错误，不可重试、终态化并上报。
	ErrFrameTargetMismatch = errors.New("direct: 请求目标与票面对局不一致")
)

// terminalReason 是终态族「reason → 哨兵 → 状态码」的唯一对照表：新增终态 reason 只改这张表
// （terminalSentinel/terminalCode/IsTerminal 都从它派生，避免多张手抄表只改一边而漂移）。
type terminalReason struct {
	reason   string
	sentinel error
	code     int32
}

// terminalReasons 是终态族的全部条目（顺序即判定顺序）。
var terminalReasons = [...]terminalReason{
	{reasonBattleEnded, ErrBattleEnded, codeBattleEnded},
	{reasonBattleNotFound, ErrBattleNotFound, codeBattleNotFound},
	{reasonBattleFull, ErrBattleFull, codeBattleFull},
	{reasonFrameTargetMismatch, ErrFrameTargetMismatch, codeFrameTargetMismatch},
}

// terminalSentinel 返回终态 reason 对应的哨兵；非终态 reason 返回 nil。
func terminalSentinel(reason string) error {
	if t, ok := terminalEntry(reason); ok {
		return t.sentinel
	}
	return nil
}

// terminalCode 返回终态 reason 对应的状态码（与服务端同口径；未知 reason 按 409 收口）。
func terminalCode(reason string) int32 {
	if t, ok := terminalEntry(reason); ok {
		return t.code
	}
	return codeBattleEnded
}

// terminalEntry 查终态族对照表（未命中返回 false）。
func terminalEntry(reason string) (terminalReason, bool) {
	for _, t := range terminalReasons {
		if t.reason == reason {
			return t, true
		}
	}
	return terminalReason{}, false
}

// IsTerminal 判定错误是否属终态族（BATTLE_ENDED / BATTLE_NOT_FOUND / BATTLE_FULL /
// FRAME_TARGET_MISMATCH）：调用方一处判定即可走「不重试」分支——有结算可展示的走 ended 收尾，
// 无结算可展示的（不存在/已满/目标不符）走失败上报并从匹配链路重新开局。
func IsTerminal(err error) bool {
	if err == nil {
		return false
	}
	for _, t := range terminalReasons {
		if errors.Is(err, t.sentinel) {
			return true
		}
	}
	return false
}

// battleEndReason 判定终态 reason 是否属「对局正常结束」（有结算可展示，走 ended 而不是 failed）。
func battleEndReason(reason string) bool { return reason == reasonBattleEnded }

// terminalMessage 返回本地终态结算的文案（错误与 Status 两种形态共用，保证同形）。
func terminalMessage(reason string) string {
	switch reason {
	case reasonBattleNotFound:
		return "对局不存在：会话已进入终态，不再上发任何帧"
	case reasonBattleFull:
		return "对局已满：会话已进入终态，不再上发任何帧"
	case reasonFrameTargetMismatch:
		return "请求目标与票面对局不一致：会话已进入终态，不再上发任何帧"
	default:
		return "对局已结束：会话已进入终态，不再上发任何帧"
	}
}

// terminalStatus 合成终态结算的 Status 形态：reason/code/class 与真实回执同构，
// metadata 标本地来源（对齐 TS 的 battleTerminalStatus()）。
func terminalStatus(reason string) *frame.Status {
	return &frame.Status{
		Code:     terminalCode(reason),
		Reason:   reason,
		Message:  terminalMessage(reason),
		Class:    frame.ClassBusiness,
		Metadata: map[string]string{localSettledKey: "true"},
	}
}

// terminalBusinessError 把终态 Status 还原成业务错误（本地拒绝与在途结算共用的上报形态）。
func terminalBusinessError(st *frame.Status) *client.BusinessError {
	return &client.BusinessError{
		Code: st.Code, Reason: st.Reason, Message: st.Message, Metadata: st.Metadata, Class: st.Class,
	}
}

// businessClass 归一终态族的错误分类：服务端未显式标注 class（0）时按业务分类上报。
// 终态族的语义就是「业务拒绝、不可重试」，分类不该因服务端是否显式 WithClass 而漂移；
// 非终态 reason 原样透传（Reason 始终是分支主键，Class 只做处置口径）。
func businessClass(reason string, class frame.Class) frame.Class {
	if class == frame.ClassUnspecified && terminalSentinel(reason) != nil {
		return frame.ClassBusiness
	}
	return class
}

// isLocalEnded 判定错误是否来自本地终态结算（metadata 标记；非业务错误返回 false）。
func isLocalEnded(err error) bool {
	var be *client.BusinessError
	return errors.As(err, &be) && be.Metadata[localSettledKey] == "true"
}

// terminalReject 组装一次终态业务拒绝的上报错误：包终态哨兵 + 保留服务端原文
// （errors.As 仍可取到 *client.BusinessError 的 code/reason/metadata）。
func terminalReject(op string, sentinel error, be *client.BusinessError) error {
	return fmt.Errorf("direct: %s: %w: %w", op, sentinel, be)
}
