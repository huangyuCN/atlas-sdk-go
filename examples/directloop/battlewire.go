// 示例侧的**最小线上载荷**构造与解析。
//
// 游戏域消息（battle.v1 的 DTO 与 op 常量）已迁出 SDK，改由游戏协议包
// （github.com/huangyuCN/atlas-game-layout/protocol）提供；本示例刻意不引协议包（示例保持零
// 游戏依赖），故按 protojson 形态手写这几条载荷。真实项目请直接用协议包的生成 DTO 与 op 常量：
// SDK 只负责把这些字节发出去、把回执原样交回来，**不解游戏消息**。
//
// 载荷形态（与 protojson 一致；64 位整数两种形态服务端都收，示例统一用数字）：
//
//	JoinBattleReq  {"battleId":"b-1"}
//	FrameInputReq  {"battleId":"b-1","input":{"frameId":1,"payload":"<base64>"}}
//	SyncFramesReq  {"battleId":"b-1","lastSeenFrame":0}
//	PingReq        {"battleId":"b-1"}

package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// joinPayload 构造入局请求载荷（战斗标识 + 目标一致性校验所需的客体字段）。
func joinPayload(battleID string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"battleId":%q}`, battleID))
}

// probePayload 构造保活探针载荷：与入局同形（探针只带客体寻址字段）。
func probePayload(battleID string) json.RawMessage { return joinPayload(battleID) }

// frameInputPayload 构造帧输入载荷：payload 是位移字节的 base64（protojson 的 bytes 形态）。
func frameInputPayload(battleID string, frameID uint64, step byte) json.RawMessage {
	encoded := base64.StdEncoding.EncodeToString([]byte{step})
	return json.RawMessage(fmt.Sprintf(`{"battleId":%q,"input":{"frameId":%d,"payload":%q}}`,
		battleID, frameID, encoded))
}

// syncPayload 构造补帧载荷（基准 = 已见帧号）。
func syncPayload(battleID string, lastSeen uint64) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"battleId":%q,"lastSeenFrame":%d}`, battleID, lastSeen))
}

// uint64Text 兼容 protojson 的 64 位整数两种形态（字符串/数字）。
type uint64Text uint64

// UnmarshalJSON 接受 `"3"` 与 `3` 两种写法（protojson 默认下发字符串）。
func (n *uint64Text) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	v, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return err
	}
	*n = uint64Text(v)
	return nil
}

// frameNumberOf 从帧广播载荷取帧号（示例侧解析；SDK 通过 WithFrameNumberFunc 消费它）。
// 真实项目用协议包解 FrameBroadcast 后取 frame.frame_id。
func frameNumberOf(payload []byte) (uint64, bool) {
	var fb struct {
		Frame struct {
			FrameID uint64Text `json:"frameId"`
		} `json:"frame"`
	}
	if err := json.Unmarshal(payload, &fb); err != nil || fb.Frame.FrameID == 0 {
		return 0, false
	}
	return uint64(fb.Frame.FrameID), true
}

// playerOut 是出局通知的最小投影（出局玩家 + 原因枚举名）。
type playerOut struct {
	PlayerID string `json:"playerId"`
	Reason   string `json:"reason"`
}

// playerOutOf 解析出局通知（载荷非本形态时返回 ok=false）。
func playerOutOf(payload []byte) (playerOut, bool) {
	var out playerOut
	if err := json.Unmarshal(payload, &out); err != nil || out.PlayerID == "" {
		return playerOut{}, false
	}
	return out, true
}

// battleEnd 是结束通知的最小投影（对局 + 胜者）。
type battleEnd struct {
	BattleID       string `json:"battleId"`
	WinnerPlayerID string `json:"winnerPlayerId"`
}

// battleEndOf 解析结束通知（载荷非本形态时返回 ok=false）。
func battleEndOf(payload []byte) (battleEnd, bool) {
	var end battleEnd
	if err := json.Unmarshal(payload, &end); err != nil || end.BattleID == "" {
		return battleEnd{}, false
	}
	return end, true
}
