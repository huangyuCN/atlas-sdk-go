// 会话剧本的**测试侧替身**：游戏域消息（battle.v1 的 DTO 与 op stub）已迁出 SDK，
// 用例改用「op 字面量 + 最小结构体/裸字节」表达线上载荷——断言因此独立于任何生成物
// （真实项目用游戏协议包的生成 DTO 与 op 常量，这里只需要线上形态等价）。
//
// 覆盖三件套（生产上同样由游戏侧提供）：
//   - 探针：`WithProbe(OpPing, PingReq 等价体)`；
//   - 重连重放：`WithReplay(入局, 按 last_seen_frame 补帧)`；
//   - 帧号钩子：ver=1 的 protojson 帧广播按 `frame.frameId` 取帧号（SDK 不再内建解码）。

package direct

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// testJoinReq 是 JoinBattleReq 的 protojson 等价形态（也用作探针载荷：两者都只带 battleId）。
type testJoinReq struct {
	BattleID string `json:"battleId"`
}

// testSyncReq 是 SyncFramesReq 的等价形态（protojson 把 uint64 编成字符串，桩侧两种都认）。
type testSyncReq struct {
	BattleID      string `json:"battleId"`
	LastSeenFrame uint64 `json:"lastSeenFrame"`
}

// testFrameInputReq 是 FrameInputReq 的等价形态（桩只校验 op 与会话槽，不关心帧体）。
type testFrameInputReq struct {
	BattleID string `json:"battleId"`
	FrameID  uint64 `json:"frameId,omitempty"`
}

// testJoinReply 是 JoinBattleReply 的等价形态（只取用例关心的字段）。
type testJoinReply struct {
	Meta struct {
		SessionID string `json:"sessionId"`
	} `json:"meta"`
}

// withTestProbe 返回测试用探针（PingReq 等价体：只带本局对局标识）。
func withTestProbe(battleID string) Option {
	return WithProbe(stubOpPing, testJoinReq{BattleID: battleID})
}

// withTestReplay 返回测试用重放步骤：先入局、再按当前补帧基准补帧（与旧内建重放等价）。
func withTestReplay(battleID string) Option {
	return WithReplay(
		Replay{Op: stubOpJoin, Req: func(uint64) any { return testJoinReq{BattleID: battleID} }},
		Replay{Op: stubOpSync, Req: func(lastSeen uint64) any {
			return testSyncReq{BattleID: battleID, LastSeenFrame: lastSeen}
		}},
	)
}

// jsonFrameNumber 是测试侧的帧号钩子：按帧广播的 protojson 形态取 `frame.frameId`
// （真实项目用协议包解码；SDK 不持游戏消息、不猜编码）。
func jsonFrameNumber(payload []byte) (uint64, bool) {
	var v struct {
		Frame struct {
			FrameID uint64 `json:"frameId"`
		} `json:"frame"`
	}
	if err := json.Unmarshal(payload, &v); err != nil {
		return 0, false
	}
	return v.Frame.FrameID, v.Frame.FrameID != 0
}

// joinOnce 发一次入局并校验回执（返回回执原始字节：调用方按需断言字段）。
func joinOnce(t *testing.T, sess *Session, battleID string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var reply json.RawMessage
	if err := sess.Invoke(ctx, stubOpJoin, testJoinReq{BattleID: battleID}, &reply); err != nil {
		t.Fatalf("入局失败: %v", err)
	}
	return reply
}

// sendFrameInput 发一帧输入（错误原样返回，供终态/拒绝类用例断言）。
func sendFrameInput(ctx context.Context, sess *Session, battleID string) error {
	return sess.Invoke(ctx, stubOpInput, testFrameInputReq{BattleID: battleID}, nil)
}

// syncFrames 补一次帧，基准取会话当前已见帧号（错误原样返回）。
func syncFrames(ctx context.Context, sess *Session) error {
	return sess.Invoke(ctx, stubOpSync,
		testSyncReq{BattleID: sess.BattleID(), LastSeenFrame: sess.LastSeenFrame()}, nil)
}

// invokeJoin 发一次入局（错误原样返回，供终态/拒绝类用例断言）。
func invokeJoin(ctx context.Context, sess *Session, battleID string) error {
	return sess.Invoke(ctx, stubOpJoin, testJoinReq{BattleID: battleID}, nil)
}
