// op 常量的线上取值钉死：游戏域消息（DTO 与 op stub）迁出 SDK 后，会话自身仍按**名字**寻址
// 探针、入局、补帧、帧广播与结束通知——常量写错不会编译失败，只会静默发错对象/认不出推送，
// 故这里用测试侧的字面量（独立于实现，不读实现常量）把取值钉住。

package direct

import "testing"

// TestOpConstantsMatchWire 断言导出的 op 常量与线上取值逐字一致。
func TestOpConstantsMatchWire(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"OpPing", OpPing, stubOpPing},
		{"OpJoinBattle", OpJoinBattle, stubOpJoin},
		{"OpSendFrameInput", OpSendFrameInput, stubOpInput},
		{"OpSyncFrames", OpSyncFrames, stubOpSync},
		{"OpFrameBroadcast", OpFrameBroadcast, stubOpFrame},
		{"OpBattleEndNotify", OpBattleEndNotify, stubOpEnd},
		{"OpPlayerOutNotify", OpPlayerOutNotify, "/battle.v1.PlayerOutNotify"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, 期望 %q（线上取值，写错即发错对象）", c.name, c.got, c.want)
		}
	}
}
