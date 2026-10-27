// 会话自身收发/观察的 op 名（协议寻址键，**不是**消息类型）。
//
// 游戏域消息（battle / game / matcher 的 DTO、枚举与 op 常量）已迁到游戏协议包
// （github.com/huangyuCN/atlas-game-layout/protocol），SDK 不再持有它们；但「会话本身要做什么」
// 是传输/会话职责，必须有个名字可寻址：探针（心跳）、重连后的入局与补帧、帧广播与结束通知的
// 识别。这些名字就是**线上的寻址键**——`op` 字段的取值本身即协议事实，与是否持有 DTO 无关
// （同 `PushOpMatchStarted` 的处理口径）。
//
// 调用方发业务 op 时应优先用自己的协议包生成的 op 常量；本文件的常量覆盖会话自身会碰到的
// 全部 op，避免 SDK 里再散落字面量。

package direct

const (
	// OpPing 是保活探针（heartbeat）的 op：无输入期间周期发送，维持帧面活跃。
	// 服务端语义是 Tell（不回业务回执），SDK 仍等帧引擎必回的空信封以捞出可判定失败。
	OpPing = "/battle.v1.BattleService/Ping"
	// OpJoinBattle 是入局 op：重连后由 `WithReplay` 配置的步骤重放。
	OpJoinBattle = "/battle.v1.BattleService/JoinBattle"
	// OpSendFrameInput 是发帧 op（调用方按需经 Session.Invoke 发送）。
	OpSendFrameInput = "/battle.v1.BattleService/SendFrameInput"
	// OpSyncFrames 是补帧 op：载荷按 `WithFrameNumberFunc` 推进的补帧基准生成。
	OpSyncFrames = "/battle.v1.BattleService/SyncFrames"
	// OpFrameBroadcast 是帧广播推送的 op（会话据此推进补帧基准）。
	OpFrameBroadcast = "/battle.v1.FrameBroadcast"
	// OpBattleEndNotify 是战斗结束通知推送的 op（会话据此进入终态）。
	OpBattleEndNotify = "/battle.v1.BattleEndNotify"
	// OpPlayerOutNotify 是出局通知推送的 op（调用方订阅，会话本身不解释其载荷）。
	OpPlayerOutNotify = "/battle.v1.PlayerOutNotify"
)
