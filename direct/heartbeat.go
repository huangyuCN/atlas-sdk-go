// 直连保活心跳：无输入期间周期发送 battle.v1.BattleService/Ping（Tell），让帧面保持活跃。
//
// 为什么需要：数据报面（KCP/UDP）没有关闭握手，掉线只能靠帧面空闲读超时发现
// （battle 侧缺省取 offline_timeout/3 = 5s，见模板 server.kcp/udp 的 idle_timeout 推导），
// 长时间静默还会让 NAT 映射失效（表现为「连接还在但收不到下行帧」）。
//
// 语义边界：
//   - 探针不参与对局（服务端 Ping 不改名单/帧号/结算）。与正常输入**互不干扰**：帧写由
//     writeMu 串行化，三面的帧都是独立消息/数据报，服务端逐帧独立分发，探针不会插进任何
//     一帧的字节中间。这里刻意**不**做「距上次发送很近就跳过」——那会让判活证据取决于无关
//     业务流量（持续发输入的客户端永远跳过探针），链路是否真的保活将不可观测。
//   - 线上是 Tell（服务端不回业务回执）；这里仍等帧引擎必回的**空信封**，只为把「op 未注册/
//     被拒」这类可判定失败捞出来。等不到信封不算链路故障，只计一次失败并在下个周期照发。
//   - 发送失败（写失败/回执未达）只计数，不终止会话：真实断连交由既有重连逻辑处理。
//   - 被服务端**明确拒绝**（业务拒绝/协议非法）记录为可判定错误并计数，不静默重试；
//     处置按 reason 分类（见 beat）：终态类入终态并停探针，其余继续探测（协议非法换代后恢复）；
//     会话本身不因此提前失败。

package direct

import (
	"context"
	"errors"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
)

// startHeartbeat 为**本代连接**启动保活循环（周期 <= 0 表示关闭；会话已关闭则不启动）。
//
// 起表时点是**连接就绪**（首连建连成功、每次重连成功）而非 JoinBattle 之后——有意为之，与 TS/C#
// 同口径：探针只证明「这条连接还活着」（服务端帧面按收包刷新空闲读超时，顺带续 NAT 映射），
// 不参与对局语义（不改名单/帧号/结算），也不需要先入局；若等入局后才起表，「建连到入局之间」的
// 静默期（含自动重连重放入局的退避窗口）就没有任何保活证据，数据报面会在这段被误判掉线。
//
// 按代启停（本代死亡即停表、换代重新起表）是**功能要求**而非风格：探针若做成「一次失败就永久
// 退出」的会话级循环，一次协议抖动就会让保活永久失效，客户端静默 >5s 便被服务端按
// idle = offline_timeout/3 判掉线（把可恢复的协议错误升级成玩家判负）。
func (s *Session) startHeartbeat(g *generation) {
	if s.opt.heartbeat <= 0 {
		return
	}
	s.genMu.Lock()
	if s.closed || s.gen != g {
		s.genMu.Unlock()
		return // 会话已关闭，或本代已被更新的连接取代：不起表（避免两个循环同时探测）
	}
	s.wg.Add(1)
	s.genMu.Unlock()
	go s.heartbeatLoop(g)
}

// heartbeatLoop 周期发探针，直到会话关闭、本代连接死亡（换代由 startGeneration 重新起表）或进入终态。
func (s *Session) heartbeatLoop(g *generation) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opt.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-g.done:
			return // 本代连接已死：停表（不留悬挂 ticker），由换代后的新循环接管
		case <-ticker.C:
		}
		if !s.beat() {
			return
		}
	}
}

// beat 发一拍探针并按拒绝类别处置；返回 false 表示本循环应当退出（只有终态与关闭会退出）。
//
// 四类处置（与 TS/C# 同一契约，见 session_heartbeat_reject_test.go）：
//   - 终态类 reason（BATTLE_ENDED/BATTLE_NOT_FOUND/BATTLE_FULL/FRAME_TARGET_MISMATCH）：终态已在
//     wrapBusiness 唯一收口置位（BATTLE_ENDED → ended；其余三种 → failed），这里停探针；
//     远端拒绝入统计（本地结算不算「被拒」，终态原因已由 EndCause/EndReason/Stats 上报）。
//   - 票类与其它业务拒绝：计数 + 经 HeartbeatErr/Stats 暴露，**继续探测**（不重连、不终态）——
//     票要上层重取，链路本身未必坏；两类差异只在上层判定（errors.Is 哨兵）。
//   - 协议非法（版本/包络非法）：计数 + 上报，**继续探测**——本代连接会被读循环拆掉，探针在
//     重连期静默跳过、换代后自动恢复。这里刻意**不退出循环**：Go 的探针是会话级 goroutine，
//     退出即永久失效，一次可恢复的协议抖动会升级成「静默 >5s 被判掉线（玩家判负）」。
//   - 网络类（写失败/回执未达）：只计数，下个周期照发。
func (s *Session) beat() bool {
	if s.Terminal() {
		return false // 终态（ended/failed）：停发（写线另有终态双检兜底，这里顺带收掉 goroutine）
	}
	if s.State() != StateConnected {
		return true // 重连中不发：探针失败无信息量，保活资格由重连成功后恢复
	}
	err := s.probe()
	switch {
	case err == nil:
		s.stats.noteHeartbeatSent()
		return true
	case errors.Is(err, ErrClosed):
		return false
	case s.Terminal():
		if !isLocalEnded(err) {
			s.stats.noteHeartbeatReject(err)
		}
		return false
	case isProtocolReject(err), isBusinessReject(err):
		s.stats.noteHeartbeatReject(err)
		return true
	default:
		s.stats.noteHeartbeatFailure(err)
		return true
	}
}

// probe 发一次保活探针并等（空的）回执信封；等不到返回超时类错误。
func (s *Session) probe() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.probeWindow())
	defer cancel()
	return s.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.Ping,
		&battlev1.PingReq{BattleId: s.plan.BattleID}, nil)
}

// probeWindow 返回单次探针的等待窗口：一个心跳周期（不超过单次请求超时）。
// 取周期而非请求超时，是为了让丢包/回执未达只拖慢这一拍，不耽误下一个周期照发。
func (s *Session) probeWindow() time.Duration {
	return min(s.opt.heartbeat, s.opt.invokeTimeout)
}

// isBusinessReject 判定错误是否为业务拒绝（*client.BusinessError，含票据类与终态类）。
func isBusinessReject(err error) bool {
	var be *client.BusinessError
	return errors.As(err, &be)
}

// isProtocolReject 判定错误是否为协议级非法（帧/包络/序列化，不可重试且连接已不可信）。
func isProtocolReject(err error) bool {
	var pe *client.ProtocolError
	return errors.As(err, &pe)
}

// HeartbeatPeriod 返回本会话的保活探针周期（0 = 未启用）。
func (s *Session) HeartbeatPeriod() time.Duration { return s.opt.heartbeat }

// HeartbeatSent 返回成功送达的探针数（写出且收到帧引擎的空信封）。
func (s *Session) HeartbeatSent() uint64 { return s.stats.hbSent.Load() }

// HeartbeatFailures 返回网络类探针失败数（写失败/回执未达；只计数，不影响会话状态）。
// 业务/协议拒绝的计数见 Stats().HeartbeatRejects。
func (s *Session) HeartbeatFailures() uint64 { return s.stats.hbFailures.Load() }

// HeartbeatErr 返回探针**首个**被拒原因（如 op 未注册 TRANSPORT_NOT_FOUND、票类
// BATTLE_TICKET_EXPIRED、终态类 BATTLE_ENDED）：nil 表示未被拒。只记首次，后续失败仅计数
// （Stats().HeartbeatRejects）——「日志只在状态首次变化时打一条」的等价口径。
//
// 处置按 reason 分类（见 beat）：终态类已让会话进终态并停探针；票类、其它业务拒绝与协议非法
// 都只上报并**继续探测**（协议非法只影响当拍，换代后自动恢复）。会话本身不因探针被拒而提前失败，
// 真实断连仍由重连逻辑处理。
func (s *Session) HeartbeatErr() error { return s.stats.firstHeartbeatErr() }
