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
//   - 被服务端**明确拒绝**（业务拒绝/协议非法）记录为可判定错误并停止探测，不静默重试；
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

// startHeartbeat 启动保活循环（周期 <= 0 表示关闭，不启 goroutine）；会话已关闭则不启动。
func (s *Session) startHeartbeat() {
	if s.opt.heartbeat <= 0 {
		return
	}
	s.genMu.Lock()
	if s.closed {
		s.genMu.Unlock()
		return
	}
	s.wg.Add(1)
	s.genMu.Unlock()
	go s.heartbeatLoop()
}

// heartbeatLoop 周期发探针，直到会话关闭、进入终态（对局已结束）、被明确拒绝或本会话不再有活跃连接。
func (s *Session) heartbeatLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opt.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
		}
		if s.Ended() {
			return // 终态：停发（写线另有终态双检兜底，这里顺带收掉 goroutine）
		}
		if s.State() != StateConnected {
			continue // 重连中不发：探针失败无信息量，保活资格由重连成功后恢复
		}
		switch err := s.probe(); {
		case err == nil:
			s.hbSent.Add(1)
		case errors.Is(err, ErrClosed):
			return
		case definiteReject(err) != nil:
			s.setHeartbeatErr(err)
			return // 明确拒绝：停止探测，不静默重试
		default:
			s.hbFailures.Add(1)
		}
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

// definiteReject 判定探针失败是否属「服务端明确拒绝」：业务拒绝（op 未注册/被拒/票据类）
// 与协议非法都不该重试；网络类（写失败/超时）返回 nil（只计数）。
func definiteReject(err error) error {
	var be *client.BusinessError
	if errors.As(err, &be) {
		return err
	}
	var pe *client.ProtocolError
	if errors.As(err, &pe) {
		return err
	}
	return nil
}

// setHeartbeatErr 记录探针被明确拒绝的原因（只写一次，探测随之停止）。
func (s *Session) setHeartbeatErr(err error) {
	s.hbErrMu.Lock()
	defer s.hbErrMu.Unlock()
	if s.hbErr == nil {
		s.hbErr = err
	}
}

// HeartbeatPeriod 返回本会话的保活探针周期（0 = 未启用）。
func (s *Session) HeartbeatPeriod() time.Duration { return s.opt.heartbeat }

// HeartbeatSent 返回成功送达的探针数（写出且收到帧引擎的空信封）。
func (s *Session) HeartbeatSent() uint64 { return s.hbSent.Load() }

// HeartbeatFailures 返回失败的探针数（写失败/回执未达；只计数，不影响会话状态）。
func (s *Session) HeartbeatFailures() uint64 { return s.hbFailures.Load() }

// HeartbeatErr 返回探针被服务端明确拒绝的原因（如 op 未注册 TRANSPORT_NOT_FOUND）；
// nil 表示未被拒。被拒即停止探测（不静默重试），会话本身不受影响：真实断连仍由重连逻辑处理。
func (s *Session) HeartbeatErr() error {
	s.hbErrMu.Lock()
	defer s.hbErrMu.Unlock()
	return s.hbErr
}
