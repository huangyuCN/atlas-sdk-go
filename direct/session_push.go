// 推送订阅与分发（帧广播 / 结束通知 / 出局通知）：载荷与帧头版本**原样交给调用方**。
//
// 设计边界（§六 已确认）：SDK 只提供传输/会话 + 契约消息，**不解游戏逻辑消息**——帧广播的
// 载荷可能是 ver=1 protojson，也可能是 ver=2 protobuf 二进制，由游戏侧生成包按版本解码。
// 故本文件的分发只做「按 op 找订阅者 → 带 (payload, version) 回调」，不做任何预解。

package direct

import (
	"fmt"
	"sync/atomic"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
)

// pushHandler 是推送订阅者：原始载荷 + 帧头载荷编码版本（版本原样透传，不据此预解）。
type pushHandler func(payload []byte, version uint8)

// OnPush 订阅指定 op 的推送（原始载荷 + 帧头载荷编码版本），返回退订函数。
// 版本取自帧头：ver=1 protojson、ver=2 protobuf 二进制；调用方按版本选自己的生成包解码。
func (s *Session) OnPush(op string, fn func(payload []byte, version uint8)) (off func()) {
	if fn == nil || op == "" {
		return func() {}
	}
	key := fmt.Sprintf("fn:%d", handlerSeq.Add(1))
	s.notifyMu.Lock()
	if s.handlers[op] == nil {
		s.handlers[op] = make(map[string]pushHandler)
	}
	s.handlers[op][key] = fn
	s.notifyMu.Unlock()
	return func() { s.unsubscribe(op, key) }
}

// OnFrameRaw 订阅帧广播推送（原始载荷 + 载荷编码版本），返回退订函数。
// SDK 不解游戏消息：ver=1 为 protojson、ver=2 为 protobuf 二进制，由游戏侧生成包解码。
func (s *Session) OnFrameRaw(fn func(payload []byte, version uint8)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnPush(battlev1opclient.BattleServicePushOps.FrameBroadcast, fn)
}

// OnNotify 订阅指定 op 的推送（载荷为帧头编码的原始字节，**不带载荷编码版本**），返回退订函数。
//
// Deprecated: 用 OnPush：ver=2 的二进制载荷需要版本才能选解码器，本回调拿不到版本。
func (s *Session) OnNotify(op string, fn func([]byte)) (off func()) {
	if fn == nil || op == "" {
		return func() {}
	}
	return s.OnPush(op, func(payload []byte, _ uint8) { fn(payload) })
}

// OnFrame 订阅帧广播推送（按 protojson 解析为 *battlev1.FrameBroadcast 后回调），返回退订函数。
// 载荷不是 ver=1 protojson（ver=2 二进制）时静默丢弃——这是引入版本透传前的既有行为，
// 保留它是为了不破坏旧调用方。
//
// Deprecated: 用 OnFrameRaw 拿原始载荷与版本，由游戏侧生成包解码（SDK 不解游戏消息）。
func (s *Session) OnFrame(fn func(*battlev1.FrameBroadcast)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnPush(battlev1opclient.BattleServicePushOps.FrameBroadcast, func(payload []byte, _ uint8) {
		var fb battlev1.FrameBroadcast
		if err := protojson.Unmarshal(payload, &fb); err != nil {
			return
		}
		fn(&fb)
	})
}

// OnBattleEnd 订阅战斗结束推送（解析为 *battlev1.BattleEndNotify 后回调），返回退订函数。
// 同一局结束通知会被服务端**有界补投**（载荷逐字一致），这里保证回调恰好一次（按会话去重，
// 补投只计数见 EndStats）；注册晚于首投的订阅者不会补放回调，用 EndNotify() 取结果。
func (s *Session) OnBattleEnd(fn func(*battlev1.BattleEndNotify)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnPush(battlev1opclient.BattleServicePushOps.BattleEndNotify, func(payload []byte, _ uint8) {
		var n battlev1.BattleEndNotify
		if err := protojson.Unmarshal(payload, &n); err != nil {
			return
		}
		fn(&n)
	})
}

// OnPlayerOut 订阅出局推送（解析为 *battlev1.PlayerOutNotify 后回调），返回退订函数：
// 收到即表示该玩家被判出局并移出参战名单（掉线超时等），是保活验收的直接证据。
func (s *Session) OnPlayerOut(fn func(*battlev1.PlayerOutNotify)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnPush(battlev1opclient.BattleServicePushOps.PlayerOutNotify, func(payload []byte, _ uint8) {
		var n battlev1.PlayerOutNotify
		if err := protojson.Unmarshal(payload, &n); err != nil {
			return
		}
		fn(&n)
	})
}

// handlerSeq 为推送订阅生成稳定去重键（同一函数重复订阅不叠加）。
var handlerSeq atomic.Uint64

// unsubscribe 退订并清理空表。
func (s *Session) unsubscribe(op, key string) {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	if set, ok := s.handlers[op]; ok {
		delete(set, key)
		if len(set) == 0 {
			delete(s.handlers, op)
		}
	}
}

// dispatchNotify 分发推送：先推进补帧基准（帧广播），再以独立 goroutine 交各订阅者。
// 结束通知另做幂等收口：服务端结算结果会**有界补投**（首投 + 关闭前重投 + 迟到/重连补投），
// 只有首投分发回调，补投只计数——回调与事件因此恰好一次。
func (s *Session) dispatchNotify(version uint8, body []byte) {
	op, payload, err := frame.ParseRequestBody(body)
	if err != nil {
		return
	}
	if op == battlev1opclient.BattleServicePushOps.FrameBroadcast {
		s.trackFrame(payload, version)
	}
	if op == battlev1opclient.BattleServicePushOps.BattleEndNotify && !s.noteEndNotify(payload) {
		return // 补投（或载荷非法）：不进终态、不重放回调
	}
	for _, fn := range s.handlersOf(op) {
		go safeCall(fn, payload, version)
	}
}

// handlersOf 取某 op 的订阅者快照。
func (s *Session) handlersOf(op string) []pushHandler {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	out := make([]pushHandler, 0, len(s.handlers[op]))
	for _, fn := range s.handlers[op] {
		out = append(out, fn)
	}
	return out
}

// safeCall 带 panic 保护的订阅者调用（单个回调异常不影响其他分发）。
func safeCall(fn pushHandler, payload []byte, version uint8) {
	defer func() { _ = recover() }()
	fn(payload, version)
}

// trackFrame 推进补帧基准：取到帧号才前进（重连 SyncFrames 用它取 last_seen_frame）。
func (s *Session) trackFrame(payload []byte, version uint8) {
	id, ok := s.frameNumber(payload, version)
	if !ok {
		return
	}
	if id > s.lastSeen.Load() {
		s.lastSeen.Store(id)
	}
}

// frameNumber 按版本提取帧广播的帧号：ver=1 恒走内建 protojson（与引入 ver=2 前同口径，注入
// 钩子也不接管 ver=1）；其余版本交给调用方注入的钩子，未注入返回 ok=false（不推进、不报错）。
func (s *Session) frameNumber(payload []byte, version uint8) (uint64, bool) {
	if version == frame.Version {
		var fb battlev1.FrameBroadcast
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload, &fb); err != nil {
			return 0, false
		}
		return fb.GetFrame().GetFrameId(), true
	}
	if s.opt.frameNumber == nil {
		return 0, false
	}
	return s.opt.frameNumber(payload)
}
