// 推送订阅与分发（帧广播 / 结束通知 / 出局通知）：载荷与帧头版本**原样交给调用方**。
//
// 设计边界（§六 已确认）：SDK 只提供传输/会话 + 契约消息，**不解游戏逻辑消息**——帧广播的
// 载荷可能是 ver=1 protojson，也可能是 ver=2 protobuf 二进制，由游戏侧协议包按版本解码。
// 故本文件的分发只做「按 op 找订阅者 → 带 (payload, version) 回调」，不做任何预解；
// 会话自身只为「补帧基准」与「终态」两个语义各认一个 op（帧广播、结束通知），字段一概不问。

package direct

import (
	"fmt"
	"sync/atomic"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// pushHandler 是推送订阅者：原始载荷 + 帧头载荷编码版本（版本原样透传，不据此预解）。
type pushHandler func(payload []byte, version uint8)

// OnPush 订阅指定 op 的推送（原始载荷 + 帧头载荷编码版本），返回退订函数。
// 版本取自帧头：ver=1 protojson、ver=2 protobuf 二进制；调用方按版本选自己的协议包解码。
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
// SDK 不解游戏消息：ver=1 为 protojson、ver=2 为 protobuf 二进制，由游戏侧协议包解码。
func (s *Session) OnFrameRaw(fn func(payload []byte, version uint8)) (off func()) {
	if fn == nil {
		return func() {}
	}
	return s.OnPush(OpFrameBroadcast, fn)
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
	if op == OpFrameBroadcast {
		s.trackFrame(payload)
	}
	if op == OpBattleEndNotify && !s.noteEndNotify(payload) {
		return // 补投（或空载荷）：不进终态、不重放回调
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

// trackFrame 推进补帧基准：取到帧号才前进（重连补帧用它取 last_seen_frame）。
func (s *Session) trackFrame(payload []byte) {
	id, ok := s.frameNumber(payload)
	if !ok {
		return
	}
	if id > s.lastSeen.Load() {
		s.lastSeen.Store(id)
	}
}

// frameNumber 交给调用方注入的钩子提取帧号（未注入则不推进、不报错）：
// 载荷编码（protojson / protobuf）与帧广播的字段布局都属游戏协议，SDK 不持类型、不猜编码。
func (s *Session) frameNumber(payload []byte) (uint64, bool) {
	if s.opt.frameNumber == nil {
		return 0, false
	}
	return s.opt.frameNumber(payload)
}
