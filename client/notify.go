package client

import (
	"reflect"
	"strconv"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// NotifyHandler 是 Notify 帧回调：收到原始 payload（SDK 不做 DTO 解码，业务侧自行处理）。
// payload 是连接协商编码的原始字节（ver=1 protojson JSON / ver=2 protobuf wire）；需要
// 按帧头版本选解码器的场景走会话接缝（PushEnvelope 携带 Version），本回调不带版本。
// handler 在独立 goroutine 执行，异常被 recover，不影响其他分发。
type NotifyHandler func(op string, payload []byte)

// PushEnvelope 是分发给订阅方与接缝的推送信封：op + 帧头编码版本 + 原始字节（不解码）。
// Version 决定 Body 的编码：ver=1 是 protojson JSON 字节、ver=2 是 protobuf wire 字节。
// 会话接缝（SessionProtocol.Kicked）据此选择解码器——丢掉版本会让 ver=2 的实现方按
// protojson 解码失败，被挤下线原因静默丢失（ok=true、reason=""，S0.5 修订 1 · c②）。
type PushEnvelope struct {
	Op      string
	Version uint8
	Body    []byte
}

// on 订阅本通道的 Notify 帧（按 operation 分发），返回退订函数。
// 幂等语义：同一 (op, handler)（函数值指针相等）重复注册只保留一份，重复退订安全
// （规范 §5.1）。
// handler 在独立 goroutine 执行，panic 被 recover，不影响其他分发；
// 重连后订阅自动重放（订阅表在通道上，跨连接代际持续生效）。
func (ch *channel) on(op string, h NotifyHandler) (off func()) {
	if h == nil {
		return func() {}
	}
	unsub := ch.onKeyed(op, handlerKey(h), wrapNotify(h))
	return func() {
		unsub()
		ch.dropNotifySet(op)
	}
}

// onAny 订阅本通道的全部 Notify 帧（不区分 operation），返回退订函数；幂等与执行语义
// 同 on。用途：协议无关的订阅方（会话接缝据此判定「被挤下线」推送——推送 op 属协议
// 事实，由接缝实现识别，SDK 内核不硬编码推送 op）。
func (ch *channel) onAny(h NotifyHandler) (off func()) {
	if h == nil {
		return func() {}
	}
	return ch.onAnyKeyed(handlerKey(h), wrapNotify(h))
}

// onAnyKeyed 以调用方给定的**稳定键**订阅全部推送（键相同即同一订阅，重复注册只保留
// 一份），handler 收到带帧头版本的信封。用途：会话状态机（键 = 会话自身）——接缝要按
// Version 选解码器，且重复 Bind 必须退化成同一订阅而非累积（S0.5 修订 1 · c③）。
func (ch *channel) onAnyKeyed(key string, h func(PushEnvelope)) (off func()) {
	if h == nil {
		return func() {}
	}
	return ch.subscribe(ch.notifyAny, notifyKey{id: key}, h)
}

// onKeyed 把信封形态 handler 加入某 op 的订阅集合（稳定键幂等去重），返回退订函数。
func (ch *channel) onKeyed(op, id string, h func(PushEnvelope)) (off func()) {
	if h == nil {
		return func() {}
	}
	return ch.subscribe(ch.notifySet(op), notifyKey{op: op, id: id}, h)
}

// notifySet 返回（必要时创建）某 op 的订阅集合。
func (ch *channel) notifySet(op string) map[notifyKey]notifyEntry {
	ch.notifyMu.Lock()
	defer ch.notifyMu.Unlock()
	set, ok := ch.notifies[op]
	if !ok {
		set = make(map[notifyKey]notifyEntry)
		ch.notifies[op] = set
	}
	return set
}

// dropNotifySet 在集合已空时删除该 op 的订阅表（避免 op 表无限增长）。
func (ch *channel) dropNotifySet(op string) {
	ch.notifyMu.Lock()
	defer ch.notifyMu.Unlock()
	if cur, ok := ch.notifies[op]; ok && len(cur) == 0 {
		delete(ch.notifies, op)
	}
}

// subscribe 把 handler 加入指定订阅集合（稳定键幂等去重），返回退订函数。
func (ch *channel) subscribe(set map[notifyKey]notifyEntry, key notifyKey, h func(PushEnvelope)) (off func()) {
	ch.notifyMu.Lock()
	defer ch.notifyMu.Unlock()
	set[key] = notifyEntry{fn: h}
	return func() {
		ch.notifyMu.Lock()
		defer ch.notifyMu.Unlock()
		delete(set, key)
	}
}

// handlerKey 返回按函数值派生的稳定去重键（闭包/方法值取代码指针；空 handler 取空键）。
func handlerKey(h NotifyHandler) string {
	if h == nil {
		return ""
	}
	return "fn:" + strconv.FormatUint(uint64(reflect.ValueOf(h).Pointer()), 16)
}

// wrapNotify 把只收原始字节的 NotifyHandler 适配成信封形态（丢弃 Version）。
func wrapNotify(h NotifyHandler) func(PushEnvelope) {
	return func(env PushEnvelope) { h(env.Op, env.Body) }
}

// dispatchNotify 解析 Notify 帧并分发到全部订阅者（按 op 订阅者 + 通配订阅者），信封
// 携带帧头 version。Notify 帧体解析失败静默丢弃：推送非请求-响应匹配路径，坏帧不影响
// 连接（与读循环对非法帧类型终止的语义区分：那是协议级错误）。
func (ch *channel) dispatchNotify(hdr frame.Header, body []byte) {
	op, payload, err := frame.ParseRequestBody(body)
	if err != nil {
		return
	}
	env := PushEnvelope{Op: op, Version: hdr.Version, Body: payload}
	ch.notifyMu.Lock()
	handlers := make([]func(PushEnvelope), 0, len(ch.notifies[op])+len(ch.notifyAny))
	for _, e := range ch.notifies[op] {
		handlers = append(handlers, e.fn)
	}
	for _, e := range ch.notifyAny {
		handlers = append(handlers, e.fn)
	}
	ch.notifyMu.Unlock()
	for _, h := range handlers {
		go ch.safeNotify(h, env)
	}
}

// safeNotify 单个 handler 的保护执行。
func (ch *channel) safeNotify(h func(PushEnvelope), env PushEnvelope) {
	defer func() { _ = recover() }()
	h(env)
}

// onReadExit 注册本通道读循环退出回调。
func (ch *channel) onReadExit(fn func(error)) {
	if fn == nil {
		return
	}
	ch.onReadExitPtr.Store(&fn)
}

// safeOnReadExit 带保护的读循环退出回调执行。
func (ch *channel) safeOnReadExit(fn func(error), err error) {
	defer func() { _ = recover() }()
	fn(err)
}
