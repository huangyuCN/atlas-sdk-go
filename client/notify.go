package client

import (
	"reflect"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// NotifyHandler 是 Notify 帧回调：收到原始 payload（SDK 不做 DTO 解码，业务侧自行处理）。
// handler 在独立 goroutine 执行，异常被 recover，不影响其他分发。
type NotifyHandler func(op string, payload []byte)

// on 订阅本通道的 Notify 帧（按 operation 分发），返回退订函数。
// 幂等语义：同一 (op, handler)（函数值相等，含方法值/闭包引用比较）重复注册
// 只保留一份，重复退订安全（规范 §5.1）。
// handler 在独立 goroutine 执行，panic 被 recover，不影响其他分发；
// 重连后订阅自动重放（订阅表在通道上，跨连接代际持续生效）。
func (ch *channel) on(op string, h NotifyHandler) (off func()) {
	if h == nil {
		return func() {}
	}
	ch.notifyMu.Lock()
	defer ch.notifyMu.Unlock()
	set, ok := ch.notifies[op]
	if !ok {
		set = make(map[uintptr]notifyEntry)
		ch.notifies[op] = set
	}
	key := reflect.ValueOf(h).Pointer()
	set[key] = notifyEntry{fn: h}
	return func() {
		ch.notifyMu.Lock()
		defer ch.notifyMu.Unlock()
		if cur, ok := ch.notifies[op]; ok {
			delete(cur, key)
			if len(cur) == 0 {
				delete(ch.notifies, op)
			}
		}
	}
}

// dispatchNotify 解析 Notify 帧并分发到全部订阅者。
// Notify 帧体解析失败静默丢弃：推送非请求-响应匹配路径，坏帧不影响连接
// （与读循环对非法帧类型终止的语义区分：那是协议级错误）。
func (ch *channel) dispatchNotify(_ frame.Header, body []byte) {
	op, payload, err := frame.ParseRequestBody(body)
	if err != nil {
		return
	}
	ch.notifyMu.Lock()
	handlers := make([]NotifyHandler, 0, len(ch.notifies[op]))
	for _, e := range ch.notifies[op] {
		handlers = append(handlers, e.fn)
	}
	ch.notifyMu.Unlock()
	for _, h := range handlers {
		go ch.safeNotify(h, op, payload)
	}
}

// safeNotify 单个 handler 的保护执行。
func (ch *channel) safeNotify(h NotifyHandler, op string, payload []byte) {
	defer func() { _ = recover() }()
	h(op, payload)
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
