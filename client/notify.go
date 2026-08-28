package client

import (
	"reflect"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// On 订阅 Notify 帧（按 operation 分发），返回退订函数。
// 幂等语义：同一 (op, handler)（函数值相等，含方法值/闭包引用比较）重复注册
// 只保留一份，重复退订安全（规范 §5.1）。
// handler 在独立 goroutine 执行，panic 被 recover，不影响其他分发；
// 重连后订阅自动重放（重连批次交付）。
func (c *Client) On(op string, h NotifyHandler) (off func()) {
	if h == nil {
		return func() {}
	}
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()
	set, ok := c.notifies[op]
	if !ok {
		set = make(map[uintptr]notifyEntry)
		c.notifies[op] = set
	}
	key := reflect.ValueOf(h).Pointer()
	set[key] = notifyEntry{fn: h}
	return func() {
		c.notifyMu.Lock()
		defer c.notifyMu.Unlock()
		if cur, ok := c.notifies[op]; ok {
			delete(cur, key)
			if len(cur) == 0 {
				delete(c.notifies, op)
			}
		}
	}
}

// dispatchNotify 解析 Notify 帧并分发到全部订阅者。
// Notify 帧体解析失败静默丢弃：推送非请求-响应匹配路径，坏帧不影响连接
// （与读循环对非法帧类型终止的语义区分：那是协议级错误）。
func (c *Client) dispatchNotify(_ frame.Header, body []byte) {
	op, payload, err := frame.ParseRequestBody(body)
	if err != nil {
		return
	}
	c.notifyMu.Lock()
	handlers := make([]NotifyHandler, 0, len(c.notifies[op]))
	for _, e := range c.notifies[op] {
		handlers = append(handlers, e.fn)
	}
	c.notifyMu.Unlock()
	for _, h := range handlers {
		go c.safeNotify(h, op, payload)
	}
}

// safeNotify 单个 handler 的保护执行。
func (c *Client) safeNotify(h NotifyHandler, op string, payload []byte) {
	defer func() { _ = recover() }()
	h(op, payload)
}
