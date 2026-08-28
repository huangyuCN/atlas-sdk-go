package client

import (
	"context"
	"sync/atomic"
	"time"
)

// queuedInvoke 是断线重连期间排队的请求（重连成功后由 supervisor 按序重发）。
// claim 是恰好一次认领标记：排队超时看护 / drain / ctx 取消 / 关闭四方竞争时，
// CAS 成功者唯一负责投递结果；已认领（过期）的请求 drain 时跳过不重发
// （评审 Important 修复：调用方已放弃的请求不再发往服务端）。
type queuedInvoke struct {
	ctx    context.Context
	op     string
	req    any
	resp   any
	result chan error

	deadline time.Time       // 排队期限（单次超时与 ctx deadline 取较早）
	ctxDone  <-chan struct{} // ctx.Done()（Background 时为 nil 语义由看护处理）
	closeCh  <-chan struct{} // Client 关闭信号
	claimed  atomic.Bool     // 恰好一次认领
}
