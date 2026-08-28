package client

import "context"

// queuedInvoke 是断线重连期间排队的请求（重连成功后由 supervisor 按序重发）。
type queuedInvoke struct {
	ctx    context.Context
	op     string
	req    any
	resp   any
	result chan error
}
