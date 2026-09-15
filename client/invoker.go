package client

import "context"

// Invoker 是强类型客户端 stub（protoc-gen-atlas-client 生成物）的最小依赖：
// 会话内核 Client 天然满足；业务测试可注入 fake。
type Invoker interface {
	Invoke(ctx context.Context, op string, req, resp any, opts ...InvokeOption) error
}
