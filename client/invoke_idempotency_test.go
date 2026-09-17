package client

import (
	"testing"
)

// TestInvokeIdempotencyKeyOptions 验证幂等键选项的取值语义：
// WithIdempotencyKey 显式覆盖、WithNoIdempotency 跳过、默认自动生成。
func TestInvokeIdempotencyKeyOptions(t *testing.T) {
	o := invokeOpts{}
	WithIdempotencyKey("order-1")(&o)
	WithNoIdempotency()(&o)
	WithFailFast()(&o)
	if !o.failFast || !o.noIdempotency || o.idempotencyKey != "order-1" {
		t.Fatalf("invokeOpts = %+v", o)
	}
	// noIdempotency 优先于显式键（显式逃生门语义）。
	if id := newRequestID(); len(id) == 0 || len(id) > 24 {
		t.Fatalf("newRequestID 长度异常: %q", id)
	}
}
