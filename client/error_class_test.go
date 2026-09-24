// 错误投影 class 测试（A.8 · 错误投影 class）：服务端 Status.class（P6 语义：0 未分类 /
// 1 业务 / 2 运行时 / 3 取消）随帧解码投影到 client.BusinessError.Class，Reason 仍是
// 业务分支主键。golden status 用例（status-full class=2、status-no-metadata class=0）的
// 字节级断言在 frame 包。
package client_test

import (
	"context"
	"errors"
	"testing"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// TestBusinessErrorProjectsClass 断言业务拒绝的 class 投影与 Reason 主键并存。
func TestBusinessErrorProjectsClass(t *testing.T) {
	const op = "/test.v1.Echo/Call"
	srv := startSessionServer(t)
	srv.setErrorReply(op, 404, 2, "PLAYER_NOT_FOUND") // class=2 运行时故障
	c, err := client.DialUDP(srv.addr(), client.WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), op, nil, nil)
	var be *client.BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("应返回业务错误，得到 %v", err)
	}
	if be.Class != frame.ClassRuntime {
		t.Fatalf("BusinessError.Class = %d, 期望 %d（运行时）", be.Class, frame.ClassRuntime)
	}
	if be.Reason != "PLAYER_NOT_FOUND" || be.Code != 404 {
		t.Fatalf("Reason/Code = %q/%d", be.Reason, be.Code)
	}
	if !client.IsBusinessError(err, "PLAYER_NOT_FOUND") {
		t.Fatal("IsBusinessError 应命中 Reason")
	}
}
