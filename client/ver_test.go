// 载荷编码 ver 分派集成测试（规范 §3.1 载荷编码协商）：
// protobuf serializer（ver=2）下 invoke 的帧头声明、响应帧 ver 校验与 DTO 填充。
package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	pbserializer "github.com/huangyuCN/atlas-sdk-go/contrib/protobuf"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/proto"
)

// TestInvokeProtobufVersion2：protobuf serializer 下请求帧头 ver=2，
// 服务端回显 ver=2 + protobuf 编码 payload → invoke 成功且 DTO 填充。
func TestInvokeProtobufVersion2(t *testing.T) {
	s := startFakeServer(t)
	defer func() { _ = s.ln.Close() }()
	s.replyVer = frame.Version2
	s.handleHook = func(op string, payload []byte) (resp []byte, closeConn bool) {
		var req wrapperspb.StringValue
		if err := proto.Unmarshal(payload, &req); err != nil {
			t.Fatalf("服务端解 protobuf 请求: %v", err)
		}
		if req.GetValue() != "你好" {
			t.Fatalf("请求 payload 不一致: %q", req.GetValue())
		}
		data, err := proto.Marshal(wrapperspb.String("pong-" + req.GetValue()))
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		// ver=2 的响应仍沿用统一 Reply 包络（编码无关），data 为 protobuf wire 字节。
		return replyBytes(nil, data), false
	}
	c, err := Dial(s.addr(), WithSerializer(pbserializer.Serializer{}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	var resp wrapperspb.StringValue
	if err := c.Invoke(context.Background(), "/test.v1.T/Echo", wrapperspb.String("你好"), &resp); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.GetValue() != "pong-你好" {
		t.Fatalf("响应不一致: %q", resp.GetValue())
	}
}

// TestInvokeVersionMismatch：服务端响应帧 ver 与客户端载荷编码不一致 → 协议级致命。
func TestInvokeVersionMismatch(t *testing.T) {
	s := startFakeServer(t)
	defer func() { _ = s.ln.Close() }()
	s.replyVer = frame.Version // 服务端违约：客户端 ver=2 却回 ver=1
	s.handleHook = func(op string, payload []byte) (resp []byte, closeConn bool) {
		data, err := proto.Marshal(wrapperspb.String("x"))
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		return replyBytes(nil, data), false
	}
	c, err := Dial(s.addr(), WithSerializer(pbserializer.Serializer{}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "/test.v1.T/Echo", wrapperspb.String("你好"), nil)
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("ver 不一致应协议错误，得到 %v", err)
	}
	// 评审 Blocker 回归：协议致命不得命中可重试哨兵 ErrNetwork（此前 failAllInflight
	// 一律包 NetworkError，调用方会误判可重试但通道已 terminate）。
	if errors.Is(err, ErrNetwork) {
		t.Fatalf("ver 不一致应仅命中 ErrProtocol（不可重试），错误却同时命中 ErrNetwork: %v", err)
	}
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("ver 不一致应命中 ErrProtocol 哨兵，实际: %v", err)
	}
	// 等待状态流转（invoke 返回与 terminate 完成之间的窗口竞态，见 reconnect_test.go 经验）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.State() == StateDisconnected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c.State() != StateDisconnected {
		t.Fatalf("失步连接应终止（不重连），状态 %s", c.State())
	}
}

// TestSerializerVersionWhitelist 验证 serializer 声明的载荷编码版本受白名单
// {1,2} 约束：非法声明（0/3+）在通道构造期即拒绝（评审 Fix：此前原样采纳，
// 版本 0 出站被改写为 1 但响应校验按 0 必失败）。
func TestSerializerVersionWhitelist(t *testing.T) {
	for _, ver := range []uint8{0, 3, 99} {
		if _, err := serializerVersion(verBadSerializerN{ver: ver}); err == nil {
			t.Fatalf("ver=%d 应被白名单拒绝", ver)
		}
	}
	// 合法版本 {1,2} 通过。
	if v, err := serializerVersion(verBadSerializerN{ver: 1}); err != nil || v != 1 {
		t.Fatalf("ver=1 应通过: v=%d err=%v", v, err)
	}
	if v, err := serializerVersion(verBadSerializerN{ver: 2}); err != nil || v != 2 {
		t.Fatalf("ver=2 应通过: v=%d err=%v", v, err)
	}
	// 未实现 Versioned → 默认 ver=1。
	if v, err := serializerVersion(JSONSerializer{}); err != nil || v != 1 {
		t.Fatalf("默认 serializer 应 ver=1: v=%d err=%v", v, err)
	}
}

// verBadSerializerN 声明指定非法载荷编码版本（实现 frame.Versioned）。
type verBadSerializerN struct{ ver uint8 }

func (v verBadSerializerN) Marshal(any) ([]byte, error) { return nil, nil }
func (v verBadSerializerN) Unmarshal([]byte, any) error { return nil }
func (v verBadSerializerN) Version() uint8              { return v.ver }
