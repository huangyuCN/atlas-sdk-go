// protobuf serializer 测试：proto.Message 往返、ver=2 声明、非 message 入参报错。
package protobuf

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestSerializerRoundTrip(t *testing.T) {
	var s Serializer
	msg := wrapperspb.String("你好 atlas")
	data, err := s.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Contains(data, []byte("你好")) {
		t.Fatalf("protobuf 二进制应含 UTF-8 字符串字节: %x", data)
	}
	var out wrapperspb.StringValue
	if err := s.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.GetValue() != "你好 atlas" {
		t.Fatalf("往返不一致: %q", out.GetValue())
	}
}

func TestSerializerVersion(t *testing.T) {
	var s Serializer
	if v, ok := any(s).(interface{ Version() uint8 }); !ok || v.Version() != 2 {
		t.Fatal("protobuf serializer 应实现 Versioned 并声明 ver=2")
	}
	// client 侧推导：WithSerializer 后通道 ver 应为 2（由 serializerVersion 推导）。
}

func TestSerializerRejectsNonMessage(t *testing.T) {
	var s Serializer
	if _, err := s.Marshal(map[string]any{"a": 1}); err == nil {
		t.Fatal("非 proto.Message 的请求 DTO 应报错")
	}
	if err := s.Unmarshal([]byte{0x0a, 0x01, 0x78}, map[string]any{}); err == nil {
		t.Fatal("非 proto.Message 的响应 DTO 应报错")
	}
}
