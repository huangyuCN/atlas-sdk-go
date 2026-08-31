package frame

import (
	"bytes"
	"errors"
	"testing"
)

// TestEncodeDecodeRoundTrip 验证消息边界编解码互逆（WS 通道的帧路径基础）。
func TestEncodeDecodeRoundTrip(t *testing.T) {
	body, err := BuildRequestBody("/gateway.v1.GatewayAuth/Login", []byte(`{"playerId":"p1"}`))
	if err != nil {
		t.Fatalf("BuildRequestBody: %v", err)
	}
	hdr := Header{Type: MsgTypeRequest, Seq: 42}

	data, err := Encode(hdr, body, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(data) != HeaderSize+len(body) {
		t.Fatalf("帧长 = %d, 期望 %d", len(data), HeaderSize+len(body))
	}

	got, gotBody, err := Decode(data, 0)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Type != MsgTypeRequest || got.Seq != 42 {
		t.Fatalf("header 不一致: %+v", got)
	}
	if got.Magic != Magic || got.Version != Version || got.Length != uint32(len(body)) {
		t.Fatalf("默认字段补齐不一致: %+v", got)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body 不一致")
	}

	// Encode 产物与流式 Write 头部逐字节一致（两传输体线格式同源）。
	if !bytes.Equal(data[:HeaderSize], mustWriteHeader(t, hdr, body)[:HeaderSize]) {
		t.Fatal("Encode 与 Write 的帧头字节不一致")
	}
}

// TestDecodeRejectsTruncatedAndMismatch 验证 Decode 对截断与长度失步的拒绝。
func TestDecodeRejectsTruncatedAndMismatch(t *testing.T) {
	body, _ := BuildRequestBody("op", []byte(`{}`))
	data, err := Encode(Header{Type: MsgTypeNotify, Seq: 1}, body, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	cases := []struct {
		name string
		msg  []byte
	}{
		{"短于帧头", data[:HeaderSize-1]},
		{"空消息", nil},
		{"声明 bodyLen 与消息长度失步", append(data, 0xFF)}, // 多出的尾部字节
	}
	for _, tc := range cases {
		if _, _, err := Decode(tc.msg, 0); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s: 期望 ErrProtocol, 实际 %v", tc.name, err)
		}
	}
}

// mustWriteHeader 用流式 Write 把帧写入内存缓冲，返回原始字节（对照用）。
func mustWriteHeader(t *testing.T, h Header, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Write(&buf, h, body, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return buf.Bytes()
}
