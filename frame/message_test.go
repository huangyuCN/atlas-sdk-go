package frame

import (
	"bytes"
	"errors"
	"testing"
)

// TestEncodeDecodeRoundTrip 验证消息边界编解码互逆（WS 通道的帧路径基础）。
func TestEncodeDecodeRoundTrip(t *testing.T) {
	body, err := BuildRequestBody("/gateway.v1.Session/Login", []byte(`{"playerId":"p1"}`))
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

	got, gotBody, err := DecodeMessage(data, 0)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
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

	// datagram 口径对同一帧给出相同结果（两个入口共享同一份头校验与 body 提取语义）。
	dgHeader, dgBody, err := Decode(data, 0)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dgHeader != got || !bytes.Equal(dgBody, body) {
		t.Fatalf("Decode 与 DecodeMessage 结果不一致: %+v / %+v", dgHeader, got)
	}

	// Encode 产物与流式 Write 头部逐字节一致（两传输体线格式同源）。
	if !bytes.Equal(data[:HeaderSize], mustWriteHeader(t, hdr, body)[:HeaderSize]) {
		t.Fatal("Encode 与 Write 的帧头字节不一致")
	}
}

// TestDecodeMessageRejectsTruncatedAndMismatch 验证消息边界口径（DecodeMessage）对截断与
// 长度失步一律按协议错误拒绝：消息边界传输下长度不等于 bodyLen 即已失步。
func TestDecodeMessageRejectsTruncatedAndMismatch(t *testing.T) {
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
		if _, _, err := DecodeMessage(tc.msg, 0); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s: 期望 ErrProtocol, 实际 %v", tc.name, err)
		}
	}
}

// TestDecodeDatagramSemantics 验证 datagram 口径（Decode）的失败归类与尾随字节容忍：
// 字节不够归 ErrIncomplete（传输类，可等更多数据），坏头归 ErrProtocol，尾随字节被容忍。
func TestDecodeDatagramSemantics(t *testing.T) {
	body, _ := BuildRequestBody("op", []byte(`{}`))
	data, err := Encode(Header{Type: MsgTypeRequest, Seq: 3}, body, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if _, _, err := Decode(data[:HeaderSize-1], 0); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("短于帧头应归 ErrIncomplete, 实际 %v", err)
	}
	if _, _, err := Decode(data[:len(data)-1], 0); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("body 未到齐应归 ErrIncomplete, 实际 %v", err)
	}
	trailing := append(append([]byte(nil), data...), 0x00, 0x01)
	if _, gotBody, err := Decode(trailing, 0); err != nil || !bytes.Equal(gotBody, body) {
		t.Fatalf("datagram 口径应容忍尾随字节: body=%x err=%v", gotBody, err)
	}

	badVersion := append([]byte(nil), data...)
	badVersion[4] = 99
	if _, _, err := Decode(badVersion, 0); !errors.Is(err, ErrProtocol) {
		t.Fatalf("坏 version 应归 ErrProtocol, 实际 %v", err)
	}
	if _, _, err := DecodeMessage(badVersion, 0); !errors.Is(err, ErrProtocol) {
		t.Fatalf("消息边界口径下坏 version 应归 ErrProtocol, 实际 %v", err)
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
