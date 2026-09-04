// frame 版本白名单测试（载荷编码协商，规范 §3.1）：ver=1/ver=2 合法，未知版本拒绝。
package frame

import (
	"bytes"
	"testing"
)

func TestCheckVersionWhitelist(t *testing.T) {
	cases := []struct {
		ver   uint8
		valid bool
	}{
		{Version, true},  // ver=1 protojson
		{Version2, true}, // ver=2 protobuf 二进制
		{0, false},       // 未知
		{3, false},       // 前向保留：未知版本即协议非法
		{99, false},
	}
	for _, tc := range cases {
		h := Header{Magic: Magic, Version: tc.ver, Type: MsgTypeRequest, Seq: 1}
		err := h.Check(MaxBodySize)
		if tc.valid && err != nil {
			t.Fatalf("ver=%d 应合法，得到 %v", tc.ver, err)
		}
		if !tc.valid && err == nil {
			t.Fatalf("ver=%d 应拒绝", tc.ver)
		}
	}
}

func TestReadWriteVersion2Frame(t *testing.T) {
	// ver=2 帧的读写往返：白名单放宽后二进制形态帧可正常编解码。
	h := Header{Magic: Magic, Version: Version2, Type: MsgTypeResponse, Seq: 7, Length: 3}
	body := []byte{0x0a, 0x01, 0x78}
	var buf bytes.Buffer
	if err := Write(&buf, h, body, MaxBodySize); err != nil {
		t.Fatalf("Write: %v", err)
	}
	gotH, gotBody, err := Read(&buf, MaxBodySize)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if gotH.Version != Version2 || gotH.Seq != 7 {
		t.Fatalf("header 不一致: %+v", gotH)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body 不一致: %x", gotBody)
	}
}
