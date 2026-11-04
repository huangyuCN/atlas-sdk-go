// 版本白名单一致性（与框架仓 transport/frame/gen/goframe/payload_versions_test.go、
// TS SDK test/frameVersions.test.ts、C# 端 FramePayloadVersionsTest 四处对称）：
//
//	① ver=3（下行增量广播）帧必须能读出帧头与 body——旧白名单下这里是真红；
//	② 未定义的 ver=4 仍被拒；
//	③ 「能声明的版本集合 == 解码器接受的版本集合」逐版本对账（防两处独立维护再漂移）。
package frame

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// TestDecodeVersion3Frame 用线格式字面量字节断言 ver=3 能读（不借 Write 自拼自解）。
func TestDecodeVersion3Frame(t *testing.T) {
	body := []byte("delta")
	raw := make([]byte, HeaderSize+len(body))
	binary.BigEndian.PutUint32(raw[0:4], 0x41544C53) // "ATLS"
	raw[4] = 3
	raw[5] = 1 // MsgTypeRequest
	binary.BigEndian.PutUint32(raw[8:12], 1)
	binary.BigEndian.PutUint32(raw[12:16], uint32(len(body)))
	copy(raw[HeaderSize:], body)

	h, gotBody, err := Read(bytes.NewReader(raw), MaxBodySize)
	if err != nil {
		t.Fatalf("ver=3 帧读取失败（白名单未扩到 ver=3？）: %v", err)
	}
	if h.Version != 3 || !bytes.Equal(gotBody, body) {
		t.Fatalf("ver=%d body=%q，期望 ver=3 body=%q", h.Version, gotBody, body)
	}
}

// TestUndefinedVersionRejected 钉住“别放太宽”：ver=4 仍是协议错误。
func TestUndefinedVersionRejected(t *testing.T) {
	h := Header{Magic: Magic, Version: 4, Type: MsgTypeRequest, Seq: 1}
	err := h.Check(MaxBodySize)
	if err == nil || !strings.Contains(err.Error(), "invalid version") {
		t.Fatalf("ver=4 Check() = %v，期望 invalid version", err)
	}
}

// TestPayloadVersionsMatchDecoder 断言声明面（PayloadVersions）与解码面（Check 探测）完全相等。
func TestPayloadVersionsMatchDecoder(t *testing.T) {
	declared := map[uint8]bool{}
	for _, v := range PayloadVersions() {
		if declared[v] {
			t.Fatalf("PayloadVersions 有重复项: %v", PayloadVersions())
		}
		declared[v] = true
	}
	for v := 0; v < 256; v++ {
		h := Header{Magic: Magic, Version: uint8(v), Type: MsgTypeRequest, Seq: 1}
		accepted := h.Check(MaxBodySize) == nil
		if accepted != declared[uint8(v)] {
			t.Fatalf("ver=%d：解码器接受=%v，声明=%v（漂移）", v, accepted, declared[uint8(v)])
		}
	}
}
