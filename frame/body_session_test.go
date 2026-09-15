package frame

import (
	"bytes"
	"strings"
	"testing"
)

// TestSessionSlotRoundTrip 验证会话槽 body 编解码往返：
// [opLen][op][sessionLen][session][payload] 与帧头 FlagSession 配对解析。
func TestSessionSlotRoundTrip(t *testing.T) {
	body, err := BuildRequestBodyWithSession("/game.v1.PlayerService/EnterMatchQueue", "tok-abc", []byte(`{"ruleset":"rank"}`))
	if err != nil {
		t.Fatal(err)
	}
	op, session, payload, err := ParseRequestBodyWithSession(body, FlagSession)
	if err != nil {
		t.Fatal(err)
	}
	if op != "/game.v1.PlayerService/EnterMatchQueue" {
		t.Fatalf("op = %q", op)
	}
	if session != "tok-abc" {
		t.Fatalf("session = %q", session)
	}
	if string(payload) != `{"ruleset":"rank"}` {
		t.Fatalf("payload = %q", payload)
	}
}

// TestSessionSlotAbsent 验证 flags 未置位时解析走旧布局（会话槽缺席不影响）。
func TestSessionSlotAbsent(t *testing.T) {
	body, err := BuildRequestBody("/game.v1.PlayerService/GetPlayer", []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	op, session, payload, err := ParseRequestBodyWithSession(body, 0)
	if err != nil {
		t.Fatal(err)
	}
	if session != "" {
		t.Fatalf("flags=0 时 session 应为空, got %q", session)
	}
	if op != "/game.v1.PlayerService/GetPlayer" || string(payload) != "{}" {
		t.Fatalf("op/payload = %q/%q", op, payload)
	}
}

// TestSessionSlotTruncated 验证置位但槽截断时报协议错误。
func TestSessionSlotTruncated(t *testing.T) {
	body, _ := BuildRequestBodyWithSession("/op", "abc", nil)
	bad := body[:len(body)-1]
	if _, _, _, err := ParseRequestBodyWithSession(bad, FlagSession); err == nil {
		t.Fatal("截断会话槽应报错")
	}
}

// TestHeaderFlagsRoundTrip 验证 flags 经流式帧与消息边界帧的完整往返。
func TestHeaderFlagsRoundTrip(t *testing.T) {
	h := Header{Type: MsgTypeRequest, Version: Version, Flags: FlagSession, Seq: 7}
	body, _ := buildRequestBody("/op", "tok", nil)

	// 流式：Read/Write 往返。
	var buf bytes.Buffer
	if err := Write(&buf, h, body, 0); err != nil {
		t.Fatal(err)
	}
	got, gotBody, err := Read(&buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Flags != FlagSession || got.Seq != 7 {
		t.Fatalf("流式 flags/seq = %x/%d", got.Flags, got.Seq)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatal("流式 body 不一致")
	}

	// 消息边界：Encode/Decode 往返。
	msg, err := Encode(h, body, 0)
	if err != nil {
		t.Fatal(err)
	}
	got2, gotBody2, err := Decode(msg, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got2.Flags != FlagSession {
		t.Fatalf("消息边界 flags = %x", got2.Flags)
	}
	if !bytes.Equal(gotBody2, body) {
		t.Fatal("消息边界 body 不一致")
	}
}

// TestHeaderRejectsUnknownFlags 验证未知 flags 位被拒绝（前向保留位白名单）。
func TestHeaderRejectsUnknownFlags(t *testing.T) {
	h := Header{Magic: Magic, Type: MsgTypeRequest, Version: Version, Flags: 0x02, Seq: 1}
	if err := h.Check(0); err == nil || !strings.Contains(err.Error(), "flags") {
		t.Fatalf("未知 flags 应报错, got %v", err)
	}
}
