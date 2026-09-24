package frame

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// jsonFloat 把期望 JSON 里的数字统一为 float64（文件解码形态）。
func jsonFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	default:
		return -1
	}
}

// assertFrameCase 执行帧解码并断言与期望一致。
func assertFrameCase(t *testing.T, c goldenCase) {
	t.Helper()
	h, body, err := Read(bytes.NewReader(c.input), c.max)
	gotErr := classifyError(err)
	if wantErr := c.want["error"]; gotErr != wantErr {
		t.Fatalf("错误分类 = %q (err=%v), 期望 %q", gotErr, err, wantErr)
	}
	if gotErr != errNone {
		return
	}
	if float64(h.Type) != jsonFloat(c.want["type"]) {
		t.Fatalf("type = %d, 期望 %v", h.Type, c.want["type"])
	}
	if uint64(h.Seq) != uint64(jsonFloat(c.want["seq"])) {
		t.Fatalf("seq = %d, 期望 %v", h.Seq, c.want["seq"])
	}
	op, payload, err := ParseRequestBody(body)
	if err != nil {
		t.Fatalf("ParseRequestBody: %v", err)
	}
	if op != c.want["operation"] {
		t.Fatalf("operation = %q, 期望 %q", op, c.want["operation"])
	}
	wantPayload, _ := hex.DecodeString(c.want["payloadHex"].(string))
	if !bytes.Equal(payload, wantPayload) {
		t.Fatalf("payload 不一致: %x", payload)
	}
	assertPayloadVersion(t, h, payload)
}

// assertPayloadVersion 按载荷形态断言协议版本语义（A.8 · ver=2 载荷，按 manifest
// 动态消费）：载荷是 JSON ⇒ ver=1（protojson）；载荷非空且不是 JSON ⇒ ver=2 且必须是
// 合法 protobuf wire。空载荷不做推断（无载荷用例对两种编码都成立）。
func assertPayloadVersion(t *testing.T, h Header, payload []byte) {
	t.Helper()
	if len(payload) == 0 {
		return
	}
	if json.Valid(payload) {
		if h.Version != Version {
			t.Fatalf("protojson（JSON）载荷的协议版本 = %d, 期望 ver=%d", h.Version, Version)
		}
		return
	}
	if h.Version != Version2 {
		t.Fatalf("protobuf wire 载荷的协议版本 = %d, 期望 ver=%d", h.Version, Version2)
	}
	if err := walkProtowire(payload); err != nil {
		t.Fatalf("ver=2 载荷不是合法 protobuf wire: %v", err)
	}
}

// walkProtowire 校验载荷是合法的 protobuf wire 字节（逐字段消费到末尾）。
func walkProtowire(b []byte) error {
	for len(b) > 0 {
		if _, _, n := protowire.ConsumeField(b); n < 0 {
			return protowire.ParseError(n)
		} else {
			b = b[n:]
		}
	}
	return nil
}

// assertReplyCase 执行响应包络解码并断言与期望一致。
func assertReplyCase(t *testing.T, c goldenCase) {
	t.Helper()
	data, st, err := DecodeReply(c.input)
	if gotErr := classifyError(err); gotErr != c.want["error"] {
		t.Fatalf("错误分类 = %q (err=%v), 期望 %q", gotErr, err, c.want["error"])
	}
	if err != nil {
		return
	}
	if hasStatus := c.want["hasStatus"].(bool); hasStatus != (st != nil) {
		t.Fatalf("hasStatus = %v, 期望 %v", st != nil, hasStatus)
	}
	if wantData, ok := c.want["dataHex"]; ok {
		wantBytes, _ := hex.DecodeString(wantData.(string))
		if !bytes.Equal(data, wantBytes) {
			t.Fatalf("data 不一致: %x", data)
		}
	}
	if want := c.want["status"]; want != nil {
		assertStatusJSON(t, st, want.(map[string]any))
	}
}

// assertStatusCase 执行独立 Status 解码并断言与期望一致。
func assertStatusCase(t *testing.T, c goldenCase) {
	t.Helper()
	st, err := DecodeStatus(c.input)
	if err != nil {
		t.Fatalf("DecodeStatus: %v", err)
	}
	assertStatusJSON(t, st, c.want["status"].(map[string]any))
}

// assertStatusJSON 对比 Status 与期望 JSON（只比非空字段，宽松于完整结构对比）。
func assertStatusJSON(t *testing.T, st *Status, want map[string]any) {
	t.Helper()
	if st == nil {
		st = &Status{}
	}
	if code, ok := want["code"].(float64); ok && st.Code != int32(code) {
		t.Fatalf("Status.Code = %d, 期望 %d", st.Code, int32(code))
	}
	if reason, ok := want["reason"].(string); ok && st.Reason != reason {
		t.Fatalf("Status.Reason = %q, 期望 %q", st.Reason, reason)
	}
	if msg, ok := want["message"].(string); ok && st.Message != msg {
		t.Fatalf("Status.Message = %q, 期望 %q", st.Message, msg)
	}
	// class 是 P6 错误投影的新字段（业务/运行时/取消），按 manifest 声明动态断言：
	// 期望值可以是数值（wire 口径）或枚举名（protojson 口径，框架 errors.proto 的
	// ErrorClass 枚举名 ERROR_CLASS_*）——两种口径都必须能解析，否则枚举化后静默
	// 降级为未分类。
	switch class := want["class"].(type) {
	case float64:
		if st.Class != Class(int32(class)) {
			t.Fatalf("Status.Class = %d, 期望 %d", st.Class, int32(class))
		}
	case string:
		if st.Class != ParseClass(class) {
			t.Fatalf("Status.Class = %d, 期望 %s（%d）", st.Class, class, ParseClass(class))
		}
	}
	if meta, ok := want["metadata"].(map[string]any); ok {
		if len(st.Metadata) != len(meta) {
			t.Fatalf("Status.Metadata 数量 = %d, 期望 %d", len(st.Metadata), len(meta))
		}
		for k, v := range meta {
			if st.Metadata[k] != v.(string) {
				t.Fatalf("Status.Metadata[%s] = %q, 期望 %q", k, st.Metadata[k], v)
			}
		}
	}
}

// 编译期引用（保持 import 干净）：io 在 errNetwork 截断用例中由 Read 间接消费。
var _ = io.EOF
var _ = json.Marshal
