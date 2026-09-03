package frame

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"testing"
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
