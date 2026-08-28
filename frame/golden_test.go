package frame

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// golden vectors：协议一致性的字节级用例（规范 §8.1）。
// 用例表内嵌于此，`go test ./frame -update` 写出 testdata/golden/；
// 默认模式逐字节对比，防四语言实现漂移。expected.json 为语言无关 JSON。

var goldenUpdate = flag.Bool("update", false, "重新生成 golden vectors 文件")

const (
	kindFrame  = "frame"
	kindReply  = "reply"
	kindStatus = "status"

	errNone     = ""
	errProtocol = "protocol"
	errNetwork  = "network"
)

// goldenCase 一个用例 = 输入字节 + 语言无关期望。
type goldenCase struct {
	id    string
	kind  string
	input []byte
	max   int // 0 取默认上限
	want  map[string]any
}

// buildCases 构造全部用例（字节构造即文档：每个用例对应规范的一个场景）。
func buildCases() []goldenCase {
	pingOp := "/atlas.internal.Heartbeat/Ping"
	pingBody, _ := BuildRequestBody(pingOp, nil)

	// protojson 零值下发 + 64 位整数字符串的 payload 样例（JSON 映射断言在 DTO 层，
	// 帧层只锁定字节）。
	payload := []byte(`{"frameId":"123456789012345","winner":"","scores":0}`)
	frameBody, _ := BuildRequestBody("/lockstep.v1.Session/OnFrame", payload)

	badMagic := append([]byte{0x00, 0x00, 0x00, 0x01, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}, pingBody...)

	oversize := make([]byte, HeaderSize)
	binaryPutU32(oversize[0:4], Magic)
	oversize[4] = Version
	oversize[5] = byte(MsgTypeRequest)
	binaryPutU32(oversize[8:12], 1)
	binaryPutU32(oversize[12:16], uint32(MaxBodySize)+1)

	statusBytes := buildTestStatus(404, "PLAYER_NOT_FOUND", "玩家不存在", map[string]string{"k": "v"})
	replyOK := buildReplyOK([]byte(`{"playerId":"p1"}`))
	replyErr := buildReplyErr(statusBytes, nil)
	replyErrEmpty := buildReplyErr(nil, nil)

	// 补充场景的输入构造
	badVersion := concatFrame(MsgTypeRequest, 1, pingBody)
	badVersion[4] = 99 // ver 字节
	badType := concatFrame(MsgTypeRequest, 1, pingBody)
	badType[5] = 0 // type 字节
	badSeqZero := concatFrame(MsgTypeRequest, 0, pingBody)
	negativeCodeStatus := buildTestStatus(-1, "INTERNAL", "", nil)
	// field 99 的 key = 99<<3|2 = 794，需多字节 varint 编码（appendProtoBytes 单字节上限 15）
	unknownFieldKey := []byte{0x9A, 0x06} // varint(794)
	unknownFieldStatus := append(append([]byte{}, buildTestStatus(7, "OK", "", nil)...),
		append(unknownFieldKey, appendProtoBytes(nil, 0, []byte("future-field"))[1:]...)...)
	int64Payload := []byte(`{"frameId":"123456789012345678","bigNum":"9007199254740993"}`)
	int64Body, _ := BuildRequestBody("/test.v1.T/Big", int64Payload)

	return []goldenCase{
		{id: "frame-request-heartbeat", kind: kindFrame, input: concatFrame(MsgTypeRequest, 1, pingBody),
			want: map[string]any{"type": 1, "seq": 1, "operation": pingOp, "payloadHex": "", "error": errNone}},
		{id: "frame-request-payload", kind: kindFrame, input: concatFrame(MsgTypeRequest, 2, frameBody),
			want: map[string]any{"type": 1, "seq": 2, "operation": "/lockstep.v1.Session/OnFrame",
				"payloadHex": hex.EncodeToString(payload), "error": errNone}},
		{id: "frame-notify", kind: kindFrame, input: concatFrame(MsgTypeNotify, 9, pingBody),
			want: map[string]any{"type": 3, "seq": 9, "operation": pingOp, "payloadHex": "", "error": errNone}},
		{id: "frame-truncated-body", kind: kindFrame, input: concatFrame(MsgTypeRequest, 3, pingBody)[:HeaderSize+5],
			want: map[string]any{"error": errNetwork}},
		{id: "frame-bad-magic", kind: kindFrame, input: badMagic,
			want: map[string]any{"error": errProtocol}},
		{id: "frame-oversize", kind: kindFrame, input: oversize,
			want: map[string]any{"error": errProtocol}},
		{id: "reply-success", kind: kindReply, input: replyOK,
			want: map[string]any{"error": errNone, "dataHex": hex.EncodeToString([]byte(`{"playerId":"p1"}`)), "hasStatus": false}},
		{id: "reply-error-status", kind: kindReply, input: replyErr,
			want: map[string]any{"error": errNone, "hasStatus": true,
				"status": map[string]any{"code": 404, "reason": "PLAYER_NOT_FOUND", "message": "玩家不存在", "metadata": map[string]any{"k": "v"}}}},
		{id: "reply-error-empty-status", kind: kindReply, input: replyErrEmpty,
			want: map[string]any{"error": errNone, "hasStatus": true,
				"status": map[string]any{"code": 0, "reason": "", "message": "", "metadata": nil}}},
		{id: "status-full", kind: kindStatus, input: statusBytes,
			want: map[string]any{"status": map[string]any{"code": 404, "reason": "PLAYER_NOT_FOUND",
				"message": "玩家不存在", "metadata": map[string]any{"k": "v"}}}},

		// —— 评审 B4 补充场景（规范 §8.1 点名）——

		// 非法 version：前向版本协商位
		{id: "frame-bad-version", kind: kindFrame, input: badVersion,
			want: map[string]any{"error": errProtocol}},
		// 非法 type=0：不在 {1,2,3}
		{id: "frame-bad-type", kind: kindFrame, input: badType,
			want: map[string]any{"error": errProtocol}},
		// 非法 seq=0：协议非法值（服务端 Check 同款拒绝）
		{id: "frame-bad-seq-zero", kind: kindFrame, input: badSeqZero,
			want: map[string]any{"error": errProtocol}},
		// 头截断（15 字节）：readHeader 阶段 EOF → network
		{id: "frame-truncated-header", kind: kindFrame, input: concatFrame(MsgTypeRequest, 4, pingBody)[:HeaderSize-1],
			want: map[string]any{"error": errNetwork}},
		// Status 负数 int32 code：proto int32 负数编码为 10 字节 varint
		{id: "status-negative-code", kind: kindStatus, input: negativeCodeStatus,
			want: map[string]any{"status": map[string]any{"code": -1, "reason": "INTERNAL", "message": "", "metadata": nil}}},
		// Status 未知字段（field 99 wire 2）静默跳过：DiscardUnknown 语义
		{id: "status-unknown-field", kind: kindStatus, input: unknownFieldStatus,
			want: map[string]any{"status": map[string]any{"code": 7, "reason": "OK", "message": "", "metadata": nil}}},
		// 空 metadata：map 字段缺失
		{id: "status-no-metadata", kind: kindStatus, input: buildTestStatus(400, "BAD", "", nil),
			want: map[string]any{"status": map[string]any{"code": 400, "reason": "BAD", "message": "", "metadata": nil}}},
		// Reply 失败包络带 data：status 与 data 同时存在
		{id: "reply-error-with-data", kind: kindReply, input: buildReplyErr(statusBytes, []byte(`{"hint":"x"}`)),
			want: map[string]any{"error": errNone, "hasStatus": true,
				"status":  map[string]any{"code": 404, "reason": "PLAYER_NOT_FOUND", "message": "玩家不存在", "metadata": map[string]any{"k": "v"}},
				"dataHex": hex.EncodeToString([]byte(`{"hint":"x"}`))}},
		// Reply 包络尾随字节：与服务端 DecodeReply 语义一致——忽略尾随（只查下界不查上界），
		// 锁定该共享语义防 SDK 单方面收紧
		{id: "reply-trailing-bytes", kind: kindReply, input: append(append([]byte{}, replyOK...), 0x00),
			want: map[string]any{"error": errNone, "dataHex": hex.EncodeToString([]byte(`{"playerId":"p1"}`)), "hasStatus": false}},
		// 64 位整数 payload（protojson 字符串形态）逐字节锁定
		{id: "frame-payload-int64-string", kind: kindFrame, input: concatFrame(MsgTypeRequest, 5, int64Body),
			want: map[string]any{"type": 1, "seq": 5, "operation": "/test.v1.T/Big", "payloadHex": hex.EncodeToString(int64Payload), "error": errNone}},
		// maxBodySize 上限可配置：用 max=64 锁定「可调小」语义
		{id: "frame-custom-limit", kind: kindFrame, input: concatFrame(MsgTypeRequest, 6, pingBody), max: 64,
			want: map[string]any{"type": 1, "seq": 6, "operation": pingOp, "payloadHex": "", "error": errNone}},
	}
}

// TestGoldenVectors 消费 golden vectors：默认逐字段对比；-update 重新生成文件。
func TestGoldenVectors(t *testing.T) {
	dir := filepath.Join("..", "testdata", "golden")
	if *goldenUpdate {
		writeGolden(t, dir)
		return
	}
	manifest := readManifest(t, dir)
	for _, c := range buildCases() {
		c := c
		t.Run(c.id, func(t *testing.T) {
			entry, ok := manifest[c.id]
			if !ok {
				t.Fatalf("manifest 缺少用例 %s", c.id)
			}
			inputBin, err := os.ReadFile(filepath.Join(dir, "cases", c.id, "input.bin"))
			if err != nil {
				t.Fatalf("读取 input.bin: %v", err)
			}
			if !bytes.Equal(inputBin, c.input) {
				t.Fatal("input.bin 与用例表不一致（用例表已变？请 -update 重新生成）")
			}
			if entry.Sha256Input != sha256Hex(c.input) {
				t.Fatal("manifest sha256_input 与 input 不一致")
			}
			// expected.json 逐文件 sha256 校验（规范 §8.1）。
			expectedBytes, err := os.ReadFile(filepath.Join(dir, "cases", c.id, "expected.json"))
			if err != nil {
				t.Fatalf("读取 expected.json: %v", err)
			}
			if entry.Sha256Expected != sha256Hex(expectedBytes) {
				t.Fatal("manifest sha256_expected 与 expected.json 不一致（文件被篡改或部分更新？）")
			}
			// 语义对比：文件 JSON 与用例表都经 JSON 往返归一后 DeepEqual，
			// 不受 MarshalIndent/紧凑格式差异影响。
			var fileWant map[string]any
			if err := json.Unmarshal(expectedBytes, &fileWant); err != nil {
				t.Fatalf("解析 expected.json: %v", err)
			}
			normalized, err := json.Marshal(c.want)
			if err != nil {
				t.Fatalf("序列化用例表: %v", err)
			}
			var caseWant map[string]any
			if err := json.Unmarshal(normalized, &caseWant); err != nil {
				t.Fatalf("解析用例表: %v", err)
			}
			if !reflect.DeepEqual(fileWant, caseWant) {
				t.Fatalf("expected 语义不一致:\n  file %s\n  case %s", expectedBytes, normalized)
			}
			// 按用例类型实际执行解码，断言结果与 expected 语义一致。
			assertCase(t, c)
		})
	}
}

// assertCase 实际执行解码并断言与期望语义一致。
func assertCase(t *testing.T, c goldenCase) {
	t.Helper()
	switch c.kind {
	case kindFrame:
		assertFrameCase(t, c)
	case kindReply:
		assertReplyCase(t, c)
	case kindStatus:
		assertStatusCase(t, c)
	}
}

// classifyError 将解码错误归类为语言无关的错误分类。
func classifyError(err error) string {
	switch {
	case err == nil:
		return errNone
	case errors.Is(err, ErrProtocol):
		return errProtocol
	default:
		return errNetwork
	}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func binaryPutU32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

// concatFrame 构造完整帧字节。
func concatFrame(t MsgType, seq uint32, body []byte) []byte {
	out := make([]byte, HeaderSize+len(body))
	binaryPutU32(out[0:4], Magic)
	out[4] = Version
	out[5] = byte(t)
	binaryPutU32(out[8:12], seq)
	binaryPutU32(out[12:16], uint32(len(body)))
	copy(out[HeaderSize:], body)
	return out
}

// buildReplyOK 构造成功响应包络：[0][dataLen][data]。
func buildReplyOK(data []byte) []byte {
	out := make([]byte, 5+len(data))
	out[0] = 0
	binaryPutU32(out[1:5], uint32(len(data)))
	copy(out[5:], data)
	return out
}

// buildReplyErr 构造失败响应包络：[1][statusLen][status][dataLen][data]。
func buildReplyErr(status []byte, data []byte) []byte {
	out := make([]byte, 1+4+len(status)+4+len(data))
	out[0] = 1
	binaryPutU32(out[1:5], uint32(len(status)))
	copy(out[5:], status)
	off := 5 + len(status)
	binaryPutU32(out[off:off+4], uint32(len(data)))
	copy(out[off+4:], data)
	return out
}

// buildTestStatus 手写编码 Status protobuf（字段号 1/2/3/4），用于构造用例输入。
func buildTestStatus(code int32, reason, message string, metadata map[string]string) []byte {
	var out []byte
	appendVarintField := func(fieldNum int, v uint64) {
		out = append(out, byte(fieldNum<<3|0))
		for v >= 0x80 {
			out = append(out, byte(v)|0x80)
			v >>= 7
		}
		out = append(out, byte(v))
	}
	appendBytesField := func(fieldNum int, b []byte) {
		out = append(out, byte(fieldNum<<3|2))
		l := uint64(len(b))
		for l >= 0x80 {
			out = append(out, byte(l)|0x80)
			l >>= 7
		}
		out = append(out, byte(l))
		out = append(out, b...)
	}
	if code != 0 {
		appendVarintField(1, uint64(code))
	}
	if reason != "" {
		appendBytesField(2, []byte(reason))
	}
	if message != "" {
		appendBytesField(3, []byte(message))
	}
	for k, v := range metadata { // map entry: field4(嵌套 bytes) → {field1 key, field2 value}
		var entry []byte
		entry = appendProtoBytes(entry, 1, []byte(k))
		entry = appendProtoBytes(entry, 2, []byte(v))
		appendBytesField(4, entry)
	}
	return out
}

// appendProtoBytes 向 dst 追加一个 length-delimited 字段。
func appendProtoBytes(dst []byte, fieldNum int, b []byte) []byte {
	dst = append(dst, byte(fieldNum<<3|2))
	l := uint64(len(b))
	for l >= 0x80 {
		dst = append(dst, byte(l)|0x80)
		l >>= 7
	}
	dst = append(dst, byte(l))
	return append(dst, b...)
}

// goldenAtlasCommit 是本向量包对应的 atlas 协议基线 commit（规范 §0/§8.1）。
// 基线分支 feat/actor；合入 main 后同步更新此处与 manifest。
const goldenAtlasCommit = "0ba52c039ae8264939bfb61c97743d8b634cdbea"

// writeGolden 生成 testdata/golden 全量文件。
// 格式符合规范 §8.1：manifest 锁定 atlas 基线 commit、逐用例 meta 信息、
// input.bin 与 expected.json 双 sha256（四语言消费时逐文件校验）。
func writeGolden(t *testing.T, dir string) {
	t.Helper()
	type caseMeta struct {
		Sha256Input    string `json:"sha256_input"`
		Sha256Expected string `json:"sha256_expected"`
		Kind           string `json:"kind"`
		MaxBodySize    int    `json:"max_body_size"`
	}
	manifest := map[string]*caseMeta{}
	for _, c := range buildCases() {
		caseDir := filepath.Join(dir, "cases", c.id)
		if err := os.MkdirAll(caseDir, 0o755); err != nil {
			t.Fatal(err)
		}
		inputPath := filepath.Join(caseDir, "input.bin")
		expectedPath := filepath.Join(caseDir, "expected.json")
		if err := os.WriteFile(inputPath, c.input, 0o644); err != nil {
			t.Fatal(err)
		}
		wantJSON, _ := json.MarshalIndent(c.want, "", "  ")
		if err := os.WriteFile(expectedPath, wantJSON, 0o644); err != nil {
			t.Fatal(err)
		}
		max := c.max
		if max == 0 {
			max = MaxBodySize
		}
		manifest[c.id] = &caseMeta{
			Sha256Input:    sha256Hex(c.input),
			Sha256Expected: sha256Hex(wantJSON),
			Kind:           c.kind,
			MaxBodySize:    max,
		}
	}
	manifestJSON, _ := json.MarshalIndent(map[string]any{
		"protocolVersion": 1,
		// 规范 §0/§8.1：协议基线 = atlas feat/actor 分支；合入 main 后改锁 main 的具体 commit。
		"atlasRepo":   "github.com/huangyuCN/atlas",
		"atlasRef":    "feat/actor",
		"atlasCommit": goldenAtlasCommit,
		"note":        "atlas 帧协议 golden vectors；由 atlas-sdk-go/frame -update 生成，四语言 CI 共同消费（逐文件 sha256 校验）",
		"cases":       manifest,
	}, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifestJSON, 0o644); err != nil {
		t.Fatal(err)
	}
	fmt.Println("golden vectors 已写入", dir)
}

// readManifest 读取 manifest 并展开 expected.json 内容。
// readManifest 读取并解析 manifest（规范 §8.1 新格式：双 sha256 + kind + max_body_size）。
func readManifest(t *testing.T, dir string) map[string]struct {
	Sha256Input    string `json:"sha256_input"`
	Sha256Expected string `json:"sha256_expected"`
	Kind           string `json:"kind"`
	MaxBodySize    int    `json:"max_body_size"`
} {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("读取 manifest: %v", err)
	}
	var m struct {
		AtlasCommit string `json:"atlasCommit"`
		Cases       map[string]struct {
			Sha256Input    string `json:"sha256_input"`
			Sha256Expected string `json:"sha256_expected"`
			Kind           string `json:"kind"`
			MaxBodySize    int    `json:"max_body_size"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("解析 manifest: %v", err)
	}
	if m.AtlasCommit != goldenAtlasCommit {
		t.Fatalf("manifest atlasCommit = %s, 期望 %s（atlas 基线已变？请 -update 重新生成）", m.AtlasCommit, goldenAtlasCommit)
	}
	return m.Cases
}
