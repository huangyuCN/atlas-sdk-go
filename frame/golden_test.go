// golden vectors 消费测试：向量包由 atlas 主仓生成（transport/frame -update），
// 各语言 SDK 消费同一份文件——单源防漂移（规范 §3.2/§8.1）。
// 向量目录：环境变量 ATLAS_GOLDEN_DIR（CI 检出 atlas 主仓后指向 testdata/golden），
// 默认 ../../atlas/testdata/golden（本仓与 atlas 主仓同级的本地常规布局）。
// 本文件执行帧/包络/Status 三类语义断言；文件级 sha256 自洽校验在主仓生成器。
package frame

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// goldenCase 一个用例 = 输入字节 + 语言无关期望。
type goldenCase struct {
	id    string
	kind  string
	input []byte
	max   int // 0 取默认上限
	want  map[string]any
}

const (
	kindFrame  = "frame"
	kindReply  = "reply"
	kindStatus = "status"

	errNone     = ""
	errProtocol = "protocol"
	errNetwork  = "network"
)

// loadGolden 加载向量包（manifest 双 sha256 逐文件校验）并展开为用例列表。
func loadGolden(t *testing.T) []goldenCase {
	t.Helper()
	dir := goldenDir(t)
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("读取 golden manifest（向量目录 %s）: %v", dir, err)
	}
	var manifest struct {
		Cases map[string]struct {
			Sha256Input    string `json:"sha256_input"`
			Sha256Expected string `json:"sha256_expected"`
			Kind           string `json:"kind"`
			MaxBodySize    int    `json:"max_body_size"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("解析 manifest: %v", err)
	}
	var cases []goldenCase
	for id, meta := range manifest.Cases {
		id, meta := id, meta
		input, err := os.ReadFile(filepath.Join(dir, "cases", id, "input.bin"))
		if err != nil {
			t.Fatalf("读取 %s/input.bin: %v", id, err)
		}
		if got := sha256Hex(input); got != meta.Sha256Input {
			t.Fatalf("%s/input.bin sha256 不匹配: %s != %s", id, got, meta.Sha256Input)
		}
		expectedRaw, err := os.ReadFile(filepath.Join(dir, "cases", id, "expected.json"))
		if err != nil {
			t.Fatalf("读取 %s/expected.json: %v", id, err)
		}
		if got := sha256Hex(expectedRaw); got != meta.Sha256Expected {
			t.Fatalf("%s/expected.json sha256 不匹配: %s != %s", id, got, meta.Sha256Expected)
		}
		var want map[string]any
		if err := json.Unmarshal(expectedRaw, &want); err != nil {
			t.Fatalf("解析 %s/expected.json: %v", id, err)
		}
		cases = append(cases, goldenCase{
			id:    id,
			kind:  meta.Kind,
			input: input,
			max:   meta.MaxBodySize,
			want:  want,
		})
	}
	return cases
}

// goldenDir 解析向量目录：env 优先，缺省为本仓同级的 atlas 主仓。
func goldenDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("ATLAS_GOLDEN_DIR"); dir != "" {
		return dir
	}
	abs, err := filepath.Abs(filepath.Join("..", "..", "atlas", "testdata", "golden"))
	if err != nil {
		t.Fatalf("解析默认向量目录: %v", err)
	}
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		t.Fatalf("golden 向量目录不存在: %s（请检出 atlas 主仓到本仓同级目录，或设置 ATLAS_GOLDEN_DIR）", abs)
	}
	return abs
}

// TestGoldenVectors 消费 golden vectors：逐用例执行本包解码器并断言与期望语义一致。
// 向量文件与用例构造的一致性校验在 atlas 主仓生成器（transport/frame）。
func TestGoldenVectors(t *testing.T) {
	for _, c := range loadGolden(t) {
		c := c
		t.Run(c.id, func(t *testing.T) {
			switch c.kind {
			case kindFrame:
				assertFrameCase(t, c)
			case kindReply:
				assertReplyCase(t, c)
			case kindStatus:
				assertStatusCase(t, c)
			default:
				t.Fatalf("未知用例类型 %q", c.kind)
			}
		})
	}
}

// ---- 消费侧辅助 ----

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
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
