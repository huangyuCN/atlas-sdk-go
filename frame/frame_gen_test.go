// 帧常量单源断言（A.2）：frame 包的协议常量必须取自 gen-frame 生成物
// （frame/gen/frame_gen.go，由 scripts/gen-dto.sh 从框架仓逐字节复制），
// 手写副本归零——魔数字面量不得再出现在 frame.go。
package frame

import (
	"bytes"
	"os"
	"testing"

	goframe "github.com/huangyuCN/atlas-sdk-go/frame/gen"
)

// TestConstantsFromGenerated 断言帧常量逐个来自生成物（改生成物即本包同步）。
func TestConstantsFromGenerated(t *testing.T) {
	if HeaderSize != goframe.HeaderSize {
		t.Errorf("HeaderSize = %d, 生成物 %d", HeaderSize, goframe.HeaderSize)
	}
	if Magic != goframe.Magic {
		t.Errorf("Magic = %#x, 生成物 %#x", Magic, goframe.Magic)
	}
	if Version != goframe.Version {
		t.Errorf("Version = %d, 生成物 %d", Version, goframe.Version)
	}
	if Version2 != goframe.Version2 {
		t.Errorf("Version2 = %d, 生成物 %d", Version2, goframe.Version2)
	}
	if MaxBodySize != goframe.MaxBodySize {
		t.Errorf("MaxBodySize = %d, 生成物 %d", MaxBodySize, goframe.MaxBodySize)
	}
	if MaxOperationLen != goframe.MaxOperationLen {
		t.Errorf("MaxOperationLen = %d, 生成物 %d", MaxOperationLen, goframe.MaxOperationLen)
	}
	if MaxSessionLen != goframe.MaxSessionLen {
		t.Errorf("MaxSessionLen = %d, 生成物 %d", MaxSessionLen, goframe.MaxSessionLen)
	}
	if MaxRequestIDLen != goframe.MaxRequestIDLen {
		t.Errorf("MaxRequestIDLen = %d, 生成物 %d", MaxRequestIDLen, goframe.MaxRequestIDLen)
	}
	if uint8(MsgTypeRequest) != goframe.MsgTypeRequest {
		t.Errorf("MsgTypeRequest = %d, 生成物 %d", MsgTypeRequest, goframe.MsgTypeRequest)
	}
	if uint8(MsgTypeResponse) != goframe.MsgTypeResponse {
		t.Errorf("MsgTypeResponse = %d, 生成物 %d", MsgTypeResponse, goframe.MsgTypeResponse)
	}
	if uint8(MsgTypeNotify) != goframe.MsgTypeNotify {
		t.Errorf("MsgTypeNotify = %d, 生成物 %d", MsgTypeNotify, goframe.MsgTypeNotify)
	}
	if FlagSession != goframe.FlagSession {
		t.Errorf("FlagSession = %d, 生成物 %d", FlagSession, goframe.FlagSession)
	}
	if FlagRequestID != goframe.FlagRequestID {
		t.Errorf("FlagRequestID = %d, 生成物 %d", FlagRequestID, goframe.FlagRequestID)
	}
}

// TestFrameGoHasNoHandwrittenMagic 断言 frame.go 不再手写魔数（手写副本会漂移）。
func TestFrameGoHasNoHandwrittenMagic(t *testing.T) {
	src, err := os.ReadFile("frame.go")
	if err != nil {
		t.Fatalf("读取 frame.go: %v", err)
	}
	for _, lit := range []string{"0x41544C53", "0x41544c53"} {
		if bytes.Contains(src, []byte(lit)) {
			t.Fatalf("frame.go 仍含手写魔数 %s（应从 frame/gen 生成物引用）", lit)
		}
	}
}
