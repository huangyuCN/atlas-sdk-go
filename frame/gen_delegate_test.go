package frame

import (
	"bytes"
	"errors"
	"testing"

	goframe "github.com/huangyuCN/atlas-sdk-go/frame/gen"
)

// 仓位校验/编解码与生成物的差分断言：本包只做类型化转换与错误归类，
// 判定与字节必须与生成物逐条一致（否则 SDK 与框架/TS/C# 四方漂移）。

// TestCheckMatchesGenerated 断言 Header.Check 与生成物 CheckHeader 判定、文案一致，
// 且 SDK 侧错误满足 errors.Is(err, ErrProtocol)。
func TestCheckMatchesGenerated(t *testing.T) {
	cases := map[string]Header{
		"合法":         {Magic: Magic, Version: Version, Type: MsgTypeRequest, Seq: 1},
		"坏 magic":    {Version: Version, Type: MsgTypeRequest, Seq: 1},
		"seq=0":      {Magic: Magic, Version: Version, Type: MsgTypeRequest},
		"坏 type":     {Magic: Magic, Version: Version, Type: MsgType(9), Seq: 1},
		"坏 version":  {Magic: Magic, Version: 99, Type: MsgTypeRequest, Seq: 1},
		"坏 flags":    {Magic: Magic, Version: Version, Type: MsgTypeRequest, Seq: 1, Flags: 0x04},
		"bodyLen 超限": {Magic: Magic, Version: Version, Type: MsgTypeRequest, Seq: 1, Length: uint32(MaxBodySize) + 1},
	}
	for name, h := range cases {
		sdkErr := h.Check(MaxBodySize)
		genErr := goframe.CheckHeader(toGen(h), MaxBodySize)
		if (sdkErr == nil) != (genErr == nil) {
			t.Errorf("[%s] 判定不一致: sdk=%v gen=%v", name, sdkErr, genErr)
			continue
		}
		if sdkErr == nil {
			continue
		}
		if !errors.Is(sdkErr, ErrProtocol) {
			t.Errorf("[%s] SDK 错误应满足 ErrProtocol: %v", name, sdkErr)
		}
		if errors.Is(genErr, ErrProtocol) {
			t.Errorf("[%s] 生成物错误不应带 SDK 的 ErrProtocol 语义", name)
		}
	}
}

// TestEncodeMatchesGenerated 断言 Encode 与生成物字节一致（含默认值补齐与超限拒绝）。
func TestEncodeMatchesGenerated(t *testing.T) {
	h := Header{Type: MsgTypeRequest, Seq: 11, Flags: FlagSession}
	body := []byte("differential-payload")
	sdkOut, err := Encode(h, body, 0)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	genOut, err := goframe.Encode(toGen(h), body, 0)
	if err != nil {
		t.Fatalf("goframe.Encode: %v", err)
	}
	if !bytes.Equal(sdkOut, genOut) {
		t.Fatalf("字节不一致:\nsdk=%x\ngen=%x", sdkOut, genOut)
	}
	if _, err := Encode(h, body, 4); err == nil {
		t.Fatal("body 超限应报错")
	}
}

// TestDecodeClassificationMatchesGenerated 断言 datagram/消息边界两个口径的错误归类
// 与生成物一致（incomplete 判等、报错判等），覆盖坏头先于截断判定的顺序。
func TestDecodeClassificationMatchesGenerated(t *testing.T) {
	gen, err := goframe.Encode(goframe.Header{Type: goframe.MsgTypeRequest, Seq: 5}, []byte("abc"), 0)
	if err != nil {
		t.Fatalf("goframe.Encode: %v", err)
	}
	badVersion := append([]byte(nil), gen...)
	badVersion[4] = 99
	oversize := goframe.EncodeHeader(goframe.Header{
		Magic: goframe.Magic, Version: goframe.Version, Type: goframe.MsgTypeRequest,
		Seq: 1, Length: uint32(goframe.MaxBodySize) + 1,
	})
	cases := map[string][]byte{
		"合法帧":       gen,
		"尾随字节":      append(append([]byte(nil), gen...), 0xFF),
		"短于帧头":      gen[:8],
		"body 截断":   gen[:len(gen)-1],
		"坏 version": badVersion,
		"长度超限":      oversize,
	}
	for name, raw := range cases {
		sdkHeader, sdkBody, sdkErr := Decode(raw, 0)
		genHeader, genBody, genErr := goframe.Decode(raw, 0)
		if (sdkErr == nil) != (genErr == nil) {
			t.Errorf("[%s] Decode 判定不一致: sdk=%v gen=%v", name, sdkErr, genErr)
			continue
		}
		if errors.Is(sdkErr, goframe.ErrIncomplete) != errors.Is(genErr, goframe.ErrIncomplete) {
			t.Errorf("[%s] incomplete 判定不一致: sdk=%v gen=%v", name, sdkErr, genErr)
		}
		if sdkErr == nil && (sdkHeader != fromGen(genHeader) || !bytes.Equal(sdkBody, genBody)) {
			t.Errorf("[%s] 解码结果不一致", name)
		}
		if sdkErr == nil {
			continue
		}
		// SDK 侧：除 incomplete 外都必须可按 ErrProtocol 归类。
		if !errors.Is(sdkErr, goframe.ErrIncomplete) && !errors.Is(sdkErr, ErrProtocol) {
			t.Errorf("[%s] SDK 错误既非 incomplete 也非 ErrProtocol: %v", name, sdkErr)
		}
	}
}
