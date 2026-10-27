package edge

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// HelloMagic 是 hello 段的固定魔数（4 字节 ASCII "ATLH"），用于在数据报面区分首包与后续包。
var HelloMagic = [4]byte{'A', 'T', 'L', 'H'}

// hello 段的格式常量。线格式：magic(4) || version(1) || 票长(2, 大端) || 票据密文。
const (
	// HelloMagicLen 是魔数字段长度。
	HelloMagicLen = 4
	// HelloVersion 是 hello 段格式版本（本轮 = 1）。
	HelloVersion uint8 = 1
	// MaxTicketLen 是 hello 段可承载的票据密文上限：票据自身上限 12+16+28+2*64 = 184 字节，
	// 取 256 留足余量；声明超过该值的 hello 一律在读正文之前拒绝（防内存放大）。
	MaxTicketLen = 256
	// HelloHeaderLen 是 hello 段固定头长度（magic + 版本 + 票长）。
	HelloHeaderLen = HelloMagicLen + 1 + 2
)

var (
	// ErrHelloMalformed 表示 hello 段格式非法：坏 magic、版本不符、长度不足或截断。
	ErrHelloMalformed = errors.New("edge: hello 段格式非法")
	// ErrHelloTooLong 表示 hello 段声明的票长超过 MaxTicketLen。
	ErrHelloTooLong = errors.New("edge: hello 段超长")
)

// HelloRejectUnavailable 是数据报面 hello 阶段的「暂不可用」快速回执（单字节 0x00）。
//
// 用途：解析失败 / 后端拨号失败时，接入层除了拒绝计数，还回这一个字节，让客户端**立即**
// 按百毫秒级重试，而不是等满读超时再重拨（诊断 §6 修法 2：一次瞬时失败在 UDP 面被放大成
// 3s 惩罚 + 重拨风暴，129 次真实失败因此计成 4907 次拒绝、建流窗口拖到 36s）。
//
// 与 flow-id 的关系：回执只有 1 字节，flow-id 是 8 字节，接收侧按**数据报长度**即可区分，
// 既不占用 flow-id 取值空间，也不改 hello / flow-id 的任何既有格式。
// 客户端兼容性：按长度校验 flow-id 的实现（含三 SDK）会把它当作「被拒绝」处理，
// 行为与改造前的读超时一致，故无需改动；只是失败发现得更快。
const HelloRejectUnavailable byte = 0x00

// rejectUnavailablePacket 是可直接写出的回执字节（包级共享，hello 失败路径复用，不每包分配）。
var rejectUnavailablePacket = []byte{HelloRejectUnavailable}

// IsHelloRejectUnavailable 报告一个数据报是否为「暂不可用」快速回执。
func IsHelloRejectUnavailable(datagram []byte) bool {
	return len(datagram) == 1 && datagram[0] == HelloRejectUnavailable
}

// EncodeHello 把票据密文编码为 hello 段（客户端/SDK 与测试用，服务端只解不编）。
func EncodeHello(ticketRaw []byte) []byte {
	out := make([]byte, 0, HelloHeaderLen+len(ticketRaw))
	out = append(out, HelloMagic[:]...)
	out = append(out, HelloVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(len(ticketRaw)))
	return append(out, ticketRaw...)
}

// IsHello 报告数据报是否为本轮的 hello 段（数据报面据此区分首包与后续包）。
func IsHello(b []byte) bool {
	return len(b) >= HelloHeaderLen &&
		bytes.Equal(b[:HelloMagicLen], HelloMagic[:]) &&
		b[HelloMagicLen] == HelloVersion
}

// DecodeHello 解析 hello 段，返回票据密文与其后的剩余字节（剩余字节由调用方决定去留）。
func DecodeHello(b []byte) (ticketRaw, rest []byte, err error) {
	if len(b) < HelloHeaderLen {
		return nil, nil, fmt.Errorf("%w: 长度 %d 不足", ErrHelloMalformed, len(b))
	}
	if !bytes.Equal(b[:HelloMagicLen], HelloMagic[:]) {
		return nil, nil, fmt.Errorf("%w: magic 不符", ErrHelloMalformed)
	}
	if b[HelloMagicLen] != HelloVersion {
		return nil, nil, fmt.Errorf("%w: 版本 %d", ErrHelloMalformed, b[HelloMagicLen])
	}
	n := int(binary.BigEndian.Uint16(b[HelloMagicLen+1:]))
	if n > MaxTicketLen {
		return nil, nil, fmt.Errorf("%w: 票长 %d 超过 %d", ErrHelloTooLong, n, MaxTicketLen)
	}
	if len(b) < HelloHeaderLen+n {
		return nil, nil, fmt.Errorf("%w: 票长 %d 越界", ErrHelloMalformed, n)
	}
	return b[HelloHeaderLen : HelloHeaderLen+n], b[HelloHeaderLen+n:], nil
}

// ReadHello 从流式面读取一个 hello 段并返回票据密文；声明的票长超上限时在读正文之前即失败。
func ReadHello(r io.Reader) ([]byte, error) {
	head := make([]byte, HelloHeaderLen)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, fmt.Errorf("%w: 读 hello 头失败: %v", ErrHelloMalformed, err)
	}
	if !bytes.Equal(head[:HelloMagicLen], HelloMagic[:]) {
		return nil, fmt.Errorf("%w: magic 不符", ErrHelloMalformed)
	}
	if head[HelloMagicLen] != HelloVersion {
		return nil, fmt.Errorf("%w: 版本 %d", ErrHelloMalformed, head[HelloMagicLen])
	}
	n := int(binary.BigEndian.Uint16(head[HelloMagicLen+1:]))
	if n > MaxTicketLen {
		return nil, fmt.Errorf("%w: 票长 %d 超过 %d", ErrHelloTooLong, n, MaxTicketLen)
	}
	raw := make([]byte, n)
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, fmt.Errorf("%w: 读票据失败: %v", ErrHelloMalformed, err)
	}
	return raw, nil
}
