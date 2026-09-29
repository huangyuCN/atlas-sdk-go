package frame

import (
	"errors"
	"fmt"

	goframe "github.com/huangyuCN/atlas-sdk-go/frame/gen"
)

// 报文级（datagram）与消息边界（一条消息 = 一个完整帧，如 UDP 数据报 / WebSocket）
// 的帧编解码。字节层实现唯一来源是生成物（frame/gen 的 goframe，由 scripts/gen-dto.sh
// 逐字节复制），本文件只做类型化转换与错误归类；流式传输体（TCP/KCP 字节流）用 Read/Write，
// 两者线格式完全一致。

// ErrIncomplete 表示输入字节尚不完整（datagram 口径的 body 未到齐、流式读取需更多数据），
// 与生成物 goframe.ErrIncomplete 同源。golden 口径把这类失败归 network，其余归 protocol。
var ErrIncomplete = goframe.ErrIncomplete

// Encode 将 header 与 body 编码为完整帧字节。长度校验与 Write 一致：超限返回错误。
// Magic/Version 零值按协议默认值补齐（与 Write 对称）；出站不校验头字段（与生成物一致）。
func Encode(h Header, body []byte, maxBodySize int) ([]byte, error) {
	return goframe.Encode(toGen(h), body, maxBodySize)
}

// Decode 从单个 datagram/报文解析帧（datagram 口径：先校验帧头，再判 body 是否到齐，
// 允许尾随字节）。body 是输入切片的子切片（非拷贝）；body 未到齐返回 ErrIncomplete。
// 消息边界传输体（长度必须恰好相等）请用 DecodeMessage。
func Decode(datagram []byte, maxBodySize int) (Header, []byte, error) {
	h, body, err := goframe.Decode(datagram, maxBodySize)
	if err != nil {
		return Header{}, nil, wrapProtocol(err)
	}
	return fromGen(h), body, nil
}

// DecodeMessage 从一条完整消息解析帧（消息边界口径：消息长度必须恰好等于 HeaderSize+bodyLen）。
// 长度失步即协议非法（ErrProtocol）——消息边界传输下已失步，由上层按协议错误终止连接。
func DecodeMessage(msg []byte, maxBodySize int) (Header, []byte, error) {
	h, body, err := goframe.DecodeMessage(msg, maxBodySize)
	if err != nil {
		return Header{}, nil, wrapProtocol(err)
	}
	return fromGen(h), body, nil
}

// wrapProtocol 把生成物的失败按 SDK 归类：字节不够（ErrIncomplete）保持原样（归传输类），
// 其余包装 ErrProtocol 供 errors.Is 判定（与 Go frame.ErrProtocol 语义同构）。
func wrapProtocol(err error) error {
	if errors.Is(err, goframe.ErrIncomplete) {
		return err
	}
	return fmt.Errorf("%w: %w", err, ErrProtocol)
}

// toGen 把 SDK 帧头转换为生成物的线格式帧头（字段一一对应）。
func toGen(h Header) goframe.Header {
	return goframe.Header{
		Magic:   h.Magic,
		Version: h.Version,
		Type:    byte(h.Type),
		Flags:   h.Flags,
		Seq:     h.Seq,
		Length:  h.Length,
	}
}

// fromGen 把生成物的线格式帧头转换为 SDK 帧头（Type 收敛为 MsgType）。
func fromGen(h goframe.Header) Header {
	return Header{
		Magic:   h.Magic,
		Version: h.Version,
		Type:    MsgType(h.Type),
		Flags:   h.Flags,
		Seq:     h.Seq,
		Length:  h.Length,
	}
}
