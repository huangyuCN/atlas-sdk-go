package frame

import (
	"encoding/binary"
	"fmt"
)

// 消息边界传输体（如 WebSocket：一条消息 = 一个完整帧）的帧编解码。
// 流式传输体（TCP/KCP 字节流）使用 Read/Write；两者线格式完全一致。

// Encode 将 header 与 body 编码为完整帧字节。长度校验与 Write 一致：超限返回错误。
// Magic/Version 零值按协议默认值补齐（与 Write 对称）。
func Encode(h Header, body []byte, maxBodySize int) ([]byte, error) {
	if maxBodySize <= 0 {
		maxBodySize = MaxBodySize
	}
	if len(body) > maxBodySize {
		return nil, fmt.Errorf("frame: body too large: %d > %d", len(body), maxBodySize)
	}
	if h.Magic == 0 {
		h.Magic = Magic
	}
	if h.Version == 0 {
		h.Version = Version
	}
	h.Length = uint32(len(body))

	out := make([]byte, HeaderSize+len(body))
	encodeHeaderInto(out[:HeaderSize], h)
	copy(out[HeaderSize:], body)
	return out, nil
}

// Decode 从一条完整消息解析帧（与 Encode 对应）。消息长度与帧头 bodyLen 不一致
// 视为协议非法——消息边界传输下即已失步，由上层按协议错误终止连接。
func Decode(msg []byte, maxBodySize int) (Header, []byte, error) {
	if len(msg) < HeaderSize {
		return Header{}, nil, fmt.Errorf("frame: message shorter than header: %d < %d: %w", len(msg), HeaderSize, ErrProtocol)
	}
	h := Header{
		Magic:   binary.BigEndian.Uint32(msg[0:4]),
		Version: msg[4],
		Type:    MsgType(msg[5]),
		Seq:     binary.BigEndian.Uint32(msg[8:12]),
		Length:  binary.BigEndian.Uint32(msg[12:16]),
	}
	if err := h.Check(maxBodySize); err != nil {
		return Header{}, nil, err
	}
	if uint64(len(msg)-HeaderSize) != uint64(h.Length) {
		return Header{}, nil, fmt.Errorf("frame: message length mismatch bodyLen: %d != %d: %w", len(msg)-HeaderSize, h.Length, ErrProtocol)
	}
	return h, msg[HeaderSize:], nil
}

// encodeHeaderInto 将 header 编码进 buf（长度必须为 HeaderSize；Write/Encode 共用）。
func encodeHeaderInto(buf []byte, h Header) {
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	buf[4] = h.Version
	buf[5] = byte(h.Type)
	binary.BigEndian.PutUint32(buf[8:12], h.Seq)
	binary.BigEndian.PutUint32(buf[12:16], h.Length)
}
