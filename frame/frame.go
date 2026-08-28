// Package frame 实现 Atlas 帧协议的客户端侧编解码。
//
// 帧格式（与服务端 transport/frame 保持一致，规范见
// atlas 仓库 docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md）：
//
//	┌──────────┬──────┬──────┬────────┬───────┬───────────┐
//	│ magic(4) │ ver  │ type │ rsv(2) │ seq(4)│ bodyLen(4)│  大端，头固定 16B
//	└──────────┴──────┴──────┴────────┴───────┴───────────┘
//
//	┌──────────────────────────────────────┐
//	│ opLen(2) │ operation utf-8 │ payload │  body 内部封装
//	└──────────────────────────────────────┘
//
// 粘包处理：先 io.ReadFull 读满 16B 头，按 bodyLen 读满 body；半包阻塞补齐、
// 多包按 Length 切分。校验失败（magic/version/type/seq/长度）返回错误，由
// 上层断连或丢帧（与服务端语义对称）。
package frame

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// 帧协议常量。
const (
	// HeaderSize 是帧头固定长度（字节）。
	HeaderSize = 16
	// Magic 是帧协议魔数（"ATLS"）。
	Magic uint32 = 0x41544C53
	// Version 是当前协议版本。
	Version uint8 = 1
	// MaxBodySize 是单帧 body 的绝对上限（2MiB，与服务端 frame.MaxBodySize 对齐）。
	MaxBodySize = 2 << 20
	// MaxOperationLen 是 operation 名的独立上限（服务端 dispatch.go 同款，防垃圾字符串耗内存）。
	MaxOperationLen = 4096
)

// MsgType 是帧类型。
type MsgType uint8

// 帧类型：请求 / 响应 / 服务端推送（不参与请求匹配）。
const (
	MsgTypeRequest  MsgType = 1
	MsgTypeResponse MsgType = 2
	MsgTypeNotify   MsgType = 3
)

// Header 是帧头的客户端侧表示。
type Header struct {
	Magic   uint32
	Version uint8
	Type    MsgType
	Seq     uint32
	Length  uint32
}

// Check 校验帧头合法性；maxBodySize ≤0 时回退绝对上限。
func (h *Header) Check(maxBodySize int) error {
	if maxBodySize <= 0 {
		maxBodySize = MaxBodySize
	}
	if h.Magic != Magic {
		return fmt.Errorf("frame: invalid magic: %x: %w", h.Magic, ErrProtocol)
	}
	if h.Seq == 0 {
		return fmt.Errorf("frame: invalid seq: %x: %w", h.Seq, ErrProtocol)
	}
	if h.Type != MsgTypeRequest && h.Type != MsgTypeResponse && h.Type != MsgTypeNotify {
		return fmt.Errorf("frame: invalid type: %x: %w", h.Type, ErrProtocol)
	}
	if h.Version != Version {
		return fmt.Errorf("frame: invalid version: %x: %w", h.Version, ErrProtocol)
	}
	if uint64(h.Length) > uint64(maxBodySize) {
		return fmt.Errorf("frame: body too large: %d > %d: %w", h.Length, maxBodySize, ErrProtocol)
	}
	return nil
}

// Write 将 header 与 body 写入 conn（header+body 帧级原子：TCP 走 writev 聚集写，
// 其余回退两次写——并发场景由上层写锁保证整帧不交错）。
func Write(w io.Writer, h Header, body []byte, maxBodySize int) error {
	if maxBodySize <= 0 {
		maxBodySize = MaxBodySize
	}
	if len(body) > maxBodySize {
		return fmt.Errorf("frame: body too large: %d > %d", len(body), maxBodySize)
	}
	if h.Magic == 0 {
		h.Magic = Magic
	}
	if h.Version == 0 {
		h.Version = Version
	}
	h.Length = uint32(len(body))

	var buf [HeaderSize]byte
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	buf[4] = h.Version
	buf[5] = byte(h.Type)
	binary.BigEndian.PutUint32(buf[8:12], h.Seq)
	binary.BigEndian.PutUint32(buf[12:16], h.Length)

	if len(body) > 0 {
		if bufs, ok := writevBuffers(w, buf[:], body); ok {
			_, err := bufs.WriteTo(w)
			return err
		}
	}
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	_, err := w.Write(body)
	return err
}

// writevBuffers 当 w 支持聚集写时返回 net.Buffers（避免 header/body 两次 syscall 之间被并发写插入）。
func writevBuffers(w io.Writer, header, body []byte) (net.Buffers, bool) {
	switch w.(type) {
	case *net.TCPConn, *net.UnixConn, *net.UDPConn:
		return net.Buffers{header, body}, true
	}
	return nil, false
}

// Read 从 r 读取一个完整帧（含 body）。maxBodySize ≤0 时取绝对上限。
func Read(r io.Reader, maxBodySize int) (Header, []byte, error) {
	h, err := readHeader(r, maxBodySize)
	if err != nil {
		return Header{}, nil, err
	}
	if h.Length == 0 {
		return h, nil, nil
	}
	body := make([]byte, h.Length)
	if _, err := io.ReadFull(r, body); err != nil {
		return Header{}, nil, err
	}
	return h, body, nil
}

// readHeader 读取并校验帧头（长度校验先于任何 body 分配，防恶意大包撑内存）。
func readHeader(r io.Reader, maxBodySize int) (Header, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, err
	}
	h := Header{
		Magic:   binary.BigEndian.Uint32(buf[0:4]),
		Version: buf[4],
		Type:    MsgType(buf[5]),
		Seq:     binary.BigEndian.Uint32(buf[8:12]),
		Length:  binary.BigEndian.Uint32(buf[12:16]),
	}
	if err := h.Check(maxBodySize); err != nil {
		return Header{}, err
	}
	return h, nil
}
