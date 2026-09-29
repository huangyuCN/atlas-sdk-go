// Package frame 实现 Atlas 帧协议的客户端侧编解码。
//
// 帧格式（与服务端 transport/frame 保持一致，规范见
// atlas 仓库 docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md）：
//
//	┌──────────┬──────┬──────┬────────┬───────┬───────────┐
//	│ magic(4) │ ver  │ type │flags(1)│rsv(1) │ seq(4)│bodyLen(4)│  大端，头固定 16B
//	└──────────┴──────┴──────┴────────┴───────┴───────────┘
//
//	┌───────────────────────────────────────────────┐
//	│ opLen(2) │ operation │ [sessionLen(2) │ session] │ payload │  body 内部封装
//	└───────────────────────────────────────────────┘
//
// 会话槽（flags bit0 = FlagSession）：无连接传输（UDP/KCP）的请求帧携带会话
// 凭据供服务端验证身份；长连接（TCP/WS）按连接绑定、不置位、body 无会话字段。
//
// 粘包处理：先 io.ReadFull 读满 16B 头，按 bodyLen 读满 body；半包阻塞补齐、
// 多包按 Length 切分。校验失败（magic/version/type/seq/长度）返回错误，由
// 上层断连或丢帧（与服务端语义对称）。
package frame

import (
	"fmt"
	"io"
	"net"

	goframe "github.com/huangyuCN/atlas-sdk-go/frame/gen"
)

// 帧协议常量：唯一来源是框架仓 gen-frame 生成物（scripts/gen-dto.sh 逐字节复制到
// frame/gen/consts_gen.go），本包只做类型化引用——手写副本会与服务端漂移。
const (
	// HeaderSize 是帧头固定长度（字节）。
	HeaderSize = goframe.HeaderSize
	// Magic 是帧协议魔数（"ATLS"）。
	Magic uint32 = goframe.Magic
	// Version 是当前默认协议版本（载荷编码 ver=1：protojson JSON，规范 §3.1）。
	Version uint8 = goframe.Version
	// Version2 是载荷编码 ver=2（protobuf 二进制 wire format；规范 §3.1 载荷编码
	// 协商，2026-09-04 v0.5 设计决策）。可选增强：服务端支持 ver=2 前勿在真实
	// 连接启用（protojson ver=1 永续支持）。
	Version2 uint8 = goframe.Version2
	// MaxBodySize 是单帧 body 的绝对上限（2MiB，与服务端 frame.MaxBodySize 对齐）。
	MaxBodySize = goframe.MaxBodySize
)

// MsgType 是帧类型。
type MsgType uint8

// 帧类型：请求 / 响应 / 服务端推送（不参与请求匹配）；取值取自生成物。
const (
	MsgTypeRequest  MsgType = MsgType(goframe.MsgTypeRequest)
	MsgTypeResponse MsgType = MsgType(goframe.MsgTypeResponse)
	MsgTypeNotify   MsgType = MsgType(goframe.MsgTypeNotify)
)

// Header 是帧头的客户端侧表示。
type Header struct {
	Magic   uint32
	Version uint8
	Type    MsgType
	Flags   uint8 // flags 位图（原 rsv 首字节；bit0 = FlagSession）
	Seq     uint32
	Length  uint32
}

// Frame flags 位图（帧头 flags 字节的位定义）；已定义位取自生成物。
const (
	// FlagSession 表示请求帧 body 携带会话槽（sessionLen + session + payload）。
	// 仅无连接传输（UDP/KCP）的请求帧置位；长连接按连接绑定身份。
	FlagSession uint8 = goframe.FlagSession
	// FlagRequestID 表示请求帧 body 携带请求幂等键（requestIDLen + requestID 段，
	// 紧随会话槽之后、payload 之前）。客户端重试/重发复用同一 ID；服务端按
	// atlas.route.v1 注解决定是否注入投递去重键。
	FlagRequestID uint8 = goframe.FlagRequestID
)

// Check 校验帧头合法性；maxBodySize ≤0 时回退绝对上限。
// 校验语义唯一来源是生成物 goframe.CheckHeader（本方法只做类型化转换与 ErrProtocol 归类）。
func (h *Header) Check(maxBodySize int) error {
	if err := goframe.CheckHeader(toGen(*h), maxBodySize); err != nil {
		return wrapProtocol(err)
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
	goframe.PutHeader(buf[:], toGen(h))

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
// 解析与校验转发生成物 goframe.DecodeHeader（唯一实现），失败按 ErrProtocol 归类。
func readHeader(r io.Reader, maxBodySize int) (Header, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, err
	}
	h, err := goframe.DecodeHeader(buf[:], maxBodySize)
	if err != nil {
		return Header{}, wrapProtocol(err)
	}
	return fromGen(h), nil
}

// Versioned 是序列化器的可选扩展接口（载荷编码版本声明，规范 §3.1 载荷编码
// 协商）：client.Serializer 的实现者（如 contrib/protobuf）可选择性实现，
// 未实现者默认 ver=1（protojson）。放在 frame 层以避免 contrib → client 的
// 依赖环（载荷编码版本本就是帧协议层概念）。
type Versioned interface {
	Version() uint8
}
