package client

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// udpMaxDatagramSize 是 UDP 通道单数据报上限（字节，含 16B 帧头）：
// 服务端读缓冲默认 64KiB，超限数据报被截断后解码丢弃——写侧提前拦截。
const udpMaxDatagramSize = 64 * 1024

// dialUDP 建立面向连接的 UDP 传输（net.DialUDP，与 atlas 服务端 udp 适配器同构）：
// 一报一帧；读缓冲 64KiB（含帧头）；坏数据报静默丢弃（服务端 ErrBadFrame
// 软跳过语义，防垃圾/放大攻击）。UDP 无连接：Resolve + DialUDP 即时完成，
// 无需握手 goroutine；「断线」由传输心跳死链判定驱动重拨。
func dialUDP(ctx context.Context, addr string) (channelTransport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	return &udpTransport{conn: conn}, nil
}

// udpTransport 基于面向连接 UDP socket 的数据报传输。
type udpTransport struct {
	conn    *net.UDPConn
	readBuf []byte // 读侧单 goroutine（readLoop 串行）复用，避免每次读分配
	writeMu sync.Mutex
}

// ReadFrame 读一个数据报并解析为帧。
// 坏数据报（垃圾字节/截断/非法头）静默丢弃并继续读——不视为连接故障
// （与 atlas 服务端 engine.ErrBadFrame 软跳过语义一致）；仅 I/O 错误（连接关闭）上抛。
func (t *udpTransport) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	if t.readBuf == nil {
		t.readBuf = make([]byte, udpMaxDatagramSize)
	}
	for {
		n, err := t.conn.Read(t.readBuf)
		if err != nil {
			return frame.Header{}, nil, err
		}
		h, body, derr := frame.Decode(t.readBuf[:n], maxBodySize)
		if derr != nil {
			continue // 坏数据报：静默丢弃
		}
		// body 是 readBuf 的子切片，而 readBuf 会被下一轮读覆盖——必须拷贝出让所有权
		//（与 TCP frame.Read 的逐帧分配语义一致；Notify payload 会逃逸到 handler goroutine）。
		out := make([]byte, len(body))
		copy(out, body)
		return h, out, nil
	}
}

// WriteFrame 编码整帧并以单个数据报发送（一报一帧）。
// 超出 64KiB（含帧头）在写侧即报错：超限帧在服务端读缓冲处被截断后丢弃，
// 提前拦截可让调用方立刻感知配置问题而非静默失败。
func (t *udpTransport) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	if len(body)+frame.HeaderSize > udpMaxDatagramSize {
		return fmt.Errorf("udp: datagram too large: %d body + %d header > %d",
			len(body), frame.HeaderSize, udpMaxDatagramSize)
	}
	msg, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	_, err = t.conn.Write(msg)
	return err
}

// Close 关闭底层 UDP socket（阻塞中的 Read 随即返回错误，触发重连/关闭收尾）。
func (t *udpTransport) Close() error { return t.conn.Close() }
