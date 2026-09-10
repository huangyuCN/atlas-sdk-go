package client

import (
	"context"
	"fmt"
	"net"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// channelTransport 是连接本体的传输抽象：帧级读写与关闭。
// TCP 为流式分帧（粘包由帧头 bodyLen 切分，聚集写保证帧级原子）；
// WebSocket 为消息边界（一条消息 = 一个完整帧）。线格式完全一致。
type channelTransport interface {
	ReadFrame(maxBodySize int) (frame.Header, []byte, error)
	WriteFrame(h frame.Header, body []byte, maxBodySize int) error
	Close() error
}

// dialTransport 按配置建立传输连接（首连与重连共用同一函数，保证语义一致）。
func dialTransport(ctx context.Context, tr Transport, addr, path string, maxBodySize int) (channelTransport, error) {
	switch tr {
	case TransportTCP:
		return dialTCP(ctx, addr)
	case TransportWS:
		return dialWS(ctx, addr, path, maxBodySize)
	case TransportKCP:
		return dialKCP(ctx, addr)
	case TransportUDP:
		return dialUDP(ctx, addr)
	default:
		return nil, fmt.Errorf("client: 不支持的传输类型 %d", int(tr))
	}
}

// dialTCP 建立 TCP 传输。
func dialTCP(ctx context.Context, addr string) (channelTransport, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return tcpTransport{conn: conn}, nil
}

// tcpTransport 基于 net.Conn 的流式传输：frame.Read/Write 已处理粘包切分、
// 校验先于内存分配与 writev 聚集写（并发整帧不交错由内核写锁保证）。
type tcpTransport struct{ conn net.Conn }

// ReadFrame 从连接读取一帧（粘包切分、校验与长度上限由 frame.Read 处理）。
func (t tcpTransport) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	return frame.Read(t.conn, maxBodySize)
}

// WriteFrame 向连接写入一帧（聚集写保证并发下帧级原子）。
func (t tcpTransport) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	return frame.Write(t.conn, h, body, maxBodySize)
}

// Close 关闭底层连接。
func (t tcpTransport) Close() error { return t.conn.Close() }
