package direct

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	"github.com/huangyuCN/atlas-sdk-go/internal/kcpcfg"
	kcpgo "github.com/xtaci/kcp-go/v5"
)

// conn 是一条直连通道的帧级读写接缝：三面线格式完全一致（16B 帧头 + body），
// 差别只在承载（WS 消息边界 / KCP 字节流 / UDP 一报一帧）与接入层握手段。
type conn interface {
	ReadFrame(maxBodySize int) (frame.Header, []byte, error)
	WriteFrame(h frame.Header, body []byte, maxBodySize int) error
	Close() error
}

// dial 按传输面建立直连通道（首连与重连共用同一函数，保证语义一致）。
func dial(ctx context.Context, kind Transport, addr string, ticket []byte, o options) (conn, error) {
	switch kind {
	case TransportWS:
		return dialWS(ctx, addr, ticket, o)
	case TransportKCP:
		return dialKCP(ctx, addr, ticket, o)
	case TransportUDP:
		return dialUDP(ctx, addr, ticket, o)
	default:
		return nil, fmt.Errorf("%w: %s", ErrTransportUnknown, kind)
	}
}

// dialWS 建立 WS 直连：升级 query 带 base64url 票（接入层据此验票），升级后即正常 WS 帧。
// 先自行拨 TCP，把「已连上但升级无回执（接入层拒绝）」与「网络不可达」区分开。
func dialWS(ctx context.Context, addr string, ticket []byte, o options) (conn, error) {
	target, err := wsTarget(addr, ticket, o)
	if err != nil {
		return nil, err
	}
	tcp, err := (&net.Dialer{Timeout: o.handshakeTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, client.NewNetworkError(fmt.Errorf("direct: 拨号 %s 失败: %w", addr, err))
	}
	d := &websocket.Dialer{
		HandshakeTimeout: o.handshakeTimeout,
		NetDial:          func(string, string) (net.Conn, error) { return tcp, nil },
	}
	conn, resp, err := d.DialContext(ctx, target, nil)
	if err != nil {
		_ = tcp.Close()
		return nil, classifyWSHandshake(resp, err)
	}
	conn.SetReadLimit(int64(frame.HeaderSize + frame.MaxBodySize))
	return &wsConn{conn: conn}, nil
}

// wsTarget 拼 WS 升级地址：经接入层时带 query 票；直连 battle 帧端口时不带。
func wsTarget(addr string, ticket []byte, o options) (string, error) {
	if !o.edgeHello {
		return wsURL(addr, o.path, "")
	}
	return EdgeWSURL(addr, ticket, o.path)
}

// classifyWSHandshake 归类 WS 握手失败：TCP 已连上却拿不到正常升级回执（含无任何回执、
// 403/404 等）按「被接入层拒绝」处理（不重试）；超时与取消按网络类（可重试）。
func classifyWSHandshake(resp *http.Response, err error) error {
	if isTimeout(err) || errors.Is(err, context.Canceled) {
		return client.NewNetworkError(fmt.Errorf("direct: WS 握手超时: %w", err))
	}
	if resp == nil {
		return fmt.Errorf("%w: WS 升级无回执: %v", ErrRejected, err)
	}
	return fmt.Errorf("%w: WS 升级被拒（HTTP %d）: %v", ErrRejected, resp.StatusCode, err)
}

// dialKCP 建立 KCP 直连：经接入层时先在**同一 UDP socket** 上发 hello 段并收 flow-id，
// 之后每个数据报双向带 flow-id 前缀；KCP 会话跑在该 socket 上（消息模式，与 battle 帧面同档）。
func dialKCP(ctx context.Context, addr string, ticket []byte, o options) (conn, error) {
	udpConn, flowID, err := dialDatagram(ctx, addr, ticket, o)
	if err != nil {
		return nil, err
	}
	sess, err := kcpgo.NewConn(addr, nil, 0, 0, flowPacketConn{PacketConn: datagramConn{conn: udpConn}, flowID: flowID})
	if err != nil {
		_ = udpConn.Close()
		return nil, client.NewNetworkError(fmt.Errorf("direct: 建立 KCP 会话失败: %w", err))
	}
	kcpcfg.Apply(sess)
	return &kcpConn{sess: sess}, nil
}

// dialUDP 建立裸 UDP 直连：经接入层时首包 hello 换 flow-id，之后一报一帧且双向带前缀。
func dialUDP(ctx context.Context, addr string, ticket []byte, o options) (conn, error) {
	udpConn, flowID, err := dialDatagram(ctx, addr, ticket, o)
	if err != nil {
		return nil, err
	}
	return &udpConnWrap{conn: udpConn, flowID: flowID, readBuf: make([]byte, 64*1024)}, nil
}

// dialDatagram 拨 UDP 并按配置完成接入层 hello 握手，返回 socket 与 flow-id（0 = 不带前缀）。
func dialDatagram(ctx context.Context, addr string, ticket []byte, o options) (*net.UDPConn, uint64, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, 0, client.NewNetworkError(fmt.Errorf("direct: 解析地址 %s 失败: %w", addr, err))
	}
	udpConn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, 0, client.NewNetworkError(fmt.Errorf("direct: 拨号 %s 失败: %w", addr, err))
	}
	if !o.edgeHello {
		return udpConn, 0, nil
	}
	flowID, err := edgeHello(udpConn, ticket, o.handshakeTimeout)
	if err != nil {
		_ = udpConn.Close()
		return nil, 0, err
	}
	return udpConn, flowID, nil
}

// edgeHello 在数据报面完成 hello 握手：发 hello 段 → 等 8 字节 flow-id。
// 无回执（超时）判为「被接入层拒绝」——接入层拒绝遵循 L4 最小语义（断开 + 指标，无应用层
// 回执），故「票无效/过期/后端不可用」与「静默丢包」同形；连接被拒（ICMP）判为网络类可重试。
func edgeHello(conn *net.UDPConn, ticket []byte, timeout time.Duration) (uint64, error) {
	if _, err := conn.Write(EncodeHello(ticket)); err != nil {
		return 0, client.NewNetworkError(fmt.Errorf("direct: 写 hello 段失败: %w", err))
	}
	buf := make([]byte, flowIDLen)
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	n, err := conn.Read(buf)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		if isTimeout(err) {
			return 0, fmt.Errorf("%w: hello 无回执（%s 内未收到 flow-id）", ErrRejected, timeout)
		}
		return 0, client.NewNetworkError(fmt.Errorf("direct: 读 flow-id 失败: %w", err))
	}
	if n != flowIDLen {
		return 0, fmt.Errorf("%w: flow-id 长度 %d, 期望 %d", ErrRejected, n, flowIDLen)
	}
	return binary.BigEndian.Uint64(buf), nil
}

// isTimeout 报告错误是否为超时类（net.Error 超时或 ctx 超期）。
func isTimeout(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// wsConn 是 gorilla 连接上的帧读写（一条 WS 消息 = 一个完整帧）。
type wsConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

// ReadFrame 读一条 WS 消息并解码为帧（消息边界口径：长度必须恰好相等）。
func (c *wsConn) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	_, msg, err := c.conn.ReadMessage()
	if err != nil {
		return frame.Header{}, nil, err
	}
	return frame.DecodeMessage(msg, maxBodySize)
}

// WriteFrame 编码整帧并以单条二进制消息发送。
func (c *wsConn) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	buf, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(websocket.BinaryMessage, buf)
}

// Close 关闭 WS 连接。
func (c *wsConn) Close() error { return c.conn.Close() }

// kcpConn 是 KCP 会话上的帧读写（消息模式，一次 Read 收回一个完整帧）。
type kcpConn struct {
	sess    *kcpgo.UDPSession
	writeMu sync.Mutex
}

// ReadFrame 从 KCP 会话读取一帧。
func (c *kcpConn) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	return frame.Read(c.sess, maxBodySize)
}

// WriteFrame 向 KCP 会话写入一帧（写超时兜底：死链时 Write 可能因窗口满永久阻塞）。
func (c *kcpConn) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.sess.SetWriteDeadline(time.Now().Add(kcpcfg.WriteTimeout))
	return frame.Write(c.sess, h, body, maxBodySize)
}

// Close 关闭 KCP 会话。
func (c *kcpConn) Close() error { return c.sess.Close() }

// udpConnWrap 是 UDP 数据报上的帧读写：一报一帧，经接入层时写侧加 flow-id 前缀、读侧剥前缀。
type udpConnWrap struct {
	conn    *net.UDPConn
	flowID  uint64
	readBuf []byte
	writeMu sync.Mutex
}

// ReadFrame 读一个数据报并解码为帧；前缀失配的包丢弃后继续读（要求重新 hello）。
func (c *udpConnWrap) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	for {
		n, err := c.conn.Read(c.readBuf)
		if err != nil {
			return frame.Header{}, nil, err
		}
		payload, err := c.strip(c.readBuf[:n])
		if err != nil {
			continue // 非本流的包：丢弃（接入层语义：失配即丢弃）
		}
		hdr, body, err := frame.DecodeMessage(payload, maxBodySize)
		if err != nil {
			return frame.Header{}, nil, err
		}
		// body 指向复用读缓冲，下一包会覆盖——必须拷出让所有权（推送会逃逸到 handler goroutine）。
		out := make([]byte, len(body))
		copy(out, body)
		return hdr, out, nil
	}
}

// WriteFrame 编码整帧并（按需加前缀后）以单个数据报发送。
func (c *udpConnWrap) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	buf, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	if len(buf)+flowIDLen > cap(c.readBuf) {
		return fmt.Errorf("direct: 数据报超限（%d 字节）", len(buf)+flowIDLen)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.conn.Write(c.wrap(buf))
	return err
}

// Close 关闭 UDP socket。
func (c *udpConnWrap) Close() error { return c.conn.Close() }

// wrap 按需给发出的数据报加 flow-id 前缀。
func (c *udpConnWrap) wrap(payload []byte) []byte {
	if c.flowID == 0 {
		return payload
	}
	return WrapFlowID(c.flowID, payload)
}

// strip 按需剥掉收到数据报的 flow-id 前缀（直接返回读缓冲切片，调用方随即解码）。
func (c *udpConnWrap) strip(data []byte) ([]byte, error) {
	if c.flowID == 0 {
		return data, nil
	}
	if len(data) < flowIDLen || binary.BigEndian.Uint64(data[:flowIDLen]) != c.flowID {
		return nil, fmt.Errorf("%w: 期望 flow-id %#x", ErrFlowIDMismatch, c.flowID)
	}
	return data[flowIDLen:], nil
}

// datagramConn 是**面向连接的 UDP socket** 的 PacketConn 适配：Go 的 *net.UDPConn 在已连接
// socket 上调用 WriteTo 会返回 ErrWriteToConnected，而 kcp-go 等按 PacketConn 使用（WriteTo
// 携带远端地址），直接内嵌会导致发出的包被静默丢弃。这里把 WriteTo 收敛为 Write（远端由连接
// 固定），ReadFrom 补回对端地址，其余能力透传。
type datagramConn struct{ conn *net.UDPConn }

// ReadFrom 从已连接 socket 读一个数据报并补回对端地址。
func (c datagramConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, err := c.conn.Read(p)
	return n, c.conn.RemoteAddr(), err
}

// WriteTo 向已连接的对端写一个数据报（忽略入参地址：远端由连接固定）。
func (c datagramConn) WriteTo(p []byte, _ net.Addr) (int, error) { return c.conn.Write(p) }

// Close 关闭底层 socket。
func (c datagramConn) Close() error { return c.conn.Close() }

// LocalAddr 返回本地地址。
func (c datagramConn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// SetDeadline 设置读写截止时间。
func (c datagramConn) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }

// SetReadDeadline 设置读截止时间。
func (c datagramConn) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

// SetWriteDeadline 设置写截止时间。
func (c datagramConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

// flowPacketConn 是 KCP 会话看到的 PacketConn：写侧加 flow-id 前缀、读侧剥前缀后交给 KCP
// （flowID=0 表示不经接入层，直通）。前缀失配的包直接丢弃，不视为会话故障；
// 本地地址与 deadline 等能力由内嵌的底层 PacketConn 提供。
type flowPacketConn struct {
	net.PacketConn
	flowID uint64
}

// ReadFrom 读一个数据报并剥掉本流前缀（失配/过短的包丢弃后继续读）。
func (c flowPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	buf := make([]byte, max(len(p), 2048))
	for {
		n, addr, err := c.PacketConn.ReadFrom(buf)
		if err != nil {
			return 0, addr, err
		}
		if c.flowID == 0 {
			return copy(p, buf[:n]), addr, nil
		}
		if payload, serr := StripFlowID(buf[:n], c.flowID); serr == nil {
			return copy(p, payload), addr, nil
		}
	}
}

// WriteTo 给回程数据报加 flow-id 前缀后写出。
func (c flowPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if c.flowID == 0 {
		return c.PacketConn.WriteTo(p, addr)
	}
	if _, err := c.PacketConn.WriteTo(WrapFlowID(c.flowID, p), addr); err != nil {
		return 0, err
	}
	return len(p), nil
}
