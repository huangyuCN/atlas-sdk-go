package direct

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/huangyuCN/atlas-sdk-go/frame"
	kcpgo "github.com/xtaci/kcp-go/v5"
)

// stubFlowID 是测试桩固定的 flow-id（8 字节大端；真实接入层为随机值）。
const stubFlowID uint64 = 0x0102030405060708

// lastSeenFromJSON 从 SyncFramesReq 的 protojson 载荷取 lastSeenFrame
// （protojson 把 uint64 编成字符串，桩侧两种形态都要认）。
func lastSeenFromJSON(payload []byte) uint64 {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return 0
	}
	switch v := m["lastSeenFrame"].(type) {
	case float64:
		return uint64(v)
	case string:
		n, _ := strconv.ParseUint(v, 10, 64)
		return n
	default:
		return 0
	}
}

// wsServerConn 把 gorilla 连接适配成服务端帧连接（一条 WS 消息 = 一帧）。
type wsServerConn struct{ conn *websocket.Conn }

// ReadFrame 读一条 WS 消息并解码为帧。
func (c wsServerConn) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	_, msg, err := c.conn.ReadMessage()
	if err != nil {
		return frame.Header{}, nil, err
	}
	return frame.DecodeMessage(msg, maxBodySize)
}

// WriteFrame 编码整帧并以单条二进制消息写出。
func (c wsServerConn) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	buf, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	return c.conn.WriteMessage(websocket.BinaryMessage, buf)
}

// Close 关闭 WS 连接。
func (c wsServerConn) Close() error { return c.conn.Close() }

// wsStub 是最小 WS 面服务端：升级请求的 query 票必须与期望一致（否则不写任何回执直接断开，
// 对齐接入层 L4 最小拒绝语义），升级后跑 battle 桩。
type wsStub struct {
	ticket []byte
	stub   *battleStub
	reject atomic.Bool // 置位后一律不回执直接断开（模拟票被拒/接入层拒绝）
	conns  atomic.Int32
	addr   string
}

// startWSStub 起最小 WS 面服务端，返回桩（桩里带监听地址）。
func startWSStub(t *testing.T, ticket []byte, stub *battleStub) *wsStub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("WS 监听失败: %v", err)
	}
	s := &wsStub{ticket: ticket, stub: stub, addr: ln.Addr().String()}
	srv := &http.Server{Handler: http.HandlerFunc(s.handle)}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return s
}

// handle 处理一次 WS 升级：验 query 票 → 升级 → 跑 battle 桩。
func (s *wsStub) handle(w http.ResponseWriter, r *http.Request) {
	if got := r.URL.Query().Get("ticket"); got != base64.RawURLEncoding.EncodeToString(s.ticket) {
		s.stub.problem("WS query ticket = %q, 期望 %q", got, base64.RawURLEncoding.EncodeToString(s.ticket))
		dropConn(w)
		return
	}
	if s.reject.Load() {
		dropConn(w)
		return
	}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.conns.Add(1)
	s.stub.serve(wsServerConn{conn: conn})
	_ = conn.Close()
}

// dropConn 不回执直接断开连接（接入层拒绝语义）。
func dropConn(w http.ResponseWriter) {
	if hj, ok := w.(http.Hijacker); ok {
		if conn, _, err := hj.Hijack(); err == nil {
			_ = conn.Close()
		}
	}
}

// udpServerConn 把数据报流适配成服务端帧连接：读侧剥 flow-id 前缀，写侧加前缀。
type udpServerConn struct {
	pc     net.PacketConn
	peer   net.Addr
	flowID uint64
	in     chan []byte
}

// ReadFrame 取下一条已被剥掉前缀的数据报并解码为帧。
func (c *udpServerConn) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	msg, ok := <-c.in
	if !ok {
		return frame.Header{}, nil, io.EOF
	}
	return frame.DecodeMessage(msg, maxBodySize)
}

// WriteFrame 编码整帧并加 flow-id 前缀后发回客户端。
func (c *udpServerConn) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	buf, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	_, err = c.pc.WriteTo(literalWrap(c.flowID, buf), c.peer)
	return err
}

// Close 关闭流（数据报面无连接，仅关闭收包通道让桩退出）。
func (c *udpServerConn) Close() error { return nil }

// udpStub 是最小数据报面服务端：首包 hello 逐字节校验 → 回 flow-id → 后续包按前缀转发。
type udpStub struct {
	ticket      []byte
	stub        *battleStub
	ignoreHello atomic.Bool // 置位后对 hello 静默丢弃（模拟拒绝/无回执）
	mu          sync.Mutex
	helloSeen   []byte
	conn        *udpServerConn
}

// startUDPStub 起最小数据报面服务端，返回监听地址。
func startUDPStub(t *testing.T, ticket []byte, stub *battleStub) (string, *udpStub) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("UDP 监听失败: %v", err)
	}
	s := &udpStub{ticket: ticket, stub: stub}
	go s.serve(pc)
	t.Cleanup(func() { _ = pc.Close() })
	return pc.LocalAddr().String(), s
}

// serve 读数据报：首包当 hello 处理，其余按 flow-id 前缀分流给桩。
func (s *udpStub) serve(pc net.PacketConn) {
	buf := make([]byte, 64*1024)
	for {
		n, peer, err := pc.ReadFrom(buf)
		if err != nil {
			s.closeIn()
			return
		}
		data := append([]byte(nil), buf[:n]...)
		if s.currentConn() == nil {
			s.handleHello(pc, peer, data)
			continue
		}
		s.forward(data)
	}
}

// handleHello 校验首包 hello（逐字节）并回 flow-id；被拒时静默丢弃（无回执）。
func (s *udpStub) handleHello(pc net.PacketConn, peer net.Addr, data []byte) {
	s.mu.Lock()
	s.helloSeen = append([]byte(nil), data...)
	s.mu.Unlock()
	if want := literalHello(s.ticket); !bytes.Equal(data, want) {
		s.stub.problem("UDP hello = % x, 期望（手写字面量）% x", data, want)
		return
	}
	if s.ignoreHello.Load() {
		return
	}
	if _, err := pc.WriteTo(literalFlowID(stubFlowID), peer); err != nil {
		return
	}
	s.mu.Lock()
	s.conn = &udpServerConn{pc: pc, peer: peer, flowID: stubFlowID, in: make(chan []byte, 64)}
	conn := s.conn
	s.mu.Unlock()
	go s.stub.serve(conn)
}

// currentConn 返回当前数据报流（未建流为 nil）。
func (s *udpStub) currentConn() *udpServerConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// forward 剥前缀后把载荷交给桩（前缀失配的包直接丢弃，要求重新 hello）。
func (s *udpStub) forward(data []byte) {
	payload, ok := literalStrip(data, stubFlowID)
	if !ok {
		s.stub.problem("UDP 包缺 flow-id 前缀或前缀失配（%d 字节）", len(data))
		return
	}
	conn := s.currentConn()
	if conn == nil {
		return
	}
	select {
	case conn.in <- payload:
	default:
		s.stub.problem("UDP 桩收包队列满，丢弃")
	}
}

// closeIn 关闭桩的收包通道（监听器关闭时）。
func (s *udpStub) closeIn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		close(s.conn.in)
		s.conn = nil
	}
}

// helloBytes 返回桩收到的首包字节（逐字节断言用）。
func (s *udpStub) helloBytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.helloSeen...)
}

// stubFlowPacketConn 是 KCP 桩侧的 PacketConn：读侧剥 flow-id 前缀、写侧加前缀（模拟接入层数据报面）。
// badPrefix 在收到不带本流前缀的数据报时回调——KCP 面要求**每个报文**都带前缀，缺前缀即违例。
type stubFlowPacketConn struct {
	net.PacketConn
	flowID    uint64
	badPrefix func(n int)
}

// ReadFrom 读一个数据报并剥掉本流前缀（非本流前缀的包原样返回，交给 KCP 丢弃）。
func (c stubFlowPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	buf := make([]byte, max(len(p), 2048))
	n, addr, err := c.PacketConn.ReadFrom(buf)
	if err != nil {
		return 0, addr, err
	}
	data := buf[:n]
	if payload, ok := literalStrip(data, c.flowID); ok {
		data = payload
	} else if c.badPrefix != nil {
		c.badPrefix(n)
	}
	return copy(p, data), addr, nil
}

// WriteTo 给回程数据报加 flow-id 前缀。
func (c stubFlowPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	framed := literalWrap(c.flowID, p)
	if _, err := c.PacketConn.WriteTo(framed, addr); err != nil {
		return 0, err
	}
	return len(p), nil
}

// kcpStub 是最小 KCP 面服务端：首包 hello（逐字节）→ 回 flow-id → 同一 socket 上跑 KCP 会话。
type kcpStub struct {
	ticket    []byte
	stub      *battleStub
	helloSeen []byte
	mu        sync.Mutex
}

// startKCPStub 起最小 KCP 面服务端，返回监听地址。
func startKCPStub(t *testing.T, ticket []byte, stub *battleStub) (string, *kcpStub) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("KCP 监听失败: %v", err)
	}
	s := &kcpStub{ticket: ticket, stub: stub}
	go s.serve(pc)
	t.Cleanup(func() { _ = pc.Close() })
	return pc.LocalAddr().String(), s
}

// serve 先消费 hello 段（逐字节校验 + 回 flow-id），再在加前缀的 PacketConn 上跑 KCP 监听。
func (s *kcpStub) serve(pc net.PacketConn) {
	buf := make([]byte, 64*1024)
	n, peer, err := pc.ReadFrom(buf)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.helloSeen = append([]byte(nil), buf[:n]...)
	s.mu.Unlock()
	if want := literalHello(s.ticket); !bytes.Equal(buf[:n], want) {
		s.stub.problem("KCP hello = % x, 期望（手写字面量）% x", buf[:n], want)
		return
	}
	if _, err := pc.WriteTo(literalFlowID(stubFlowID), peer); err != nil {
		return
	}
	ln, err := kcpgo.ServeConn(nil, 0, 0, stubFlowPacketConn{PacketConn: pc, flowID: stubFlowID})
	if err != nil {
		return
	}
	defer func() { _ = ln.Close() }()
	for {
		sess, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer func() { _ = sess.Close() }()
			s.stub.serve(kcpServerConn{sess})
		}()
	}
}

// helloBytes 返回桩收到的 hello 段字节。
func (s *kcpStub) helloBytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.helloSeen...)
}

// kcpServerConn 把 KCP 会话适配成服务端帧连接（流式分帧，与真实 battle 一致）。
type kcpServerConn struct{ conn net.Conn }

// ReadFrame 从 KCP 会话读取一帧（流式：粘包由帧头切分）。
func (c kcpServerConn) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	return frame.Read(c.conn, maxBodySize)
}

// WriteFrame 向 KCP 会话写入一帧。
func (c kcpServerConn) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	return frame.Write(c.conn, h, body, maxBodySize)
}

// Close 关闭 KCP 会话。
func (c kcpServerConn) Close() error { return c.conn.Close() }
