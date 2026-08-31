package client

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// udpTestServer 是 UDP 测试服务端：一报一帧（数据报 = 完整帧），
// 64KiB 读缓冲（含 16B 头），operation 语义经 echoService 与 TCP 替身保持一致；
// 解码失败静默丢包（对齐服务端 ErrBadFrame 软跳过语义）。
type udpTestServer struct {
	conn *net.UDPConn
	echo echoService

	mu       sync.Mutex
	clientID map[string]struct{} // 出现过的客户端地址（重连观测用）

	silentUntil atomic.Int64 // 静默截止时间戳（UnixMilli）：期间不回包模拟服务端无响应
}

func startUDPServer(t *testing.T) *udpTestServer {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("解析地址失败: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("UDP 监听失败: %v", err)
	}
	s := &udpTestServer{
		conn:     conn,
		echo:     echoService{pingSeen: make(chan struct{}, 1)},
		clientID: make(map[string]struct{}),
	}
	go s.serve()
	t.Cleanup(func() { _ = conn.Close() })
	return s
}

func (s *udpTestServer) addr() string { return s.conn.LocalAddr().String() }

func (s *udpTestServer) serve() {
	buf := make([]byte, 64*1024) // 与服务端读缓冲默认一致（含 16B 头）
	for {
		n, raddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if time.Now().UnixMilli() < s.silentUntil.Load() {
			continue // 静默窗口：模拟服务端无响应（kick 语义的 UDP 形态）
		}
		s.mu.Lock()
		s.clientID[raddr.String()] = struct{}{}
		s.mu.Unlock()

		hdr, body, err := frame.Decode(buf[:n], frame.MaxBodySize)
		if err != nil {
			continue // 坏数据报静默丢弃，不拆「连接」
		}
		op, payload, err := frame.ParseRequestBody(body)
		if err != nil {
			continue
		}
		if op == "garbage-echo" {
			s.sendGarbage(raddr) // 坏帧丢弃验证：先发垃圾数据报，再正常回包
		}
		resp, notifyBody, _ := s.echo.handleRequest(op, payload)
		if notifyBody != nil {
			s.sendTo(raddr, frame.Header{Type: frame.MsgTypeNotify, Seq: 999}, notifyBody)
		}
		s.sendTo(raddr, frame.Header{Type: frame.MsgTypeResponse, Seq: hdr.Seq}, resp)
	}
}

// sendGarbage 向客户端发送三种坏数据报：
// ① 空数据报（type=0 非法）；② 短于帧头；③ 帧头声明 bodyLen=100 但无 body（截断）。
// 客户端应全部静默丢弃并继续处理后续正常帧。
func (s *udpTestServer) sendGarbage(raddr *net.UDPAddr) {
	empty, _ := frame.Encode(frame.Header{}, nil, frame.MaxBodySize) // type=0：Check 拒绝
	_, _ = s.conn.WriteToUDP(empty, raddr)

	short := []byte{'A', 'T', 'L', 'S', 0x01, 0x03} // 6B：短于帧头
	_, _ = s.conn.WriteToUDP(short, raddr)

	trunc := make([]byte, 16) // 声明 bodyLen=100 但无 body：长度失步
	trunc[0], trunc[1], trunc[2], trunc[3] = 'A', 'T', 'L', 'S'
	trunc[4] = frame.Version
	trunc[5] = byte(frame.MsgTypeResponse)
	binary.BigEndian.PutUint32(trunc[12:16], 100)
	_, _ = s.conn.WriteToUDP(trunc, raddr)
}

// sendTo 编码整帧并发送给指定地址（一报一帧）。
func (s *udpTestServer) sendTo(raddr *net.UDPAddr, h frame.Header, body []byte) {
	msg, err := frame.Encode(h, body, frame.MaxBodySize)
	if err != nil {
		return
	}
	_, _ = s.conn.WriteToUDP(msg, raddr)
}

// clientCount 返回出现过的客户端地址数（redial 会产生新端口）。
func (s *udpTestServer) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clientID)
}

// TestUDPDialInvokeEcho 验证 UDP 通道请求-响应闭环（一报一帧）。
func TestUDPDialInvokeEcho(t *testing.T) {
	s := startUDPServer(t)
	c, err := DialUDP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "hi"}, &resp); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.Msg != "hi" {
		t.Fatalf("回显不一致: %+v", resp)
	}
}

// TestUDPBusinessError 验证 UDP 通道业务拒绝还原为 *BusinessError。
func TestUDPBusinessError(t *testing.T) {
	s := startUDPServer(t)
	c, err := DialUDP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "boom", nil, nil)
	var be *BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("期望 *BusinessError, 实际 %T: %v", err, err)
	}
	if be.Reason != "NOT_ENOUGH_GOLD" || be.Code != 400 {
		t.Fatalf("BusinessError 还原不一致: %+v", be)
	}
}

// TestUDPHeartbeatKeepAlive 验证 UDP 通道传输保活心跳按周期发送。
func TestUDPHeartbeatKeepAlive(t *testing.T) {
	s := startUDPServer(t)
	c, err := DialUDP(s.addr(), WithHeartbeatInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case <-s.echo.pingSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("心跳未按周期发送")
	}
}

// TestUDPNotifyDispatch 验证 UDP 通道 Notify 按 operation 分发。
func TestUDPNotifyDispatch(t *testing.T) {
	s := startUDPServer(t)
	c, err := DialUDP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	got := make(chan string, 1)
	off := c.On("/push.test", func(_ string, payload []byte) { got <- string(payload) })
	defer off()

	if err := c.Invoke(context.Background(), "notify-op", nil, nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	select {
	case p := <-got:
		if p != `{"k":"v"}` {
			t.Fatalf("Notify payload 不一致: %q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("等待 Notify 超时")
	}
}

// TestUDPGarbageDatagramSkipped 验证坏数据报静默丢弃：
// 服务端对 garbage-echo 先发三种坏数据报再正常回包，客户端应继续工作。
func TestUDPGarbageDatagramSkipped(t *testing.T) {
	s := startUDPServer(t)
	c, err := DialUDP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "garbage-echo", map[string]string{"msg": "hi"}, &resp); err != nil {
		t.Fatalf("垃圾帧后 Invoke 应成功: %v", err)
	}
	if resp.Msg != "hi" {
		t.Fatalf("回显不一致: %+v", resp)
	}
}

// TestUDPKickReconnect 验证 UDP 死链检测与重拨：
// 服务端静默 → 传输心跳连续失败判定死链 → 关闭重拨（新客户端地址）→ 恢复。
// （UDP 无连接：服务端失联不会让客户端读侧报错，死链只能由心跳发现——规范 §5.2。）
func TestUDPKickReconnect(t *testing.T) {
	s := startUDPServer(t)
	c, err := DialUDP(s.addr(),
		WithHeartbeatInterval(20*time.Millisecond),
		WithInvokeTimeout(30*time.Millisecond),
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 首个心跳到达：链路可用。
	select {
	case <-s.echo.pingSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("首个心跳未到达")
	}

	// 服务端静默 500ms：心跳连续 3 次失败 → 死链 → 重拨。
	s.silentUntil.Store(time.Now().Add(500 * time.Millisecond).UnixMilli())
	time.Sleep(700 * time.Millisecond)

	waitConnected(t, c)
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "r2"}, &resp); err != nil {
		t.Fatalf("重拨后 Invoke: %v", err)
	}
	if resp.Msg != "r2" {
		t.Fatalf("重拨后回显不一致: %+v", resp)
	}
	if got := s.clientCount(); got < 2 {
		t.Fatalf("客户端地址数 = %d, 期望 ≥ 2（发生过重拨）", got)
	}
}

// TestUDPOversizeWriteRejected 验证 UDP 单帧 64KiB 上限（含 16B 头）：
// 超限的请求在写侧即被拒绝，连接不受影响（后续小请求正常）。
func TestUDPOversizeWriteRejected(t *testing.T) {
	s := startUDPServer(t)
	c, err := DialUDP(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	big := make([]byte, 70*1024) // 70KiB payload → 帧 > 64KiB
	err = c.Invoke(context.Background(), "echo", map[string]string{"k": string(big)}, nil)
	if err == nil {
		t.Fatal("超 64KiB 帧的 UDP 写应被拒绝")
	}
	var ne *NetworkError
	if !errors.As(err, &ne) {
		t.Fatalf("期望 NetworkError, 实际 %T: %v", err, err)
	}

	// 连接不受影响：小请求正常。
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "echo", map[string]string{"msg": "ok"}, &resp); err != nil {
		t.Fatalf("超限写后小请求应正常: %v", err)
	}
}

// TestUDPGarbageThenAlive 验证评审缺口：坏数据报（含超长截断声明）后连接保持可用——
// 服务端 sendGarbage 发三类坏报（空报/短帧/截断声明），客户端全部静默丢弃且不断连，
// 后续正常请求往返成功（规范 §2：UDP 坏帧静默丢弃语义 + 连接存活）。
func TestUDPGarbageThenAlive(t *testing.T) {
	srv := startUDPServer(t)
	c, err := DialUDP(srv.addr())
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	// garbage-echo：服务端先发三类坏数据报，再正常回包——一次往返覆盖全场景
	var resp struct {
		Msg string `json:"msg"`
	}
	if err := c.Invoke(context.Background(), "garbage-echo", map[string]string{"msg": "ok"}, &resp); err != nil {
		t.Fatalf("坏帧后往返失败（连接应可用）: %v", err)
	}
	if resp.Msg != "ok" {
		t.Fatalf("回显不一致: %+v", resp)
	}

	// 再来一次确认链路稳定
	if err := c.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("第二次往返失败: %v", err)
	}
}

// TestUDPEpochIsolation 验证评审缺口：UDP 重拨后 (epoch, seq) 隔离——
// 静默期（服务端无响应）耗尽旧代 in-flight 后重拨，新代请求正常匹配，无旧代串扰。
func TestUDPEpochIsolation(t *testing.T) {
	srv := startUDPServer(t)
	c, err := DialUDP(srv.addr(),
		WithHeartbeatInterval(0),
		WithInvokeTimeout(200*time.Millisecond), // 短超时：静默期快速失败
	)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("首请求: %v", err)
	}

	// 服务端静默 500ms：期间请求全部超时（旧代 in-flight 耗尽）
	srv.silentUntil.Store(time.Now().Add(500 * time.Millisecond).UnixMilli())
	for i := 0; i < 3; i++ {
		_ = c.Invoke(context.Background(), "echo", nil, nil) // 全部超时
	}
	time.Sleep(600 * time.Millisecond) // 静默期结束

	// 新代请求正常往返（epoch 已换代，旧 seq 不可能匹配）
	if err := c.Invoke(context.Background(), "echo", nil, nil); err != nil {
		t.Fatalf("静默期结束后请求失败（epoch 隔离不应影响新请求）: %v", err)
	}
}
