package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// echoService 是 TCP/WS 测试服务端共用的请求处理逻辑（operation 语义两侧行一致）：
// Heartbeat 记录并回空包；boom 回业务错误 Status；notify-op 先推一条 Notify 再回空包；
// kick 回空包后断开（模拟服务端踢线）；hold 等待放行信号后回显（in-flight 隔离测试用）；
// 其余 operation 回显 payload。
type echoService struct {
	pingSeen chan struct{} // 收到心跳时写入（容量 1）
	reqN     atomic.Int32  // 已处理请求数
	holdSeen chan struct{} // hold 请求到达信号（容量 1；nil = 未启用）
	release  chan struct{} // hold 放行信号（nil = 未启用）

	hbBusinessErr atomic.Bool // 心跳回业务拒绝（验证「业务拒绝不计死链」用）
}

// handleRequest 处理一帧请求：返回回包 body、需先推的 Notify body（可空）、是否随后断连。
func (e *echoService) handleRequest(op string, payload []byte) (resp, notifyBody []byte, closeConn bool) {
	e.reqN.Add(1)
	switch op {
	case HeartbeatOperation:
		select {
		case e.pingSeen <- struct{}{}:
		default:
		}
		if e.hbBusinessErr.Load() {
			// 模拟网关 UDP 通道未注册内置 Ping：往返完成但业务拒绝。
			return replyBytes(testStatus(500, "", "engine: not registered"), nil), nil, false
		}
		return replyBytes(nil, nil), nil, false
	case "boom":
		return replyBytes(testStatus(400, "NOT_ENOUGH_GOLD", "金币不足"), nil), nil, false
	case "notify-op":
		nb, _ := frame.BuildRequestBody("/push.test", []byte(`{"k":"v"}`))
		return replyBytes(nil, nil), nb, false
	case "kick":
		return replyBytes(nil, nil), nil, true
	case "hold":
		if e.holdSeen != nil {
			e.holdSeen <- struct{}{}
		}
		if e.release != nil {
			<-e.release
		}
		return replyBytes(nil, payload), nil, false
	default: // echo
		return replyBytes(nil, payload), nil, false
	}
}

// requestCount 返回已处理请求数（断言「请求只到达指定通道」用）。
func (e *echoService) requestCount() int32 { return e.reqN.Load() }

// fakeServer 是最小 TCP 服务端替身：按 operation 回响应，可主动推 Notify。
type fakeServer struct {
	ln net.Listener
	echoService
	connClosed atomic.Int32 // 服务端侧连接被对端关闭的次数（回滚测试用）
	connN      atomic.Int32 // 接受的连接总数（重连观测用）
}

func startFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	s := &fakeServer{ln: ln, echoService: echoService{
		pingSeen: make(chan struct{}, 1),
		holdSeen: make(chan struct{}, 1),
		release:  make(chan struct{}),
	}}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeServer) handle(conn net.Conn) {
	s.connN.Add(1)
	defer func() {
		_ = conn.Close()
		s.connClosed.Add(1)
	}()
	for {
		hdr, body, err := frame.Read(conn, frame.MaxBodySize)
		if err != nil {
			return
		}
		if hdr.Type != frame.MsgTypeRequest {
			continue
		}
		op, payload, err := frame.ParseRequestBody(body)
		if err != nil {
			return
		}
		resp, notifyBody, closeConn := s.handleRequest(op, payload)
		if notifyBody != nil {
			_ = s.push(conn, notifyBody)
		}
		s.reply(conn, hdr.Seq, resp)
		if closeConn {
			return
		}
	}
}

// closedCount 返回服务端侧观测到的连接关闭次数。
func (s *fakeServer) closedCount() int32 { return s.connClosed.Load() }

func (s *fakeServer) reply(conn net.Conn, seq uint32, body []byte) {
	_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: seq}, body, frame.MaxBodySize)
}

// push 主动向客户端推送一条 Notify 帧（body 为完整帧 body）。
func (s *fakeServer) push(conn net.Conn, body []byte) error {
	return frame.Write(conn, frame.Header{Type: frame.MsgTypeNotify, Seq: 999}, body, frame.MaxBodySize)
}

func replyBytes(status []byte, data []byte) []byte {
	if status == nil {
		out := make([]byte, 5+len(data))
		out[0] = 0
		putU32(out[1:5], uint32(len(data)))
		copy(out[5:], data)
		return out
	}
	out := make([]byte, 1+4+len(status)+4+len(data))
	out[0] = 1
	putU32(out[1:5], uint32(len(status)))
	copy(out[5:], status)
	off := 5 + len(status)
	putU32(out[off:off+4], uint32(len(data)))
	copy(out[off+4:], data)
	return out
}

func putU32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

// testStatus 手写编码 Status protobuf（字段号 1/2/3；metadata 未用到则省略）。
func testStatus(code int32, reason, message string) []byte {
	var out []byte
	appendVarint := func(fieldNum int, v uint64) {
		out = append(out, byte(fieldNum<<3|0))
		for v >= 0x80 {
			out = append(out, byte(v)|0x80)
			v >>= 7
		}
		out = append(out, byte(v))
	}
	appendBytes := func(fieldNum int, b []byte) {
		out = append(out, byte(fieldNum<<3|2))
		l := uint64(len(b))
		for l >= 0x80 {
			out = append(out, byte(l)|0x80)
			l >>= 7
		}
		out = append(out, byte(l))
		out = append(out, b...)
	}
	appendVarint(1, uint64(code))
	appendBytes(2, []byte(reason))
	appendBytes(3, []byte(message))
	return out
}

// TestInvokeEcho 验证请求-响应闭环与 payload 回显。
func TestInvokeEcho(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
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

// TestInvokeBusinessError 验证业务拒绝还原为 *BusinessError（按 Reason 分支，规范 §7）。
func TestInvokeBusinessError(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "boom", nil, nil)
	var be *BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("期望 *BusinessError, 实际 %T: %v", err, err)
	}
	if be.Reason != "NOT_ENOUGH_GOLD" || be.Code != 400 || be.Message != "金币不足" {
		t.Fatalf("BusinessError 还原不一致: %+v", be)
	}
	// 规范 §7.1：Reason 为主键的统一判断辅助。
	if !IsBusinessError(err, "NOT_ENOUGH_GOLD") {
		t.Error("IsBusinessError(NOT_ENOUGH_GOLD) 应为 true")
	}
	if IsBusinessError(err, "OTHER") {
		t.Error("IsBusinessError(OTHER) 应为 false")
	}
}

// TestInvokeTimeout 验证无响应请求按超时失败（TimeoutError）。
func TestInvokeTimeout(t *testing.T) {
	ln := startSilentServer(t)

	c, err := Dial(ln.Addr().String(), WithInvokeTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "never-reply", nil, nil)
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("期望 TimeoutError, 实际 %T: %v", err, err)
	}
}

// TestNotifyDispatch 验证 Notify 按 operation 分发（含 payload 透传）与退订。
func TestNotifyDispatch(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	got := make(chan string, 1)
	off := c.On("/push.test", func(_ string, payload []byte) {
		var m struct {
			K string `json:"k"`
		}
		_ = json.Unmarshal(payload, &m)
		got <- m.K
	})
	defer off()

	if err := c.Invoke(context.Background(), "notify-op", nil, nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	select {
	case k := <-got:
		if k != "v" {
			t.Fatalf("Notify payload 不一致: %q", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待 Notify 超时")
	}
}

// TestHeartbeatKeepAlive 验证心跳按周期发送。
func TestHeartbeatKeepAlive(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr(), WithHeartbeatInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case <-s.pingSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("心跳未按周期发送")
	}
}

// TestCloseCancelsInflight 验证 Close 后 in-flight 请求收到 NetworkError。
func TestCloseCancelsInflight(t *testing.T) {
	ln := startSilentServer(t)

	c, err := Dial(ln.Addr().String(), WithInvokeTimeout(time.Second))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Invoke(context.Background(), "x", nil, nil) }()
	time.Sleep(50 * time.Millisecond)
	_ = c.Close()
	select {
	case err := <-errCh:
		var ne *NetworkError
		if !errors.As(err, &ne) {
			t.Fatalf("期望 NetworkError, 实际 %T: %v", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close 未取消 in-flight")
	}
}

// TestOnIdempotent 验证同一 (op, handler) 重复注册幂等（规范 §5.1）。
func TestOnIdempotent(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	var mu sync.Mutex
	count := 0
	handler := func(op string, payload []byte) {
		mu.Lock()
		count++
		mu.Unlock()
	}
	off1 := c.On("/push.test", handler)
	off2 := c.On("/push.test", handler) // 同一函数值重复注册

	off2() // 一次退订即移除唯一条目
	off1() // 重复退订安全

	// 重新注册并触发一次，确认状态干净
	c.On("/push.test", handler)
	_ = c.Invoke(context.Background(), "notify-op", nil, nil)
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	got := count
	mu.Unlock()
	if got != 1 {
		t.Fatalf("幂等去重失效: handler 执行 %d 次, 期望 1", got)
	}
}

// TestProtocolErrorOnBadFrame 验证服务端发非法帧时连接终止且错误为 ProtocolError（规范 §7）。
func TestProtocolErrorOnBadFrame(t *testing.T) {
	// 恶意服务端：回一个非法 version 的 Response 帧
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		hdr, body, err := frame.Read(conn, frame.MaxBodySize)
		if err != nil {
			return
		}
		hdr.Type = frame.MsgTypeResponse
		hdr.Version = 99 // 非法版本
		_ = frame.Write(conn, hdr, body, frame.MaxBodySize)
	}()

	c, err := Dial(ln.Addr().String(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Invoke(context.Background(), "echo", map[string]string{"msg": "x"}, nil)
	if err == nil {
		t.Fatal("非法帧后 Invoke 应失败")
	}
	// 连接应标记为已关闭（读循环终止后 shutdown）
	if !c.business.closed.Load() {
		t.Error("协议错误后连接应已关闭")
	}
	// 读循环对协议错误的分类：classifyReadError 直接验证（invoke 侧可能被
	// closeCh 分支抢先返回 NetworkError，属正常竞态——两者都是合法结果）
	classified := c.business.classifyReadError(fmt.Errorf("frame: invalid version: %w", frame.ErrProtocol))
	var pe *ProtocolError
	if !errors.As(classified, &pe) {
		t.Fatalf("classifyReadError 对 ErrProtocol 应返回 ProtocolError, 实际 %T: %v", classified, classified)
	}
}

// TestSeqZeroSkipped 验证 seq 回绕守卫（nextSeq 跳过 0）。
func TestSeqZeroSkipped(t *testing.T) {
	s := startFakeServer(t)
	c, err := Dial(s.addr(), WithHeartbeatInterval(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 直接把 seq 推到回绕边界，验证下一个 seq 不为 0
	c.business.seq.Store(0xFFFFFFFF)
	seq := c.business.nextSeq()
	if seq == 0 {
		t.Fatal("回绕守卫失效: nextSeq 返回 0")
	}
	if seq != 1 {
		t.Fatalf("回绕后 seq = %d, 期望 1", seq)
	}
}

// TestHeartbeatBusinessErrorNotDeadLink 验证心跳业务拒绝（往返完成）不计入死链：
// 服务端持续对 Ping 回业务拒绝，通道不触发重连、连接保持复用（规范 §5.2 语义修正）。
func TestHeartbeatBusinessErrorNotDeadLink(t *testing.T) {
	s := startFakeServer(t)
	s.hbBusinessErr.Store(true) // 心跳一律业务拒绝（模拟网关 UDP 通道未注册 Ping）

	c, err := Dial(s.addr(),
		WithHeartbeatInterval(20*time.Millisecond),
		WithInvokeTimeout(100*time.Millisecond),
		WithBackoff(20*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 远超 3 次心跳失败窗口：若业务拒绝被误计死链，此处已发生重连（新客户端端口）。
	time.Sleep(500 * time.Millisecond)

	if got := c.State(); got != StateConnected {
		t.Fatalf("状态 = %s, 期望 connected（业务拒绝不应触发重连）", got)
	}
	if got := s.connN.Load(); got != 1 {
		t.Fatalf("服务端观测连接数 = %d, 期望 1（不应重拨）", got)
	}
}
