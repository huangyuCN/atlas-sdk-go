package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// fakeServer 是最小服务端替身：按 operation 回响应，可主动推 Notify。
type fakeServer struct {
	ln       net.Listener
	pingSeen chan struct{} // 收到心跳时写入（容量 1）
}

func startFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	s := &fakeServer{ln: ln, pingSeen: make(chan struct{}, 1)}
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

// handle 单连接循环：echo 回显 payload；Heartbeat 记录并回空包；
// boom 回业务错误 Status；notify-op 先推一条 Notify 再回空包；silent 只收不回。
func (s *fakeServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
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
		switch op {
		case HeartbeatOperation:
			select {
			case s.pingSeen <- struct{}{}:
			default:
			}
			s.reply(conn, hdr.Seq, replyBytes(nil, nil))
		case "boom":
			s.reply(conn, hdr.Seq, replyBytes(testStatus(400, "NOT_ENOUGH_GOLD", "金币不足"), nil))
		case "notify-op":
			_ = s.push(conn, "/push.test", []byte(`{"k":"v"}`))
			s.reply(conn, hdr.Seq, replyBytes(nil, nil))
		default: // echo
			s.reply(conn, hdr.Seq, replyBytes(nil, payload))
		}
	}
}

func (s *fakeServer) reply(conn net.Conn, seq uint32, body []byte) {
	_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: seq}, body, frame.MaxBodySize)
}

// push 主动向客户端推送一条 Notify 帧。
func (s *fakeServer) push(conn net.Conn, op string, payload []byte) error {
	body, err := frame.BuildRequestBody(op, payload)
	if err != nil {
		return err
	}
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
	// 只收不回的静默服务端。
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer func() { _ = silent.Close() }()
	go func() {
		for {
			c2, e := silent.Accept()
			if e != nil {
				return
			}
			_ = c2
		}
	}()

	c, err := Dial(silent.Addr().String(), WithInvokeTimeout(100*time.Millisecond))
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
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer func() { _ = silent.Close() }()
	go func() {
		for {
			c2, e := silent.Accept()
			if e != nil {
				return
			}
			_ = c2
		}
	}()

	c, err := Dial(silent.Addr().String(), WithInvokeTimeout(time.Second))
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
	if !c.closed.Load() {
		t.Error("协议错误后连接应已关闭")
	}
	// 读循环对协议错误的分类：classifyReadError 直接验证（invoke 侧可能被
	// closeCh 分支抢先返回 NetworkError，属正常竞态——两者都是合法结果）
	classified := c.classifyReadError(fmt.Errorf("frame: invalid version: %w", frame.ErrProtocol))
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
	c.seq.Store(0xFFFFFFFF)
	seq := c.nextSeq()
	if seq == 0 {
		t.Fatal("回绕守卫失效: nextSeq 返回 0")
	}
	if seq != 1 {
		t.Fatalf("回绕后 seq = %d, 期望 1", seq)
	}
}
