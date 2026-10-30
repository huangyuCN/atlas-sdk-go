// 假接入层（测试桩）：模拟 edge 数据报面的 hello 换 flow-id、重发 hello 接管（takeover）与
// 后端流不重拨。后端只跑一条 KCP 监听（同一 conv 会话跨切网存活），所有数据报对后端呈现
// 同一个稳定对端地址——这正是真实 edge「每流固定一条后端 socket」的语义（客户端换网不可见）。

package direct

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kcpgo "github.com/xtaci/kcp-go/v5"
)

// fakeEdgeBackendAddr 是假接入层对后端 KCP 监听呈现的稳定对端地址（不随客户端换 socket 变）。
var fakeEdgeBackendAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}

// fakeEdge 是最小接入层桩：首包 hello 建流并回 flow-id；再次 hello 走接管（换发新 flow-id、
// 回程改绑新对端），后端 KCP 监听与既有会话都不重建。
type fakeEdge struct {
	t    *testing.T
	pc   net.PacketConn
	mux  *edgeMux
	stub *battleStub

	ticket []byte
	mu     sync.Mutex
	flow   *edgeFlow
	next   uint64

	busy      atomic.Int32 // >0 时对 hello 回 1 字节 0x00（背压）
	silent    atomic.Bool  // 置位后丢弃一切数据报（模拟断网）
	hellos    atomic.Int32
	takeovers atomic.Int32
	dropped   atomic.Int32
}

// edgeFlow 是一条后端流的当前回程绑定（id 与对端在接管时整体替换）。
type edgeFlow struct {
	id   uint64
	peer net.Addr
}

// edgeDatagram 是交给后端 KCP 监听的一条已剥前缀数据报。
type edgeDatagram struct {
	data []byte
	from net.Addr
}

// startFakeEdge 起假接入层（UDP 监听 + 一条后端 KCP 监听），返回桩。
func startFakeEdge(t *testing.T, ticket []byte, stub *battleStub) *fakeEdge {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("假 edge 监听失败: %v", err)
	}
	e := &fakeEdge{t: t, pc: pc, stub: stub, ticket: ticket}
	e.mux = &edgeMux{e: e, in: make(chan edgeDatagram, 256), done: make(chan struct{})}
	go e.serve()
	go e.serveKCP()
	t.Cleanup(func() {
		_ = pc.Close()
		e.mux.close()
	})
	return e
}

// addr 返回假接入层地址。
func (e *fakeEdge) addr() string { return e.pc.LocalAddr().String() }

// dialUDP 拨一个连到假接入层的 UDP socket（测试注入用）。
func (e *fakeEdge) dialUDP() (*net.UDPConn, error) {
	raddr, err := net.ResolveUDPAddr("udp", e.addr())
	if err != nil {
		return nil, err
	}
	return net.DialUDP("udp", nil, raddr)
}

// serve 读数据报：hello 段走握手/接管，其余按 flow-id + 对端双键分发。
func (e *fakeEdge) serve() {
	buf := make([]byte, 64*1024)
	for {
		n, peer, err := e.pc.ReadFrom(buf)
		if err != nil {
			return
		}
		data := append([]byte(nil), buf[:n]...)
		if e.silent.Load() {
			e.dropped.Add(1)
			continue
		}
		if bytes.HasPrefix(data, HelloMagic[:]) {
			e.handleHello(peer, data)
			continue
		}
		e.handleData(peer, data)
	}
}

// handleHello 处理 hello 段：建流或接管，回 8 字节 flow-id（名额满回 1 字节 0x00）。
func (e *fakeEdge) handleHello(peer net.Addr, data []byte) {
	e.hellos.Add(1)
	if !bytes.Equal(data, literalHello(e.ticket)) {
		e.stub.problem("假 edge 收到非法 hello: % x", data)
		return
	}
	if e.busy.Load() > 0 {
		e.busy.Add(-1)
		_, _ = e.pc.WriteTo([]byte{0x00}, peer)
		return
	}
	e.mu.Lock()
	if e.flow == nil {
		e.flow = &edgeFlow{}
	} else {
		e.takeovers.Add(1)
	}
	e.next++
	e.flow.id, e.flow.peer = e.next, peer
	id := e.flow.id
	e.mu.Unlock()
	_, _ = e.pc.WriteTo(literalFlowID(id), peer)
}

// handleData 按「flow-id + 对端」双键查流：任一不符即丢弃（真实 edge 的换网窗口语义）。
func (e *fakeEdge) handleData(peer net.Addr, data []byte) {
	if len(data) < flowIDLen {
		e.dropped.Add(1)
		return
	}
	id := binary.BigEndian.Uint64(data[:flowIDLen])
	e.mu.Lock()
	ok := e.flow != nil && e.flow.id == id && e.flow.peer.String() == peer.String()
	e.mu.Unlock()
	if !ok {
		e.dropped.Add(1)
		return
	}
	e.mux.push(edgeDatagram{data: data[flowIDLen:], from: fakeEdgeBackendAddr})
}

// serveKCP 在假接入层内部跑唯一一条 KCP 监听（后端流：切网不重建）。
func (e *fakeEdge) serveKCP() {
	ln, err := kcpgo.ServeConn(nil, 0, 0, e.mux)
	if err != nil {
		e.t.Errorf("假 edge 起 KCP 监听失败: %v", err)
		return
	}
	for {
		sess, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer func() { _ = sess.Close() }()
			e.stub.serve(kcpServerConn{sess})
		}()
	}
}

// edgeMux 是假接入层后端侧的 PacketConn：读侧给 KCP 监听喂已剥前缀的数据报（远端恒为稳定
// 地址）；写侧按当前回程绑定加 flow-id 前缀后发给客户端。
type edgeMux struct {
	e    *fakeEdge
	in   chan edgeDatagram
	done chan struct{}
	once sync.Once
}

// ReadFrom 读一条已剥前缀的数据报。
func (m *edgeMux) ReadFrom(p []byte) (int, net.Addr, error) {
	select {
	case pkt := <-m.in:
		return copy(p, pkt.data), pkt.from, nil
	case <-m.done:
		return 0, nil, net.ErrClosed
	}
}

// WriteTo 给 KCP 报文加当前 flow-id 前缀并写到当前客户端对端（忽略入参地址：后端不知道换网）。
func (m *edgeMux) WriteTo(p []byte, _ net.Addr) (int, error) {
	m.e.mu.Lock()
	flow := m.e.flow
	var id uint64
	var peer net.Addr
	if flow != nil {
		id, peer = flow.id, flow.peer
	}
	m.e.mu.Unlock()
	if flow == nil {
		return 0, fmt.Errorf("假 edge: 尚无流")
	}
	return m.e.pc.WriteTo(literalWrap(id, p), peer)
}

// Close 关闭后端连接（停止 KCP 监听读循环）。
func (m *edgeMux) Close() error {
	m.close()
	return nil
}

// close 幂等关闭 done。
func (m *edgeMux) close() { m.once.Do(func() { close(m.done) }) }

// push 把数据报投递给后端（关闭后丢弃，避免向已关闭通道发送）。
func (m *edgeMux) push(pkt edgeDatagram) {
	select {
	case m.in <- pkt:
	case <-m.done:
	}
}

// LocalAddr 返回稳定后端地址。
func (m *edgeMux) LocalAddr() net.Addr { return fakeEdgeBackendAddr }

// SetDeadline 是空实现（假 edge 不需要截止时间）。
func (m *edgeMux) SetDeadline(time.Time) error { return nil }

// SetReadDeadline 是空实现。
func (m *edgeMux) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline 是空实现。
func (m *edgeMux) SetWriteDeadline(time.Time) error { return nil }

// sendToClient 按当前回程绑定给客户端发一条原始数据报（模拟后端下行，测试下行路径用）。
func (e *fakeEdge) sendToClient(payload []byte) {
	e.mu.Lock()
	flow := e.flow
	var id uint64
	var peer net.Addr
	if flow != nil {
		id, peer = flow.id, flow.peer
	}
	e.mu.Unlock()
	if flow == nil {
		return
	}
	_, _ = e.pc.WriteTo(literalWrap(id, payload), peer)
}
