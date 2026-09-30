package direct

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas-sdk-go/api/lockstep"
)

// TestOpenUDPHelloFlowIDAndSlot 验证 UDP 面直连：
// 首包 hello 逐字节（magic/版本/票长/票密文）→ 收 flow-id → 之后每个包双向带前缀，且逐帧带票。
func TestOpenUDPHelloFlowIDAndSlot(t *testing.T) {
	stub := newBattleStub(testTicket)
	addr, srv := startUDPStub(t, testTicket, stub)

	sess, err := Open(context.Background(), planFor(TransportUDP, addr, testTicket))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if sess.Transport() != TransportUDP {
		t.Fatalf("实际传输面 = %s, 期望 udp", sess.Transport())
	}
	if got, want := srv.helloBytes(), literalHello(testTicket); !bytes.Equal(got, want) {
		t.Fatalf("首包 hello = % x, 期望（手写字面量）% x", got, want)
	}
	pushed := make(chan uint64, 4)
	sess.OnFrame(func(fb *battlev1.FrameBroadcast) { pushed <- fb.GetFrame().GetFrameId() })

	joinOnce(t, sess, "b-1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = sess.SendFrameInput(ctx, &battlev1.FrameInputReq{
		BattleId: "b-1", Input: &locksteppb.LockstepInput{FrameId: 1, Payload: []byte{0x07}},
	})
	if err != nil {
		t.Fatalf("SendFrameInput 失败: %v", err)
	}
	if _, err := sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: "b-1", LastSeenFrame: 0}); err != nil {
		t.Fatalf("SyncFrames 失败: %v", err)
	}
	waitPushedFrame(t, pushed, 1)
	assertWireOK(t, stub, 1, []string{stubOpJoin, stubOpInput, stubOpSync})
}

// TestOpenUDPRejectedNoReceipt 验证数据报面无回执（hello 被静默丢弃）判为「被接入层拒绝」
// 而非网络断开：Open 明确失败且不重试。
func TestOpenUDPRejectedNoReceipt(t *testing.T) {
	stub := newBattleStub(testTicket)
	addr, srv := startUDPStub(t, testTicket, stub)
	srv.ignoreHello.Store(true)

	start := time.Now()
	_, err := Open(context.Background(), planFor(TransportUDP, addr, testTicket),
		WithHandshakeTimeout(150*time.Millisecond))
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("Open 错误 = %v, 期望 ErrRejected", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("拒绝判定耗时 %s，超过握手上限语义", elapsed)
	}
	if got, want := srv.helloBytes(), literalHello(testTicket); !bytes.Equal(got, want) {
		t.Fatalf("首包 hello = % x, 期望（手写字面量）% x", got, want)
	}
}

// TestOpenKCPHelloFlowIDAndSlot 验证 KCP 面直连：hello 段（逐字节）→ flow-id 前缀 →
// 同一 socket 上的 KCP 会话跑帧协议，逐帧带票。
func TestOpenKCPHelloFlowIDAndSlot(t *testing.T) {
	stub := newBattleStub(testTicket)
	addr, srv := startKCPStub(t, testTicket, stub)

	sess, err := Open(context.Background(), planFor(TransportKCP, addr, testTicket))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if sess.Transport() != TransportKCP {
		t.Fatalf("实际传输面 = %s, 期望 kcp", sess.Transport())
	}
	if got, want := srv.helloBytes(), literalHello(testTicket); !bytes.Equal(got, want) {
		t.Fatalf("hello 段 = % x, 期望（手写字面量）% x", got, want)
	}
	pushed := make(chan uint64, 4)
	sess.OnFrame(func(fb *battlev1.FrameBroadcast) { pushed <- fb.GetFrame().GetFrameId() })

	joinOnce(t, sess, "b-1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: "b-1", LastSeenFrame: 0}); err != nil {
		t.Fatalf("SyncFrames 失败: %v", err)
	}
	waitPushedFrame(t, pushed, 1)
	assertWireOK(t, stub, 1, []string{stubOpJoin, stubOpSync})
}
