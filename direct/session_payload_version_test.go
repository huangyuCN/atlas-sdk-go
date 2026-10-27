// 载荷编码版本（ver）透传与放行：直连会话把帧头的 `version` 原样交给调用方，SDK **不解游戏
// 消息**（ver=2 的 protobuf 二进制由游戏侧的生成包解码）。本文件钉死四件事：
//
//  1. 推送分发带版本：OnPush / OnFrameRaw 拿到 (原始载荷, 帧头版本)；(payload, version) 原样透传，
//     不按 protojson 预解（旧实现 ver=2 会解析失败后静默丢弃）。
//  2. 帧号提取可注入：ver=1 恒走内建 protojson；ver=2 用 WithFrameNumberFunc 注入的钩子推进补帧
//     基准，未注入时**不推进也不报错**（与 TS/C# 同口径：二进制非自描述，不猜编码）。
//  3. 放行 ver=2：出站版本由序列化器推导（缺省 ver=1 不变），回执版本只接受白名单 {1,2} 且必须
//     与本会话一致，未知版本（如 3）是明确的协议级错误。
//  4. 兼容：OnNotify / OnFrame 等旧回调照旧工作（ver=1 行为逐字节不变）。

package direct

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	"github.com/huangyuCN/atlas-sdk-go/client"
	pbserializer "github.com/huangyuCN/atlas-sdk-go/contrib/protobuf"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// frameObserved 是一次帧广播回调的观测值（原始载荷 + 帧头版本）。
type frameObserved struct {
	payload []byte
	version uint8
}

// pushBroadcastVersion 以 Notify 帧推一条帧广播（指定帧头版本与原始载荷）。
func pushBroadcastVersion(t *testing.T, fc *fakeConn, ver uint8, payload []byte) {
	t.Helper()
	body, err := frame.BuildRequestBody(stubOpFrame, payload)
	if err != nil {
		t.Fatalf("帧广播 body 封装失败: %v", err)
	}
	if err := fc.push(frame.Header{Type: frame.MsgTypeNotify, Version: ver, Seq: 2}, body); err != nil {
		t.Fatalf("帧广播推入失败: %v", err)
	}
}

// TestRawFramePushCarriesVersion ②：ver=2 帧广播经原始载荷回调下发，回调拿到帧头透传的
// version=2 与**逐字节一致**的载荷。载荷是手写二进制（0x08 + 帧号），故意不是合法 JSON——
// 只要 SDK 仍按 protojson 预解，回调就不会触发（旧分发正是解析失败后静默丢弃）。
func TestRawFramePushCarriesVersion(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))

	got := make(chan frameObserved, 4)
	sess.OnFrameRaw(func(payload []byte, version uint8) {
		got <- frameObserved{payload: append([]byte(nil), payload...), version: version}
	})
	want := []byte{0x08, 0x09}
	pushBroadcastVersion(t, fc, frame.Version2, want)

	select {
	case observed := <-got:
		if observed.version != frame.Version2 {
			t.Fatalf("回调版本 = %d, 期望 %d（帧头版本必须原样透传）", observed.version, frame.Version2)
		}
		if !bytes.Equal(observed.payload, want) {
			t.Fatalf("回调载荷 = % x, 期望 % x（SDK 不得改写/预解载荷）", observed.payload, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ver=2 帧广播未回调（SDK 不应再按 protojson 预解游戏消息）")
	}
}

// secondByteFrameNumber 是本组用例的帧号钩子：载荷是手写的两位字节（0x08 + 帧号），按约定取
// 第二个字节——不依赖任何生成的 protobuf 代码（重点在版本透传与基准推进，不在具体编码）。
func secondByteFrameNumber(payload []byte) (uint64, bool) {
	if len(payload) != 2 || payload[0] != 0x08 {
		return 0, false
	}
	return uint64(payload[1]), true
}

// TestFrameNumberHookAdvancesVer2Baseline ③：注入 FrameNumberFunc 后 ver=2 帧广播的补帧基准
// 按钩子推进（二进制载荷非自描述，帧号由调用方按游戏侧生成包提取）。
func TestFrameNumberHookAdvancesVer2Baseline(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithFrameNumberFunc(secondByteFrameNumber))
	pushBroadcastVersion(t, fc, frame.Version2, []byte{0x08, 9})

	waitFor(t, 2*time.Second, "ver=2 补帧基准推进", func() bool { return sess.LastSeenFrame() == 9 })
}

// TestVer2PushWithoutHookKeepsBaseline ③b（降级口径）：未注入帧号钩子时 ver=2 帧广播**不推进**
// 补帧基准、也**不报错**（连接与分发都不受影响）；随后 ver=1 的帧广播照常推进——降级只影响
// ver=2 的帧号提取，不影响会话可用性（与 TS/C# 同口径）。
func TestVer2PushWithoutHookKeepsBaseline(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))
	versions := make(chan uint8, 4)
	sess.OnFrameRaw(func(_ []byte, version uint8) { versions <- version })

	pushBroadcastVersion(t, fc, frame.Version2, []byte{0x08, 9})
	select {
	case version := <-versions:
		if version != frame.Version2 {
			t.Fatalf("回调版本 = %d, 期望 %d", version, frame.Version2)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未注入钩子不应影响 ver=2 帧广播的分发")
	}
	if got := sess.LastSeenFrame(); got != 0 {
		t.Fatalf("未注入钩子时 ver=2 不应推进补帧基准，得到 %d", got)
	}
	if sess.State() != StateConnected || sess.Err() != nil {
		t.Fatalf("未注入钩子不得让会话失败：state=%s err=%v", sess.State(), sess.Err())
	}

	pushBroadcastVersion(t, fc, frame.Version, []byte(`{"battleId":"b-1","frame":{"frameId":3}}`))
	waitFor(t, 2*time.Second, "ver=1 帧广播照常推进", func() bool { return sess.LastSeenFrame() == 3 })
}

// TestUnknownResponseVersionIsProtocolError ④：回执版本不在载荷编码白名单（如 3）→ 明确的协议级
// 错误（errors.Is(client.ErrProtocol) 可判定，错误文本指明版本与白名单），在途请求立即失败；
// 不做「猜编码再解码」的容忍。
func TestUnknownResponseVersionIsProtocolError(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithAutoReconnect(false))
	fc.setReplyVersion(3) // 未知版本：白名单 {1,2} 之外

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID})
	if !errors.Is(err, client.ErrProtocol) {
		t.Fatalf("错误 = %v, 期望 errors.Is(err, client.ErrProtocol)（版本非法属协议级失败）", err)
	}
	if !strings.Contains(err.Error(), "3") || !strings.Contains(err.Error(), "白名单") {
		t.Fatalf("错误文本未指明非法版本与白名单: %v", err)
	}
}

// TestResponseVersionMismatchIsProtocolError ④b：回执版本在白名单内、但与本会话载荷编码不一致
// （ver=1 会话收到 ver=2 回执）→ 明确的协议级错误（而不是拿 protojson 去解二进制再报解码失败）。
func TestResponseVersionMismatchIsProtocolError(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithAutoReconnect(false))
	fc.setReplyVersion(frame.Version2)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID})
	if !errors.Is(err, client.ErrProtocol) {
		t.Fatalf("错误 = %v, 期望 errors.Is(err, client.ErrProtocol)", err)
	}
	if !strings.Contains(err.Error(), "不一致") {
		t.Fatalf("错误文本未指明版本不一致: %v", err)
	}
}

// TestVer2RequestAndResponseAccepted ③'：换成 ver=2 序列化器后，请求帧头版本为 2、载荷是 protobuf
// 二进制（手写字面量 `0x0a 票长 battle_id`），且 ver=2 的回执被接受——请求/响应不再被「只认
// ver=1」拒绝。ver=1 调试路径仍是缺省（见 TestDefaultSessionKeepsVer1Wire）。
func TestVer2RequestAndResponseAccepted(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithSerializer(pbserializer.Serializer{}))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}); err != nil {
		t.Fatalf("ver=2 发帧失败: %v", err)
	}
	if versions := fc.requestVersions(); len(versions) != 1 || versions[0] != frame.Version2 {
		t.Fatalf("请求帧头版本 = %v, 期望 [%d]（出站版本由序列化器推导）", versions, frame.Version2)
	}
	_, _, payloads := fc.snapshot()
	want := append([]byte{0x0a, byte(len(testBattleID))}, testBattleID...)
	if len(payloads) != 1 || !bytes.Equal(payloads[0], want) {
		t.Fatalf("请求载荷 = % x, 期望 protobuf 字面量 % x", payloads, want)
	}
}

// badVersionSerializer 声明白名单外的载荷编码版本（Open 期校验用；不做真实编解码）。
type badVersionSerializer struct{ ver uint8 }

// Marshal 占位实现（本用例不会走到编码：非法版本在拨号前就被拒绝）。
func (badVersionSerializer) Marshal(any) ([]byte, error) { return nil, nil }

// Unmarshal 占位实现（同上）。
func (badVersionSerializer) Unmarshal([]byte, any) error { return nil }

// Version 返回声明的载荷编码版本（frame.Versioned：出站版本由此推导）。
func (s badVersionSerializer) Version() uint8 { return s.ver }

// TestOpenRejectsUnknownSerializerVersion ④'：序列化器声明版本 3（白名单外）→ Open 在拨号前
// 明确报错（ErrPayloadVersion）：不浪费拨号，也不把服务端不可能接受的版本发上线。
func TestOpenRejectsUnknownSerializerVersion(t *testing.T) {
	plan := planFor(TransportWS, "127.0.0.1:1", testTicket)
	_, err := Open(context.Background(), plan, WithSerializer(badVersionSerializer{ver: 3}))
	if !errors.Is(err, ErrPayloadVersion) {
		t.Fatalf("Open 错误 = %v, 期望 errors.Is(err, ErrPayloadVersion)", err)
	}
	if !strings.Contains(err.Error(), "3") || !strings.Contains(err.Error(), "白名单") {
		t.Fatalf("错误文本未指明非法版本与白名单: %v", err)
	}
}

// TestDefaultSessionKeepsVer1Wire ①：缺省会话的出站帧头版本仍是 ver=1、载荷仍是逐字节的
// protojson——ver=1（调试路径）的出站行为不变。
func TestDefaultSessionKeepsVer1Wire(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}); err != nil {
		t.Fatalf("缺省会话发帧失败: %v", err)
	}
	if versions := fc.requestVersions(); len(versions) != 1 || versions[0] != frame.Version {
		t.Fatalf("请求帧头版本 = %v, 期望缺省仍为 [%d]", versions, frame.Version)
	}
	_, _, payloads := fc.snapshot()
	if want := fmt.Sprintf(`{"battleId":%q}`, testBattleID); len(payloads) != 1 || string(payloads[0]) != want {
		t.Fatalf("请求载荷 = %q, 期望 protojson 逐字节 %q", payloads, want)
	}
}

// TestLegacyPushCallbacksKeepWorking ⑤：旧回调 OnNotify（原始字节）与 OnFrame（解析
// FrameBroadcast）照旧工作，新的 OnPush 同时拿到 version=1——推送分发对旧调用方完全兼容。
func TestLegacyPushCallbacksKeepWorking(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0))

	notified := make(chan []byte, 4)
	sess.OnNotify(stubOpFrame, func(payload []byte) { notified <- append([]byte(nil), payload...) })
	parsed := make(chan uint64, 4)
	sess.OnFrame(func(fb *battlev1.FrameBroadcast) { parsed <- fb.GetFrame().GetFrameId() })
	versioned := make(chan uint8, 4)
	sess.OnPush(stubOpFrame, func(_ []byte, version uint8) { versioned <- version })

	payload := []byte(`{"battleId":"b-1","frame":{"frameId":3}}`)
	pushBroadcastVersion(t, fc, frame.Version, payload)
	select {
	case got := <-notified:
		if !bytes.Equal(got, payload) {
			t.Fatalf("旧 OnNotify 载荷 = %q, 期望逐字节 %q", got, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("旧 OnNotify 回调未触发（ver=1 行为不得改变）")
	}
	select {
	case id := <-parsed:
		if id != 3 {
			t.Fatalf("旧 OnFrame 解析帧号 = %d, 期望 3", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("旧 OnFrame 回调未触发（ver=1 行为不得改变）")
	}
	select {
	case version := <-versioned:
		if version != frame.Version {
			t.Fatalf("OnPush 版本 = %d, 期望 %d", version, frame.Version)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnPush 回调未触发")
	}
	waitFor(t, 2*time.Second, "ver=1 补帧基准推进", func() bool { return sess.LastSeenFrame() == 3 })
}

// TestWSVer2FramePush ②'（真传输端到端）：ver=2 的帧广播经 WS 面到达（帧层白名单放行 ver=2）→
// 原始载荷回调拿到 version=2 与逐字节载荷；注入的帧号钩子把补帧基准推进到载荷约定的帧号。
func TestWSVer2FramePush(t *testing.T) {
	stub := newBattleStub(testTicket)
	srv := startWSStub(t, testTicket, stub)
	binaryPayload := []byte{0x08, 9}
	stub.setPushFrame(frame.Version2, binaryPayload)

	sess, err := Open(context.Background(), planFor(TransportWS, srv.addr, testTicket),
		WithHeartbeat(0), WithFrameNumberFunc(secondByteFrameNumber))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer func() { _ = sess.Close() }()

	got := make(chan frameObserved, 4)
	sess.OnFrameRaw(func(payload []byte, version uint8) {
		got <- frameObserved{payload: append([]byte(nil), payload...), version: version}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: "b-1", LastSeenFrame: 0}); err != nil {
		t.Fatalf("SyncFrames 失败: %v", err)
	}
	select {
	case observed := <-got:
		if observed.version != frame.Version2 {
			t.Fatalf("回调版本 = %d, 期望 %d（真传输上帧头版本必须原样透传）", observed.version, frame.Version2)
		}
		if !bytes.Equal(observed.payload, binaryPayload) {
			t.Fatalf("回调载荷 = % x, 期望 % x", observed.payload, binaryPayload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("未收到 ver=2 帧广播（帧层应放行 ver=2）")
	}
	waitFor(t, 2*time.Second, "ver=2 补帧基准推进", func() bool { return sess.LastSeenFrame() == 9 })
}
