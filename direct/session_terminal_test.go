// 终态类业务拒绝（BATTLE_ENDED / BATTLE_NOT_FOUND(3001) / BATTLE_FULL(3002)）单测：
// 三者与 ErrBattleEnded **同族**——可判定哨兵（errors.Is）+ 业务分类（Class=business）+
// 终态化（停发、不重连）+ 上报（EndCause() 与错误返回同形）。
//
// reason/code 在用例里写字面量（不引实现常量）：断言独立于实现，改实现常量不会让用例
// 一起改口径——与 wiretest_test.go 的桩 op 字面量同一约定。

package direct

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// assertTerminalReject 断言错误是「终态类业务拒绝」：可判定哨兵 + 业务形态（code/reason/class）。
func assertTerminalReject(t *testing.T, err error, sentinel error, reason string, code int32) {
	t.Helper()
	if err == nil {
		t.Fatalf("错误为 nil, 期望终态类业务拒绝（reason=%s）", reason)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("错误 = %v, 期望 errors.Is(err, %v)", err, sentinel)
	}
	var be *client.BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("错误 = %v, 期望保留 *client.BusinessError（上报形态）", err)
	}
	if be.Reason != reason || be.Code != code || be.Class != frame.ClassBusiness {
		t.Fatalf("业务形态 = {code:%d reason:%s class:%s}, 期望 {code:%d reason:%s class:business}",
			be.Code, be.Reason, be.Class, code, reason)
	}
}

// assertEndedState 断言会话进入「正常结束」终态（有结算可展示）：ended 为真、failed 为假。
func assertEndedState(t *testing.T, sess *Session, sentinel error, reason string, code int32) {
	t.Helper()
	if !sess.Terminal() {
		t.Fatal("Terminal() = false, 期望 true（终态：停发、不重连）")
	}
	if !sess.Ended() || sess.Failed() {
		t.Fatalf("ended=%v failed=%v, 期望 ended=true failed=false（对局正常结束、有结算可展示）",
			sess.Ended(), sess.Failed())
	}
	if got := sess.State(); got != StateEnded {
		t.Fatalf("状态 = %s, 期望 ended", got)
	}
	assertTerminalReject(t, sess.EndCause(), sentinel, reason, code)
}

// assertFailedState 断言会话进入「终态失败」（无结算可展示）：failed 为真、ended 为假、状态 failed
// ——上层据此「回匹配链路重新开局」而不是去取一份不存在的结算。
func assertFailedState(t *testing.T, sess *Session, sentinel error, reason string, code int32) {
	t.Helper()
	if !sess.Terminal() {
		t.Fatal("Terminal() = false, 期望 true（终态：停发、不重连）")
	}
	if sess.Ended() || !sess.Failed() {
		t.Fatalf("ended=%v failed=%v, 期望 ended=false failed=true（终态失败、无结算可展示）",
			sess.Ended(), sess.Failed())
	}
	if got := sess.State(); got != StateFailed {
		t.Fatalf("状态 = %s, 期望 failed", got)
	}
	assertTerminalReject(t, sess.EndCause(), sentinel, reason, code)
}

// assertReleasedQuickly 断言连接在 1s 内被释放（终态失败无结算可读，不等收尾窗口）。
func assertReleasedQuickly(t *testing.T, fc *fakeConn) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fc.isClosed() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("终态失败后 1s 内未释放连接（无结算可读，不应等收尾窗口）")
}

// TestBattleNotFoundTerminalAndReported ①：对局不存在（BATTLE_NOT_FOUND，biz_code 3001）与
// ErrBattleEnded 同族——可判定、不可重试、终态化（停发 + 不重连）并上报；终态后再发仍同族拒绝。
//
// 但它走的是**终态失败**（failed）而不是 ended：没有结算可展示，上层应回匹配链路重新开局。
func TestBattleNotFoundTerminalAndReported(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond), WithEndLinger(30*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	joinOnce(t, sess, testBattleID)
	fc.replyStatusNext(fakeStatus{code: 404, reason: "BATTLE_NOT_FOUND", class: frame.ClassBusiness})
	err := sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID})
	assertTerminalReject(t, err, ErrBattleNotFound, "BATTLE_NOT_FOUND", 404)
	assertFailedState(t, sess, ErrBattleNotFound, "BATTLE_NOT_FOUND", 404)
	assertReleasedQuickly(t, fc) // 收尾窗口配了 30s：连接仍被立刻释放，证明失败态不等窗口

	ops, _, _ := fc.snapshot()
	time.Sleep(150 * time.Millisecond) // 15 个心跳周期：终态后不得再写线
	assertNoWireWrites(t, fc, len(ops))

	// 终态下一切发送与重连同族拒绝（不写线、不重连）。
	assertTerminalReject(t, sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}),
		ErrBattleNotFound, "BATTLE_NOT_FOUND", 404)
	assertTerminalReject(t, sess.Reconnect(ctx), ErrBattleNotFound, "BATTLE_NOT_FOUND", 404)
	assertNoWireWrites(t, fc, len(ops))
}

// TestBattleFullTerminalAndReported ①b：入局被拒（BATTLE_FULL，biz_code 3002）同样不可重试、
// 终态化并上报——重连只会再撞同一拒绝，会话对上层立即失效而不是退避到超时；无结算可展示，
// 故状态是 failed 而非 ended。
func TestBattleFullTerminalAndReported(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fc.replyStatusNext(fakeStatus{code: 409, reason: "BATTLE_FULL", class: frame.ClassBusiness})
	_, err := sess.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: testBattleID})
	assertTerminalReject(t, err, ErrBattleFull, "BATTLE_FULL", 409)
	assertFailedState(t, sess, ErrBattleFull, "BATTLE_FULL", 409)

	ops, _, _ := fc.snapshot()
	time.Sleep(150 * time.Millisecond)
	assertNoWireWrites(t, fc, len(ops))
	assertTerminalReject(t, sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}),
		ErrBattleFull, "BATTLE_FULL", 409)
}

// TestFrameTargetMismatchTerminalAndReported ①c：票面对局与请求正文目标不一致
// （FRAME_TARGET_MISMATCH，403，框架 frameops 的目标一致性校验）同样不可重试、终态化并上报——
// 按未知 403 处理会退化成可重试的重试循环，且反复拉起不该拉起的对局。
//
// 服务端该 reason 由 errors.Forbidden 构造（未显式标注 class），这里就以**未分类**回执喂入：
// SDK 按终态族归一为 business（分类不因服务端是否 WithClass 而漂移）。
func TestFrameTargetMismatchTerminalAndReported(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fc.replyStatusNext(fakeStatus{code: 403, reason: "FRAME_TARGET_MISMATCH"})
	err := sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID})
	assertTerminalReject(t, err, ErrFrameTargetMismatch, "FRAME_TARGET_MISMATCH", 403)
	assertFailedState(t, sess, ErrFrameTargetMismatch, "FRAME_TARGET_MISMATCH", 403)
	if got := sess.EndReason(); got != "FRAME_TARGET_MISMATCH" {
		t.Fatalf("EndReason = %q, 期望 FRAME_TARGET_MISMATCH（上报按 reason 区分终态来源）", got)
	}

	ops, _, _ := fc.snapshot()
	time.Sleep(150 * time.Millisecond) // 15 个心跳周期：收到即入终态、不再发送
	assertNoWireWrites(t, fc, len(ops))
	assertTerminalReject(t, sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}),
		ErrFrameTargetMismatch, "FRAME_TARGET_MISMATCH", 403)
}

// assertLocalSettledMeta 断言错误带「本地结算」标记（三 SDK 统一键 x-atlas-sdk-local-settled）。
func assertLocalSettledMeta(t *testing.T, err error) {
	t.Helper()
	var be *client.BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("错误 = %v, 期望 *client.BusinessError", err)
	}
	if be.Metadata["x-atlas-sdk-local-settled"] != "true" {
		t.Fatalf("metadata = %v, 期望含 x-atlas-sdk-local-settled=true（本地结算标记）", be.Metadata)
	}
}

// TestBattleEndedKeepsEndedState ①d：BATTLE_ENDED 走「正常结束」终态（有结算可展示）——
// ended 为真、failed 为假、状态 ended；收尾窗口照旧保留（窗口内仍可读结算补投）。
func TestBattleEndedKeepsEndedState(t *testing.T) {
	fc := newFakeConn()
	sess := newHeartbeatSession(t, fc, WithHeartbeat(10*time.Millisecond), WithEndLinger(300*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	joinOnce(t, sess, testBattleID)
	fc.replyStatusNext(fakeStatus{code: 409, reason: "BATTLE_ENDED", class: frame.ClassBusiness})
	err := sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID})
	assertTerminalReject(t, err, ErrBattleEnded, "BATTLE_ENDED", 409)
	assertEndedState(t, sess, ErrBattleEnded, "BATTLE_ENDED", 409)
	if fc.isClosed() {
		t.Fatal("正常结束瞬间就关了连接（窗口内应保留可读，等结算补投）")
	}
	waitFor(t, 2*time.Second, "收尾窗口到期关连接", fc.isClosed)
}

// TestIsTerminalCoversFamily ①e：IsTerminal 覆盖终态族四种哨兵（票类与网络类不在其列），
// 调用方一处判定即可「不重试、转收尾/回匹配」。
func TestIsTerminalCoversFamily(t *testing.T) {
	terminals := []error{ErrBattleEnded, ErrBattleNotFound, ErrBattleFull, ErrFrameTargetMismatch}
	for _, sentinel := range terminals {
		if !IsTerminal(fmt.Errorf("包装一层: %w", sentinel)) {
			t.Fatalf("IsTerminal(%v) = false, 期望 true（终态族）", sentinel)
		}
	}
	for _, notTerminal := range []error{nil, ErrTicketExpired, ErrTicketInvalid, ErrRejected, ErrClosed} {
		if IsTerminal(notTerminal) {
			t.Fatalf("IsTerminal(%v) = true, 期望 false（非终态族）", notTerminal)
		}
	}
}

// TestTerminalSettlesInflightWithStatus ③：终态置位时在途请求**立即**以终态 Status 结算——
// 不等回执（本用例的连接只收不回）、不等超时、不报成网络错误；形态与真实回执同构
// （reason/code/class=business + 本地标记），调用方一处判定即可收尾。
func TestTerminalSettlesInflightWithStatus(t *testing.T) {
	fc := newFakeConn()
	fc.silent = true // 在途请求永远等不到回执：若靠超时结算，本用例必然失败
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithInvokeTimeout(30*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}) }()
	waitFor(t, 2*time.Second, "在途请求已上线", func() bool { return fc.count(stubOpInput) == 1 })

	pushEndNotifyPayload(t, fc, endNotifyPayload(t, "p-1")) // 终态由结束通知触发

	select {
	case err := <-done:
		assertTerminalReject(t, err, ErrBattleEnded, "BATTLE_ENDED", 409)
		assertLocalSettledMeta(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("终态后 3s 内在途请求仍未结算（不得等回执/超时）")
	}
}

// TestTerminalSettlesInflightOnReject ③b：一条请求被终态 reason 拒绝时，**其余**在途请求
// 立即以同一终态 reason 结算（同一张票已废，各自的等待没有意义）。
func TestTerminalSettlesInflightOnReject(t *testing.T) {
	fc := newFakeConn()
	fc.silent = true
	sess := newHeartbeatSession(t, fc, WithHeartbeat(0), WithInvokeTimeout(30*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 第一条：静默在途（脚本为空 + silent=true，永远等不到回执）。
	inflight := make(chan error, 1)
	go func() { inflight <- errOfSync(sess.SyncFrames(ctx, &battlev1.SyncFramesReq{BattleId: testBattleID})) }()
	waitFor(t, 2*time.Second, "静默请求已上线", func() bool { return fc.count(stubOpSync) == 1 })

	// 第二条：上线即拿到 BATTLE_NOT_FOUND 拒绝（脚本按写出顺序消费，故在其后登记）。
	fc.replyStatusNext(fakeStatus{code: 404, reason: "BATTLE_NOT_FOUND", class: frame.ClassBusiness})
	rejected := make(chan error, 1)
	go func() { rejected <- sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: testBattleID}) }()
	waitFor(t, 2*time.Second, "被拒请求已上线", func() bool { return fc.count(stubOpInput) == 1 })

	if err := <-rejected; err == nil {
		t.Fatal("被拒请求未返回错误")
	} else {
		assertTerminalReject(t, err, ErrBattleNotFound, "BATTLE_NOT_FOUND", 404)
	}
	select {
	case err := <-inflight:
		assertTerminalReject(t, err, ErrBattleNotFound, "BATTLE_NOT_FOUND", 404)
		assertLocalSettledMeta(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("终态后其余在途请求未结算（不得等回执/超时）")
	}
}
