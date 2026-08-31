package client

import (
	"context"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
	kcpgo "github.com/xtaci/kcp-go/v5"
)

// KCP 会话参数（与 atlas 服务端 transport/kcp 默认会话配置同源）：
// 明文（block=nil）、无 FEC（dataShards=parityShards=0，两端必须一致）；
// 四元组与窗口/MTU 为服务端 session_config 默认档，两端宜匹配。
const (
	kcpNoDelay  = 0    // 关闭极速模式（常规 RTO 退避）
	kcpInterval = 40   // 内部 flush 定时器间隔（ms）
	kcpResend   = 0    // 快速重传阈值（0=关闭）
	kcpNC       = 0    // 启用拥塞控制
	kcpSndWnd   = 128  // 发送窗口（KCP 包）
	kcpRcvWnd   = 128  // 接收窗口（KCP 包）
	kcpMTU      = 1400 // 单包 MTU（字节）

	// kcpWriteTimeout 是单次帧写的兜底超时：kcp-go 死链（对端消失）后 Write 可能
	// 因发送窗口满而永久阻塞（state 不触发错误、未确认数据不清空），无超时则心跳
	// 与业务写全部挂起、死链检测失效。对齐服务端 WithWriteTimeout 思路。
	kcpWriteTimeout = 10 * time.Second
)

// dialKCP 建立 KCP 传输（kcp-go UDPSession 实现 net.Conn；**消息模式**——kcp-go
// 默认 stream=0，未调用 SetStreamMode，与服务端 transport/kcp 同为消息模式：一次
// Write 对端一次 Read 完整收回。SDK 帧按「头消息 + body 消息」两次 Write，服务端
// io.ReadFull 分两次读并在 body>mss 时由 WriteBuffers 切块 + ReadFull 循环拼接，
// 两端兼容。切勿按「流式」理解加 SetStreamMode——会破坏与服务端的互通）。
// 死链语义（kcp-go 固有）：对端消失后 state=dead_link 不触发 Read/Write 错误，
// 未确认数据堆满发送窗口时 Write 永久阻塞——本实现为每次写设置写超时兜底
// （评审修复：对齐服务端 WithWriteTimeout 思路），阻塞时返回超时错误由上层
// 判死链重连。kcp-go 拨号无 ctx 变体：后台拨号 + select 实现可取消（取消后成功会话被关闭）。
func dialKCP(ctx context.Context, addr string) (channelTransport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type dialResult struct {
		sess *kcpgo.UDPSession
		err  error
	}
	ch := make(chan dialResult, 1)
	go func() {
		// 服务端基线：明文（block=nil）、无 FEC（0/0）；会话参数拨号后按默认档应用。
		sess, err := kcpgo.DialWithOptions(addr, nil, 0, 0)
		ch <- dialResult{sess: sess, err: err}
	}()
	var res dialResult
	select {
	case <-ctx.Done():
		// ctx 取消后后台拨号仍可能成功：成功则关闭会话避免泄漏。
		go func() {
			if r := <-ch; r.sess != nil {
				_ = r.sess.Close()
			}
		}()
		return nil, ctx.Err()
	case res = <-ch:
	}
	if res.err != nil {
		return nil, res.err
	}
	sess := res.sess
	// 会话参数对齐服务端默认（session_config.applyTo 同款）。
	sess.SetNoDelay(kcpNoDelay, kcpInterval, kcpResend, kcpNC)
	sess.SetWindowSize(kcpSndWnd, kcpRcvWnd)
	_ = sess.SetMtu(kcpMTU)
	sess.SetACKNoDelay(true)
	sess.SetWriteDelay(false)
	return &kcpTransport{sess: sess}, nil
}

// kcpTransport 基于 kcp-go UDPSession 的传输（消息模式，互通细节见 dialKCP 文档）。
type kcpTransport struct {
	sess    *kcpgo.UDPSession
	writeMu sync.Mutex // kcp 写侧整帧单次写（与内核通道写锁双保险）
}

func (t *kcpTransport) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	return frame.Read(t.sess, maxBodySize)
}

func (t *kcpTransport) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	// 写超时兜底（评审修复）：死链窗口满时 kcp-go Write 永久阻塞，无超时则
	// 心跳/业务写全部挂起、死链检测失效。超时错误由上层按网络失败判死链重连。
	_ = t.sess.SetWriteDeadline(time.Now().Add(kcpWriteTimeout))
	return frame.Write(t.sess, h, body, maxBodySize)
}

func (t *kcpTransport) Close() error { return t.sess.Close() }
