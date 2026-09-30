package client

import (
	"context"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/frame"
	"github.com/huangyuCN/atlas-sdk-go/internal/kcpcfg"
	kcpgo "github.com/xtaci/kcp-go/v5"
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
	// 会话参数对齐服务端默认（直接/直连两个包共用 internal/kcpcfg，避免副本漂移）。
	kcpcfg.Apply(sess)
	return &kcpTransport{sess: sess}, nil
}

// kcpTransport 基于 kcp-go UDPSession 的传输（消息模式，互通细节见 dialKCP 文档）。
type kcpTransport struct {
	sess    *kcpgo.UDPSession
	writeMu sync.Mutex // kcp 写侧整帧单次写（与内核通道写锁双保险）
}

// ReadFrame 从 KCP 会话读取一帧（消息模式，一次 Read 收回一个完整帧）。
func (t *kcpTransport) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	return frame.Read(t.sess, maxBodySize)
}

// WriteFrame 向 KCP 会话写入一帧（整帧单次写，写超时兜底防死链永久阻塞）。
func (t *kcpTransport) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	// 写超时兜底（评审修复）：死链窗口满时 kcp-go Write 永久阻塞，无超时则
	// 心跳/业务写全部挂起、死链检测失效。超时错误由上层按网络失败判死链重连。
	_ = t.sess.SetWriteDeadline(time.Now().Add(kcpcfg.WriteTimeout))
	return frame.Write(t.sess, h, body, maxBodySize)
}

// Close 关闭底层 KCP 会话。
func (t *kcpTransport) Close() error { return t.sess.Close() }
