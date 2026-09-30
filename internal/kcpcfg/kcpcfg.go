// Package kcpcfg 收敛 KCP 会话参数的服务端对齐缺省档：client（网关/战斗通道）与
// direct（战斗直连会话）共用同一份，避免两处手写副本与服务端漂移。
package kcpcfg

import (
	"time"

	kcpgo "github.com/xtaci/kcp-go/v5"
)

// 会话参数（与 atlas 服务端 transport/kcp 默认会话配置同源）：明文（block=nil）、
// 无 FEC（dataShards=parityShards=0，两端必须一致）；四元组与窗口/MTU 为服务端默认档。
const (
	// NoDelay 关闭极速模式（常规 RTO 退避）。
	NoDelay = 0
	// Interval 是内部 flush 定时器间隔（ms）。
	Interval = 40
	// Resend 是快速重传阈值（0=关闭）。
	Resend = 0
	// NC 启用拥塞控制。
	NC = 0
	// SndWnd 是发送窗口（KCP 包）。
	SndWnd = 128
	// RcvWnd 是接收窗口（KCP 包）。
	RcvWnd = 128
	// MTU 是单包 MTU（字节）。
	MTU = 1400
	// WriteTimeout 是单次帧写的兜底超时：kcp-go 死链（对端消失）后 Write 可能因发送
	// 窗口满而永久阻塞（state 不触发错误、未确认数据不清空），无超时则心跳与业务写全部挂起。
	WriteTimeout = 10 * time.Second
)

// Apply 把缺省档应用到已建立的 KCP 会话（与服务端 session_config.applyTo 同款）。
func Apply(sess *kcpgo.UDPSession) {
	sess.SetNoDelay(NoDelay, Interval, Resend, NC)
	sess.SetWindowSize(SndWnd, RcvWnd)
	_ = sess.SetMtu(MTU)
	sess.SetACKNoDelay(true)
	sess.SetWriteDelay(false)
}
