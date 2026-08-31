package client

import (
	"errors"
	"fmt"
)

// 错误四分类（规范 §7）：业务错误即 BusinessError（含 *frame.Status）；
// 其余三类为非业务故障。使用方按类型分支决定重试策略：
// NetworkError 可重试、TimeoutError 谨慎重试、ProtocolError 不可重试（连接已断）。
// 判定用 errors.As/Is（本文件提供哨兵）。

// ErrNetwork / ErrTimeout / ErrProtocol 是三类故障的哨兵（errors.Is 判定用）。
var (
	ErrNetwork  = errors.New("network")
	ErrTimeout  = errors.New("timeout")
	ErrProtocol = errors.New("protocol")
)

// NetworkError 表示连接断开、发送失败等网络类故障（可自动重连后重试）。
type NetworkError struct{ cause error }

func (e *NetworkError) Error() string { return fmt.Sprintf("network: %v", e.cause) }
func (e *NetworkError) Unwrap() error { return e.cause }
func (e *NetworkError) Is(target error) bool {
	return target == ErrNetwork
}

// TimeoutError 表示 Invoke 超时（请求可能已到达服务端，谨重重试）。
type TimeoutError struct{ cause error }

func (e *TimeoutError) Error() string { return fmt.Sprintf("timeout: %v", e.cause) }
func (e *TimeoutError) Unwrap() error { return e.cause }
func (e *TimeoutError) Is(target error) bool {
	return target == ErrTimeout
}

// ProtocolError 表示帧/包络/序列化非法（不可重试，连接已断）。
type ProtocolError struct{ cause error }

func (e *ProtocolError) Error() string { return fmt.Sprintf("protocol: %v", e.cause) }
func (e *ProtocolError) Unwrap() error { return e.cause }
func (e *ProtocolError) Is(target error) bool {
	return target == ErrProtocol
}

// NewNetworkError / NewTimeoutError / NewProtocolError 构造对应错误类型。
func NewNetworkError(cause error) error  { return &NetworkError{cause: cause} }
func NewTimeoutError(cause error) error  { return &TimeoutError{cause: cause} }
func NewProtocolError(cause error) error { return &ProtocolError{cause: cause} }

// BusinessError 是业务拒绝（服务端返回 Reply 错误包络）的 SDK 级错误类型（规范 §7）。
// 判断主键是 Reason（规范 §7.1：跨语言只比 Reason，不比 Code/metadata）。
type BusinessError struct {
	Code     int32
	Reason   string
	Message  string
	Metadata map[string]string
}

// Error 实现 error。
func (e *BusinessError) Error() string {
	return fmt.Sprintf("business: code=%d reason=%s message=%s", e.Code, e.Reason, e.Message)
}

// IsBusinessError 判断 err 是否为 Reason 匹配的业务错误（规范 §7.1 统一判断规则）。
func IsBusinessError(err error, reason string) bool {
	var be *BusinessError
	if !errors.As(err, &be) {
		return false
	}
	return be.Reason == reason
}
