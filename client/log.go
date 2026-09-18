// client_log.go 是 SDK 的内置调试日志：显式级别 Option 控制（接入时参数决定，
// 无环境变量），按级别过滤输出——开发时全开核对收发，生产默认 Error 只看异常。
package client

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// logLevel 是 SDK 调试日志的级别（数值越大越细）。
type logLevel int

// logLevel 的取值（WithLog* Option 与之对应；默认 Error）。
const (
	logLevelOff     logLevel = -1 // 静默（WithLogSilence）
	logLevelError   logLevel = 1
	logLevelWarn    logLevel = 2
	logLevelInfo    logLevel = 3
	logLevelDebug   logLevel = 4
	logDefaultLevel          = logLevelError // 不设置时的默认级别
)

// sdkLogger 是内置调试日志实现：级别过滤 + stderr 输出（零外部依赖）。
type sdkLogger struct {
	mu  sync.Mutex
	min logLevel
	out *os.File
}

// logf 按级别输出（时间戳 + 级别 + 消息）；静默或低于最小级别时丢弃。
func (l *sdkLogger) logf(lv logLevel, format string, args ...any) {
	if l == nil || lv < l.min {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = fmt.Fprintf(l.out, "[atlas-sdk %s %s] %s\n",
		time.Now().Format("15:04:05.000"), levelName(lv), fmt.Sprintf(format, args...))
}

// debugf 调试级（收发内容、seq、幂等键）。
func (l *sdkLogger) debugf(format string, args ...any) { l.logf(logLevelDebug, format, args...) }

// infof 信息级（连接事件）。
func (l *sdkLogger) infof(format string, args ...any) { l.logf(logLevelInfo, format, args...) }

// warnf 警告级（超时/重发）。
func (l *sdkLogger) warnf(format string, args ...any) { l.logf(logLevelWarn, format, args...) }

// errorf 错误级（断连失败/协议错误）。
func (l *sdkLogger) errorf(format string, args ...any) { l.logf(logLevelError, format, args...) }

// levelName 返回级别名（日志行前缀）。
func levelName(lv logLevel) string {
	switch lv {
	case logLevelDebug:
		return "debug"
	case logLevelInfo:
		return "info"
	case logLevelWarn:
		return "warn"
	default:
		return "error"
	}
}

// WithLogSilence 完全关闭 SDK 调试日志（覆盖默认的 Error 级输出）。
func WithLogSilence() Option {
	return func(s *channelSettings) { s.logger = nil; s.logOff = true }
}

// WithLogError 只打印 Error（与不设置等价的显式写法；stderr 输出）。
func WithLogError() Option {
	return func(s *channelSettings) { s.logger = newSDKLogger(logLevelError) }
}

// WithLogWarn 打印 Warn 及以上（含超时/重发；stderr 输出）。
func WithLogWarn() Option {
	return func(s *channelSettings) { s.logger = newSDKLogger(logLevelWarn) }
}

// WithLogInfo 打印 Info 及以上（含连接事件；stderr 输出）。
func WithLogInfo() Option {
	return func(s *channelSettings) { s.logger = newSDKLogger(logLevelInfo) }
}

// WithLogDebug 打印 Debug 及以上（全开：每次收发的请求/响应 JSON、seq、幂等键）。
func WithLogDebug() Option {
	return func(s *channelSettings) { s.logger = newSDKLogger(logLevelDebug) }
}

// newSDKLogger 构造内置 stderr 日志实现。
func newSDKLogger(min logLevel) *sdkLogger {
	return &sdkLogger{min: min, out: os.Stderr}
}
