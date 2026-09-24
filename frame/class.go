package frame

import (
	"strconv"
	"strings"
)

// Class 是错误分类（数值与框架 errors.Class 及 errors.proto 的 ErrorClass 枚举一一对应）：
// 业务拒绝可提示、运行时故障需上报、取消忽略。Reason 仍是业务分支主键，Class 只做处置
// 口径（日志定级/故障率口径）。
type Class int32

// Class 的取值（数值与框架 errors.Class 一致；框架 errors.proto 已枚举化为 ErrorClass，
// 字段号 5 与 wire type（varint）不变，故枚举化前后字节级兼容）。
const (
	// ClassUnspecified 未分类（0，ERROR_CLASS_UNSPECIFIED）：历史错误或未显式标注，按运行时定级。
	ClassUnspecified Class = 0
	// ClassBusiness 业务拒绝（1，ERROR_CLASS_BUSINESS）：玩家可理解的业务错误，可提示。
	ClassBusiness Class = 1
	// ClassRuntime 运行时故障（2，ERROR_CLASS_RUNTIME）：基础设施或编程缺陷，需上报。
	ClassRuntime Class = 2
	// ClassCanceled 取消（3，ERROR_CLASS_CANCELED）：客户端主动断开或超时取消，不算故障。
	ClassCanceled Class = 3
)

// String 返回分类的稳定标签（与框架 errors.Class.String 同口径，供日志与诊断）。
func (c Class) String() string {
	switch c {
	case ClassBusiness:
		return "business"
	case ClassRuntime:
		return "runtime"
	case ClassCanceled:
		return "canceled"
	default:
		return "unspecified"
	}
}

// ParseClass 解析错误分类的文本形态：接受 protojson 枚举名（ERROR_CLASS_RUNTIME，框架
// errors.proto 的 ErrorClass 枚举名，protojson 默认下发形态）、框架标签（runtime）与
// 十进制数值（"2"，枚举化前的 int32 形态）；空串/未知名返回未分类。两种口径都必须能
// 解析——只认一种会让枚举化后客户端静默降级为未分类。
func ParseClass(text string) Class {
	switch strings.ToUpper(strings.TrimSpace(text)) {
	case "ERROR_CLASS_BUSINESS", "CLASS_BUSINESS", "BUSINESS":
		return ClassBusiness
	case "ERROR_CLASS_RUNTIME", "CLASS_RUNTIME", "RUNTIME":
		return ClassRuntime
	case "ERROR_CLASS_CANCELED", "ERROR_CLASS_CANCELLED", "CLASS_CANCELED", "CLASS_CANCELLED", "CANCELED", "CANCELLED":
		return ClassCanceled
	case "", "ERROR_CLASS_UNSPECIFIED", "CLASS_UNSPECIFIED", "UNSPECIFIED":
		return ClassUnspecified
	}
	n, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return ClassUnspecified
	}
	return Class(n)
}
