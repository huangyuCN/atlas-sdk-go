package frame

import (
	"encoding/binary"
	"fmt"
)

// 错误分类标记（errors.Is 判定用）：协议非法与业务拒绝之外的失败一律视为传输类。
var ErrProtocol = fmt.Errorf("frame: protocol")

// DecodeReply 解析响应包络：
//
//	成功 [hasError=0][dataLen:u32][data...]
//	失败 [hasError=1][statusLen:u32][Status protobuf][dataLen:u32][data...]
//
// 返回业务 data 与错误（失败时为 *StatusError，业务方按 Reason 分支；
// statusLen=0 是合法包络——服务端 Status 序列化失败时的降级形态）。
func DecodeReply(b []byte) ([]byte, *Status, error) {
	if len(b) < 5 {
		return nil, nil, fmt.Errorf("frame: reply 过短: %w", ErrProtocol)
	}
	hasError := b[0]
	offset := 1
	if hasError == 0 {
		dataLen := int(binary.BigEndian.Uint32(b[offset : offset+4]))
		if len(b) < offset+4+dataLen {
			return nil, nil, fmt.Errorf("frame: reply data 截断: %w", ErrProtocol)
		}
		return b[offset+4 : offset+4+dataLen], nil, nil
	}

	statusLen := int(binary.BigEndian.Uint32(b[offset : offset+4]))
	offset += 4
	if len(b) < offset+statusLen {
		return nil, nil, fmt.Errorf("frame: reply status 截断: %w", ErrProtocol)
	}
	var st *Status
	if statusLen > 0 {
		var err error
		if st, err = DecodeStatus(b[offset : offset+statusLen]); err != nil {
			return nil, nil, err
		}
	} else {
		st = &Status{} // statusLen=0：服务端 Status 序列化失败，容忍为零值 Status
	}
	offset += statusLen
	if len(b) < offset+4 {
		return nil, nil, fmt.Errorf("frame: reply data 截断: %w", ErrProtocol)
	}
	dataLen := int(binary.BigEndian.Uint32(b[offset : offset+4]))
	if len(b) < offset+4+dataLen {
		return nil, nil, fmt.Errorf("frame: reply data 截断: %w", ErrProtocol)
	}
	return b[offset+4 : offset+4+dataLen], st, nil
}

// Status 是 atlas errors.Status 的客户端侧还原（字段号见 atlas errors/errors.proto：
// code=1 int32、reason=2 string、message=3 string、metadata=4 map<string,string>）。
// 手写 protobuf wire 解码，避免引入完整 protobuf 运行时。
type Status struct {
	Code     int32
	Reason   string
	Message  string
	Metadata map[string]string
}

// Error 实现 error（业务分支主键是 Reason，见规范 §7.1）。
func (s *Status) Error() string {
	if s == nil {
		return "status: nil"
	}
	return fmt.Sprintf("status: code=%d reason=%s message=%s", s.Code, s.Reason, s.Message)
}
