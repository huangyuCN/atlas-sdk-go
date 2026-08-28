package frame

import (
	"encoding/binary"
	"fmt"
)

// BuildRequestBody 封装帧 body：[opLen:u16 大端][operation][payload]。
// 与服务端 transport/internal/client.BuildRawBody 格式一致。
func BuildRequestBody(operation string, payload []byte) ([]byte, error) {
	if len(operation) == 0 {
		return nil, fmt.Errorf("frame: operation 不能为空")
	}
	if len(operation) > MaxOperationLen {
		return nil, fmt.Errorf("frame: operation 长度 %d 超过上限 %d", len(operation), MaxOperationLen)
	}
	body := make([]byte, 2+len(operation)+len(payload))
	binary.BigEndian.PutUint16(body[0:2], uint16(len(operation)))
	copy(body[2:], operation)
	copy(body[2+len(operation):], payload)
	return body, nil
}

// ParseRequestBody 解析帧 body，返回 operation 与 payload。
// 服务端对超长 operation 回 ProtocolError 不断连；客户端侧同样不视作断连条件。
func ParseRequestBody(body []byte) (op string, payload []byte, err error) {
	if len(body) < 2 {
		return "", nil, fmt.Errorf("frame: body 过短，缺少 opLen: %w", ErrProtocol)
	}
	opLen := int(binary.BigEndian.Uint16(body[0:2]))
	if opLen > MaxOperationLen {
		return "", nil, fmt.Errorf("frame: operation 长度 %d 超过上限 %d: %w", opLen, MaxOperationLen, ErrProtocol)
	}
	if len(body) < 2+opLen {
		return "", nil, fmt.Errorf("frame: operation 截断: %w", ErrProtocol)
	}
	return string(body[2 : 2+opLen]), body[2+opLen:], nil
}
