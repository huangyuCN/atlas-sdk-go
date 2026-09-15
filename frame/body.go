package frame

import (
	"encoding/binary"
	"fmt"
)

// BuildRequestBody 封装帧 body（无会话槽）：[opLen:u16 大端][operation][payload]。
// 与服务端 transport/internal/client.BuildRawBody 格式一致。
func BuildRequestBody(operation string, payload []byte) ([]byte, error) {
	return buildRequestBody(operation, "", payload)
}

// BuildRequestBodyWithSession 封装携带会话槽的请求 body：
// [opLen][operation][sessionLen][session][payload]，配合帧头 FlagSession 使用。
// 无连接传输（UDP/KCP）的请求帧调用；session 为空时与无会话布局等价（匿名请求）。
func BuildRequestBodyWithSession(operation, session string, payload []byte) ([]byte, error) {
	return buildRequestBody(operation, session, payload)
}

// buildRequestBody 是 body 封装的唯一实现源；session 非空时追加会话槽。
func buildRequestBody(operation, session string, payload []byte) ([]byte, error) {
	if len(operation) == 0 {
		return nil, fmt.Errorf("frame: operation 不能为空")
	}
	if len(operation) > MaxOperationLen {
		return nil, fmt.Errorf("frame: operation 长度 %d 超过上限 %d", len(operation), MaxOperationLen)
	}
	if len(session) > MaxSessionLen {
		return nil, fmt.Errorf("frame: session 长度 %d 超过上限 %d", len(session), MaxSessionLen)
	}
	n := 2 + len(operation) + len(payload)
	if session != "" {
		n += 2 + len(session)
	}
	body := make([]byte, n)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(operation)))
	copy(body[2:], operation)
	p := 2 + len(operation)
	if session != "" {
		binary.BigEndian.PutUint16(body[p:p+2], uint16(len(session)))
		copy(body[p+2:], session)
		p += 2 + len(session)
	}
	copy(body[p:], payload)
	return body, nil
}

// ParseRequestBody 解析帧 body，返回 operation 与 payload（不解析会话槽）。
// 服务端对超长 operation 回 ProtocolError 不断连；客户端侧同样不视作断连条件。
func ParseRequestBody(body []byte) (op string, payload []byte, err error) {
	o, _, p, e := parseRequestBody(body, 0)
	return o, p, e
}

// ParseRequestBodyWithSession 解析帧 body，返回 operation、会话槽与 payload。
// flags 未置位（或未携带会话槽）时 session 为空串；携带则校验完整性。
func ParseRequestBodyWithSession(body []byte, flags uint8) (op, session string, payload []byte, err error) {
	return parseRequestBody(body, flags)
}

// parseRequestBody 统一解析：flags 置位 FlagSession 时在 operation 后读会话槽。
func parseRequestBody(body []byte, flags uint8) (op, session string, payload []byte, err error) {
	if len(body) < 2 {
		return "", "", nil, fmt.Errorf("frame: body 过短，缺少 opLen: %w", ErrProtocol)
	}
	opLen := int(binary.BigEndian.Uint16(body[0:2]))
	if opLen > MaxOperationLen {
		return "", "", nil, fmt.Errorf("frame: operation 长度 %d 超过上限 %d: %w", opLen, MaxOperationLen, ErrProtocol)
	}
	if len(body) < 2+opLen {
		return "", "", nil, fmt.Errorf("frame: operation 截断: %w", ErrProtocol)
	}
	op = string(body[2 : 2+opLen])
	rest := body[2+opLen:]
	if flags&FlagSession == 0 {
		return op, "", rest, nil
	}
	if len(rest) < 2 {
		return "", "", nil, fmt.Errorf("frame: 会话槽缺少长度: %w", ErrProtocol)
	}
	sLen := int(binary.BigEndian.Uint16(rest[0:2]))
	if len(rest) < 2+sLen {
		return "", "", nil, fmt.Errorf("frame: 会话槽截断: %w", ErrProtocol)
	}
	return op, string(rest[2 : 2+sLen]), rest[2+sLen:], nil
}
