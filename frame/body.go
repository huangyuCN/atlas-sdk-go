package frame

import (
	"encoding/binary"
	"fmt"
)

// BuildRequestBody 封装帧 body（无可选段）：[opLen:u16 大端][operation][payload]。
// 与服务端 transport/internal/client.BuildRawBody 格式一致。
func BuildRequestBody(operation string, payload []byte) ([]byte, error) {
	return buildRequestBody(operation, "", "", payload)
}

// BuildRequestBodyWithSession 封装携带会话槽与请求幂等键的请求 body：
// [opLen][operation][sessionLen][session][requestIDLen][requestID][payload]，
// 配合帧头 FlagSession / FlagRequestID 使用。无连接传输（UDP/KCP）的请求帧
// 携带会话槽；requestID 非空时携带幂等键段（客户端重试/重发复用同一 ID，
// 服务端按 atlas.route.v1 注解决定是否去重）。空串段不写入（保持布局最小）。
func BuildRequestBodyWithSession(operation, session, requestID string, payload []byte) ([]byte, error) {
	return buildRequestBody(operation, session, requestID, payload)
}

// buildRequestBody 是 body 封装的唯一实现源；session/requestID 非空时依序追加对应段。
func buildRequestBody(operation, session, requestID string, payload []byte) ([]byte, error) {
	if len(operation) == 0 {
		return nil, fmt.Errorf("frame: operation 不能为空")
	}
	if len(operation) > MaxOperationLen {
		return nil, fmt.Errorf("frame: operation 长度 %d 超过上限 %d", len(operation), MaxOperationLen)
	}
	if len(session) > MaxSessionLen {
		return nil, fmt.Errorf("frame: session 长度 %d 超过上限 %d", len(session), MaxSessionLen)
	}
	if len(requestID) > MaxRequestIDLen {
		return nil, fmt.Errorf("frame: requestID 长度 %d 超过上限 %d", len(requestID), MaxRequestIDLen)
	}
	n := 2 + len(operation) + len(payload)
	if session != "" {
		n += 2 + len(session)
	}
	if requestID != "" {
		n += 2 + len(requestID)
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
	if requestID != "" {
		binary.BigEndian.PutUint16(body[p:p+2], uint16(len(requestID)))
		copy(body[p+2:], requestID)
		p += 2 + len(requestID)
	}
	copy(body[p:], payload)
	return body, nil
}

// BuildRequestBodyFull 封装全部可选段（会话槽 + 请求幂等键）：
// [opLen][operation][sessionLen][session][requestIDLen][requestID][payload]，
// 空串段不写入（布局最小化）；调用方按置位的 flags 解析。配合帧头使用。
func BuildRequestBodyFull(operation, session, requestID string, payload []byte) ([]byte, error) {
	return buildRequestBody(operation, session, requestID, payload)
}

// ParseRequestBody 解析帧 body，返回 operation 与 payload（不解析可选段）。
// 服务端对超长 operation 回 ProtocolError 不断连；客户端侧同样不视作断连条件。
func ParseRequestBody(body []byte) (op string, payload []byte, err error) {
	o, _, _, p, e := parseRequestBody(body, 0)
	return o, p, e
}

// ParseRequestBodyWithSession 解析帧 body，返回 operation、会话槽与 payload。
// flags 未置位（或未携带会话槽）时 session 为空串；携带则校验完整性。
func ParseRequestBodyWithSession(body []byte, flags uint8) (op, session string, payload []byte, err error) {
	o, s, _, p, e := parseRequestBody(body, flags)
	return o, s, p, e
}

// ParseRequestBodyFull 解析帧 body 的全部可选段，返回 operation、会话槽、
// 请求幂等键与 payload（服务端五协议引擎与此同构）。
func ParseRequestBodyFull(body []byte, flags uint8) (op, session, requestID string, payload []byte, err error) {
	return parseRequestBody(body, flags)
}

// parseRequestBody 统一解析：flags 置位时在 operation 后依序读取可选段
// （会话槽 FlagSession → 请求幂等键 FlagRequestID）。
func parseRequestBody(body []byte, flags uint8) (op, session, requestID string, payload []byte, err error) {
	if len(body) < 2 {
		return "", "", "", nil, fmt.Errorf("frame: body 过短，缺少 opLen: %w", ErrProtocol)
	}
	opLen := int(binary.BigEndian.Uint16(body[0:2]))
	if opLen > MaxOperationLen {
		return "", "", "", nil, fmt.Errorf("frame: operation 长度 %d 超过上限 %d: %w", opLen, MaxOperationLen, ErrProtocol)
	}
	if len(body) < 2+opLen {
		return "", "", "", nil, fmt.Errorf("frame: operation 截断: %w", ErrProtocol)
	}
	op = string(body[2 : 2+opLen])
	rest := body[2+opLen:]
	if flags&FlagSession != 0 {
		if len(rest) < 2 {
			return "", "", "", nil, fmt.Errorf("frame: 会话槽缺少长度: %w", ErrProtocol)
		}
		sLen := int(binary.BigEndian.Uint16(rest[0:2]))
		if len(rest) < 2+sLen {
			return "", "", "", nil, fmt.Errorf("frame: 会话槽截断: %w", ErrProtocol)
		}
		session = string(rest[2 : 2+sLen])
		rest = rest[2+sLen:]
	}
	if flags&FlagRequestID != 0 {
		if len(rest) < 2 {
			return "", "", "", nil, fmt.Errorf("frame: 请求幂等键缺少长度: %w", ErrProtocol)
		}
		iLen := int(binary.BigEndian.Uint16(rest[0:2]))
		if len(rest) < 2+iLen {
			return "", "", "", nil, fmt.Errorf("frame: 请求幂等键截断: %w", ErrProtocol)
		}
		requestID = string(rest[2 : 2+iLen])
		rest = rest[2+iLen:]
	}
	return op, session, requestID, rest, nil
}
