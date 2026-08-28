package frame

import (
	"fmt"
)

// DecodeStatus 手写解析 atlas errors.Status 的 protobuf wire 格式。
// 字段号：code=1 (varint int32)、reason=2 (string)、message=3 (string)、
// metadata=4 (map<string,string>，每个 entry 为嵌套 message：key=1、value=2)。
// 未知字段跳过（与服务端 DiscardUnknown 语义对齐）。
func DecodeStatus(b []byte) (*Status, error) {
	st := &Status{}
	if err := walkFields(b, func(fieldNum int, wire int, val []byte, num uint64) error {
		switch {
		case fieldNum == 1 && wire == wireVarint:
			st.Code = int32(num)
		case fieldNum == 2 && wire == wireBytes:
			st.Reason = string(val)
		case fieldNum == 3 && wire == wireBytes:
			st.Message = string(val)
		case fieldNum == 4 && wire == wireBytes:
			k, v, err := decodeMapEntry(val)
			if err != nil {
				return err
			}
			if st.Metadata == nil {
				st.Metadata = make(map[string]string)
			}
			st.Metadata[k] = v
		}
		return nil // 未知字段静默跳过
	}); err != nil {
		return nil, fmt.Errorf("frame: 解析 Status 失败: %w", err)
	}
	return st, nil
}

// protobuf wire types（本包仅需两种：varint 与 length-delimited）。
const (
	wireVarint = 0
	wireBytes  = 2
)

// walkFields 遍历 protobuf message 的顶层字段。
func walkFields(b []byte, fn func(fieldNum, wire int, val []byte, num uint64) error) error {
	for len(b) > 0 {
		key, n := binaryUvarint(b)
		if n <= 0 {
			return fmt.Errorf("字段 key 非法")
		}
		b = b[n:]
		fieldNum, wire := int(key>>3), int(key&0x7)
		switch wire {
		case wireVarint:
			num, n := binaryUvarint(b)
			if n <= 0 {
				return fmt.Errorf("field %d varint 非法", fieldNum)
			}
			b = b[n:]
			if err := fn(fieldNum, wire, nil, num); err != nil {
				return err
			}
		case wireBytes:
			l, n := binaryUvarint(b)
			if n <= 0 || uint64(len(b)-n) < l {
				return fmt.Errorf("field %d bytes 长度非法", fieldNum)
			}
			b = b[n:]
			if err := fn(fieldNum, wire, b[:l], 0); err != nil {
				return err
			}
			b = b[l:]
		default:
			return fmt.Errorf("field %d 不支持的 wire type %d", fieldNum, wire)
		}
	}
	return nil
}

// decodeMapEntry 解析 map<string,string> 的 entry（key=1、value=2）。
func decodeMapEntry(b []byte) (string, string, error) {
	var k, v string
	err := walkFields(b, func(fieldNum, wire int, val []byte, _ uint64) error {
		switch {
		case fieldNum == 1 && wire == wireBytes:
			k = string(val)
		case fieldNum == 2 && wire == wireBytes:
			v = string(val)
		}
		return nil
	})
	return k, v, err
}

// binaryUvarint 从 b 解析 varint（封装 encoding/binary，n≤0 表示非法）。
func binaryUvarint(b []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, byteVal := range b {
		if i == 10 { // varint 最长 10 字节
			return 0, -1
		}
		if byteVal < 0x80 {
			if i == 9 && byteVal > 1 {
				return 0, -1
			}
			return x | uint64(byteVal)<<s, i + 1
		}
		x |= uint64(byteVal&0x7f) << s
		s += 7
	}
	return 0, 0
}
