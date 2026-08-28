package client

import "encoding/json"

// Serializer 是序列化插槽（规范 §3.1）：当前实现为 protojson 兼容 JSON；
// 将来切二进制 protobuf 时更换实现即可，帧头 version 为协商位。
type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONSerializer 是默认实现：Go 原生 JSON。
// DTO 字段名按 protojson 规则（camelCase）生成 json tag（DTO 层由生成器产出）；
// 64 位整数字段必须是 string 类型（protojson 线上形态，见规范 §6.2）。
type JSONSerializer struct{}

func (JSONSerializer) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONSerializer) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
