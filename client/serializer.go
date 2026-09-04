package client

import (
	"encoding/json"
	"fmt"

	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// Serializer 是序列化插槽（规范 §3.1）：当前实现为 protojson 兼容 JSON；
// 将来切二进制 protobuf 时更换实现即可，帧头 version 为协商位。
type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// serializerVersion 返回 serializer 的载荷编码版本（未实现 frame.Versioned
// 接口则默认 ver=1；接口定义在 frame 层以避免 contrib → client 依赖环）。
// 白名单 {1,2}：自定义/非法版本（0、3+ 等）拒绝——出站编码不执行白名单校验，
// 通道构造期把关（评审 Fix：此前原样采纳，版本 0 出站被改写为 1 但响应校验仍按 0
// 必失败；版本 3 会被发出到只认 {1,2} 的服务端）。
func serializerVersion(s Serializer) (uint8, error) {
	if v, ok := s.(frame.Versioned); ok {
		ver := v.Version()
		if ver != frame.Version && ver != frame.Version2 {
			return 0, fmt.Errorf("client: 序列化器声明非法载荷编码版本 %d（白名单 {1,2}）", ver)
		}
		return ver, nil
	}
	return frame.Version, nil
}

// JSONSerializer 是默认实现：Go 原生 JSON。
// DTO 字段名按 protojson 规则（camelCase）生成 json tag（DTO 层由生成器产出）；
// 64 位整数字段必须是 string 类型（protojson 线上形态，见规范 §6.2）。
type JSONSerializer struct{}

func (JSONSerializer) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONSerializer) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
