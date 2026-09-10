package client

import (
	"encoding/json"
	"fmt"

	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Serializer 是序列化插槽（规范 §3.1）：ver=1 默认双通道 protojson——
// proto message 走 protojson（零值省略，三库统一语义），非 proto 回退
// encoding/json（Go 特有轻量兼容）；ver=2 用 contrib/protobuf（二进制）。
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

// ProtoJSONSerializer 是默认序列化器（双通道，载荷编码 ver=1）：
// proto message 经 protojson 编码（默认零值省略——三库统一客户端请求语义，
// 对齐 C# JsonFormatter / TS 生成 schema）；非 proto 类型（plain struct/map）
// 回退 encoding/json——Go 特有轻量兼容（不背 protobuf 依赖的调用方仍可用
// 手写 DTO）。
// 解码：proto message 经 protojson（DiscardUnknown 容忍未知字段——服务端加
// 字段不破坏旧客户端），非 proto 回退 encoding/json。
// 零值可用；并发安全（无共享可变状态）。
type ProtoJSONSerializer struct{}

// Marshal 编码请求：proto message 经 protojson（零值省略），其余经 encoding/json。
func (ProtoJSONSerializer) Marshal(v any) ([]byte, error) {
	if m, ok := v.(proto.Message); ok {
		return protojson.MarshalOptions{}.Marshal(m)
	}
	return json.Marshal(v)
}

// Unmarshal 解码响应：proto message 经 protojson（DiscardUnknown），其余经 encoding/json。
func (ProtoJSONSerializer) Unmarshal(data []byte, v any) error {
	if m, ok := v.(proto.Message); ok {
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, m)
	}
	return json.Unmarshal(data, v)
}

// JSONSerializer 是纯 Go 原生 JSON 序列化器（仅非 proto 手写 DTO 场景显式选用；
// 默认请用 ProtoJSONSerializer——proto message 会因 int64 线上 string 形态而与
// protojson 不兼容）。DTO 字段名按 protojson 规则（camelCase）生成 json tag；
// 64 位整数字段必须是 string 类型（protojson 线上形态，见规范 §6.2）。
type JSONSerializer struct{}

// Marshal 直接以 encoding/json 编码（无 protojson 分支）。
func (JSONSerializer) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal 直接以 encoding/json 解码（无 protojson 分支）。
func (JSONSerializer) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
