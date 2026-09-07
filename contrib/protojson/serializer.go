// Package protojson 提供基于 protojson 的 client.Serializer 可选实现：
// 使 protoc-gen-go 生成的 proto message 可直接作为 Invoke 的 req/resp，
// 无需经 `atlas sdk gen` 生成 JSON DTO。
//
// 线上载荷是 protojson 风格 JSON。编码（客户端请求）：零值字段省略
// （protojson 默认，对齐 C# JsonFormatter/TS 手写——三库统一语义；服务端
// Unmarshal 用 DiscardUnknown，缺失字段 ≡ 零值，互通不受影响）；解码
// DiscardUnknown 对齐「未知字段忽略」（服务端加字段不破坏旧客户端）。
// 服务端响应仍 EmitUnpopulated 下发零值（服务端 Marshal 语义），客户端
// Unmarshal 容忍显式零值——请求编码与响应解码解耦，无需对齐。
// 非 proto 类型回退 encoding/json（行为同默认 JSONSerializer），两类 req/resp
// 可在同一 Client 内混用。
//
// 用法（包名与 google.golang.org/protobuf/encoding/protojson 同名，按需别名）：
//
//	import sdkprotojson "github.com/huangyuCN/atlas-sdk-go/contrib/protojson"
//
//	c, err := client.Dial(addr, client.WithSerializer(sdkprotojson.Serializer{}))
//	var resp pb.LoginReply
//	err = c.Invoke(ctx, "/gateway.v1.GatewayAuth/Login", &pb.LoginRequest{...}, &resp)
//
// 引入本包会使模块携带 google.golang.org/protobuf 依赖；仅 import client 核心
// 包的使用方不受影响（依赖分层同 atlas 主仓 contrib 约定：第三方实现不进核心）。
package protojson

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Serializer 是 protojson 序列化器：proto message 走 protojson，其余类型回退
// encoding/json。零值可用；并发安全（无共享可变状态，client 的 Invoke/On 会
// 并发调用）。Marshal 输出为 protojson 形态——语义稳定，但字节级空白不确定
// （protojson 官方行为），勿做字节级比对。
type Serializer struct{}

// Marshal 编码请求：proto message 经 protojson（默认零值省略，三库统一
// 客户端请求语义），其余类型经 encoding/json。
func (Serializer) Marshal(v any) ([]byte, error) {
	if m, ok := v.(proto.Message); ok {
		return protojson.MarshalOptions{}.Marshal(m)
	}
	return json.Marshal(v)
}

// Unmarshal 解码响应：proto message 经 protojson（DiscardUnknown 容忍未知字段，
// 并同时接受 int64 的字符串/数值两种形态），其余类型经 encoding/json。
func (Serializer) Unmarshal(data []byte, v any) error {
	if m, ok := v.(proto.Message); ok {
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, m)
	}
	return json.Unmarshal(data, v)
}
