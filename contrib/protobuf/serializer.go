// Package protobuf 提供基于 protobuf 二进制的 client.Serializer 可选实现
// （载荷编码 ver=2，规范 §3.1 载荷编码协商；2026-09-04 v0.5 打样）。
//
// 请求/响应 DTO 必须是 protoc 生成的 message 类型（实现 proto.Message）；
// 帧头 version 声明为 2（实现 client.Versioned）。protojson（ver=1）永续支持，
// 服务端支持 ver=2 前勿在真实连接启用本实现。
//
// 用法：
//
//	import pbserializer "github.com/huangyuCN/atlas-sdk-go/contrib/protobuf"
//	c, err := client.Dial(addr, client.WithSerializer(pbserializer.Serializer{}))
package protobuf

import (
	"fmt"

	"github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/proto"
)

// Serializer 是 protobuf 二进制序列化器（载荷编码 ver=2）。
// Marshal/Unmarshal 的请求与响应 DTO 须实现 proto.Message（protoc 生成类型）。
type Serializer struct{}

func (Serializer) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("protobuf: 请求 DTO 须实现 proto.Message，得到 %T", v)
	}
	return proto.Marshal(m)
}

func (Serializer) Unmarshal(data []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protobuf: 响应 DTO 须实现 proto.Message，得到 %T", v)
	}
	return proto.Unmarshal(data, m)
}

// Version 实现 client.Versioned：protobuf 二进制 = 载荷编码 ver=2。
func (Serializer) Version() uint8 { return frame.Version2 }
