// serializer 适配层：真机冒烟按 -serializer 参数切换载荷编码。
// json/protojson = proto message + 默认 ProtoJSONSerializer（三库统一：
// protojson 语义、零值省略——json 为历史名，语义同 protojson，与 TS 一致）；
// protobuf = proto message + contrib/protobuf（ver=2 二进制）。
package main

import (
	"github.com/huangyuCN/atlas-sdk-go/client"
	pbserializer "github.com/huangyuCN/atlas-sdk-go/contrib/protobuf"
)

// smokeMode 是编码运行模式（json/protojson 语义相同，protobuf 独立）。
type smokeMode int

const (
	modeJSON smokeMode = iota // 历史名：语义同 protojson（零值省略）
	modeProtoJSON
	modeProtobuf
)

// serializerOf 返回该模式的 serializer 选项（默认 ProtoJSONSerializer——proto
// message 走 protojson、非 proto 回退 encoding/json）。
func serializerOf(m smokeMode) client.Option {
	switch m {
	case modeProtobuf:
		return client.WithSerializer(pbserializer.Serializer{})
	default:
		// modeJSON/modeProtoJSON 一致：默认双通道 ProtoJSONSerializer（pb.go 请求
		// 经 protojson 编码，int64 线上为字符串；零值省略——三库统一语义）。
		return client.WithSerializer(client.ProtoJSONSerializer{})
	}
}

// serializerName 供日志展示。
func serializerName(m smokeMode) string {
	switch m {
	case modeProtobuf:
		return "protobuf"
	case modeProtoJSON:
		return "protojson"
	default:
		return "json"
	}
}

// parseMode 解析 -serializer 参数值。
func parseMode(s string) (smokeMode, bool) {
	switch s {
	case "json":
		return modeJSON, true
	case "protojson":
		return modeProtoJSON, true
	case "protobuf":
		return modeProtobuf, true
	default:
		return modeJSON, false
	}
}
