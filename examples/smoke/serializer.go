// serializer 适配层：真机冒烟按 -serializer 参数切换三种载荷编码。
// json = plain struct + JSONSerializer（无 EmitUnpopulated，零值字段不下发）；
// protojson = proto message + contrib/protojson（EmitUnpopulated 零值下发）；
// protobuf = proto message + contrib/protobuf（ver=2 二进制）。
package main

import (
	"github.com/huangyuCN/atlas-sdk-go/client"
	pbserializer "github.com/huangyuCN/atlas-sdk-go/contrib/protobuf"
	pjson "github.com/huangyuCN/atlas-sdk-go/contrib/protojson"
)

// smokeMode 是三编码的运行模式。
type smokeMode int

const (
	modeJSON smokeMode = iota
	modeProtoJSON
	modeProtobuf
)

// serializerOf 返回该模式的 serializer 选项（nil = 默认 JSON）。
func serializerOf(m smokeMode) client.Option {
	switch m {
	case modeProtoJSON:
		return client.WithSerializer(pjson.Serializer{})
	case modeProtobuf:
		return client.WithSerializer(pbserializer.Serializer{})
	default:
		return client.WithSerializer(client.JSONSerializer{})
	}
}

// serializerName 供日志展示。
func serializerName(m smokeMode) string {
	switch m {
	case modeProtoJSON:
		return "protojson"
	case modeProtobuf:
		return "protobuf"
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
