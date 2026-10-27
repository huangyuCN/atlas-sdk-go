// 载荷编码版本（规范 §3.1 载荷编码协商）：帧头 version 字段声明本帧载荷的编码方式。
//
//   - ver=1：protojson（可读，保留为**调试开关**，永续支持）；
//   - ver=2：protobuf 二进制（生产主路径，游戏侧生成包解码）。
//
// 帧级自描述：收发两侧只看帧头即可判定编码方式，SDK 因此不需要（也不应该）猜载荷编码。

package direct

import (
	"errors"
	"fmt"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// ErrPayloadVersion 表示序列化器声明的载荷编码版本不在白名单 {1,2}（出站编码版本非法）。
var ErrPayloadVersion = errors.New("direct: 非法载荷编码版本")

// supportedVersion 报告版本是否在载荷编码白名单内（ver=1 protojson / ver=2 protobuf 二进制）。
func supportedVersion(ver uint8) bool { return ver == frame.Version || ver == frame.Version2 }

// payloadVersion 返回序列化器声明的出站载荷编码版本：实现 frame.Versioned 者用声明值，
// 未实现者按 ver=1（protojson）；声明值不在白名单（0、3+ 等）返回 ErrPayloadVersion——
// 在拨号前把关，避免把一个服务端只认 {1,2} 的版本发上线（与 client 通道同一白名单口径）。
func payloadVersion(serial client.Serializer) (uint8, error) {
	v, ok := serial.(frame.Versioned)
	if !ok {
		return frame.Version, nil
	}
	ver := v.Version()
	if !supportedVersion(ver) {
		return 0, fmt.Errorf("%w: 序列化器声明 %d（白名单 {1,2}）", ErrPayloadVersion, ver)
	}
	return ver, nil
}
