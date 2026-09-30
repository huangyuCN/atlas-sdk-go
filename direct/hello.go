package direct

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	edgewire "github.com/huangyuCN/atlas-sdk-go/direct/gen/edge"
)

// 接入层 hello 段与 flow-id 前缀的**唯一手写实现**在框架仓 contrib/edge；本包不再自行拼装字节，
// 只做薄封装（同步文件由 scripts/gen-dto.sh 从框架逐字节复制到 direct/gen/edge，CI 有
// 「重生成无 diff」门禁）。hello 线格式：magic(4)="ATLH" || version(1) || 票长(2,大端) || 票密文。

// HelloMagic 是 hello 段的固定魔数（薄封装框架 contrib/edge.HelloMagic）。
var HelloMagic = edgewire.HelloMagic

// hello 段与 flow-id 的格式常量（薄封装框架 contrib/edge 的同名常量）。
const (
	// HelloVersion 是 hello 段格式版本（本轮 = 1）。
	HelloVersion = edgewire.HelloVersion
	// HelloHeaderLen 是 hello 段固定头长度（magic + 版本 + 票长）。
	HelloHeaderLen = edgewire.HelloHeaderLen
	// MaxTicketLen 是 hello 段可承载的票密文上限（字节）。
	MaxTicketLen = edgewire.MaxTicketLen
	// flowIDLen 是数据报面 flow-id 的线格式长度（8 字节大端）。
	flowIDLen = edgewire.FlowIDLen
)

// EncodeHello 把票密文编码为 hello 段（薄封装框架 edge.EncodeHello）：
// KCP/UDP 面在会话开始前先发该段（同一 socket），接入层据此验票并建流。
func EncodeHello(ticket []byte) []byte { return edgewire.EncodeHello(ticket) }

// TicketSlot 返回帧会话槽取值：**base64url 无填充**（RawURLEncoding）的票密文。
// 三面统一逐帧携带该槽（battle 侧按同一张票验身份），WS 升级 query 亦用同一编码。
func TicketSlot(ticket []byte) string {
	return base64.RawURLEncoding.EncodeToString(ticket)
}

// EdgeWSURL 拼接入层 WS 升级地址：ws://<host:port><path>?ticket=<base64url(票密文)>。
// path 为空取 "/"（接入层原样转发升级请求头，路径与 battle 帧面监听路径一致）。
func EdgeWSURL(addr string, ticket []byte, path string) (string, error) {
	return wsURL(addr, path, TicketSlot(ticket))
}

// wsURL 拼 WS 地址；ticket 为空时不带 query（直连 battle 帧端口的联调路径用）。
func wsURL(addr, path, ticket string) (string, error) {
	if addr == "" {
		return "", fmt.Errorf("%w: 接入层地址为空", ErrNotifyNoEndpoint)
	}
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u := url.URL{Scheme: "ws", Host: addr, Path: path}
	if ticket != "" {
		u.RawQuery = url.Values{"ticket": {ticket}}.Encode()
	}
	return u.String(), nil
}

// WrapFlowID 给数据报加 flow-id 前缀（薄封装框架前缀编码）：发出的包要加。
func WrapFlowID(id uint64, payload []byte) []byte {
	out := make([]byte, 0, edgewire.FlowIDLen+len(payload))
	out = append(out, edgewire.EncodeFlowID(id)...)
	return append(out, payload...)
}

// StripFlowID 剥掉数据报的 flow-id 前缀并返回载荷副本（收到的包要剥；拷贝出让所有权，
// 读缓冲逐包复用）。前缀缺失或与期望不符返回 ErrFlowIDMismatch——失配即丢弃并要求重新 hello。
func StripFlowID(data []byte, id uint64) ([]byte, error) {
	got, payload, err := edgewire.DecodeFlowID(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFlowIDMismatch, err)
	}
	if got != id {
		return nil, fmt.Errorf("%w: 期望 flow-id %#x，实际 %#x", ErrFlowIDMismatch, id, got)
	}
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, nil
}
