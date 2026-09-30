package edge

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// FlowIDLen 是数据报面 flow-id 的线格式长度（8 字节大端）。
//
// 数据报面（KCP 与 UDP 两面，规格 §2.1/§3.2）在首包 hello 换到 flow-id 之后，
// **双向每个数据报**都带该前缀：客户端发出的（含 KCP 报文）要加、接入层回程要加，
// 接收侧各自剥离。客户端侧（SDK）与接入层共用本文件的编解码，避免各写一份导致前缀漂移。
const FlowIDLen = 8

// ErrFlowIDShort 表示数据报短于 flow-id 前缀：格式非法，接收侧应丢弃并（客户端）重新 hello。
var ErrFlowIDShort = errors.New("edge: 数据报短于 flow-id 前缀")

// EncodeFlowID 把 flow-id 编码为线格式前缀（新分配的 8 字节大端）。
//
// 接入层回程热路径不调用本函数（它直接写进池化缓冲，见 udpFace 的回程循环），
// 本函数服务客户端与测试。
func EncodeFlowID(id uint64) []byte {
	out := make([]byte, FlowIDLen)
	binary.BigEndian.PutUint64(out, id)
	return out
}

// DecodeFlowID 解析数据报的 flow-id 前缀并返回其后的载荷。
//
// 载荷是入参的子切片（不复制）：接入层每包都走这条路径，多一次拷贝即多一次分配；
// 调用方须保证在使用载荷期间原数据报有效。
func DecodeFlowID(datagram []byte) (flowID uint64, payload []byte, err error) {
	if len(datagram) < FlowIDLen {
		return 0, nil, fmt.Errorf("%w: 长度 %d", ErrFlowIDShort, len(datagram))
	}
	return binary.BigEndian.Uint64(datagram[:FlowIDLen]), datagram[FlowIDLen:], nil
}
