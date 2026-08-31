package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// defaultWSHandshakeTimeout 是 WS 握手超时（gorilla 默认无超时，显式兜底防永久挂起）。
const defaultWSHandshakeTimeout = 10 * time.Second

// dialWS 建立 WebSocket 传输（一条 WS 消息 = 一个完整帧，规范 §4 浏览器唯一通道）。
// addr 支持 host:port（自动补 ws:// 前缀并拼接 path）或完整 ws://wss:// URL（path 忽略）。
// maxBodySize 用于设置读侧消息上限（与服务端 bodyLen 上限对称，防超大消息占内存）。
func dialWS(ctx context.Context, addr, path string, maxBodySize int) (channelTransport, error) {
	url := normalizeWSURL(addr, path)
	d := &websocket.Dialer{HandshakeTimeout: defaultWSHandshakeTimeout}
	conn, resp, err := d.DialContext(ctx, url, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("websocket 握手失败（HTTP %d）: %w", resp.StatusCode, err)
		}
		return nil, err
	}
	if maxBodySize <= 0 {
		maxBodySize = frame.MaxBodySize
	}
	conn.SetReadLimit(int64(frame.HeaderSize + maxBodySize))
	return &wsTransport{conn: conn}, nil
}

// normalizeWSURL 规整 WS 拨号地址：host:port + path（空则 "/ws"，对齐模板网关约定）；
// 完整 ws://wss:// URL 原样返回。
func normalizeWSURL(addr, path string) string {
	if strings.HasPrefix(addr, "ws://") || strings.HasPrefix(addr, "wss://") {
		return addr
	}
	if path == "" {
		path = "/ws"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return "ws://" + addr + path
}

// wsTransport 基于 gorilla/websocket 的消息边界传输：
// 读侧 ReadMessage 取整帧后 frame.Decode；写侧 frame.Encode 整帧单次 WriteMessage。
type wsTransport struct {
	conn    *websocket.Conn
	writeMu sync.Mutex // gorilla 约定写侧单 goroutine；与内核写锁双保险
}

// ReadFrame 读一条 WS 消息并解析为帧。读超上限（SetReadLimit）与 TCP 侧
// 「bodyLen 超限」同语义：协议违规（不可重试终止），而非网络故障。
func (t *wsTransport) ReadFrame(maxBodySize int) (frame.Header, []byte, error) {
	_, msg, err := t.conn.ReadMessage()
	if err != nil {
		if errors.Is(err, websocket.ErrReadLimit) {
			return frame.Header{}, nil, fmt.Errorf("websocket: %w（%w）", err, frame.ErrProtocol)
		}
		return frame.Header{}, nil, err
	}
	return frame.Decode(msg, maxBodySize)
}

// WriteFrame 编码整帧并以单条二进制消息发送（WS 无粘包：一条消息 = 一个完整帧）。
func (t *wsTransport) WriteFrame(h frame.Header, body []byte, maxBodySize int) error {
	buf, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.conn.WriteMessage(websocket.BinaryMessage, buf)
}

// Close 关闭底层 WS 连接。
func (t *wsTransport) Close() error { return t.conn.Close() }
