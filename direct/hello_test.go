package direct

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/url"
	"testing"
)

// TestEncodeHelloBytes 逐字节断言 hello 段：magic(4)="ATLH" || version(1)=1 || 票长(2,大端) || 票密文。
func TestEncodeHelloBytes(t *testing.T) {
	ticket := []byte{0xde, 0xad, 0xbe, 0xef}
	got := EncodeHello(ticket)
	want := []byte{'A', 'T', 'L', 'H', 0x01, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef}
	if !bytes.Equal(got, want) {
		t.Fatalf("hello = % x, 期望 % x", got, want)
	}
	if len(EncodeHello(nil)) != 7 {
		t.Fatalf("空票 hello 长度 = %d, 期望 7（票长 0）", len(EncodeHello(nil)))
	}
}

// TestEncodeHelloTicketLenBigEndian 验证票长按 2 字节大端写入（300 字节票 → 0x01 0x2C）。
func TestEncodeHelloTicketLenBigEndian(t *testing.T) {
	ticket := bytes.Repeat([]byte{0x5a}, 300)
	got := EncodeHello(ticket)
	if binary.BigEndian.Uint16(got[5:7]) != 300 {
		t.Fatalf("票长字节 = % x（=%d）, 期望 01 2c（300）", got[5:7], binary.BigEndian.Uint16(got[5:7]))
	}
	if got[4] != HelloVersion {
		t.Fatalf("版本字节 = %#x, 期望 %#x", got[4], HelloVersion)
	}
	if len(got) != HelloHeaderLen+len(ticket) {
		t.Fatalf("hello 长度 = %d, 期望 %d", len(got), HelloHeaderLen+len(ticket))
	}
}

// TestTicketSlot 验证帧会话槽取值：base64url 无填充（RawURLEncoding），且服务端按同口径可解回原票。
func TestTicketSlot(t *testing.T) {
	ticket := []byte{0xfb, 0xff, 0x00, 0x01, 0xfe}
	slot := TicketSlot(ticket)
	if want := base64.RawURLEncoding.EncodeToString(ticket); slot != want {
		t.Fatalf("帧槽 = %q, 期望 RawURLEncoding %q", slot, want)
	}
	if bytes.ContainsAny([]byte(slot), "+/=") {
		t.Fatalf("帧槽 %q 含 URL 不安全字符或填充", slot)
	}
	back, err := base64.RawURLEncoding.DecodeString(slot)
	if err != nil || !bytes.Equal(back, ticket) {
		t.Fatalf("服务端口径解码失败: %v, % x", err, back)
	}
}

// TestEdgeWSURLQuery 验证 WS 升级地址：票据放 query `?ticket=<base64url(票密文)>`，路径可配。
func TestEdgeWSURLQuery(t *testing.T) {
	ticket := []byte{0x01, 0x02, 0x03}
	raw, err := EdgeWSURL("10.0.0.1:7100", ticket, "/ws")
	if err != nil {
		t.Fatalf("拼装 WS 地址失败: %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("地址不可解析: %v", err)
	}
	if u.Scheme != "ws" || u.Host != "10.0.0.1:7100" || u.Path != "/ws" {
		t.Fatalf("地址 = %s", raw)
	}
	if got := u.Query().Get("ticket"); got != base64.RawURLEncoding.EncodeToString(ticket) {
		t.Fatalf("query ticket = %q, 期望 %q", got, base64.RawURLEncoding.EncodeToString(ticket))
	}
}

// TestEdgeWSURLPathDefault 验证路径缺省为 "/"（battle 帧面与接入层转发都按该路径放行）。
func TestEdgeWSURLPathDefault(t *testing.T) {
	raw, err := EdgeWSURL("h:1", []byte{0x01}, "")
	if err != nil {
		t.Fatalf("拼装失败: %v", err)
	}
	u, _ := url.Parse(raw)
	if u.Path != "/" {
		t.Fatalf("缺省路径 = %q, 期望 /", u.Path)
	}
}

// TestWrapStripFlowID 验证数据报 flow-id 前缀：8 字节大端，双向加/剥。
func TestWrapStripFlowID(t *testing.T) {
	const id uint64 = 0x0102030405060708
	payload := []byte{0xaa, 0xbb}
	got := WrapFlowID(id, payload)
	want := []byte{1, 2, 3, 4, 5, 6, 7, 8, 0xaa, 0xbb}
	if !bytes.Equal(got, want) {
		t.Fatalf("封装 = % x, 期望 % x", got, want)
	}
	back, err := StripFlowID(got, id)
	if err != nil || !bytes.Equal(back, payload) {
		t.Fatalf("剥离失败: %v, % x", err, back)
	}
	// 剥离必须拷贝：读缓冲逐包复用，返回切片若指向入参会随下一包被覆盖。
	back[0] = 0x00
	if got[flowIDLen] != 0xaa {
		t.Fatalf("剥离应拷贝出载荷，不能指向读缓冲: % x", got)
	}
}

// TestStripFlowIDRejects 验证前缀不符与过短的拒绝：失配即丢弃并要求重新 hello。
func TestStripFlowIDRejects(t *testing.T) {
	good := WrapFlowID(7, []byte{0x01})
	if _, err := StripFlowID(good, 8); !errors.Is(err, ErrFlowIDMismatch) {
		t.Fatalf("flow-id 失配错误 = %v, 期望 ErrFlowIDMismatch", err)
	}
	if _, err := StripFlowID([]byte{1, 2, 3}, 7); !errors.Is(err, ErrFlowIDMismatch) {
		t.Fatalf("短包错误 = %v, 期望 ErrFlowIDMismatch", err)
	}
	empty, err := StripFlowID(WrapFlowID(7, nil), 7)
	if err != nil || len(empty) != 0 {
		t.Fatalf("空载荷剥离 = % x, %v", empty, err)
	}
}
