package protojson

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas-sdk-go/frame"
)

// 编译期断言：本包实现满足 client.Serializer 接口。
var _ client.Serializer = Serializer{}

// compact 序列化结果压缩为紧凑 JSON：protojson 的输出含非确定性空白
// （官方行为，劝阻字节级依赖），语义比较须先压缩。
func compact(t *testing.T, b []byte) string {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatalf("输出不是合法 JSON: %v\n原始输出: %s", err, b)
	}
	return buf.String()
}

// loginReplyDesc 进程内组装一个模拟游戏 DTO 的 proto3 message 描述符
// （免 protoc；wrappers/NullValue 等 WKT 在 protojson 下有特殊 JSON 映射，
// 不能代表普通业务 message 的形态）。等价于：
//
//	message LoginReply {
//	  string player_id = 1;  // json_name = playerId
//	  int64  gold      = 2;  // json_name = gold（int64 → 字符串）
//	  Mode   mode      = 3;  // json_name = mode（枚举 → 字符串名）
//	}
//	enum Mode { MODE_UNSPECIFIED = 0; MODE_RANKED = 1; }
func loginReplyDesc(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("game/v1/test.proto"),
		Package: proto.String("game.v1"),
		Syntax:  proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Mode"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("MODE_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("MODE_RANKED"), Number: proto.Int32(1)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("LoginReply"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("player_id"), Number: proto.Int32(1),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					JsonName: proto.String("playerId"),
				},
				{
					Name: proto.String("gold"), Number: proto.Int32(2),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					JsonName: proto.String("gold"),
				},
				{
					Name: proto.String("mode"), Number: proto.Int32(3),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
					TypeName: proto.String(".game.v1.Mode"),
					JsonName: proto.String("mode"),
				},
			},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("组装测试描述符失败: %v", err)
	}
	return fd.Messages().ByName("LoginReply")
}

// newLoginReply 构造 LoginReply 实例并可选用测试数据填充。
func newLoginReply(t *testing.T, md protoreflect.MessageDescriptor, fill bool) *dynamicpb.Message {
	t.Helper()
	m := dynamicpb.NewMessage(md)
	if fill {
		m.Set(md.Fields().ByName("player_id"), protoreflect.ValueOfString("p1"))
		m.Set(md.Fields().ByName("gold"), protoreflect.ValueOfInt64(123))
		m.Set(md.Fields().ByName("mode"), protoreflect.ValueOfEnum(1))
	}
	return m
}

// TestMarshalProtoMessage 验证普通 proto message 走 protojson 编码：
// 字段名 camelCase、int64 → 字符串、枚举 → 字符串名、EmitUnpopulated 零值下发。
func TestMarshalProtoMessage(t *testing.T) {
	md := loginReplyDesc(t)
	var s Serializer

	// 全字段填充：camelCase + int64 字符串形态 + 枚举名。
	got, err := s.Marshal(newLoginReply(t, md, true))
	if err != nil {
		t.Fatalf("Marshal 失败: %v", err)
	}
	want := `{"playerId":"p1","gold":"123","mode":"MODE_RANKED"}`
	if cs := compact(t, got); cs != want {
		t.Errorf("映射规则不符:\n got: %s\nwant: %s", cs, want)
	}

	// 零值 message：EmitUnpopulated 使零值字段也下发（对齐服务端编码）。
	got, err = s.Marshal(newLoginReply(t, md, false))
	if err != nil {
		t.Fatalf("Marshal 失败: %v", err)
	}
	want = `{"playerId":"","gold":"0","mode":"MODE_UNSPECIFIED"}`
	if cs := compact(t, got); cs != want {
		t.Errorf("零值字段应下发:\n got: %s\nwant: %s", cs, want)
	}
}

// TestUnmarshalProtoMessage 验证 protojson JSON 解码回 proto message：
// int64 接受字符串/数值两种形态、枚举接受字符串名、未知字段静默忽略
// （DiscardUnknown，对齐「服务端加字段不破坏旧客户端」）。
func TestUnmarshalProtoMessage(t *testing.T) {
	md := loginReplyDesc(t)
	var s Serializer

	// 字符串形态 int64 + 枚举名 + 未知字段。
	m := dynamicpb.NewMessage(md)
	err := s.Unmarshal([]byte(`{"playerId":"p1","gold":"456","mode":"MODE_RANKED","mystery":true}`), m)
	if err != nil {
		t.Fatalf("Unmarshal 失败: %v", err)
	}
	if got := m.Get(md.Fields().ByName("gold")).Int(); got != 456 {
		t.Errorf("int64 字符串应还原为数值, got: %d", got)
	}
	if got := m.Get(md.Fields().ByName("mode")).Enum(); got != 1 {
		t.Errorf("枚举名应还原为数值, got: %d", got)
	}
	if got := m.Get(md.Fields().ByName("player_id")).String(); got != "p1" {
		t.Errorf("字段应正常解码, got: %q", got)
	}

	// protojson 亦接受数值形态 int64。
	m2 := dynamicpb.NewMessage(md)
	if err := s.Unmarshal([]byte(`{"gold":789}`), m2); err != nil {
		t.Fatalf("Unmarshal 失败: %v", err)
	}
	if got := m2.Get(md.Fields().ByName("gold")).Int(); got != 789 {
		t.Errorf("int64 数值形态应可解码, got: %d", got)
	}
}

// TestFallbackEncodingJSON 验证非 proto 类型回退 encoding/json（如 map 形态请求）。
func TestFallbackEncodingJSON(t *testing.T) {
	var s Serializer
	got, err := s.Marshal(map[string]any{"playerId": "p1"})
	if err != nil {
		t.Fatalf("Marshal 失败: %v", err)
	}
	if cs := compact(t, got); cs != `{"playerId":"p1"}` {
		t.Errorf("非 proto 类型应走 encoding/json, got: %s", cs)
	}

	var m map[string]any
	var fb Serializer
	if err := fb.Unmarshal([]byte(`{"a":1}`), &m); err != nil {
		t.Fatalf("Unmarshal 失败: %v", err)
	}
	if m["a"] != float64(1) {
		t.Errorf("非 proto 类型应走 encoding/json, got: %v", m)
	}
}

// TestProtojsonErrorIsProtocol 验证非法 JSON 解码失败时返回错误
// （client 层会包装为 ProtocolError，序列化器本身只需返回 error）。
func TestProtojsonErrorIsProtocol(t *testing.T) {
	var s Serializer
	err := s.Unmarshal([]byte(`{"gold":`), dynamicpb.NewMessage(loginReplyDesc(t)))
	if err == nil {
		t.Fatal("非法 JSON 应返回错误")
	}
}

// --- 端到端：经真实 Dial/Invoke 链路验证 proto message 直通 ---

// e2eServer 是最小 TCP 服务端替身（与 client 包内测试基建同构但独立复刻）：
// echo 回显 payload；gold 固定回服务端 protojson 产物形态的 JSON。
type e2eServer struct {
	ln net.Listener
	wg sync.WaitGroup
}

func startE2EServer(t *testing.T) *e2eServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	s := &e2eServer{ln: ln}
	s.wg.Add(1)
	go s.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		s.wg.Wait()
	})
	return s
}

func (s *e2eServer) addr() string { return s.ln.Addr().String() }

func (s *e2eServer) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

func (s *e2eServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	for {
		hdr, body, err := frame.Read(conn, frame.MaxBodySize)
		if err != nil {
			return
		}
		if hdr.Type != frame.MsgTypeRequest {
			continue
		}
		op, payload, err := frame.ParseRequestBody(body)
		if err != nil {
			return
		}
		resp := payload
		if op == "/test.gold" {
			// 模拟服务端 protojson 编码产物：camelCase + int64 字符串形态。
			resp = []byte(`{"playerId":"p1","gold":"123","mode":"MODE_RANKED"}`)
		}
		s.reply(conn, hdr.Seq, resp)
	}
}

func (s *e2eServer) reply(conn net.Conn, seq uint32, data []byte) {
	// 成功包络：[hasError=0][dataLen:u32][data...]。
	out := make([]byte, 5+len(data))
	out[1] = byte(len(data) >> 24)
	out[2] = byte(len(data) >> 16)
	out[3] = byte(len(data) >> 8)
	out[4] = byte(len(data))
	copy(out[5:], data)
	_ = frame.Write(conn, frame.Header{Type: frame.MsgTypeResponse, Seq: seq}, out, frame.MaxBodySize)
}

// TestEndToEndProtoMessage 验证完整调用链：Invoke(req proto message) → 帧编码 →
// 服务端回 protojson JSON → resp proto message 还原。请求/响应两端均不写手写 DTO。
func TestEndToEndProtoMessage(t *testing.T) {
	srv := startE2EServer(t)
	md := loginReplyDesc(t)

	c, err := client.Dial(srv.addr(), client.WithSerializer(Serializer{}))
	if err != nil {
		t.Fatalf("Dial 失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 请求：proto message（服务端回显链路，请求与响应同型）。
	req, resp := newLoginReply(t, md, true), dynamicpb.NewMessage(md)
	if err := c.Invoke(ctx, "/test.echo", req, resp); err != nil {
		t.Fatalf("Invoke 失败: %v", err)
	}
	if got := resp.Get(md.Fields().ByName("gold")).Int(); got != 123 {
		t.Errorf("回显不符, gold = %d", got)
	}

	// 响应：服务端发 protojson 形态 JSON（camelCase + int64 字符串），proto message 直接还原。
	resp2 := dynamicpb.NewMessage(md)
	if err := c.Invoke(ctx, "/test.gold", map[string]any{"any": "req"}, resp2); err != nil {
		t.Fatalf("Invoke 失败: %v", err)
	}
	if got := resp2.Get(md.Fields().ByName("gold")).Int(); got != 123 {
		t.Errorf("int64 字符串应还原为 123, got: %d", got)
	}
	if got := resp2.Get(md.Fields().ByName("mode")).Enum(); got != 1 {
		t.Errorf("枚举名应还原, got: %d", got)
	}

	// 默认 JSON 与 proto 混用：map 请求 + map 响应走回退路径。
	var m map[string]any
	if err := c.Invoke(ctx, "/test.echo", map[string]any{"k": "v"}, &m); err != nil {
		t.Fatalf("Invoke 失败: %v", err)
	}
	if m["k"] != "v" {
		t.Errorf("map 回退路径应正常, got: %v", m)
	}
}
