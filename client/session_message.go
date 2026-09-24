package client

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// 会话消息字段名约定：与模板生成物提取器同源（token / player_id / expires_at），
// 客户端版本字段为 client_version（M1 协议演进，登录/恢复请求复述）。SDK 内核只在
// 「自建恢复/登出请求」与「注入客户端版本」时按这些名字写字段——名字集是接缝共约，
// 不构成对任何会话消息类型的依赖。
const (
	sessionFieldToken         = "token"
	sessionFieldPlayerID      = "player_id"
	sessionFieldClientVersion = "client_version"
)

// sessionMessageForOp 按 op（/<包>.<Service>/<Method>）从 protobuf 全局注册表解析
// 输入/回执消息类型并新建空实例；未注册（手写 DTO、fake 接缝）返回 false。
//
// **项目二进制必须链接生成的会话 DTO 包**（模板仓 api/gateway/v1 的 Go 产物）：会话
// 回执只能按 op 解析成生成 DTO，未链接即解析失败——调用方（invokeSession）据此返回
// ErrSessionReplyUnresolved，绝不用通用载体兜底（否则凭据取到空串却报成功）。
// 依据：生成物 DTO 在包初始化时登记全局注册表，SDK 内核因此无需认识具体会话消息
// 类型即可解码回执（ver=1 protojson 与 ver=2 protobuf 皆可）——协议事实仍只来自
// 接缝提供的 op 名与生成物类型。
func sessionMessageForOp(op string, output bool) (proto.Message, bool) {
	service, method, ok := splitSessionOp(op)
	if !ok {
		return nil, false
	}
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return nil, false
	}
	sd, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, false
	}
	md := sd.Methods().ByName(protoreflect.Name(method))
	if md == nil {
		return nil, false
	}
	target := md.Input()
	if output {
		target = md.Output()
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(target.FullName())
	if err != nil {
		return nil, false
	}
	return mt.New().Interface(), true
}

// splitSessionOp 拆分 op："<包>.<Service>/<Method>" → 服务全名与方法名。
func splitSessionOp(op string) (service, method string, ok bool) {
	trimmed := strings.TrimPrefix(op, "/")
	i := strings.LastIndex(trimmed, "/")
	if i <= 0 || i == len(trimmed)-1 {
		return "", "", false
	}
	return trimmed[:i], trimmed[i+1:], true
}

// newSessionRequest 构造会话请求：op 的输入类型已注册时建具体生成消息并按字段名约定
// 填值；未注册时退化为通用载体 map（protojson 字段名，仅 ver=1 可用；无字段时返回 nil
// 表示空载荷）。map 回退只服务**请求构造**的兼容路径（fake 接缝、未链接生成 DTO 的
// 自建请求），不参与凭据提取——回执一律要求解析成生成 DTO（见 invokeSession）。
func newSessionRequest(op string, fields map[string]string) any {
	if m, ok := sessionMessageForOp(op, false); ok {
		setSessionFields(m, fields)
		return m
	}
	if len(fields) == 0 {
		return nil
	}
	generic := make(map[string]any, len(fields))
	for name, value := range fields {
		generic[jsonNameOf(name)] = value
	}
	return generic
}

// setSessionFields 按字段名约定写消息字段（proto 名与 protojson 名都可命中；非 string
// 字段与重复字段跳过）。
func setSessionFields(m proto.Message, fields map[string]string) {
	if m == nil || len(fields) == 0 {
		return
	}
	ref := m.ProtoReflect()
	for name, value := range fields {
		fd := sessionField(ref.Descriptor(), name)
		if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() || fd.IsMap() {
			continue
		}
		ref.Set(fd, protoreflect.ValueOfString(value))
	}
}

// applyClientVersion 把客户端版本写进会话请求（M1：登录/恢复请求带 client_version）：
// proto 消息按字段写、通用载体（map）按 protojson 字段名写；请求无该字段时静默跳过。
func applyClientVersion(req any) {
	switch r := req.(type) {
	case nil:
		return
	case proto.Message:
		setSessionFields(r, map[string]string{sessionFieldClientVersion: Version})
	case map[string]any:
		r[jsonNameOf(sessionFieldClientVersion)] = Version
	}
}

// sessionField 按约定名查找字段描述符：先 proto 名，再 protojson 名。
func sessionField(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if fd := md.Fields().ByName(protoreflect.Name(name)); fd != nil {
		return fd
	}
	return md.Fields().ByJSONName(jsonNameOf(name))
}

// jsonNameOf 把 proto 字段名转为 protojson 名（snake_case → lowerCamel）。
func jsonNameOf(name string) string {
	parts := strings.Split(name, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] == "" {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}
