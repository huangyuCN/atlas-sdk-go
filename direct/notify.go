package direct

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// notifyFields 是成局推送（MatchStartedNotify）的字段名（proto 名口径）；
// protojson 默认下发 lowerCamelCase，两种键名都要认，故解析时按键名归一比对。
const (
	fieldMatchID   = "match_id"
	fieldBattleID  = "battle_id"
	fieldTicket    = "battle_ticket"
	fieldEndpoints = "endpoints"
	fieldTransport = "transport"
	fieldAddress   = "address"
	fieldType      = "type"
	fieldPayload   = "payload"
)

// PushOpMatchStarted 是成局推送的 op，**消息完整名**（不是服务/方法名；服务/方法名写给
// RPC，推送按消息寻址）。取值与模板生成物
// api/game/v1/opclient.PlayerServicePushOps.MatchStartedNotify 逐字一致；SDK 尚未内置
// game.v1 生成物，故以命名常量收敛唯一字面量（调用方不得手写）。
const PushOpMatchStarted = "/game.v1.MatchStartedNotify"

// PlanFromPush 按推送 op + 载荷解析直连计划：op 不是成局通知即返回 ErrNotifyMalformed，
// 避免把别的推送（对局失败/名册变更等）误当开局计划。
func PlanFromPush(op string, payload []byte) (Plan, error) {
	if op != PushOpMatchStarted {
		return Plan{}, fmt.Errorf("%w: 推送 op %q 不是成局通知（%s）", ErrNotifyMalformed, op, PushOpMatchStarted)
	}
	return PlanFromNotify(payload)
}

// PlanFromNotify 解析成局推送载荷并返回直连计划。
//
// 规范形态：客户端收到的是 **Notify 帧**（op = PushOpMatchStarted 消息完整名），payload 是
// MatchStartedNotify 的 protojson 字节：
//
//	{"match_id":"m-1","battle_id":"b-1","player_ids":[...],
//	 "battle_ticket":"<标准 base64（带填充）>",
//	 "endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"host:port"}, ...]}
//
// 键名两种口径都认（protojson 默认 lowerCamelCase，UseProtoNames 时为 proto 名）。{type,payload}
// 信封只存在于 NATS 事件总线一侧（网关自己解包后转发），SDK 侧**不会**看到；这里仅在载荷恰好
// 形如信封时做防御性解包（规范形态仍是上面的裸 payload）。
//
// 缺票、空票与无可用面（面未知或地址为空）都返回明确错误——不猜端口、不静默换面。
func PlanFromNotify(payload []byte) (Plan, error) {
	fields, err := notifyObject(payload)
	if err != nil {
		return Plan{}, err
	}
	ticket, err := notifyTicket(fields)
	if err != nil {
		return Plan{}, err
	}
	endpoints, err := notifyEndpoints(fields)
	if err != nil {
		return Plan{}, err
	}
	return Plan{
		MatchID:   rawString(fields, fieldMatchID),
		BattleID:  rawString(fields, fieldBattleID),
		Ticket:    ticket,
		Endpoints: endpoints,
	}, nil
}

// notifyObject 解析载荷为归一键名的字段表；推送信封 {type, payload} 取内层 payload。
func notifyObject(payload []byte) (map[string]json.RawMessage, error) {
	fields, err := decodeObject(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotifyMalformed, err)
	}
	inner, ok := fields[normalizeKey(fieldPayload)]
	if !ok || rawString(fields, fieldType) == "" {
		return fields, nil
	}
	var nested map[string]json.RawMessage
	if err := json.Unmarshal(inner, &nested); err != nil {
		return fields, nil // payload 非对象：按裸载荷继续（由缺票/缺面报错兜底）
	}
	return normalizeMap(nested), nil
}

// decodeObject 把 JSON 对象解成归一键名的字段表（非对象即报错）。
func decodeObject(payload []byte) (map[string]json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, fmt.Errorf("载荷不是 JSON 对象")
	}
	return normalizeMap(raw), nil
}

// normalizeMap 归一全部键名（去下划线 + 小写），使 snake_case 与 lowerCamelCase 同键。
func normalizeMap(raw map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(raw))
	for k, v := range raw {
		out[normalizeKey(k)] = v
	}
	return out
}

// normalizeKey 归一单个键名：去掉下划线后小写（battle_ticket 与 battleTicket 同键）。
func normalizeKey(key string) string {
	return strings.ToLower(strings.ReplaceAll(key, "_", ""))
}

// rawString 取字符串字段（缺失或类型不符返回空串）。
func rawString(fields map[string]json.RawMessage, name string) string {
	raw, ok := fields[normalizeKey(name)]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// notifyTicket 取票密文：protojson 的 bytes 是标准 base64（带填充）；无填充的容错也接受。
// 缺失与空票都返回 ErrNotifyNoTicket——空票连不上接入层，不得当作可用票。
func notifyTicket(fields map[string]json.RawMessage) ([]byte, error) {
	raw, ok := fields[normalizeKey(fieldTicket)]
	if !ok {
		return nil, fmt.Errorf("%w: 缺少 %s 字段", ErrNotifyNoTicket, fieldTicket)
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return nil, fmt.Errorf("%w: %s 不是 base64 字符串", ErrNotifyNoTicket, fieldTicket)
	}
	if encoded == "" {
		return nil, fmt.Errorf("%w: %s 为空票", ErrNotifyNoTicket, fieldTicket)
	}
	ticket, err := decodeTicketBase64(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotifyNoTicket, err)
	}
	if len(ticket) == 0 {
		return nil, fmt.Errorf("%w: %s 解出空票", ErrNotifyNoTicket, fieldTicket)
	}
	return ticket, nil
}

// decodeTicketBase64 解码票：先按 protojson 标准（带填充），再容错无填充口径。
func decodeTicketBase64(encoded string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(encoded); err == nil {
		return b, nil
	}
	return base64.RawStdEncoding.DecodeString(encoded)
}

// notifyEndpoints 解析「面 → 地址」列表：面名未知或地址为空的条目被跳过（不算可用面）；
// 一条可用面都没有即返回 ErrNotifyNoEndpoint。
func notifyEndpoints(fields map[string]json.RawMessage) (map[Transport]string, error) {
	raw, ok := fields[normalizeKey(fieldEndpoints)]
	if !ok {
		return nil, fmt.Errorf("%w: 缺少 %s 字段", ErrNotifyNoEndpoint, fieldEndpoints)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("%w: %s 不是数组", ErrNotifyNoEndpoint, fieldEndpoints)
	}
	out := make(map[Transport]string, len(items))
	for _, item := range items {
		transport, addr, ok := endpointItem(item)
		if !ok {
			continue // 未知面/空地址：跳过（缺面由下方兜底报错，绝不猜端口）
		}
		out[transport] = addr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: 列表里没有已知面或地址为空", ErrNotifyNoEndpoint)
	}
	return out, nil
}

// endpointItem 解析单个面条目：返回「面 + 地址」，未知面或空地址返回 ok=false。
func endpointItem(item json.RawMessage) (Transport, string, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(item, &fields); err != nil {
		return 0, "", false
	}
	normalized := normalizeMap(fields)
	transport, err := ParseTransport(rawString(normalized, fieldTransport))
	if err != nil {
		return 0, "", false
	}
	addr := rawString(normalized, fieldAddress)
	if addr == "" {
		return 0, "", false
	}
	return transport, addr, true
}
