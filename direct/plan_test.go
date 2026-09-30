package direct

import (
	"encoding/base64"
	"errors"
	"testing"
)

// notifySnake 是服务端 UseProtoNames=true 口径的成局推送载荷（proto 字段名，票为带填充标准 base64）。
const notifySnake = `{
  "match_id": "m-1",
  "battle_id": "b-1",
  "player_ids": ["p-1", "p-2"],
  "battle_ticket": "AAECAwQ=",
  "endpoints": [
    {"transport": "EDGE_TRANSPORT_WS", "address": "10.0.0.1:7100"},
    {"transport": "EDGE_TRANSPORT_KCP", "address": "10.0.0.1:7101"},
    {"transport": "EDGE_TRANSPORT_UDP", "address": "10.0.0.1:7102"}
  ]
}`

// notifyCamel 是 protojson 默认口径（lowerCamelCase）的同一份通知：两种键名都必须能解析。
const notifyCamel = `{
  "matchId": "m-1",
  "battleId": "b-1",
  "playerIds": ["p-1", "p-2"],
  "battleTicket": "AAECAwQ=",
  "endpoints": [
    {"transport": "EDGE_TRANSPORT_WS", "address": "10.0.0.1:7100"},
    {"transport": "EDGE_TRANSPORT_KCP", "address": "10.0.0.1:7101"},
    {"transport": "EDGE_TRANSPORT_UDP", "address": "10.0.0.1:7102"}
  ]
}`

// TestPlanFromNotifyParsesSnakeCase 验证 proto 字段名口径的成局推送解析：三面地址、标准 base64 票与战斗标识。
func TestPlanFromNotifyParsesSnakeCase(t *testing.T) {
	plan, err := PlanFromNotify([]byte(notifySnake))
	if err != nil {
		t.Fatalf("PlanFromNotify 失败: %v", err)
	}
	if plan.MatchID != "m-1" || plan.BattleID != "b-1" {
		t.Fatalf("对局标识不符: match=%q battle=%q", plan.MatchID, plan.BattleID)
	}
	if string(plan.Ticket) != "\x00\x01\x02\x03\x04" {
		t.Fatalf("票解码不符: %v", plan.Ticket)
	}
	want := map[Transport]string{
		TransportWS:  "10.0.0.1:7100",
		TransportKCP: "10.0.0.1:7101",
		TransportUDP: "10.0.0.1:7102",
	}
	for tr, addr := range want {
		got, err := plan.Endpoint(tr)
		if err != nil {
			t.Fatalf("面 %s 取地址失败: %v", tr, err)
		}
		if got != addr {
			t.Fatalf("面 %s 地址 = %q, 期望 %q", tr, got, addr)
		}
	}
}

// TestPlanFromNotifyParsesCamelCase 验证 protojson 默认键名口径（lowerCamelCase）同样可解析。
func TestPlanFromNotifyParsesCamelCase(t *testing.T) {
	plan, err := PlanFromNotify([]byte(notifyCamel))
	if err != nil {
		t.Fatalf("PlanFromNotify 失败: %v", err)
	}
	if plan.BattleID != "b-1" || string(plan.Ticket) != "\x00\x01\x02\x03\x04" {
		t.Fatalf("camelCase 载荷解析不符: battle=%q ticket=%v", plan.BattleID, plan.Ticket)
	}
}

// TestPlanFromNotifyAcceptsEnvelope 验证推送信封形态 {type, payload} 的载荷可被解出。
func TestPlanFromNotifyAcceptsEnvelope(t *testing.T) {
	env := `{"type": "/game.v1.PlayerService/MatchStartedNotify", "payload": ` + notifyCamel + `}`
	plan, err := PlanFromNotify([]byte(env))
	if err != nil {
		t.Fatalf("信封形态解析失败: %v", err)
	}
	if plan.BattleID != "b-1" {
		t.Fatalf("信封内 battle_id = %q", plan.BattleID)
	}
}

// TestPlanFromNotifyTicketBase64Variants 验证票的 base64 容错：带填充（protojson 标准）与无填充都能解。
func TestPlanFromNotifyTicketBase64Variants(t *testing.T) {
	raw := []byte{0xff, 0xfe, 0xfd, 0x00}
	cases := map[string]string{
		"带填充（protojson 标准）": base64.StdEncoding.EncodeToString(raw),
		"无填充":               base64.RawStdEncoding.EncodeToString(raw),
	}
	for name, enc := range cases {
		payload := `{"battle_id":"b-1","battle_ticket":"` + enc + `","endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"h:1"}]}`
		plan, err := PlanFromNotify([]byte(payload))
		if err != nil {
			t.Fatalf("%s 解析失败: %v", name, err)
		}
		if len(plan.Ticket) != len(raw) {
			t.Fatalf("%s 票长 = %d, 期望 %d", name, len(plan.Ticket), len(raw))
		}
	}
}

// TestPlanFromNotifyRejectsMissingTicket 验证缺 battle_ticket 字段即报错，不返回空票计划。
func TestPlanFromNotifyRejectsMissingTicket(t *testing.T) {
	payload := `{"battle_id":"b-1","endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"h:1"}]}`
	_, err := PlanFromNotify([]byte(payload))
	if !errors.Is(err, ErrNotifyNoTicket) {
		t.Fatalf("缺票错误 = %v, 期望 ErrNotifyNoTicket", err)
	}
}

// TestPlanFromNotifyRejectsEmptyTicket 验证空票（base64 空串）即报错：空票连不上接入层，
// 不得把「服务端没出票」伪装成「客户端连不上」。
func TestPlanFromNotifyRejectsEmptyTicket(t *testing.T) {
	payload := `{"battle_id":"b-1","battle_ticket":"","endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"h:1"}]}`
	_, err := PlanFromNotify([]byte(payload))
	if !errors.Is(err, ErrNotifyNoTicket) {
		t.Fatalf("空票错误 = %v, 期望 ErrNotifyNoTicket", err)
	}
}

// TestPlanFromNotifyRejectsNoEndpoint 验证缺面报错：不得回退猜端口。
func TestPlanFromNotifyRejectsNoEndpoint(t *testing.T) {
	for name, payload := range map[string]string{
		"无 endpoints 字段": `{"battle_id":"b-1","battle_ticket":"AAECAwQ="}`,
		"endpoints 为空":   `{"battle_id":"b-1","battle_ticket":"AAECAwQ=","endpoints":[]}`,
		"面名未知": `{"battle_id":"b-1","battle_ticket":"AAECAwQ=",` +
			`"endpoints":[{"transport":"EDGE_TRANSPORT_QUIC","address":"h:1"}]}`,
	} {
		_, err := PlanFromNotify([]byte(payload))
		if !errors.Is(err, ErrNotifyNoEndpoint) {
			t.Fatalf("%s 错误 = %v, 期望 ErrNotifyNoEndpoint", name, err)
		}
	}
}

// TestPlanFromNotifyRejectsEmptyAddress 验证面地址为空即报错（缺失的面不会被猜出来）。
func TestPlanFromNotifyRejectsEmptyAddress(t *testing.T) {
	payload := `{"battle_id":"b-1","battle_ticket":"AAECAwQ=","endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":""}]}`
	_, err := PlanFromNotify([]byte(payload))
	if !errors.Is(err, ErrNotifyNoEndpoint) {
		t.Fatalf("空地址错误 = %v, 期望 ErrNotifyNoEndpoint", err)
	}
}

// TestPlanFromNotifyRejectsMalformedJSON 验证非 JSON 载荷报格式错误。
func TestPlanFromNotifyRejectsMalformedJSON(t *testing.T) {
	if _, err := PlanFromNotify([]byte("not-json")); !errors.Is(err, ErrNotifyMalformed) {
		t.Fatalf("非法载荷错误 = %v, 期望 ErrNotifyMalformed", err)
	}
}

// TestPlanEndpointRejectsMissingFace 验证取未下发的面即报错（不猜端口、不静默换面）。
func TestPlanEndpointRejectsMissingFace(t *testing.T) {
	plan := Plan{Endpoints: map[Transport]string{TransportWS: "h:1"}}
	if _, err := plan.Endpoint(TransportKCP); !errors.Is(err, ErrTransportNotFound) {
		t.Fatalf("缺面错误 = %v, 期望 ErrTransportNotFound", err)
	}
}

// TestParseTransport 验证枚举名字符串与短名都能解析为传输面，未知取值报错。
func TestParseTransport(t *testing.T) {
	cases := map[string]Transport{
		"EDGE_TRANSPORT_WS":  TransportWS,
		"EDGE_TRANSPORT_KCP": TransportKCP,
		"EDGE_TRANSPORT_UDP": TransportUDP,
		"ws":                 TransportWS,
		"kcp":                TransportKCP,
		"udp":                TransportUDP,
	}
	for name, want := range cases {
		got, err := ParseTransport(name)
		if err != nil || got != want {
			t.Fatalf("ParseTransport(%q) = %v, %v; 期望 %v", name, got, err, want)
		}
	}
	if _, err := ParseTransport("EDGE_TRANSPORT_UNSPECIFIED"); !errors.Is(err, ErrTransportUnknown) {
		t.Fatalf("未指定面错误 = %v, 期望 ErrTransportUnknown", err)
	}
	if _, err := ParseTransport(""); !errors.Is(err, ErrTransportUnknown) {
		t.Fatalf("空面名错误 = %v, 期望 ErrTransportUnknown", err)
	}
}

// TestTransportString 验证面的文本口径（短名，与 Option/日志一致）。
func TestTransportString(t *testing.T) {
	for tr, want := range map[Transport]string{
		TransportWS: "ws", TransportKCP: "kcp", TransportUDP: "udp",
	} {
		if got := tr.String(); got != want {
			t.Fatalf("Transport(%d).String() = %q, 期望 %q", tr, got, want)
		}
	}
}

// TestPlanFromPushChecksOp 验证按推送 op 解析：op 必须是成局通知的消息完整名，其余推送明确报错。
func TestPlanFromPushChecksOp(t *testing.T) {
	if PushOpMatchStarted != "/game.v1.MatchStartedNotify" {
		t.Fatalf("成局推送 op = %q（须为消息完整名，与生成物同值）", PushOpMatchStarted)
	}
	plan, err := PlanFromPush(PushOpMatchStarted, []byte(notifySnake))
	if err != nil || plan.BattleID != "b-1" {
		t.Fatalf("成局通知解析失败: %v（battle=%q）", err, plan.BattleID)
	}
	if _, err := PlanFromPush("/game.v1.PlayerService/MatchStartedNotify", []byte(notifySnake)); !errors.Is(err, ErrNotifyMalformed) {
		t.Fatalf("服务/方法名口径应被拒: %v", err)
	}
	if _, err := PlanFromPush("/game.v1.MatchFailedNotify", []byte(notifySnake)); !errors.Is(err, ErrNotifyMalformed) {
		t.Fatalf("非成局推送应被拒: %v", err)
	}
}
