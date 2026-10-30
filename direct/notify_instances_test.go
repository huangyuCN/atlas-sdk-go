// 逐人 endpoints 用例：多实例接入层按 player_id 哈希选实例，服务端把「该玩家被选中的实例」的
// 面列表写进**票条目**（BattleTicketEntry.endpoints）。SDK 必须优先读自己票条目里的地址，
// 票条目缺失/为空（旧服务端）才回落到回执/事件级 endpoints（= 首实例）。
package direct

import (
	"encoding/base64"
	"testing"
)

// 参考票密文（两玩家各不相同；身份由票本身确定，SDK 不猜、不取第一条）。
const (
	ticketP1 = "AQIDBA==" // 0x01 0x02 0x03 0x04
	ticketP2 = "BQYHCA==" // 0x05 0x06 0x07 0x08
)

// firstInstance 是回执/事件级 endpoints（服务端取**首个实例**的面列表，仅作旧客户端兜底）。
const firstInstance = "10.0.0.1:7100"

// notifyWithEntries 组装一份「带逐人票条目」的成局推送：battle_ticket = 本人那张票，
// endpoints = 回执级兜底列表，tickets = 逐人票条目（各自带自己实例的面列表）。
func notifyWithEntries(ownTicket, entries string) string {
	return `{"match_id":"m-1","battle_id":"b-1","player_ids":["p-1","p-2"],` +
		`"battle_ticket":"` + ownTicket + `",` +
		`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"` + firstInstance + `"}],` +
		`"tickets":[` + entries + `]}`
}

// entriesAB 是两玩家的票条目：p-1 → 实例 A（edge-a），p-2 → 实例 B（edge-b）。
func entriesAB() string {
	return `{"player_id":"p-1","ticket":"` + ticketP1 + `",` +
		`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"edge-a.example.com:7100"}]},` +
		`{"player_id":"p-2","ticket":"` + ticketP2 + `",` +
		`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"edge-b.example.com:7100"}]}`
}

// TestPlanFromNotifyPrefersTicketEntryEndpoints ① 票条目带逐人 endpoints ⇒ 用它
// （而非回执级首实例）：这是多实例真正分散的唯一依据。
func TestPlanFromNotifyPrefersTicketEntryEndpoints(t *testing.T) {
	plan, err := PlanFromNotify([]byte(notifyWithEntries(ticketP1, entriesAB())))
	if err != nil {
		t.Fatalf("PlanFromNotify 失败: %v", err)
	}
	got, err := plan.Endpoint(TransportWS)
	if err != nil {
		t.Fatalf("取 WS 面地址失败: %v", err)
	}
	if want := "edge-a.example.com:7100"; got != want {
		t.Fatalf("WS 面地址 = %q, 期望 %q（票条目里的逐人实例地址，而不是回执级 %q）",
			got, want, firstInstance)
	}
}

// TestPlanFromNotifyFallsBackWithoutTicketEntryEndpoints ② 票条目没有逐人 endpoints
// （旧服务端：无列表 / 条目无面 / 条目面为空 / 列表里没有本人）⇒ 回落回执级列表。
func TestPlanFromNotifyFallsBackWithoutTicketEntryEndpoints(t *testing.T) {
	cases := map[string]string{
		"没有逐人票条目列表": notifyWithEntries(ticketP1, ""),
		"票条目无 endpoints 字段": `{"match_id":"m-1","battle_id":"b-1","battle_ticket":"` + ticketP1 + `",` +
			`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"` + firstInstance + `"}],` +
			`"tickets":[{"player_id":"p-1","ticket":"` + ticketP1 + `"}]}`,
		"票条目 endpoints 为空": `{"match_id":"m-1","battle_id":"b-1","battle_ticket":"` + ticketP1 + `",` +
			`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"` + firstInstance + `"}],` +
			`"tickets":[{"player_id":"p-1","ticket":"` + ticketP1 + `","endpoints":[]}]}`,
		"票条目里没有本人": `{"match_id":"m-1","battle_id":"b-1","battle_ticket":"` + ticketP1 + `",` +
			`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"` + firstInstance + `"}],` +
			`"tickets":[{"player_id":"p-2","ticket":"` + ticketP2 + `",` +
			`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"edge-b.example.com:7100"}]}]}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			plan, err := PlanFromNotify([]byte(payload))
			if err != nil {
				t.Fatalf("PlanFromNotify 失败: %v", err)
			}
			got, err := plan.Endpoint(TransportWS)
			if err != nil {
				t.Fatalf("取 WS 面地址失败: %v", err)
			}
			if got != firstInstance {
				t.Fatalf("WS 面地址 = %q, 期望回执级兜底 %q", got, firstInstance)
			}
		})
	}
}

// TestPlanFromNotifySamePlayerSameAddress ③ 同一玩家多次解析（含条目顺序颠倒）⇒ 同一地址；
// 不同玩家 ⇒ 各自实例（SDK 侧不得「取第一条」，否则玩家会落到别人的实例上）。
func TestPlanFromNotifySamePlayerSameAddress(t *testing.T) {
	payload := notifyWithEntries(ticketP1, entriesAB())
	first, err := PlanFromNotify([]byte(payload))
	if err != nil {
		t.Fatalf("PlanFromNotify 失败: %v", err)
	}
	want, err := first.Endpoint(TransportWS)
	if err != nil {
		t.Fatalf("取 WS 面地址失败: %v", err)
	}
	for i := 0; i < 3; i++ {
		again, err := PlanFromNotify([]byte(payload))
		if err != nil {
			t.Fatalf("第 %d 次解析失败: %v", i+1, err)
		}
		got, err := again.Endpoint(TransportWS)
		if err != nil {
			t.Fatalf("第 %d 次取 WS 面地址失败: %v", i+1, err)
		}
		if got != want {
			t.Fatalf("第 %d 次解析地址 = %q, 期望 %q（同一玩家恒定同址）", i+1, got, want)
		}
	}
	// 同一份票条目列表、换个本人票：必须落到另一个实例（不是条目顺序决定的）。
	other, err := PlanFromNotify([]byte(notifyWithEntries(ticketP2, entriesAB())))
	if err != nil {
		t.Fatalf("p-2 解析失败: %v", err)
	}
	otherAddr, err := other.Endpoint(TransportWS)
	if err != nil {
		t.Fatalf("p-2 取 WS 面地址失败: %v", err)
	}
	if otherAddr != "edge-b.example.com:7100" {
		t.Fatalf("p-2 的地址 = %q, 期望 edge-b.example.com:7100（按票匹配自己的条目）", otherAddr)
	}
	if otherAddr == want {
		t.Fatalf("两玩家解析到同一实例（%q）：逐人选择没有生效", want)
	}
}

// TestPlanFromNotifyTicketEntrySnakeAndCamelKeys 票条目与列表键名两种口径都要认
// （protojson 默认 lowerCamelCase / UseProtoNames 的 proto 名），否则「配了就回落」看不出差别。
func TestPlanFromNotifyTicketEntrySnakeAndCamelKeys(t *testing.T) {
	camel := `{"matchId":"m-1","battleId":"b-1","battleTicket":"` + ticketP1 + `",` +
		`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"` + firstInstance + `"}],` +
		`"battleTickets":[{"playerId":"p-1","ticket":"` + ticketP1 + `",` +
		`"endpoints":[{"transport":"EDGE_TRANSPORT_WS","address":"edge-a.example.com:7100"}]}]}`
	plan, err := PlanFromNotify([]byte(camel))
	if err != nil {
		t.Fatalf("PlanFromNotify 失败: %v", err)
	}
	got, err := plan.Endpoint(TransportWS)
	if err != nil {
		t.Fatalf("取 WS 面地址失败: %v", err)
	}
	if want := "edge-a.example.com:7100"; got != want {
		t.Fatalf("WS 面地址 = %q, 期望 %q", got, want)
	}
	if ticketB64 := base64.StdEncoding.EncodeToString(plan.Ticket); ticketB64 != ticketP1 {
		t.Fatalf("票 = %q, 期望本人那张 %q", ticketB64, ticketP1)
	}
}
