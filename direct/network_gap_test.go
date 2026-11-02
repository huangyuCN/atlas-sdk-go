package direct

import (
	"testing"
	"time"
)

// notifyWithGapString 是带 max_network_gap_ms 的成局通知：protojson 的 int64 按约定下发为字符串。
const notifyWithGapString = `{
  "battle_id": "b-1",
  "battle_ticket": "AAECAwQ=",
  "max_network_gap_ms": "30000",
  "endpoints": [{"transport": "EDGE_TRANSPORT_WS", "address": "10.0.0.1:7100"}]
}`

// notifyWithGapNumber 是同一字段以 JSON 数字下发的容错口径（手写/旧工具链可能这么发）。
const notifyWithGapNumber = `{
  "battle_id": "b-1",
  "battle_ticket": "AAECAwQ=",
  "maxNetworkGapMs": 30000,
  "endpoints": [{"transport": "EDGE_TRANSPORT_WS", "address": "10.0.0.1:7100"}]
}`

// TestPlanFromNotifyParsesMaxNetworkGap 验证客户端从成局通知读取服务端的
// 「玩家最多断网多久还能回来」：字符串与数字两种口径都认；旧服务端不带字段 ⇒ 0（未下发）。
func TestPlanFromNotifyParsesMaxNetworkGap(t *testing.T) {
	for _, c := range []struct {
		name    string
		payload string
		want    time.Duration
	}{
		{name: "字符串口径", payload: notifyWithGapString, want: 30 * time.Second},
		{name: "数字口径", payload: notifyWithGapNumber, want: 30 * time.Second},
		{name: "旧服务端未下发", payload: notifySnake, want: 0},
	} {
		plan, err := PlanFromNotify([]byte(c.payload))
		if err != nil {
			t.Fatalf("%s：PlanFromNotify: %v", c.name, err)
		}
		if plan.MaxNetworkGap != c.want {
			t.Fatalf("%s：Plan.MaxNetworkGap = %s, 期望 %s", c.name, plan.MaxNetworkGap, c.want)
		}
	}
}

// TestApplyPlanDefaultsUsesNotifyMaxNetworkGap 验证缺省 T2 从通知值推导，且显式选项优先：
// 通知 30s ⇒ T2 = 45s（晚于服务端 30s 的会话静默窗口）；未下发 ⇒ 保持 15s 缺省；
// 显式 WithIdleDeadline ⇒ 用显式值（逃生阀）。
func TestApplyPlanDefaultsUsesNotifyMaxNetworkGap(t *testing.T) {
	o := applyPlanDefaults(Plan{MaxNetworkGap: 30 * time.Second}, defaultOptions())
	if o.idleDead != 45*time.Second {
		t.Fatalf("通知 30s：T2 = %s, 期望 45s（gap × 1.5）", o.idleDead)
	}

	o = applyPlanDefaults(Plan{}, defaultOptions())
	if o.idleDead != defaultIdleDead {
		t.Fatalf("未下发：T2 = %s, 期望缺省 %s", o.idleDead, defaultIdleDead)
	}

	o = defaultOptions()
	WithIdleDeadline(20 * time.Second)(&o)
	o = applyPlanDefaults(Plan{MaxNetworkGap: 30 * time.Second}, o)
	if o.idleDead != 20*time.Second {
		t.Fatalf("显式 T2 被通知值覆盖：%s, 期望 20s（显式优先）", o.idleDead)
	}
}

// TestClientIdleDeadlineStaysAboveServerWindow 验证 T2 永远大于两个下界：
// 服务端的会话静默窗口（= max_network_gap，客户端先重建会让无感切网失效）与 T1（0<T1<T2 校验）。
func TestClientIdleDeadlineStaysAboveServerWindow(t *testing.T) {
	for _, gap := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 30 * time.Second, 300 * time.Second} {
		got := clientIdleDeadline(gap, defaultIdleSwitch)
		if got <= gap {
			t.Fatalf("gap=%s：T2 = %s，客户端会先于服务端拆会话（必须 > gap）", gap, got)
		}
		if got <= defaultIdleSwitch {
			t.Fatalf("gap=%s：T2 = %s，不满足 0<T1<T2（T1=%s）", gap, got, defaultIdleSwitch)
		}
	}
}
