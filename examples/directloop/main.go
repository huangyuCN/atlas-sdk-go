// directloop 是战斗直连的**服务器闭环驱动**：经网关注册登录入队拿到成局推送（battle_ticket +
// 接入层面地址），再用 SDK 的 direct 包**按生产路径直连接入层**（WS 升级 query 票 / KCP·UDP
// 首包 hello + flow-id）跑 JoinBattle → SendFrameInput → SyncFrames → 收到帧广播，三面各跑一局。
//
// 地址来源：一律取本局推送的 MatchStartedNotify.endpoints（按面取，缺面即报错、不猜端口）；
// -kcp/-udp/-ws 只是**联调地址覆盖**，不改变握手方式。-without-edge-hello 才关闭接入层 hello
// 握手段（直连 battle 帧端口，配合上面的覆盖地址使用），用于回归调试开关。
//
// 票：每局重新匹配取新票（TTL 内即用），故本驱动没有「票过期后重新取票」分支；SDK 侧以
// direct.ErrTicketExpired/ErrTicketInvalid 返回可判定错误，由调用方决定回业务链路重取。
//
// 用法（在能访问接入层地址的机器上跑；接入层面地址是 127.0.0.1:7100/7101/7102）：
//
//	go run ./examples/directloop -gateway 127.0.0.1:9001 -transports kcp,udp,ws
//	go run ./examples/directloop -gateway 127.0.0.1:9001 -transports kcp \
//	  -without-edge-hello -kcp 127.0.0.1:9401
//
// 保活验收：-idle-hold 8s 让 A 在局中只发心跳（不发任何输入），断言 A 未被判出局、仍收帧、
// 静默后还能继续发输入与补帧；对照组加 -no-heartbeat -idle-drop，断言同参数下 A 掉线。
//
//	go run ./examples/directloop -gateway 10.10.9.36:9001 -transports kcp,udp,ws -idle-hold 8s
//	go run ./examples/directloop -gateway 10.10.9.36:9001 -transports kcp,udp \
//	  -idle-hold 8s -no-heartbeat -idle-drop
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/direct"
)

// roundOpts 是一局闭环的参数：网关地址、所选传输面、地址覆盖（空 = 取本局推送）、路径开关、
// 保活验收开关（静默时段/心跳周期/对照组）。
type roundOpts struct {
	gateway     string
	kind        direct.Transport
	addr        string // 接入层地址覆盖（空 = 取本局推送 endpoints；非空仅为联调）
	edgeHello   bool   // 是否走接入层 hello 握手段（生产路径；-without-edge-hello 关闭）
	ruleset     string
	frames      int
	reconnect   bool
	timeout     time.Duration
	idleHold    time.Duration // > 0：A 只发心跳的静默时段（保活验收）
	expectDrop  bool          // 对照组：断言 A 在静默期被判掉线（配 -no-heartbeat）
	heartbeat   time.Duration // 心跳周期覆盖（0 = SDK 缺省 2s）
	noHeartbeat bool          // 关闭心跳（对照组）
}

// main 解析参数并逐面跑闭环，任一面失败即以非 0 退出。
func main() {
	var (
		gateway    = flag.String("gateway", "127.0.0.1:9001", "网关业务通道地址（host:port）")
		kcpAddr    = flag.String("kcp", "", "覆盖 KCP 面的接入层地址（空 = 取本局推送 endpoints）")
		udpAddr    = flag.String("udp", "", "覆盖 UDP 面的接入层地址（空 = 取本局推送 endpoints）")
		wsAddr     = flag.String("ws", "", "覆盖 WS 面的接入层地址（空 = 取本局推送 endpoints）")
		transports = flag.String("transports", "kcp,udp,ws", "要跑的传输面（逗号分隔）")
		ruleset    = flag.String("ruleset", "casual", "匹配规则集")
		frames     = flag.Int("frames", 3, "每玩家发送的输入帧数")
		timeout    = flag.Duration("timeout", 60*time.Second, "单局总超时")
		reconnect  = flag.Bool("reconnect", false, "闭环内额外验证一次显式重连（重放 JoinBattle/SyncFrames）")
		noHello    = flag.Bool("without-edge-hello", false,
			"关闭接入层 hello 握手段（联调：直连 battle 帧端口，须用 -kcp/-udp/-ws 指定帧端口）")
		idleHold  = flag.Duration("idle-hold", 0, "保活验收：A 在局中只发心跳的静默时长（如 8s；0 = 关闭）")
		idleDrop  = flag.Bool("idle-drop", false, "保活对照组：断言 A 在静默期被判掉线（配 -no-heartbeat）")
		heartbeat = flag.Duration("heartbeat", 0, "保活探针周期覆盖（0 = SDK 缺省 2s）")
		noBeat    = flag.Bool("no-heartbeat", false, "关闭 A（静默方）的保活探针；B 保留缺省探针作同局参照")
	)
	flag.Parse()

	overrides := map[direct.Transport]string{
		direct.TransportKCP: *kcpAddr,
		direct.TransportUDP: *udpAddr,
		direct.TransportWS:  *wsAddr,
	}
	for _, name := range strings.Split(*transports, ",") {
		kind, err := direct.ParseTransport(strings.TrimSpace(name))
		if err != nil {
			fmt.Printf("未知传输面: %v\n", err)
			os.Exit(2)
		}
		opts := roundOpts{
			gateway: *gateway, kind: kind, addr: overrides[kind], edgeHello: !*noHello,
			ruleset: *ruleset, frames: *frames, reconnect: *reconnect, timeout: *timeout,
			idleHold: *idleHold, expectDrop: *idleDrop, heartbeat: *heartbeat, noHeartbeat: *noBeat,
		}
		if err := runRound(opts); err != nil {
			fmt.Printf("闭环失败（%s 面）: %v\n", kind, err)
			os.Exit(1)
		}
	}
}

// runRound 跑一局闭环：取票 → 建直连 → 入局/输入/补帧（可选重连）→ 等帧广播。
func runRound(o roundOpts) error {
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	fmt.Printf("[闭环] 传输面=%s 路径=%s 网关=%s\n", o.kind, pathLabel(o.edgeHello), o.gateway)

	players, err := openPlayers(ctx, o.gateway, 2)
	if err != nil {
		return err
	}
	defer closePlayers(players)
	if err := queueAll(ctx, players, o.ruleset); err != nil {
		return err
	}
	plans, err := awaitPlans(ctx, players)
	if err != nil {
		return err
	}
	return battleRound(ctx, o, players, plans)
}

// pathLabel 返回本次建连路径的展示名（生产路径 = 接入层 hello）。
func pathLabel(edgeHello bool) string {
	if edgeHello {
		return "接入层 hello（地址取自本局推送 endpoints）"
	}
	return "绕过接入层（-without-edge-hello：直连 battle 帧端口）"
}
