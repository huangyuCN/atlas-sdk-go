// smoke 是 atlas-sdk-go 的真连接冒烟程序：
// 连接真实 gateway 服务端，执行 注册 → 登录 → 业务心跳 → 传输心跳保活，
// 并演练断线自动重连（-reconnect-after 触发等待，由外层脚本重启服务端）。
//
// 协议素材全部取自本仓生成物（scripts/gen-dto.sh 产出）：op 名与凭据提取来自模板仓
// 生成的会话 stub（api/gateway/v1/opclient），会话状态机经 client.WithSessionProtocol
// 接入；战斗绑定走生成的战斗 stub（api/battle/v1/opclient）。冒烟内不写字面量契约。
//
// 形态（TCP / WS / KCP / UDP 单通道 + dual 组合）：
//
//	go run ./examples/smoke -addr 127.0.0.1:9001                    # TCP 单通道
//	go run ./examples/smoke -transport ws -ws-addr 127.0.0.1:9002   # WebSocket 单通道
//	go run ./examples/smoke -dual                                   # dual：业务 TCP + 战斗 WS
//
// 退出码 0 = 冒烟通过；非 0 = 失败。通过时输出「冒烟通过」结尾行。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-sdk-go/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-sdk-go/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-sdk-go/client"
)

// mode 是当前 -serializer 选定的载荷编码模式（main 中设置）。
var mode smokeMode

// dialFn 封装形态差异的拨号入口（TCP / WS / dual）。
type dialFn func(opts ...client.Option) (*client.Client, error)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "gateway TCP 地址")
	transport := flag.String("transport", "tcp", "单通道形态 tcp|ws|kcp|udp")
	wsAddr := flag.String("ws-addr", "127.0.0.1:9002", "gateway WS 地址（-transport ws / -dual 时使用）")
	kcpAddr := flag.String("kcp-addr", "127.0.0.1:9003", "gateway KCP 地址（-transport kcp / -dual -battle-transport kcp 时使用）")
	udpAddr := flag.String("udp-addr", "127.0.0.1:9004", "gateway UDP 地址（-transport udp 时使用）")
	wsPath := flag.String("ws-path", "/ws", "gateway WS 路径")
	battleTransport := flag.String("battle-transport", "ws", "dual 形态战斗通道传输 ws|kcp（-dual 时生效）")
	dual := flag.Bool("dual", false, "dual 双通道编排：业务 TCP + 战斗通道（-battle-transport 指定，默认 WS；忽略 -transport）")
	reconnectAfter := flag.Duration("reconnect-after", 0, "该时长后进入重连演练等待（0 = 不演练）")
	serializer := flag.String("serializer", "json", "载荷编码 json|protojson|protobuf")
	flag.Parse()

	m, ok := parseMode(*serializer)
	if !ok {
		fail("未知 -serializer %q（json|protojson|protobuf）", *serializer)
	}
	mode = m
	fmt.Printf("[冒烟] 载荷编码: %s\n", serializerName(mode))

	account := fmt.Sprintf("smoke-%d", rand.Int63())
	switch {
	case *dual:
		runDual(*addr, *wsAddr, *kcpAddr, *wsPath, *battleTransport, account, *reconnectAfter)
	case *transport == "ws":
		runSingle(func(opts ...client.Option) (*client.Client, error) {
			return client.DialWS(*wsAddr, *wsPath, opts...)
		}, account, *reconnectAfter)
	case *transport == "kcp":
		// KCP/UDP 仅注册战斗协议（模板 D6），走通道验证形态而非认证闭环。
		runBattleChannelSmoke(func(opts ...client.Option) (*client.Client, error) {
			return client.DialKCP(*kcpAddr, opts...)
		}, "KCP", *reconnectAfter)
	case *transport == "udp":
		runBattleChannelSmoke(func(opts ...client.Option) (*client.Client, error) {
			return client.DialUDP(*udpAddr, opts...)
		}, "UDP", *reconnectAfter)
	default:
		runSingle(func(opts ...client.Option) (*client.Client, error) {
			return client.Dial(*addr, opts...)
		}, account, *reconnectAfter)
	}
}

// newSmokeSession 装配冒烟会话：接缝取自模板生成的会话描述符（项目侧一行接入）；
// 自动 Resume 关闭——本冒烟演练的是服务端重启（会话随之丢失）后的业务重登。
// 内置会话心跳周期 2s（远小于网关会话租期 30s）。
func newSmokeSession() *client.Session {
	return client.NewSession(
		client.WithSessionProtocol(sessionProtocol{}),
		client.WithAutoResume(false),
		client.WithSessionHeartbeatInterval(2*time.Second),
	)
}

// dialSession 拨号并绑定会话：冒烟内核参数 + 会话通道选项（凭据提供者/内置心跳）+ 形态
// 特有选项；Bind 订阅全部推送并由接缝判定被挤下线。
func dialSession(dial dialFn, sess *client.Session, extra ...client.Option) (*client.Client, error) {
	opts := append(smokeDialOpts(), sess.ChannelOptions()...)
	opts = append(opts, extra...)
	c, err := dial(opts...)
	if err != nil {
		return nil, err
	}
	if err := sess.Bind(c); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("会话接缝接入失败: %w", err)
	}
	return c, nil
}

// runSingle 单通道冒烟：注册 → 登录 → 业务心跳 →（可选）重连演练 → 传输心跳确认。
// 会话重登钩子：服务端重启后会话丢失，重登由业务层负责（规范 §5.2 双层心跳）；
// SDK 内置会话心跳调度（Session.ChannelOptions）按周期续租会话。
func runSingle(dial dialFn, account string, reconnectAfter time.Duration) {
	var (
		player      string
		rebindCalls atomic.Int32
		reloggedIn  = make(chan struct{})
	)
	sess := newSmokeSession()
	c, err := dialSession(dial, sess, client.WithOnReconnected(func() error {
		return relogin(sess, player, reloggedIn, &rebindCalls)
	}))
	if err != nil {
		fail("连接失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	player, err = registerAndLogin(c, sess, account)
	if err != nil {
		fail("%v", err)
	}
	fmt.Printf("[冒烟] 登录成功 playerId=%s token 已存\n", player)

	businessHeartbeats(sess, 3)
	fmt.Println("[冒烟] 业务心跳 3 次往返 OK")

	reconnectDrill(c, reconnectAfter, &rebindCalls, reloggedIn, func() {
		businessHeartbeats(sess, 3)
		fmt.Println("[冒烟] 重连后业务心跳 3 次往返 OK")
	})

	assertConnected(c, "单通道")
	fmt.Println("冒烟通过（真连接闭环：注册/登录/业务心跳/传输心跳/自动重连+重登）")
}

// runBattleChannelSmoke 战斗协议通道（KCP/UDP）单通道冒烟：
// 网关按用途绑定（模板 D6）——KCP/UDP 仅注册战斗协议、无认证业务 op，
// 本形态只验证通道本身：拨号 → 传输心跳往返 →（可选）重启演练（死链重拨）。
func runBattleChannelSmoke(dial dialFn, form string, reconnectAfter time.Duration) {
	c, err := dial(smokeDialOpts()...)
	if err != nil {
		fail("连接失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 往返探针：Ping（StreamEngine 通道内置）或业务拒绝（如网关 UDP 通道的
	// DatagramEngine 未注册 Ping）均证明往返完成、链路存活；网络类错误才算失败。
	if !probeAlive(c) {
		fail("%s 通道往返探针失败", form)
	}
	fmt.Printf("[冒烟] %s 通道往返探针 OK\n", form)

	// 战斗 payload 编解码验证（三编码统一）：发 JoinBattle（伪造目标）——
	// 服务端按 ver 分派 codec 解码后因会话无效回业务拒绝（BusinessError）即证明
	// payload 编解码正确（协议错误/解码失败才说明编解码问题）。
	{
		err := joinBattle(context.Background(), c)
		if err == nil {
			fail("%s JoinBattle 应被拒绝（伪造会话），却成功", form)
		}
		var be *client.BusinessError
		if !errors.As(err, &be) {
			fail("%s JoinBattle 收到非业务错误 %v（payload 编解码可能失败）", form, err)
		}
		fmt.Printf("[冒烟] %s JoinBattle 业务拒绝（%s 编码解码正确）\n", form, serializerName(mode))
	}

	// 重连演练：重启 gateway 后死链由传输心跳发现，自动重拨恢复。
	if reconnectAfter > 0 {
		fmt.Printf("[冒烟] %s 后请重启 gateway（等待死链重拨）\n", reconnectAfter)
		time.Sleep(reconnectAfter)
		deadline := time.Now().Add(30 * time.Second)
		for {
			if probeAlive(c) {
				break
			}
			if time.Now().After(deadline) {
				fail("等待重拨恢复超时（当前状态 %s）", c.State())
			}
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Println("[冒烟] 重拨后往返探针 OK")
	}

	assertConnected(c, form)
	fmt.Printf("冒烟通过（%s 战斗协议通道：拨号/传输心跳保活/自动重拨）\n", form)
}

// probeAlive 通道存活探针：传输心跳 Invoke 完成「往返」即存活——
// 返回 nil（内置 Ping 成功）或业务拒绝（如网关 UDP 通道未注册 Ping）都算往返完成；
// 网络类错误（超时/断连）视为链路未恢复。
func probeAlive(c *client.Client) bool {
	err := c.Invoke(context.Background(), client.HeartbeatOperation, nil, nil)
	if err == nil {
		return true
	}
	var be *client.BusinessError
	return errors.As(err, &be)
}

// joinBattle 战斗绑定探针：走生成的战斗 stub（op 来自生成描述符）；invoker 由调用方按
// 形态给定（单通道 = 默认业务通道；dual = 战斗通道视图）。
func joinBattle(ctx context.Context, invoker client.Invoker) error {
	_, err := battlev1opclient.NewBattleService(invoker).JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: "smoke-b1"})
	return err
}

// dualRun 是 dual 冒烟运行态：通道、会话与两个重连钩子的共享状态。
type dualRun struct {
	c           *client.Client
	sess        *client.Session
	player      string
	rebindCalls atomic.Int32
	joinCalls   atomic.Int32
	reloggedIn  chan struct{}
	rejoined    chan struct{}
}

// dial 拨号 dual 双通道并绑定会话：业务通道装配重登钩子，战斗通道装配重绑定钩子。
func (d *dualRun) dial(tcpAddr, wsPath string, battleKind client.Transport, battleAddr string) error {
	c, err := client.DialDual(
		client.ChannelConfig{
			Addr: tcpAddr,
			Opts: append(d.sess.ChannelOptions(), client.WithOnReconnected(d.relogin)),
		},
		client.ChannelConfig{
			Transport: battleKind,
			Addr:      battleAddr,
			Path:      wsPath,
			Opts:      []client.Option{client.WithOnReconnected(d.rebindBattle)},
		},
		smokeDialOpts()...,
	)
	if err != nil {
		return err
	}
	d.c = c
	return d.sess.Bind(c)
}

// relogin 业务通道重连后的重登钩子（凭据由 Session 保管）。
func (d *dualRun) relogin() error {
	return relogin(d.sess, d.player, d.reloggedIn, &d.rebindCalls)
}

// rebindBattle 战斗通道重连后的重绑定钩子（模板 JoinBattle 语义的冒烟替身）：在战斗
// 通道上做一次传输心跳往返，证明通道重连后可用。注意战斗通道不做业务 Login——网关
// 会话为每玩家单会话，二次登录会顶掉业务通道会话（规范 §5.2：会话绑定业务通道）。
func (d *dualRun) rebindBattle() error {
	d.joinCalls.Add(1)
	if err := battlePing(d.c); err != nil {
		fmt.Printf("[冒烟] 战斗通道重绑定失败（随下一轮重连重试）: %v\n", err)
		return err
	}
	fmt.Println("[冒烟] 战斗通道重连后重绑定成功")
	signalOnce(d.rejoined)
	return nil
}

// runDual dual 双通道冒烟（模板 dual 形态）：业务 TCP + 战斗通道（WS 或 KCP）。
// 业务通道重登钩子 + 战斗通道 Join 重绑定钩子各自独立触发与演练。
func runDual(tcpAddr, wsAddr, kcpAddr, wsPath, battleTransport, account string, reconnectAfter time.Duration) {
	battleKind, battleAddr := client.TransportWS, wsAddr
	if battleTransport == "kcp" {
		battleKind, battleAddr = client.TransportKCP, kcpAddr
	}
	d := &dualRun{
		sess:       newSmokeSession(),
		reloggedIn: make(chan struct{}),
		rejoined:   make(chan struct{}),
	}
	if err := d.dial(tcpAddr, wsPath, battleKind, battleAddr); err != nil {
		fail("dual 连接失败: %v", err)
	}
	defer func() { _ = d.c.Close() }()

	player, err := registerAndLogin(d.c, d.sess, account)
	if err != nil {
		fail("%v", err)
	}
	d.player = player
	fmt.Printf("[冒烟] 业务通道登录成功 playerId=%s token 已存\n", player)
	battlePing(d.c)
	fmt.Printf("[冒烟] 战斗通道（%s）传输心跳往返 OK\n", battleKind)

	businessHeartbeats(d.sess, 3)
	fmt.Println("[冒烟] 业务心跳 3 次往返 OK")

	if reconnectAfter > 0 {
		dualReconnectDrill(d, reconnectAfter)
	}
	assertConnected(d.c, "dual")
	fmt.Printf("冒烟通过（dual 双通道闭环：业务TCP/战斗%s 独立重连+重登+重绑定）\n", battleKind)
}

// dualReconnectDrill 重连演练：等待业务重登与战斗重绑定都完成后复跑业务心跳。
func dualReconnectDrill(d *dualRun, reconnectAfter time.Duration) {
	fmt.Printf("[冒烟] %s 后请重启 gateway（等待双通道自动重连+重登/重绑定）\n", reconnectAfter)
	time.Sleep(reconnectAfter)
	waitSignal(d.reloggedIn, "业务重登", d.c, &d.rebindCalls)
	waitSignal(d.rejoined, "战斗重绑定", d.c, &d.joinCalls)
	businessHeartbeats(d.sess, 3)
	fmt.Println("[冒烟] 重连后业务心跳 3 次往返 OK")
}

// smokeDialOpts 公共拨号参数（冒烟内加速心跳与重连节奏 + 载荷编码 serializer）；
// 会话钩子由会话通道选项单独配置。
func smokeDialOpts() []client.Option {
	return []client.Option{
		serializerOf(mode),
		client.WithHeartbeatInterval(5 * time.Second), // 冒烟内加速验证传输心跳
		client.WithInvokeTimeout(5 * time.Second),
		client.WithBackoff(200*time.Millisecond, 3*time.Second),
	}
}

// relogin 会话重登：服务端重启后会话丢失，重登由业务层负责；凭据由 Session 保管
// （重登成功即更新令牌）。
func relogin(sess *client.Session, player string, done chan struct{}, calls *atomic.Int32) error {
	calls.Add(1)
	if err := login(context.Background(), sess, player); err != nil {
		fmt.Printf("[冒烟] 重连后重登失败（随下一轮重连重试）: %v\n", err)
		return err
	}
	fmt.Println("[冒烟] 重连后重登成功（新令牌已存）")
	signalOnce(done)
	return nil
}

// battlePing 战斗通道连通性验证：传输心跳往返（服务端引擎内置 handler，不触碰业务会话）。
func battlePing(c *client.Client) error {
	return c.Channel(client.KindBattle).Invoke(context.Background(), client.HeartbeatOperation, nil, nil)
}

// signalOnce 非阻塞通知（容量 1）：钩子多次成功触发（网关反复抖动）时只保留首个信号。
func signalOnce(done chan struct{}) {
	select {
	case done <- struct{}{}:
	default:
	}
}

// businessHeartbeats 业务心跳往返（双层心跳的会话续租层；传输心跳由 SDK 周期自动发送）。
func businessHeartbeats(sess *client.Session, n int) {
	for i := 0; i < n; i++ {
		if err := sessionHeartbeat(context.Background(), sess); err != nil {
			fail("业务心跳失败: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// reconnectDrill 重连演练（单通道）：等待外层脚本重启 gateway，SDK 自动重连 + 重登。
func reconnectDrill(c *client.Client, reconnectAfter time.Duration, rebindCalls *atomic.Int32, reloggedIn chan struct{}, after func()) {
	if reconnectAfter <= 0 {
		return
	}
	fmt.Printf("[冒烟] %s 后请重启 gateway（等待自动重连+重登）\n", reconnectAfter)
	time.Sleep(reconnectAfter)
	waitSignal(reloggedIn, "自动重连+重登", c, rebindCalls)
	after()
}

// waitSignal 等待钩子完成信号，超时输出现场并退出。
func waitSignal(done chan struct{}, what string, c *client.Client, calls *atomic.Int32) {
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		fail("等待%s超时（当前状态 %s，调用 %d 次）", what, c.State(), calls.Load())
	}
}

// assertConnected 最终状态校验（dual 聚合任一通道非 Connected 即失败）。
func assertConnected(c *client.Client, form string) {
	time.Sleep(2 * time.Second) // 传输心跳保活观测窗口
	if c.State() != client.StateConnected {
		fail("最终状态 %s ≠ connected（%s 形态）", c.State(), form)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[冒烟] 失败: "+format+"\n", args...)
	os.Exit(1)
}
