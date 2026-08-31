// smoke 是 atlas-sdk-go 的真连接冒烟程序：
// 连接真实 gateway 服务端，执行 注册 → 登录 → 业务心跳 → 传输心跳保活，
// 并演练断线自动重连（-reconnect-after 触发等待，由外层脚本重启服务端）。
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

	"github.com/huangyuCN/atlas-sdk-go/client"
)

// 协议常量与消息 DTO（与模板 api/gateway/v1 一致；正式 DTO 将由 atlas sdk gen 生成）。
// 字段名按 protojson 规则 camelCase；int64 线上为字符串（规范 §6.2）。
const (
	opRegister  = "/gateway.v1.GatewayAuth/Register"
	opLogin     = "/gateway.v1.GatewayAuth/Login"
	opHeartbeat = "/gateway.v1.GatewayAuth/Heartbeat"
)

const smokePassword = "pw-123456"

type registerReq struct {
	Account  string `json:"account"`
	Password string `json:"password"`
	Nickname string `json:"nickname"`
}

type registerReply struct {
	PlayerId string `json:"playerId"`
}

type loginReq struct {
	PlayerId string `json:"playerId"`
	Password string `json:"password"`
}

type loginReply struct {
	PlayerId string `json:"playerId"`
	Token    string `json:"token"`
}

type heartbeatReq struct {
	Token    string `json:"token"`
	PlayerId string `json:"playerId"`
	Ts       string `json:"ts"` // int64 → protojson 字符串
}

// dialFn 封装形态差异的拨号入口（TCP / WS / dual）。
type dialFn func(opts ...client.Option) (*client.Client, error)

// smokeOpts 是一次冒烟运行的形态参数。
type smokeOpts struct {
	dial           dialFn
	account        string
	reconnectAfter time.Duration
	// dual 形态专用：战斗通道视图与 Join 重绑定完成信号。
	battleView *client.ChannelView
	rejoined   chan struct{}
}

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
	flag.Parse()

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

// runSingle 单通道冒烟：注册 → 登录 → 业务心跳 →（可选）重连演练 → 传输心跳确认。
// 会话重登钩子：服务端重启后会话丢失，重登由业务层负责（规范 §5.2 双层心跳）；
// SDK 内置会话心跳调度（WithSessionHeartbeat）按周期续租会话。
func runSingle(dial dialFn, account string, reconnectAfter time.Duration) {
	var (
		c           *client.Client
		player      string
		token       string
		rebindCalls atomic.Int32
		reloggedIn  = make(chan struct{})
	)
	var err error
	c, err = dial(append(smokeDialOpts(),
		client.WithOnReconnected(func() error {
			return relogin(c, account, &player, &token, reloggedIn, &rebindCalls)
		}),
		sessionHeartbeatOpt(&player, &token),
	)...)
	if err != nil {
		fail("连接失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	player, token = registerAndLogin(c, account)
	fmt.Printf("[冒烟] 登录成功 playerId=%s token 已存\n", player)

	businessHeartbeats(c, player, token, 3)
	fmt.Println("[冒烟] 业务心跳 3 次往返 OK")

	reconnectDrill(c, reconnectAfter, &rebindCalls, reloggedIn, func() {
		businessHeartbeats(c, player, token, 3)
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

// runDual dual 双通道冒烟（模板 dual 形态）：业务 TCP + 战斗通道（WS 或 KCP）。
// 业务通道重登钩子 + 战斗通道 Join 重绑定钩子各自独立触发与演练。
func runDual(tcpAddr, wsAddr, kcpAddr, wsPath, battleTransport, account string, reconnectAfter time.Duration) {
	battleKind, battleAddr := client.TransportWS, wsAddr
	if battleTransport == "kcp" {
		battleKind, battleAddr = client.TransportKCP, kcpAddr
	}
	var (
		c           *client.Client
		player      string
		token       string
		rebindCalls atomic.Int32
		joinCalls   atomic.Int32
		reloggedIn  = make(chan struct{})
		rejoined    = make(chan struct{})
	)
	var err error
	c, err = client.DialDual(
		client.ChannelConfig{
			Addr: tcpAddr,
			Opts: []client.Option{
				client.WithOnReconnected(func() error {
					return relogin(c, account, &player, &token, reloggedIn, &rebindCalls)
				}),
				sessionHeartbeatOpt(&player, &token), // 会话心跳仅业务通道生效
			},
		},
		client.ChannelConfig{
			Transport: battleKind,
			Addr:      battleAddr,
			Path:      wsPath,
			Opts: []client.Option{client.WithOnReconnected(func() error {
				// 战斗通道重绑定（模板 JoinBattle 语义的冒烟替身）：
				// 在战斗通道上做一次传输心跳往返，证明 WS 通道重连后可用。
				// 注意战斗通道不做业务 Login——网关会话为每玩家单会话，
				// 二次登录会顶掉业务通道会话（规范 §5.2：会话绑定业务通道）。
				joinCalls.Add(1)
				if err := battlePing(c); err != nil {
					fmt.Printf("[冒烟] 战斗通道重绑定失败（随下一轮重连重试）: %v\n", err)
					return err
				}
				fmt.Println("[冒烟] 战斗通道重连后重绑定成功")
				signalOnce(rejoined)
				return nil
			})},
		},
		smokeDialOpts()...,
	)
	if err != nil {
		fail("dual 连接失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	player, token = registerAndLogin(c, account)
	fmt.Printf("[冒烟] 业务通道登录成功 playerId=%s token 已存\n", player)
	battlePing(c)
	fmt.Printf("[冒烟] 战斗通道（%s）传输心跳往返 OK\n", battleKind)

	businessHeartbeats(c, player, token, 3)
	fmt.Println("[冒烟] 业务心跳 3 次往返 OK")

	// 重连演练：等待业务重登与战斗重绑定都完成。
	if reconnectAfter > 0 {
		fmt.Printf("[冒烟] %s 后请重启 gateway（等待双通道自动重连+重登/重绑定）\n", reconnectAfter)
		time.Sleep(reconnectAfter)
		waitSignal(reloggedIn, "业务重登", c, &rebindCalls)
		waitSignal(rejoined, "战斗重绑定", c, &joinCalls)
		businessHeartbeats(c, player, token, 3)
		fmt.Println("[冒烟] 重连后业务心跳 3 次往返 OK")
	}

	assertConnected(c, "dual")
	fmt.Printf("冒烟通过（dual 双通道闭环：业务TCP/战斗%s 独立重连+重登+重绑定）\n", battleKind)
}

// smokeDialOpts 公共拨号参数（冒烟内加速心跳与重连节奏）；会话钩子由各通道单独配置。
func smokeDialOpts() []client.Option {
	return []client.Option{
		client.WithHeartbeatInterval(5 * time.Second), // 冒烟内加速验证传输心跳
		client.WithInvokeTimeout(5 * time.Second),
		client.WithBackoff(200*time.Millisecond, 3*time.Second),
	}
}

// relogin 会话重登：调用方持久化新令牌（player 与 token 均写回闭包变量）。
func relogin(c *client.Client, account string, player, token *string, done chan struct{}, rebindCalls *atomic.Int32) error {
	rebindCalls.Add(1)
	var rep loginReply
	if err := c.Invoke(context.Background(), opLogin, loginReq{
		PlayerId: *player, Password: smokePassword,
	}, &rep); err != nil {
		fmt.Printf("[冒烟] 重连后重登失败（随下一轮重连重试）: %v\n", err)
		return err
	}
	*player, *token = rep.PlayerId, rep.Token
	fmt.Println("[冒烟] 重连后重登成功（新令牌已存）")
	signalOnce(done)
	return nil
}

// battlePing 战斗通道连通性验证：传输心跳往返（服务端引擎内置 handler，不触碰业务会话）。
func battlePing(c *client.Client) error {
	return c.Channel(client.KindBattle).Invoke(context.Background(), client.HeartbeatOperation, nil, nil)
}

// sessionHeartbeatOpt 会话心跳配置（规范 §5.2 业务层；闭包携带最新 token/player，
// 未登录时跳过本轮）。网关会话租期 30s，周期取 2s（远小于 租期/2）。
func sessionHeartbeatOpt(player, token *string) client.Option {
	return client.WithSessionHeartbeat(2*time.Second, func() (string, any) {
		if *token == "" {
			return "", nil // 未登录：跳过
		}
		return opHeartbeat, heartbeatReq{
			Token: *token, PlayerId: *player, Ts: fmt.Sprintf("%d", time.Now().UnixMilli()),
		}
	})
}

// signalOnce 非阻塞通知（容量 1）：钩子多次成功触发（网关反复抖动）时只保留首个信号。
func signalOnce(done chan struct{}) {
	select {
	case done <- struct{}{}:
	default:
	}
}

// registerAndLogin 注册并登录，返回 playerId 与 token。
func registerAndLogin(c *client.Client, account string) (string, string) {
	var reg registerReply
	if err := c.Invoke(context.Background(), opRegister,
		registerReq{Account: account, Password: smokePassword, Nickname: "冒烟玩家"}, &reg); err != nil {
		fail("注册失败: %v", err)
	}
	fmt.Printf("[冒烟] 注册成功 playerId=%s\n", reg.PlayerId)

	var rep loginReply
	if err := c.Invoke(context.Background(), opLogin, loginReq{
		PlayerId: reg.PlayerId, Password: smokePassword,
	}, &rep); err != nil {
		fail("登录失败: %v", err)
	}
	return rep.PlayerId, rep.Token
}

// businessHeartbeats 业务心跳往返（双层心跳的会话续租层；传输心跳由 SDK 周期自动发送）。
func businessHeartbeats(c *client.Client, player, token string, n int) {
	for i := 0; i < n; i++ {
		var hb struct {
			Ok bool `json:"ok"`
		}
		if err := c.Invoke(context.Background(), opHeartbeat, heartbeatReq{
			Token: token, PlayerId: player, Ts: fmt.Sprintf("%d", time.Now().UnixMilli()),
		}, &hb); err != nil {
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
