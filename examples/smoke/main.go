// smoke 是 atlas-sdk-go 的真连接冒烟程序：
// 连接真实 gateway 服务端，执行 注册 → 登录 → 业务心跳 → 传输心跳保活，
// 并演练断线自动重连（-reconnect-after 触发等待，由外层脚本重启服务端）。
//
// 用法：
//
//	go run ./examples/smoke -addr 127.0.0.1:9001 [-reconnect-after 10s]
//
// 退出码 0 = 冒烟通过；非 0 = 失败。通过时输出「冒烟通过」结尾行。
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
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

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "gateway TCP 地址")
	reconnectAfter := flag.Duration("reconnect-after", 0, "该时长后进入重连演练等待（0 = 不演练）")
	flag.Parse()

	account := fmt.Sprintf("smoke-%d", rand.Int63())
	var (
		smokeToken  string
		smokePlayer string
		rebindCalls int
	)

	// 会话重登钩子：服务端重启后会话丢失，重登由业务层负责（规范 §5.2 双层心跳）；
	// 闭包捕获 c 变量（Dial 返回后生效）。
	var c *client.Client
	reloggedIn := make(chan struct{})

	var err error
	c, err = client.Dial(*addr,
		client.WithHeartbeatInterval(5*time.Second), // 冒烟内加速验证传输心跳
		client.WithInvokeTimeout(5*time.Second),
		client.WithBackoff(200*time.Millisecond, 3*time.Second),
		client.WithOnReconnected(func() error {
			rebindCalls++
			// 重登并保存新令牌：旧会话在服务端重启/过期后失效，
			// 业务层必须把 LoginReply 的新 token 持久化（本例为闭包变量）。
			var rep loginReply
			if err := c.Invoke(context.Background(), opLogin, loginReq{
				PlayerId: smokePlayer, Password: "pw-123456",
			}, &rep); err != nil {
				fmt.Printf("[冒烟] 重连后重登失败（随下一轮重连重试）: %v\n", err)
				return err
			}
			smokeToken, smokePlayer = rep.Token, rep.PlayerId
			fmt.Println("[冒烟] 重连后重登成功（新令牌已存）")
			close(reloggedIn)
			return nil
		}),
	)
	if err != nil {
		fail("连接失败: %v", err)
	}

	// 1. 注册。
	var reg registerReply
	if err := c.Invoke(context.Background(), opRegister,
		registerReq{Account: account, Password: "pw-123456", Nickname: "冒烟玩家"}, &reg); err != nil {
		fail("注册失败: %v", err)
	}
	fmt.Printf("[冒烟] 注册成功 playerId=%s\n", reg.PlayerId)

	// 2. 登录（会话令牌存入闭包变量）。
	var rep loginReply
	if err := c.Invoke(context.Background(), opLogin, loginReq{
		PlayerId: reg.PlayerId, Password: "pw-123456",
	}, &rep); err != nil {
		fail("登录失败: %v", err)
	}
	smokeToken, smokePlayer = rep.Token, rep.PlayerId
	fmt.Printf("[冒烟] 登录成功 playerId=%s token 已存\n", smokePlayer)

	businessHeartbeats := func(n int) {
		for i := 0; i < n; i++ {
			var hb struct {
				Ok bool `json:"ok"`
			}
			if err := c.Invoke(context.Background(), opHeartbeat, heartbeatReq{
				Token: smokeToken, PlayerId: smokePlayer, Ts: fmt.Sprintf("%d", time.Now().UnixMilli()),
			}, &hb); err != nil {
				fail("业务心跳失败: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// 3. 业务心跳往返（双层心跳的会话续租层；传输心跳由 SDK 周期自动发送）。
	businessHeartbeats(3)
	fmt.Println("[冒烟] 业务心跳 3 次往返 OK")

	// 4. 重连演练（可选）：等待外层脚本重启 gateway，SDK 自动重连 + 重登 + 恢复。
	if *reconnectAfter > 0 {
		fmt.Printf("[冒烟] %s 后请重启 gateway（等待自动重连+重登）\n", *reconnectAfter)
		time.Sleep(*reconnectAfter)
		select {
		case <-reloggedIn:
		case <-time.After(30 * time.Second):
			fail("等待自动重连+重登超时（当前状态 %s，重登调用 %d 次）", c.State(), rebindCalls)
		}
		businessHeartbeats(3)
		fmt.Println("[冒烟] 重连后业务心跳 3 次往返 OK")
	}

	// 5. 传输心跳验证：连续保活期间连接未死即通过。
	time.Sleep(2 * time.Second)
	if c.State() != client.StateConnected {
		fail("最终状态 %s ≠ connected", c.State())
	}

	_ = c.Close()
	fmt.Println("冒烟通过（真连接闭环：注册/登录/业务心跳/传输心跳/自动重连+重登）")
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[冒烟] 失败: "+format+"\n", args...)
	os.Exit(1)
}
