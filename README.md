# atlas-sdk-go

[![CI](https://github.com/huangyuCN/atlas-sdk-go/actions/workflows/ci.yml/badge.svg)](https://github.com/huangyuCN/atlas-sdk-go/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Atlas 帧协议的 Go 客户端 SDK。用于游戏客户端、机器人与压测脚本连接
[Atlas](https://github.com/huangyuCN/atlas) 游戏服务端，提供开箱即用的长连接能力：
**请求-响应匹配、服务端推送订阅、心跳保活、断线自动重连、双通道编排**。

支持四种传输通道，与 Atlas 网关的通道形态一一对应：

| 通道 | 典型用途 | 拨号函数 |
|------|---------|---------|
| TCP | 业务通道（登录 / 会话 / 匹配） | `Dial` |
| WebSocket | 浏览器形态的单通道业务+战斗 | `DialWS` |
| KCP | 战斗通道（可靠 UDP，低延迟） | `DialKCP` |
| UDP | 战斗通道（低延迟，尽力而为） | `DialUDP` |
| TCP/WS + KCP/UDP 组合 | dual 形态：业务 + 战斗双通道 | `DialDual` |

## 特性

- **四通道矩阵**：TCP（流式分帧）、WebSocket（一条消息 = 一个完整帧）、KCP（kcp-go
  可靠 UDP，会话参数与服务端基线对齐）、UDP（一报一帧，单数据报上限 64KiB 含帧头，
  坏数据报静默丢弃）。KCP/UDP 无连接关闭通知，死链由传输心跳发现。
- **双通道编排（dual 形态）**：业务 + 战斗通道各自独立连接、心跳、重连与请求排队；
  业务重登成功后自动触发战斗通道重新绑定（Join 语义）。
- **请求-响应匹配**：`seq` 单调递增 + 按连接代次隔离匹配，超时取消、迟到响应静默
  丢弃、断连统一失败——全部路径恰好一次投递。
- **服务端推送（Notify）**：按 operation 分发到订阅者，handler 在独立 goroutine
  执行、panic 隔离，支持幂等注册与退订。
- **双层心跳**：传输保活（周期 Ping，只保活连接、不续租业务会话）+ 可选的
  **会话心跳**（`WithSessionHeartbeat`，仅业务通道）：按业务协议周期续租会话，
  会话过期自动触发重登钩子。
- **断线自动重连**：指数退避（500ms 起 ×2 封顶 30s，带抖动）；重连期间请求排队
  （默认 64 条，重连成功后按序重发）；重连成功后执行会话重登钩子。
- **结构化错误**：业务拒绝还原为带 `Code/Reason/Metadata` 的 `BusinessError`；
  网络故障、超时、协议错误各自成类，`errors.Is/As` 判定。
- **协议一致性**：22 个字节级 golden vectors（含截断、非法头、64 位整数等边界）
  逐用例校验；向量源在 atlas 主仓（规范与向量同仓），各语言 SDK 消费同一份向量，
  行为跨语言一致。

## 安装

```bash
go get github.com/huangyuCN/atlas-sdk-go
```

要求 Go 1.26+。核心包（`client`/`frame`）第三方依赖只有 WebSocket 与 KCP 通道的
两个库（[gorilla/websocket](https://github.com/gorilla/websocket)、
[kcp-go v5](https://github.com/xtaci/kcp-go)）；可选的 protojson 序列化器
（[contrib/protojson](contrib/protojson/)）会额外携带
[protobuf](https://github.com/protocolbuffers/protobuf-go) 依赖，仅 import 该包时参与编译。

## 快速开始

### 连接、请求与推送（TCP）

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-sdk-go/client"
)

func main() {
	c, err := client.Dial("127.0.0.1:9001",
		client.WithHeartbeatInterval(30*time.Second),
		client.WithInvokeTimeout(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// 订阅服务端推送（handler 在独立 goroutine 执行）。
	off := c.On("/gateway.v1.GatewayAuth/Notify", func(op string, payload []byte) {
		fmt.Println("收到推送:", op, string(payload))
	})
	defer off()

	// 请求-响应（payload 为 JSON，字段名 camelCase）。
	var resp struct {
		PlayerId string `json:"playerId"`
		Nickname string `json:"nickname"`
	}
	err = c.Invoke(context.Background(),
		"/gateway.v1.GatewayAuth/Login",
		map[string]any{"playerId": "p1", "password": "***"},
		&resp,
	)
	if err == nil {
		fmt.Println("登录成功:", resp.PlayerId)
		return
	}
	var be *client.BusinessError
	if errors.As(err, &be) && be.Reason == "PLAYER_NOT_FOUND" {
		fmt.Println("玩家不存在")
		return
	}
	panic(err)
}
```

### dual 双通道（业务 TCP + 战斗通道）

业务通道承载登录/会话，战斗通道承载高频帧输入，两通道独立心跳与重连。
`DialDual` 自动链式编排：**业务重登成功后自动触发战斗重绑**；战斗通道自身断线
重连时仅重绑。

```go
c, err := client.DialDual(
	// 业务通道（默认 TCP）：重连成功后由业务侧重登。
	client.ChannelConfig{
		Addr: "127.0.0.1:9001",
		Opts: []client.Option{client.WithOnReconnected(func() error {
			return relogin()
		})},
	},
	// 战斗通道（WebSocket）：重连成功后重新绑定战斗（JoinBattle 语义）。
	client.ChannelConfig{
		Transport: client.TransportWS,
		Addr:      "127.0.0.1:9002",
		Path:      "/ws",
		Opts: []client.Option{
			client.WithInvokeTimeout(time.Second), // 战斗高频短超时
			client.WithOnReconnected(func() error {
				return joinBattle()
			}),
		},
	},
)
if err != nil {
	panic(err)
}
defer c.Close()

// Client 级 Invoke/On 默认走业务通道；战斗通道用视图。
if err := c.Channel(client.KindBattle).Invoke(ctx, "/battle.v1.Battle/Join", req, &resp); err != nil {
	// ...
}
```

单通道 WebSocket：`client.DialWS("127.0.0.1:9002", "/ws")`；
KCP / UDP：`client.DialKCP("127.0.0.1:9003")` / `client.DialUDP("127.0.0.1:9004")`。
完整的可运行示例见 [examples/smoke](examples/smoke/)。

## 错误处理

`Invoke` 返回的错误分四类，按类型决定处理策略：

| 类型 | 含义 | 建议 |
|------|------|------|
| `*client.BusinessError` | 服务端业务拒绝 | 按 `Reason` 分支；辅助函数 `client.IsBusinessError(err, "REASON")` |
| `*client.NetworkError` | 连接断开、写失败 | 可重试（重连后） |
| `*client.TimeoutError` | 请求超时 | 谨重重试（请求可能已到达服务端） |
| `*client.ProtocolError` | 帧解码/包络非法 | 不可重试，需排查两端版本 |

```go
var be *client.BusinessError
if errors.As(err, &be) {
	// 业务分支：be.Reason / be.Code / be.Metadata
}
```

## 配置项

| Option | 默认 | 说明 |
|--------|------|------|
| `WithHeartbeatInterval(d)` | 30s | 传输心跳周期；连续 3 次失败判定死链；`≤0` 关闭（每通道独立） |
| `WithInvokeTimeout(d)` | 10s | 请求默认超时（可 per-call 覆盖） |
| `WithMaxBodySize(n)` | 2MiB | 单帧 body 上限（需与服务端对齐，单端调大有断连风险） |
| `WithSerializer(s)` | JSON | 序列化插槽；[contrib/protojson](contrib/protojson/) 提供可选实现，让 protoc 生成的 proto message 直通 Invoke |
| `WithAutoReconnect(b)` | true | 断线自动重连开关（每通道独立） |
| `WithBackoff(base, max)` | 500ms/30s | 重连退避参数（×2 封顶 + 抖动） |
| `WithReconnectQueueSize(n)` | 64 | 重连期间请求排队上限（满后立即失败） |
| `WithFailFast()` | — | per-call：重连期间不排队、立即失败 |
| `WithOnReconnected(fn)` | 无 | 本通道重连成功后的会话钩子（重登/重绑；超时 10s 视为失败）；dual 下自动链式编排 |
| `WithSessionHeartbeat(interval, opFactory)` | 无 | SDK 内置会话心跳（仅业务通道）：周期调用业务 Heartbeat 续租会话，会话过期自动触发重登钩子；interval 需小于会话租期 |

## API 一览

| 方法 | 说明 |
|------|------|
| `Dial(addr, opts...) (*Client, error)` | 建立 TCP 长连接，启动读循环与心跳 |
| `DialWS(addr, path, opts...) (*Client, error)` | WebSocket 长连接；`path` 空则 `/ws`；addr 亦可为完整 `ws://` URL |
| `DialKCP(addr, opts...) (*Client, error)` | KCP 长连接（明文、无 FEC，参数对齐服务端基线） |
| `DialUDP(addr, opts...) (*Client, error)` | UDP 长连接（面向连接 socket；一报一帧） |
| `DialDual(business, battle ChannelConfig, opts...) (*Client, error)` | dual 双通道编排 |
| `Invoke(ctx, op, req, resp any, opts...) error` | 请求-响应（默认业务通道）；`req=nil` 时不带 payload |
| `On(op, handler) (off func())` | 订阅推送；同一 handler 幂等去重；返回退订函数 |
| `OnReadExit(fn func(error))` | 默认业务通道读循环退出回调（诊断用；自动重连场景通常无需关心） |
| `Channel(kind) *ChannelView` | 通道视图：独立 `Invoke/On/State`；生命周期归 Client |
| `State() State` | 聚合状态 `connected/reconnecting/disconnected`：任一通道非 connected 即向下降级 |
| `Close() error` | 优雅关闭全部通道：取消全部 in-flight（`NetworkError`）、停止心跳与读循环 |

`ChannelConfig` 字段：`Kind`（`KindBusiness`/`KindBattle`）、`Transport`
（`TransportTCP`/`TransportWS`/`TransportKCP`/`TransportUDP`）、`Addr`（`host:port`）、
`Path`（WS 路径，空则 `/ws`）、`Opts`（本通道覆盖项，如战斗通道短超时、每通道钩子）。

并发与生命周期：`Invoke`/`On` 并发安全；回调在独立 goroutine 执行（panic 隔离）；
`Close` 幂等且等待内部 goroutine 退出。

## 协议概览

每个应用层消息封装为一个「帧」。TCP/KCP 字节流上按帧头声明的长度切分，
天然解决粘包/半包：

```
帧头（16 字节，大端）                              帧 body
┌────────┬──────┬──────┬────────┬───────┬───────────┐   ┌──────────┬───────────┬─────────┐
│ magic 4│ ver 1│ type 1│ rsv  2 │ seq 4 │ bodyLen 4 │ + │ opLen 2  │ operation │ payload │
└────────┴──────┴──────┴────────┴───────┴───────────┘   └──────────┴───────────┴─────────┘
  "ATLS"   =1    1=请求            单调递增   ≤2MiB      长度前缀   如 "/gateway.v1.GatewayAuth/Login"
                       2=响应
                       3=推送(Notify)
```

- **请求-响应**：客户端发出请求（seq 自行分配），服务端回同 seq 的响应，SDK 按 seq 匹配。
- **服务端推送**：seq 由服务端独立分配，不参与请求匹配，按 operation 分发到订阅者。
- **载荷编码**：payload 为 JSON（protojson 风格），编写 DTO 时注意：

| 规则 | 说明 |
|------|------|
| 字段名 camelCase | `player_id` → `json:"playerId"` |
| **64 位整数为字符串** | 线上是 `"123"` 而非 `123`——DTO 字段用 `string`，避免精度丢失 |
| 零值字段会下发 | 服务端编码请求/响应时零值字段也下发，判空不能依赖「字段缺失」 |
| 未知字段被忽略 | 旧 SDK 对新服务端字段向后兼容 |
| 枚举是字符串名 | 未知枚举值可能以数字出现，DTO 判别不要穷举失败 |
| message 字段未设置为 `null` | 仅标量字段保证零值下发 |

> 游戏项目的 DTO 无需手写：`atlas sdk gen --lang go` 可从 proto 定义直接生成；
> 若项目已有 protoc-gen-go 生成类型，可换用
> [contrib/protojson](contrib/protojson/) 序列化器直通 `Invoke`
> （默认 JSON 序列化器与 protoc 类型的 json tag 形态不匹配，不可混用）。

## 兼容性

当前代码（含 v0.1–v0.5 全部能力）与 atlas 服务端 `feat/actor` 分支（golden
manifest 锁定 commit `40d8e74`）的帧协议对齐，由 22 个字节级 golden 用例校验
（向量源在 [atlas](https://github.com/huangyuCN/atlas) 主仓 `testdata/golden/`，
协议单点；本仓测试消费同一份文件）。服务端协议变更时向量随之更新，保证行为
变更可查。

## 开发

```bash
make build    # 构建
make test     # 全量单测（-race，含 golden vectors）
make lint     # gofmt + go vet
```

> golden vectors 向量包在 atlas 主仓 `testdata/golden/`（协议用例变更时在主仓
> `go test ./transport/frame -update` 重新生成）。本地测试默认读取与本仓同级的
> `../atlas/testdata/golden`，或用环境变量 `ATLAS_GOLDEN_DIR` 指定。

可运行冒烟示例：`examples/smoke`（支持 `-transport tcp|ws|kcp|udp` 与 `-dual` 形态，
可对接真实网关验证注册/登录/心跳/重连流程）。

## 路线图

- [x] v0.1：TCP 通道、Invoke/Notify/心跳、golden vectors、错误四分类
- [x] v0.2：断线自动重连（退避 + 排队 + 会话重登钩子 + 连接状态机）
- [x] v0.3：dual 双通道编排、WebSocket 通道
- [x] v0.4：KCP / UDP 通道（四通道矩阵补齐）
- [x] v0.5：`atlas sdk gen` DTO 生成器（Go/TS 后端，随 [atlas CLI](https://github.com/huangyuCN/atlas) 交付，不在本仓）

> v0.x 为功能里程碑编号：v0.1–v0.5 均已交付至 main 分支，尚未发布对应的 Git tag，
> `go get` 默认安装 main 分支最新提交。

TypeScript / C# 版 SDK、跨仓 CI 机器人等生态级后续规划由
[atlas](https://github.com/huangyuCN/atlas) 主仓统一推进，见
[多语言客户端 SDK 设计规范](https://github.com/huangyuCN/atlas/blob/main/docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md)。

## License

[Apache License 2.0](LICENSE)
