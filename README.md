# atlas-sdk-go

[![CI](https://github.com/huangyuCN/atlas-sdk-go/actions/workflows/ci.yml/badge.svg)](https://github.com/huangyuCN/atlas-sdk-go/actions/workflows/ci.yml)

Atlas 帧协议的 Go 客户端 SDK。用于游戏客户端、机器人与压测脚本连接
[Atlas](https://github.com/huangyuCN/atlas) 游戏服务端（TCP / WebSocket 长连接），提供
**双通道编排、请求-响应匹配、服务端推送订阅、心跳保活、断线自动重连**等开箱即用的长连接能力。

> v0.3：dual 双通道编排（业务 + 战斗）与 WebSocket 通道。KCP / UDP 通道与 DTO 生成器在路线图（见下文）。

## 特性

- **帧协议编解码**：与服务端 `transport/frame` 同一套线格式（16B 头 + 变长 body），
  粘包/半包由帧头 `bodyLen` 精确切分，校验先于内存分配。
- **双通道编排（dual 形态）**：业务 + 战斗通道各自独立连接、心跳、重连与请求排队
  （规范 §5.2）；`Invoke/On` 默认走业务通道，`Channel(kind)` 提供通道视图；
  `Client.State()` 聚合向下降级，细粒度走 `Channel(kind).State()`。
- **多传输通道**：TCP（流式分帧）与 WebSocket（一条消息 = 一个完整帧，浏览器形态同协议）；
  拨号入口 `Dial` / `DialWS` / `DialDual`。
- **请求-响应匹配**：`seq` 单调递增 + in-flight 表按 `(epoch, seq)` 匹配，超时取消、
  迟到响应静默丢弃、断连统一失败——全部路径恰好一次投递。
- **服务端推送（Notify）**：按 operation 分发到订阅者，handler 独立 goroutine 执行、
  panic 隔离，支持幂等注册与退订。
- **双层心跳**：传输保活（周期 Ping，默认 30s，连续 3 次失败判定死链）+
  **会话心跳（`WithSessionHeartbeat`）**：SDK 在业务通道按周期 Invoke 业务 Heartbeat
  （op/req 由业务闭包提供，携带最新 token），业务错误自动触发重登钩子（规范 §5.2）。
  注意它只保活连接，**不续租业务会话**——会话续租请周期调用业务协议的 Heartbeat
  （如 `/gateway.v1.GatewayAuth/Heartbeat`）。
- **结构化错误**：业务拒绝还原为带 `Code/Reason/Metadata` 的 `BusinessError`；
  网络故障、超时、协议错误各自成类，`errors.Is/As` 判定。
- **断线自动重连**：指数退避（500ms 起 ×2 封顶 30s，带抖动）；重连期间请求排队
  （默认 64 条，重连成功后按序重发，`WithFailFast` 可跳过）；协议级错误终止不重试。
- **连接状态机**：`State()` 返回 `connected / reconnecting / disconnected`，
  重连成功后触发会话重登钩子（`WithOnReconnected`）。
- **协议一致性测试**：`testdata/golden/` 内置 21 个字节级 golden vectors
  （含截断、非法头、64 位整数字符串、负数编码等边界），CI 逐用例校验。

## 协议概览

每个应用层消息封装为一个「帧」。TCP 字节流上按帧头声明的长度切分，天然解决粘包/半包：

```
帧头（16 字节，大端）                              帧 body
┌────────┬──────┬──────┬────────┬───────┬───────────┐   ┌────────────┬──────────────────┐
│ magic 4│ ver 1│ type1│ rsv  2 │ seq 4 │ bodyLen 4 │ + │ opLen 2    │ operation │ payload │
└────────┴──────┴──────┴────────┴───────┴───────────┘   └────────────┴──────────────────┘
  "ATLS"   =1    1=请求            单调递增   ≤2MiB       长度前缀      如 "/gateway.v1.GatewayAuth/Login"
                       2=响应
                       3=推送(Notify)
```

- **请求-响应**：客户端发出 `type=1`（seq 自行分配），服务端回 `type=2`（同 seq），SDK 按 seq 匹配。
- **服务端推送**：`type=3`，seq 由服务端独立分配，不参与请求匹配，按 operation 分发到订阅者。
- **载荷编码**：payload 为 JSON（protojson 规则，字段 camelCase），详见下文「载荷编码约定」。
- **上限**：单帧 body ≤ 2MiB；两端 `MaxBodySize` 配置需对齐，单端调大有断连风险。

## 安装

```bash
go get github.com/huangyuCN/atlas-sdk-go
```

要求 Go 1.26+；唯一第三方依赖为 [gorilla/websocket](https://github.com/gorilla/websocket)（WebSocket 通道）。

## 快速开始

### 单通道（TCP）

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

	// 请求-响应（payload 为 JSON，DTO 字段名 camelCase）。
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

### dual 双通道（业务 TCP + 战斗 WebSocket）

对应模板 dual 形态：业务通道承载登录/会话，战斗通道承载高频帧输入。
两通道独立心跳与重连；业务通道钩子做重登，战斗通道钩子做 Join 重绑定（规范 §5.2）。

```go
c, err := client.DialDual(
	// 业务通道（默认 TCP）：重连成功后由业务侧重登。
	client.ChannelConfig{
		Addr: "127.0.0.1:9001",
		Opts: []client.Option{client.WithOnReconnected(func() error {
			return relogin() // 会话失效，业务层重登
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
				return joinBattle() // 战斗重绑定
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

单通道 WebSocket（浏览器形态同协议）：`client.DialWS("127.0.0.1:9002", "/ws", opts...)`。

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
if errors.Is(err, client.ErrTimeout) {
	// 超时分支
}
```

## API

### Client 方法

| 方法 | 说明 |
|------|------|
| `Dial(addr, opts...) (*Client, error)` | 建立 TCP 长连接（单通道 = 业务通道），启动读循环与心跳 |
| `DialWS(addr, path, opts...) (*Client, error)` | 建立 WebSocket 长连接；`path` 空则 `/ws`；addr 亦可为完整 `ws://` URL |
| `DialDual(business, battle ChannelConfig, opts...) (*Client, error)` | dual 双通道编排：业务 + 战斗通道独立心跳/重连/排队 |
| `Invoke(ctx, op, req, resp any, opts...InvokeOption) error` | 请求-响应（默认业务通道）；`req=nil` 时不带 payload；重连期间默认排队，`WithFailFast()` 立即失败 |
| `On(op string, h NotifyHandler) (off func())` | 订阅推送（默认业务通道）；同一 handler 幂等去重；返回退订函数 |
| `Channel(kind Kind) *ChannelView` | 通道视图：独立 `Invoke/On/State/OnReadExit`；未知 kind 返回 nil；生命周期归 Client（视图不单独 Close） |
| `State() State` | 聚合状态：任一通道非 Connected 即向下降级；细粒度走 `Channel(kind).State()` |
| `OnReadExit(fn func(error))` | 业务通道读循环退出回调（每通道版本见 ChannelView） |
| `Close() error` | 优雅关闭全部通道：取消全部 in-flight（`NetworkError`）、停止心跳与读循环 |

### ChannelConfig 字段

| 字段 | 说明 |
|------|------|
| `Kind` | 通道角色：`KindBusiness`（默认）/ `KindBattle`；DialDual 战斗参数零值按位置推断 |
| `Transport` | 传输类型：`TransportTCP`（默认）/ `TransportWS` |
| `Addr` | 服务端地址 `host:port`；WS 亦接受完整 `ws://`/`wss://` URL（此时 Path 忽略） |
| `Path` | WS 服务端包装路径（空则 `/ws`，对齐模板网关） |
| `Opts` | 本通道覆盖项：在顶层 Option 之后应用（战斗短超时、每通道钩子等） |

### 配置项

| Option | 默认 | 说明 |
|--------|------|------|
| `WithHeartbeatInterval(d)` | 30s | 心跳周期；连续 3 次失败判定死链；`≤0` 关闭（每通道独立） |
| `WithInvokeTimeout(d)` | 10s | 请求默认超时 |
| `WithMaxBodySize(n)` | 2MiB | 单帧 body 上限（只能调小，需与服务端对齐） |
| `WithSerializer(s)` | JSON | 序列化插槽（可替换为二进制实现） |
| `WithAutoReconnect(b)` | true | 断线自动重连开关（每通道独立） |
| `WithBackoff(base, max)` | 500ms/30s | 重连退避参数（×2 封顶 + 抖动） |
| `WithReconnectQueueSize(n)` | 64 | 重连期间请求排队上限（满后立即失败，每通道独立） |
| `WithOnReconnected(fn)` | 无 | 本通道重连成功后的会话钩子（dual 下业务通道配重登、战斗通道配 Join 重绑定）；返回错误则继续退避重试 |

### 并发与生命周期

- `Invoke` / `On` 并发安全；回调在独立 goroutine 执行（panic 隔离），不阻塞读循环。
- `Close` 幂等且会等待内部 goroutine 退出；任何时刻调用都不会死锁。
- 关闭时全部 in-flight 请求立即收到 `NetworkError`。

## 载荷编码约定（编写 DTO 时）

payload 采用 protojson 风格 JSON。手写或生成 DTO 时注意：

| 规则 | 说明 |
|------|------|
| 字段名 camelCase | `player_id` → `json:"playerId"` |
| **64 位整数为字符串** | proto 里的 `uint64/int64` 线上是 `"123"` 而非 `123`——DTO 字段用 `string`，避免精度丢失 |
| 零值字段会下发 | 服务端以 `EmitUnpopulated` 编码请求/响应，DTO 判空不能依赖「字段缺失」 |
| 未知字段被忽略 | 服务端 `DiscardUnknown`：旧 SDK 对新服务端字段向后兼容 |
| 枚举是字符串名 | 未知枚举值可能以数字出现，DTO 判别不要穷举失败 |
| message 字段未设置为 `null` | 仅标量字段保证零值下发 |

## 测试

```bash
make test     # 全量单测（-race）
make update   # 协议用例变更后重新生成 golden vectors
make lint     # gofmt + go vet
```

golden vectors 位于 `testdata/golden/`：每个用例包含输入字节（`input.bin`）、
语言无关的期望结果（`expected.json`）与 manifest 校验（双 sha256 + atlas 基线 commit）。
其他语言实现（TypeScript / C#）应以同一份 vectors 校验解码行为。

## 兼容性

| atlas-sdk-go | 服务端基线 |
|--------------|-----------|
| v0.1–v0.3 | atlas `feat/actor` 分支（golden manifest 锁定 commit `0ba52c0`） |

`feat/actor` 合入 main 后，基线将改为 main 的对应 commit。

## 路线图

- [x] v0.1：TCP 通道、Invoke/Notify/心跳、golden vectors、错误四分类
- [x] v0.2：断线自动重连（退避 + 排队 + 会话重登钩子 + 连接状态机）
- [x] v0.3：dual 形态双通道编排（业务 + 战斗通道）、WebSocket 通道（浏览器 / 单通道形态）
- [ ] v0.4：KCP / UDP 通道
- [ ] v0.5：`atlas sdk gen` DTO 生成器（从游戏项目 proto 生成 Go/TS/C# 客户端 DTO）

## License

MIT
