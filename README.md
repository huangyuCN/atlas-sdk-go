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
| KCP | 战斗帧旧通道（阶段 3 起改走 `direct` 直连；保留联调/压测） | `DialKCP` |
| UDP | 战斗帧旧通道（阶段 3 起改走 `direct` 直连；保留联调/压测） | `DialUDP` |
| TCP/WS + KCP/UDP 组合 | dual 形态：业务 + 战斗双通道（旧形态，同上） | `DialDual` |

> **阶段 3 起战斗帧改走直连**：成局后客户端凭「接入层地址 + 战斗票据」直连接入层
> （KCP/UDP/WS 三面，见「战斗直连」一节），战斗帧不再经过网关。上表的 KCP/UDP 通道与
> `DialDual` 保留给旧形态、联调与压测；生产路径的业务 op（登录/会话/匹配）仍走 `client` 通道。

## 特性

- **四通道矩阵**：TCP（流式分帧）、WebSocket（一条消息 = 一个完整帧）、KCP（kcp-go
  可靠 UDP，会话参数与服务端基线对齐）、UDP（一报一帧，单数据报上限 64KiB 含帧头，
  坏数据报静默丢弃）。KCP/UDP 无连接关闭通知，死链由传输心跳发现。
- **战斗直连（阶段 3，`direct` 包）**：成局推送给「战斗票据 + 三面接入层地址」，
  按面直连接入层，逐帧带票跑战斗 op（入局/输入/补帧），带保活探针、断线重连重放、
  **终态语义**（对局结束/不存在/满员/目标不符：可判定哨兵 + 停发 + 2s 收尾窗口）与
  `Stats()` 观测快照。
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
go get github.com/huangyuCN/atlas-sdk-go@v0.7.0
```

要求 Go 1.26+。依赖情况：
- `frame`（协议层）：零第三方依赖（手写 wire 解码，字节级 golden 校验）。
- `client`（编排器）：WebSocket / KCP 通道库（[gorilla/websocket](https://github.com/gorilla/websocket)、
  [kcp-go v5](https://github.com/xtaci/kcp-go)）+ 默认序列化器所需的
  [protobuf-go](https://github.com/protocolbuffers/protobuf-go)
  （v0.6 起：默认 `ProtoJSONSerializer` 对 proto message 走官方 protojson）。

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

	// 订阅服务端推送（handler 在独立 goroutine 执行；op 名用生成物常量，不要手写）。
	off := c.OnAny(func(op string, payload []byte) {
		fmt.Println("收到推送:", op, len(payload), "bytes")
	})
	defer off()

	// 会话经接缝：op 名与凭据提取全部取自生成物（见「生成物来源」一节）。
	// 接缝的 Kicked(op, msg) 收到 client.PushEnvelope（op + 帧头 version + 原始字节）：
	// ver=1 走 protojson、ver=2 走 protobuf wire，实现方按 Version 选解码器。
	sess := client.NewSession(
		client.WithSessionProtocol(sessionProtocol{}), // 项目侧接入（见 examples/smoke/protocol.go）
		client.WithSessionHeartbeatInterval(30*time.Second),
	)
	if err := sess.Bind(c); err != nil { // 未注入接缝即 ErrNoSessionProtocol
		panic(err)
	}
	defer sess.Close() // 退订推送并解绑（重复 Bind 会先退订旧订阅）
	reply, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p1", Password: "***"})
	if err == nil {
		fmt.Println("登录成功:", reply.GetPlayer().GetPlayerId())
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

### 战斗直连（阶段 3：KCP/UDP/WS 直连接入层）

阶段 3 起**战斗帧不再经过网关**：成局后客户端凭「接入层地址 + 战斗票据」直连接入层
（KCP/UDP/WS 三面），接入层按 `battle_id` 定位战斗属主节点并做 L4 转发；业务 op
（登录/会话/匹配）仍走上面的 `client` 通道。完整闭环示例见
[examples/directloop](examples/directloop/)。

**票据与地址的唯一来源是本局成局推送**（不读本地配置、不猜端口）：

```go
// 收到成局推送（Notify 帧，op = direct.PushOpMatchStarted）后解析直连计划：
plan, err := direct.PlanFromPush(op, payload) // 等价于 direct.PlanFromNotify(payload)
// plan.Ticket    ← 推送里的 battle_ticket（AEAD 密文；接入层与 battle 共持密钥验票）
// plan.Endpoints ← 推送里的 endpoints[]：{transport, address} 各面接入层地址（可只下发部分面）

sess, err := direct.Open(ctx, plan, direct.WithTransport(direct.TransportKCP))
if err != nil {
	// 缺面 / 缺票 / 被接入层拒绝：见下方错误表；不重试、不猜端口
}
defer sess.Close()

sess.OnFrame(func(fb *battlev1.FrameBroadcast) { /* 帧广播 */ })
sess.OnBattleEnd(func(n *battlev1.BattleEndNotify) { /* 结算（回调恰一次） */ })
_, err = sess.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: plan.BattleID})
err = sess.SendFrameInput(ctx, &battlev1.FrameInputReq{BattleId: plan.BattleID, Input: in})
```

**三面地址选择**：`direct.WithTransport(direct.TransportWS|TransportKCP|TransportUDP)`
显式指定；不指定则按 **ws → kcp → udp** 取本局推送里第一个存在的面。所指定的面本局未下发
即返回 `direct.ErrTransportNotFound`——**不猜端口、不静默换面**（换面等于换一条链路，
必须由调用方决定）。

**票据来源与重取**：`battle_ticket` 随业务链路的成局推送下发，SDK **不自动重取**
（取票要回匹配/业务链路）。票据被 battle 侧拒绝时返回可判定哨兵
`direct.ErrTicketExpired` / `direct.ErrTicketInvalid`（业务拒绝原文仍可用 `errors.As` 取到），
由上层重新匹配取新票——**不重连**（同一张票重连只会再被拒）。

**保活心跳**：缺省 2s 发一次 `battle.v1.BattleService/Ping`（Tell，服务端不回业务回执）。
数据报面（KCP/UDP）靠收包刷新帧面空闲读超时（battle 侧缺省 `offline_timeout/3` = 5s），
静默会被判掉线、NAT 映射也会失效，故周期必须**严格小于**该阈值（`WithHeartbeat` 可调，
`<=0` 关闭）。心跳在**连接就绪**时起表（早于 `JoinBattle`，与 TS/C# 同口径）——探针只证明
链路活着，不参与对局语义；被拒的处置见下。

**断线重连**：缺省自动重连（退避 ×2 封顶），重连成功后自动重放 `JoinBattle` +
`SyncFrames(last_seen_frame)` 补断点；被接入层拒绝（`direct.ErrRejected`）与票据失效**不重试**。

**结束语义（终态）**：对局结束 / 对局不存在 / 入局被拒 / 请求目标不符时，会话进入**终态**
（`Terminal()` 为真）：一切上发（业务帧 + 探针）被拒并返回终态族哨兵，不再重连。终态分两种，
**「有没有结算可展示」是分界线**（跨 SDK 同一裁定）：

- **正常结束 `ended`**（`Ended()` 为真、`State() == direct.StateEnded`）：收到结束通知或
  `BATTLE_ENDED` 拒绝——有结算可展示；收尾窗口（缺省 **2s**，`WithEndLinger` 可调）内**仍可读**
  推送（结算结果会被服务端有界补投），窗口到期由客户端关连接（数据报面没有关闭握手）。
- **终态失败 `failed`**（`Failed()` 为真、`State() == direct.StateFailed`）：对局不存在 / 已满 /
  请求目标不符——**无结算可展示**，没有结果要等，故置终态即回收连接（不等 2s 窗口）；
  上层据此**回匹配链路重新开局**，而不是去取一份不存在的结算。
  **2s 收尾窗口只服务 `ended` 族**：`failed` 族没有结算可等，留窗口只会白占连接（勿"补上"）。

| 终态族哨兵（`errors.Is` 判定） | reason / code | 语义 | 终态 |
|---|---|---|---|
| `direct.ErrBattleEnded` | `BATTLE_ENDED` / 409（3003） | 对局已结束（结束通知，或迟到帧 op 被稳定拒绝） | `ended` |
| `direct.ErrBattleNotFound` | `BATTLE_NOT_FOUND` / 404（3001） | 对局不存在 | `failed` |
| `direct.ErrBattleFull` | `BATTLE_FULL` / 409（3002） | 入局被拒（对局已满） | `failed` |
| `direct.ErrFrameTargetMismatch` | `FRAME_TARGET_MISMATCH` / 403 | 票面对局与请求正文目标不一致 | `failed` |

四条都**不可重试、终态化并上报**：错误链上保留 `*client.BusinessError`
（`errors.As` 取 code/reason/metadata，`Class` 恒为 business），`IsTerminal(err)` 一处判定整个族，
`EndCause()`/`EndReason()` 给同一份上报；进入终态时**在途请求立即以终态 Status 结算**
（不等回执也不等超时，metadata 标 `x-atlas-sdk-local-settled=true`，与真实回执同构），
调用方一处判定即可收尾。

**观测**：`Stats()` 返回只读快照（重连/握手/心跳的计数与最近错误，零依赖）；
`HeartbeatErr()` 给探针**首个**被拒原因（只记一次，后续仅计数）。心跳被拒按 reason 分类处置：
终态类 → 入终态并停探针；票类（`ErrTicketExpired`/`ErrTicketInvalid`）→ 不终态、上报
「需重新取票」并继续探测；其它业务拒绝 → 计数 + 暴露、继续探测；协议非法 → 计数 + 上报、
**继续探测**（只影响当拍，换代后自动恢复——探针停了就永久失效，静默会被判掉线）。

**直连面载荷编码固定 ver=1（protojson）**：`direct` 不暴露序列化器插槽，请求帧恒按 ver=1
发出，响应帧版本按 ver=1 严格校验（不符即协议级失败，连接断开后按重连策略处理）——服务端帧
引擎按请求版本原样回显，版本不符只可能是拨错了帧面或协议缺陷；与 TS（按序列化器推导校验）、
C#（`FrameGen.Version` 校验）同一口径，不做「猜编码再解码」的容忍。

| 直连 Option | 默认 | 说明 |
|---|---|---|
| `direct.WithTransport(t)` | ws → kcp → udp 取首个下发面 | 显式指定接入层面 |
| `direct.WithHeartbeat(d)` | 2s | 保活探针周期（`<=0` 关闭）；须 < 服务端 `offline_timeout/3` |
| `direct.WithEndLinger(d)` | 2s | 终态收尾窗口（`<=0` = 进入终态即关连接） |
| `direct.WithInvokeTimeout(d)` | 10s | 单次战斗 op 超时 |
| `direct.WithHandshakeTimeout(d)` | 3s | 握手段超时（WS 升级 / hello 等 flow-id） |
| `direct.WithReconnectBackoff(base, max)` | 500ms / 10s | 重连退避（×2 封顶） |
| `direct.WithAutoReconnect(b)` | true | 断线自动重连开关 |
| `direct.WithoutEdgeHello()` | — | 关闭接入层 hello（直连 battle 帧端口，仅联调/闭环验证用） |

## 错误处理

`Invoke` 返回的错误分四类，按类型决定处理策略：

| 类型 | 含义 | 建议 |
|------|------|------|
| `*client.BusinessError` | 服务端业务拒绝 | 按 `Reason` 分支；辅助函数 `client.IsBusinessError(err, "REASON")` |
| `*client.NetworkError` | 连接断开、写失败 | 可重试（重连后） |
| `*client.TimeoutError` | 请求超时 | 谨重重试（请求可能已到达服务端） |
| `*client.ProtocolError` | 帧解码/包络非法 | 不可重试，需排查两端版本 |
| `client.ErrSessionReplyUnresolved` | 会话回执未解析成已注册的生成 DTO | 项目二进制须链接生成的会话 DTO 包（`api/gateway/v1`） |
| `client.ErrSessionCredentialsEmpty` | 回执解析成功但关键凭据为空 | 登录/注册至少要有 token 或 playerId；恢复必须有 playerId |

```go
var be *client.BusinessError
if errors.As(err, &be) {
	// 业务分支：be.Reason / be.Code / be.Metadata
}
```

直连会话（`direct`）的错误同样是这四类，另有三组可判定哨兵：**终态族**
（`ErrBattleEnded` / `ErrBattleNotFound` / `ErrBattleFull` / `ErrFrameTargetMismatch`：
不可重试、终态化并上报）、**票据类**（`ErrTicketExpired` / `ErrTicketInvalid`：回业务链路
重新取票，不重连）、**接入层拒绝**（`ErrRejected`：无应用层回执即断开，不重试）；
计划解析类为 `ErrNotifyNoTicket` / `ErrNotifyNoEndpoint` / `ErrTransportNotFound`。
详见「战斗直连」一节。

## 配置项

| Option | 默认 | 说明 |
|--------|------|------|
| `WithHeartbeatInterval(d)` | 30s | 传输心跳周期；连续 3 次失败判定死链；`≤0` 关闭（每通道独立） |
| `WithInvokeTimeout(d)` | 10s | 请求默认超时（可 per-call 覆盖） |
| `WithMaxBodySize(n)` | 2MiB | 单帧 body 上限（需与服务端对齐，单端调大有断连风险） |
| `WithSerializer(s)` | `ProtoJSONSerializer`（双通道） | 序列化插槽；默认对 proto message 走官方 protojson（零值省略）、非 proto 回退 encoding/json。显式传 `contrib/protobuf.Serializer` 切 ver=2 二进制 |
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
  "ATLS"   =1    1=请求            单调递增   ≤2MiB      长度前缀   如 "/gateway.v1.Session/Login"
                       2=响应
                       3=推送(Notify)
```

- **请求-响应**：客户端发出请求（seq 自行分配），服务端回同 seq 的响应，SDK 按 seq 匹配。
- **服务端推送**：seq 由服务端独立分配，不参与请求匹配，按 operation 分发到订阅者。
- **载荷编码**：payload 为 JSON（protojson 风格），编写 DTO 时注意：

| 规则 | 说明 |
|------|------|
| 字段名 camelCase | `player_id` → protojson camelCase |
| **64 位整数为字符串** | 线上是 `"123"` 而非 `123`——避免精度丢失 |
| **客户端请求零值省略** | v0.6 起默认序列化器（protojson）零值字段省略——三库（Go/TS/C#）统一客户端请求语义；判空不要依赖「字段省略」来表达业务含义 |
| 服务端响应零值下发 | 服务端编码响应时零值字段下发；客户端解码容忍显式零值（protojson 默认接受） |
| 未知字段被忽略 | 客户端解码 DiscardUnknown——服务端加字段不破坏旧客户端 |
| 枚举是字符串名 | 未知枚举值可能以数字出现，DTO 判别不要穷举失败 |
| message 字段未设置为 `null` | proto message 有 presence：nil message 字段在 protojson 下输出 `null`（与标量零值省略不同）——区分未设置 vs 显式零值请用 proto3 optional 或 message 字段 |

### 生成物来源（模板仓 descriptor set）

本 SDK **不携带手写协议副本**：帧常量与域 DTO/会话 stub 全部由生成脚本从**模板仓**
（`atlas-game-layout`）导出的 descriptor set 生成：

```bash
ATLAS_LAYOUT_DIR=../atlas-game-layout ATLAS_DIR=../atlas bash scripts/gen-dto.sh
```

| 产物 | 来源 | 用途 |
|------|------|------|
| `frame/gen/consts_gen.go`、`frame/gen/codec_gen.go` | 逐字节复制框架 `transport/frame/gen/goframe/{consts_gen,codec_gen}.go` | 帧协议常量与编解码（唯一来源；`frame` 包只做转发） |
| `api/gateway/v1/**`、`api/battle/v1/**` 等 | 模板 descriptor set（两个 include 根：模板仓 + 框架仓）→ `--go_out`（M 映射到本仓包） | 域 DTO（`proto.Message`，ver=1 protojson / ver=2 protobuf 共用） |
| `api/*/v1/opclient/*.pb.go` | `--atlas-client_out` + `go_client_package=github.com/huangyuCN/atlas-sdk-go/client` | 强类型客户端 stub 与**协议描述符**（5 会话 op + 3 提取器 + 推送 op） |

> **DTO 直接用生成类型**：`proto.Message` 作 `Invoke` 的 req/resp，默认序列化器自动走
> 官方 protojson（camelCase + int64 字符串 + 零值省略）；非 proto 的 struct/map 仍可
> 作为兼容形态（回退 `encoding/json`，64 位整数须自行用 string 表达）。
> 非 proto 手写 DTO（plain struct / map）仍可用——默认序列化器回退
> encoding/json（v0.6 兼容保留）；但 64 位整数字段须自行用 `string` 表达
> （线上字符串形态），并注意 camelCase json tag。

## 兼容性

当前代码（含 v0.1–v0.7 全部能力）与 atlas 服务端 `feat/actor` 分支（golden
manifest 锁定 commit `40d8e74`）的帧协议对齐，由 22 个字节级 golden 用例校验
（向量源在 [atlas](https://github.com/huangyuCN/atlas) 主仓 `testdata/golden/`，
协议单点；本仓测试消费同一份文件）。服务端协议变更时向量随之更新，保证行为
变更可查。

直连面（`direct`）的载荷编码**固定 ver=1（protojson）**：请求帧恒按 ver=1 发出，响应帧
版本按 ver=1 严格校验（不符即协议级失败）。依据与服务端帧引擎「按请求版本原样回显」的
行为一致，也与 TS/C# 直连会话同口径；直连不提供 ver=2 插槽，将来支持时改
`dispatchResponse` 一处即可（详见「战斗直连」一节）。

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

战斗直连闭环示例：`examples/directloop`（注册/登录 → 匹配 → 按成局推送直连三面
→ 入局/发帧/补帧 → 结算收尾）：

```bash
go run ./examples/directloop -gateway 127.0.0.1:9001 -transports kcp,udp,ws
# 跨机（接入层与 battle 部署在服务器上）：
go run ./examples/directloop -gateway 10.10.9.36:9001 -transports kcp,udp,ws
```

## 路线图

- [x] v0.1：TCP 通道、Invoke/Notify/心跳、golden vectors、错误四分类
- [x] v0.2：断线自动重连（退避 + 排队 + 会话重登钩子 + 连接状态机）
- [x] v0.3：dual 双通道编排、WebSocket 通道
- [x] v0.4：KCP / UDP 通道（四通道矩阵补齐）
- [x] v0.5：载荷编码 ver=2（protobuf 二进制）打样 + `atlas sdk gen` DTO 生成器
  （Go/TS 后端，随 [atlas CLI](https://github.com/huangyuCN/atlas) 交付，不在本仓）
- [x] v0.6：三库官方栈统一——默认序列化器改双通道 protojson（见下方「v0.6 破坏性变更」）
- [x] v0.7：战斗帧直连（阶段 3）——成局后凭票直连接入层（KCP/UDP/WS）+ 保活心跳 +
  结束语义收口（**破坏性**：老客户端须改走 `direct` 直连，见 [CHANGELOG.md](CHANGELOG.md)）

> v0.x 为功能里程碑编号：v0.1–v0.7 均已交付至 main 分支；**自 v0.7.0 起发布 semver Git tag**
> （根模块 tag 直接是 `v0.7.0`，无子目录前缀；v0.1–v0.6 无 tag，能力已含在 v0.7.0 内），
> 消费方按 tag 引用：`go get github.com/huangyuCN/atlas-sdk-go@v0.7.0`。
> 各版本变更要点与破坏性变更见 [CHANGELOG.md](CHANGELOG.md)。

## v0.6 破坏性变更（官方栈统一，2026-09-07）

**背景**：Go/TS/C# 三库序列化语义统一。C#/TS 已用官方 protobuf 栈（DTO 即生成
类型、json 即官方 protojson）；Go v0.6 跟进——默认序列化器从纯 Go JSON 改为
双通道 `ProtoJSONSerializer`，DTO 推荐 protoc-gen-go 产物。

### 变更点

1. **默认序列化器**：`client.Dial` 等构造器默认 `serializer` 从
   `JSONSerializer`（纯 `encoding/json`）改为 `ProtoJSONSerializer`（双通道）。
   - 影响：以 proto message（pb.go）作 req/resp 的调用方，之前需显式
     `WithSerializer(contrib/protojson.Serializer{})`——现在默认即可；
   - 非 proto 类型（plain struct / map）行为不变（回退 `encoding/json`）；
   - 纯手写 DTO + 显式 `WithSerializer(client.JSONSerializer{})` 的调用方
     不受影响（该类型保留）。
2. **客户端请求零值省略**：默认 protojson 编码零值字段省略（此前
   contrib/protojson 为 `EmitUnpopulated` 零值下发）。与 C#（JsonFormatter
   默认）、TS（手写 plain object）一致。服务端解析不受影响（缺失字段 ≡
   零值）；**服务端响应仍零值下发**（服务端语义，客户端解码容忍显式零值）。
3. **DTO 推荐 pb.go**：`atlas sdk gen` 的 Go/TS 后端随官方栈统一退役（atlas
   主仓 sdkgen），DTO 一律 protoc-gen-go（Go）/ protoc-gen-es（TS）生成。
4. **contrib/protojson**：逻辑已并入核心 `client.ProtoJSONSerializer`，
   contrib 子包退役（见 docs/roadmap.md v0.6 增量段）。

### 升级指引

- 已用 pb.go + `WithSerializer(contrib/protojson...)`：删掉该 Option，默认即同语义
  （唯一差异：请求零值从下发改省略——若依赖下发，需显式传自建
  `EmitUnpopulated` serializer）。
- 已用 plain struct / map + 默认：行为不变。
- 已用 `JSONSerializer` 显式 + plain struct：不变（该类型保留）。

TypeScript / C# 版 SDK、跨仓 CI 机器人等生态级后续规划由
[atlas](https://github.com/huangyuCN/atlas) 主仓统一推进，见
[多语言客户端 SDK 设计规范](https://github.com/huangyuCN/atlas/blob/main/docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md)。

## License

[Apache License 2.0](LICENSE)
