# 开发路线与实施指引

> 本文档是下一批次开发的交接说明。协议规范：atlas 主仓
> `docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md`（只读参考，
> README 不依赖它）。服务端对接基准：atlas `feat/actor` 分支（golden manifest
> `atlasCommit` 字段锁定，当前 `0ba52c0`）。

## 当前状态（2026-09-01，v0.5 完成——SDK 侧路线图收官）

- **v0.5 交付**：`atlas sdk gen` DTO 生成器（atlas 主仓 `internal/atlascli/sdkgen` +
  CLI 接线，接管并废弃旧 HTTP 骨架生成器）。
  - 输入 FileDescriptorSet（protoc `--descriptor_set_out` 产物；CLI 不调 protoc、
    不解析 .proto 源文件）；protojson 映射规则（§6.2 逐条单测 + golden 快照 +
    独立模块真编译往返对拍）；WKT 映射表（§6.1）；Reason 常量与 `IsBusinessError`
    与 `api/error/v1` ErrorReason 同源生成（§6.3）。
  - 产物布局按 proto 包分目录（每包一个 Go 子包；跨包引用经 `--go-import-prefix`
    显式导入——模板 gateway.v1/battle.v1 同名 JoinBattleReply 的 e2e 实证决策）；
    TS 侧相对导入天然无冲突，helpers 按目录生成。
  - 语言后端：Go + TypeScript（C# 后置，同一中间模型可扩展）；模板 api/** 端到端
    验证：Go 生成物真实模块 `go build`+`go vet` 全绿，TS 产物 tsc strict 零错误。
  - Go SDK 本仓不受影响（生成物消费本仓的 protojson 形态约定；`--sdk-import`
    默认指向本仓 client 包生成 Reason 判断辅助）。

## 前序状态（2026-08-31，v0.4 完成）

- **v0.4 交付**：KCP / UDP 通道（四通道矩阵补齐）。
  - KCP：`transport_kcp.go`（kcp-go v5.6.72 与服务端同源；明文、无 FEC，
    会话参数对齐服务端 session_config 默认档；`frame.Read/Write` 流式分帧与 TCP 同构；
    拨号后台化支持 ctx 取消）；`DialKCP` / `TransportKCP`；读循环串行保序对齐服务端默认。
  - UDP：`transport_udp.go`（面向连接 `DialUDP`；一报一帧；读缓冲 64KiB 含帧头；
    坏数据报静默丢弃对齐服务端 `ErrBadFrame` 软跳过；写侧拦截超 64KiB 帧；
    body 拷贝出让所有权——Notify payload 会逃逸到 handler goroutine）；
    `DialUDP` / `TransportUDP`。
  - **心跳语义修正**：心跳收到业务拒绝（往返完成）不计入死链——服务端拒绝属
    配置/语义问题而非链路故障（回归测试 `TestHeartbeatBusinessErrorNotDeadLink`）。
  - **服务端偏差已修复**：`NewDatagramEngine`（UDP）曾未像 `NewStreamEngine`
    一样自动注册内置 Ping handler——网关 UDP 通道对 `/atlas.internal.Heartbeat/Ping`
    回业务错误「not registered」。SDK 侧曾以「业务拒绝不计死链」+ 冒烟往返探针
    兼容；atlas feat/actor 已补同款自动注册（`TestDatagramEngineHeartbeatHandlerAutoRegistered`），
    真服务复验 UDP/KCP Ping 均返回 nil（SDK 的兼容语义保留：业务拒绝不计死链对
    任何服务端形态都成立）。
  - KCP/UDP 无连接关闭通知：死链只能由传输心跳发现（KCP 服务端 Close 不通知对端、
    回包可能未冲刷即丢；UDP 无连接）——测试与文档均已明确。
  - 验收全绿：golden vectors 全绿；`-race` 全绿；真服务 KCP（9003）/UDP（9004）
    冒烟通过（往返探针 + 重启演练重拨恢复）；dual 战斗通道 KCP 真服务验证通过。
  - 冒烟：`-transport kcp|udp` 走战斗协议通道验证形态（模板 D6：KCP/UDP 仅注册
    战斗协议，无认证业务 op）；`-dual -battle-transport kcp` 支持模板主 dual 形态。

## 前序状态（2026-08-31，v0.3 完成）

- v0.1/v0.2/v0.3 已完成：帧编解码、golden vectors（21 用例）、TCP 通道、Invoke 匹配、
  传输心跳 + **SDK 内置会话心跳调度（v0.3 评审修复）**、断线自动重连（supervisor 多代连接 +
  状态机 + 请求排队 + 会话重登钩子）、错误四分类、**dual 双通道编排（v0.3）**、
  **WebSocket 通道（v0.3）**、**业务重登成功后自动触发战斗重绑（v0.3 评审修复）**。
- v0.3 交付内容：
  - Channel 抽象拆分：`Client` 为编排器（门面），`channel`（内部）为连接本体；
    `Dial/DialWS/DialDual` 构造，`Channel(kind)` 视图（独立 Invoke/On/State），
    `Client.State()` 聚合向下降级；每通道独立心跳/重连/排队/会话钩子
    （`ChannelConfig.Opts` 按通道覆盖，业务重登 + 战斗 Join 重绑定）。
  - WebSocket 通道：`transport_ws.go`（gorilla/websocket，一条消息 = 一个完整帧，
    `frame.Encode/Decode` 消息边界编解码），`DialWS(addr, path)` 默认路径 `/ws`。
  - 验收全绿：golden vectors 全绿；`-race` 全绿；真服务（10.10.9.36）TCP/WS/dual
    三形态冒烟通过；dual 重连演练（重启 gateway）双通道独立恢复（业务重登 + 战斗重绑定）。
- 目录速览：`frame/`（协议层，零依赖）、`client/`（client 编排器 / channel 连接本体 /
  options / transport（TCP+WS）/ invoke / notify / heartbeat / reconnect / queue / errors /
  serializer）、`examples/smoke/`（`-transport tcp|ws` / `-dual` 三形态）、`testdata/golden/`。
- 实测经验（对接真实网关时注意）：**战斗通道不要做业务 Login**——网关会话为
  每玩家单会话，二次登录会顶掉业务通道会话（规范 §5.2 会话绑定业务通道）；
  战斗通道连通性验证用传输心跳 `client.HeartbeatOperation` 往返即可。
- 第二轮评审修复（v0.3 合入前，2026-08-31）：
  1. **P0 `DialDual` Opts 交叉污染**：`wrapBattleRebind` 的返回值曾被整体赋给
     `business.Opts`——战斗通道的按通道配置（短超时/退避/排队等）泄漏到业务通道
     （有单测回归：`TestDualOptsIsolation` 白盒断言 + 行为验证）。现链式钩子
     追加到业务 Opts 自身，battle.Opts 永不外溢。
  2. **P1 hookActive 接线补齐**：钩子同步执行期间本通道保持 Reconnecting，
     `invoke()`/`invokeOnce()` 对 hookActive 直通（钩子的重登请求与传输心跳不排队），
     外部请求保持排队；Connected 置位与 drainQueue 回到同一 genMu 临界区
     （`settleGeneration`）——恢复「排队请求严格先于新请求」的 FIFO，
     且未认证连接不再接收新请求。
  3. **P1 重连拨号 watcher 泄漏**：`rebindDialCtx` 改为返回 (ctx, cancel)，
     每轮拨号尝试返回后立即 cancel，goroutine 不再累积到通道关闭。
  4. **P1 会话心跳门控与单飞**：`startConnLoops` 按 `kind == KindBusiness` 门控
     （顶层配置不再波及战斗通道）；业务错误触发重登钩子改为 CAS 单飞
     （`sessionHookBusy`）+ hookActive 跳过 + nil 守卫；新增
     `session_heartbeat_test.go` 五用例（周期调度/业务错误触发钩子/网络错误静默/
     工厂未就绪跳过/仅业务通道）。
  5. P2：`reconnectOnce` 合一为 `reconnectFrom(backoffBase)` 的委托；
     `WithOnReconnected`/`WithSessionHeartbeat` 文档对齐同步钩子与门控语义；
     冒烟程序三形态接入 `WithSessionHeartbeat` 真服务验证。

## v0.3：dual 双通道编排 + WebSocket 通道（已完成）

### Channel 抽象拆分（先做，重构性质）

现状：~~`client.Client` 既是「连接本体」又是「门面」~~ → 已拆为：

```
Client（编排器，对外 API 不变）
 ├─ channels map[Kind]*Channel      // Kind 业务/战斗（dual 形态）
 ├─ Invoke/On → 默认业务通道         // 现有调用方零改动
 └─ Channel(kind) *ChannelView      // 视图：独立 Invoke/On/State()；
                                    //   生命周期归 Client（视图不单独 Close）
Channel（连接本体，内部）
 ├─ 现 client.go 的连接字段（conn/connDone/epoch/读写循环/心跳/重连）
 └─ 每通道独立重连与心跳；独立 in-flight（epoch 已按代隔离，天然支持）
```

要点：
- `WithOnReconnected` 拆为**每通道钩子**（业务通道重登 + 战斗通道 Join 重绑定，
  对应模板 `JoinBattle` 语义，spec §5.2「dual 双通道独立重连」）。
- 状态聚合：任一通道非 Connected → `Client.State()` 向下降级；细粒度走
  `Channel(kind).State()`。
- 请求排队/退避参数允许按通道覆盖（战斗通道高频短超时）。

### WebSocket 通道

- `channel/ws.go`：gorilla/websocket，一条 WS 消息 = 一个完整帧
  （`ReadMessage` → 帧解析；写侧整帧单次 `WriteMessage` + 写锁）。
- 浏览器/单通道形态对齐模板约定：服务端 `/ws` 路径包装（模板 e2e 的 WSURL
  形如 `ws://host:port/ws`）；SDK 侧 `DialWS(addr, path)`。
- WS 库选型已定：**gorilla/websocket**（已回归维护；`transport_ws.go`，
  读侧 `SetReadLimit` 对齐 bodyLen 上限、超限归类协议错误）。
- 验收：对模板 gateway WS 通道（9002）跑通注册/登录/心跳，`examples/smoke`
  加 `-transport ws` 形态。

### 验收（v0.3 退出条件）

1. dual 形态 e2e：TCP 注册登录 → 匹配 → 战斗（KCP/WS 通道可后置到 v0.4，
   dual 编排先用 WS 战斗通道或 mock）。
2. 断线重连演练对**每条通道**独立成立（kick 通道 A 不影响通道 B 的 in-flight）。
3. golden vectors 全绿；`-race` 全绿；冒烟双形态通过。

## v0.4：KCP / UDP 通道（已完成）

- KCP：kcp-go v5.6.72（与服务端 go.mod 同源），流式语义与 TCP 同构；
  默认串行处理保序（对齐服务端 KCP 引擎默认）。
- UDP：数据报一报一帧（`DatagramEngine` 对称）；**单帧实际上限 64KiB**
  （读缓冲默认，含 16B 头）；解码失败静默丢包语义对齐。
- 服务端 KCP/UDP 端口见模板 `services/gateway/configs/config.yaml`（9003/9004）。
- 交接备注：atlas 服务端 `NewDatagramEngine` 缺内置 Ping 自动注册（见上方「服务端
  偏差」），SDK 已兼容；v0.5 起若服务端补齐则 UDP 通道心跳将自动恢复语义一致。

## v0.5：atlas sdk gen DTO 生成器（已完成，atlas 主仓交付）

- 输入 `FileDescriptorSet`（protoc `--descriptor_set_out`，include roots
  `-I. -Ithird_party`）；只消费游戏项目 `api/**` 的 message（被引用依赖自动并入）。
- protojson 映射规则与 WKT 映射表见规范 §6.1/§6.2（64 位整数→string、
  message null、未知枚举数字形态、map key 字符串化、NaN/Infinity）。
- 接管 CLI 现有 `atlas sdk gen`（旧 HTTP 骨架生成器已删除）。
- 产物按 proto 包分目录 + 跨包导入前缀（模板 e2e 实证决策，详见 atlas 主仓
  `docs/superpowers/plans/2026-09-01-sdk-gen-dto-generator.md`）。

## 开发约定

- 新增/变更线格式：先改 atlas 规范 + golden vectors（`-update` 重新生成，
  manifest 双 sha256 随之更新），四语言实现跟进——规范先行。
- 环境与验证：集成服务器 10.10.9.36（SSH `shimmer-bi@10.10.9.36`，
  常驻 etcd 12379 / redis 16379 / nats 14222 / mongo 27018）；
  同步用 atlas 仓 `.template-workspace/it-run.sh sync` + `git archive feat/actor`
  导出（**不要** rsync 主树工作区——主树分支可能不在 feat/actor）。
- 服务器四服务启动：`go build -o /tmp/bin/<svc> ./services/<svc>/cmd` 后
  nohup 运行，日志 `/tmp/svc-<svc>.log`；gateway 端口 TCP 9001 / WS 9002 /
  KCP 9003 / UDP 9004 / HTTP 10080。
- 全部提交 `-race` 全绿；重连类测试注意消除「kick 回包与断连」的窗口竞态
  （等待状态流转后再断言，见 `reconnect_test.go` 的 waitReconnecting 用法）。
