# 开发路线与实施指引

> 本文档是下一批次开发的交接说明。协议规范：atlas 主仓
> `docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md`（只读参考，
> README 不依赖它）。服务端对接基准：atlas `feat/actor` 分支（golden manifest
> `atlasCommit` 字段锁定，当前 `0ba52c0`）。

## 当前状态（2026-08-31）

- v0.1/v0.2 已完成并推送：帧编解码、golden vectors（21 用例）、TCP 通道、
  Invoke 匹配、双层心跳语义、断线自动重连（supervisor 多代连接 + 状态机 +
  请求排队 + 会话重登钩子）、错误四分类。
- 真连接冒烟已通过：`examples/smoke` 对集成服务器（10.10.9.36）模板四服务
  跑通 注册→登录→双层心跳→kill/重启 gateway 重连演练 全链路。
- 目录速览：`frame/`（协议层，零依赖）、`client/`（内核：client/invoke/notify/
  heartbeat/reconnect/queue/errors/serializer）、`examples/smoke/`、
  `testdata/golden/`。

## v0.3：dual 双通道编排 + WebSocket 通道

### Channel 抽象拆分（先做，重构性质）

现状：`client.Client` 既是「连接本体」又是「门面」。v0.3 需拆为：

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
- Node 下 WS 客户端同实现；`nhooyr.io/websocket` 或 gorilla 二选一
  （gorilla 已回归维护，推荐）。
- 验收：对模板 gateway WS 通道（9002）跑通注册/登录/心跳，`examples/smoke`
  加 `-transport ws` 形态。

### 验收（v0.3 退出条件）

1. dual 形态 e2e：TCP 注册登录 → 匹配 → 战斗（KCP/WS 通道可后置到 v0.4，
   dual 编排先用 WS 战斗通道或 mock）。
2. 断线重连演练对**每条通道**独立成立（kick 通道 A 不影响通道 B 的 in-flight）。
3. golden vectors 全绿；`-race` 全绿；冒烟双形态通过。

## v0.4：KCP / UDP 通道

- KCP：kcp-go，流式语义与 TCP 同构（`frame.ReadInto` 直接可用）；
  默认串行处理保序（对齐服务端 KCP 引擎默认）。
- UDP：数据报一报一帧（`DatagramEngine` 对称）；**单帧实际上限 64KiB**
  （读缓冲默认，含 16B 头）；解码失败静默丢包语义对齐。
- 服务端 KCP/UDP 端口见模板 `services/gateway/configs/config.yaml`（9003/9004）。

## v0.5：atlas sdk gen DTO 生成器

- 输入 `FileDescriptorSet`（protoc `--descriptor_set_out`，include roots
  `-I. -Ithird_party`）；只消费游戏项目 `api/**` 的 message。
- protojson 映射规则与 WKT 映射表见规范 §6.1/§6.2（64 位整数→string、
  message null、未知枚举数字形态、map key 字符串化、NaN/Infinity——
  golden vectors 已有对应用例）。
- 接管 CLI 现有 `atlas sdk gen`（旧 HTTP 骨架生成器废弃）。

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
