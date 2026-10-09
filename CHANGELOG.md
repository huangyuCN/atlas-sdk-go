# 变更日志

本仓是 Go module `github.com/huangyuCN/atlas-sdk-go`（**根模块**，仓内无嵌套 module），
版本**只由 Git tag 承载**：根模块 tag 直接叫 `v0.7.0`（无 `子目录/` 前缀），消费方按
`go get github.com/huangyuCN/atlas-sdk-go@v0.7.0` 引用。仓内另有 `client.Version` 常量
（登录/恢复请求上报的 `client_version` 字段值），发布时与 tag 同步，是本仓唯一的版本字面量来源。

破坏性变更一律以 **破坏性** 显式标注。

## v0.7.0（2026-10-09）

对应 main 分支 `032f614`（发布提交在此之上把 `client.Version` 由 `0.5.0` 升为 `0.7.0`），
即阶段 3「战斗帧直连」的收口版本。

### 破坏性

1. **战斗帧改走直连接入层，老客户端必须升级**：成局后客户端凭「接入层地址 + 战斗票据」直连
   接入层（本仓 `direct` 包，KCP/UDP/WS 三面）——接入层按 `battle_id` 定位战斗属主并做 L4 转发；
   网关只剩单一业务通道，战斗帧不再经网关。仍用网关战斗通道（`client.DialDual` / `DialKCP` /
   `DialUDP`）承载战斗帧的 v0.6 及以前客户端会直接失去战斗链路，须升级到本版本并改走
   `direct.Open`；`DialKCP` / `DialUDP` / `DialDual` 保留仅供联调与压测。
2. **生成 DTO/stub 跟随模板 proto 重新生成**（`85578aa`）：战斗面改用 `IssueEntryTicket`
   （替代原 `GetState`）下发「按传输面的接入层地址」`EdgeEndpoint[]` + `EdgeTransport` 枚举 +
   逐玩家 `BattleTicketEntry` 票据，并新增 `PlayerOutNotify` / `PlayerOutReason` 推送。
   直接消费本仓 `api/battle/v1` 生成物的调用方需同步改字段并重生成。
3. **`client.Version` 由 `0.5.0` 升为 `0.7.0`**：登录/恢复请求线上 `client_version` 上报值随之
   变化（网关按 `runtime.min_client_version` 判准入）；依赖具体版本串做分支判断的调用方需同步。

### 新增与修复

- **直连保活心跳**（`084fcdf`）：`direct` 周期发 `battle.v1.BattleService/Ping`（Tell，无业务回执），
  缺省 2s（`WithHeartbeat` 可调、`<=0` 关闭），严格小于 battle 侧空闲判掉线阈值
  `offline_timeout/3`（缺省 5s）；网络类失败只计数（`HeartbeatFailures()`），业务拒绝或协议非法
  给可判定原因（`HeartbeatErr()`）。`examples/directloop` 补会话观测与 `-idle-hold` /
  `-heartbeat` / `-no-heartbeat` 验收开关。
- **对局结束语义收口**（`1876be3`）：终态停发（业务帧、心跳与重连重放一律拒绝，不重连）、
  结束通知幂等（按「首投」去重，补投只计数不重放回调）、终态后 2s 收尾窗口
  （`WithEndLinger` 可调，窗口内仍可读结算推送）。
- **评审 P0/P1 修复**（`032f614`）：终态族哨兵归一（`ErrBattleNotFound` / `ErrBattleFull` /
  `ErrFrameTargetMismatch` 与 `ErrBattleEnded` 同族，`IsTerminal` 一处判定）；
  `ended`（有结算，留 2s 窗口）/ `failed`（无结算，即刻回收连接）语义拆分；
  心跳被拒四分类并**按代次**起停（一次协议抖动不再让保活永久失效）；终态时在途请求立即以终态
  Status 结算（metadata 标 `x-atlas-sdk-local-settled=true`）；新增 `Stats()` 只读快照。
- **文档**：README 补「战斗直连」整节与直连 Option 表。

### 升级指引

- 旧双通道（`DialDual`）业务侧不动：登录/会话/匹配仍走 `client` 通道；
  仅把战斗帧路径换成 `direct.Open` + `JoinBattle` / `SendFrameInput` / `SyncFrames`。
- 直连计划（票据 + 三面地址）只从本局成局推送解析（`direct.PlanFromPush` / `PlanFromNotify`），
  不要读本地配置或猜端口。
- 终态判定统一用 `errors.Is(err, direct.ErrBattleEnded)` 等族内哨兵，并按 `Ended()` / `Failed()`
  分流（前者展示结算，后者回匹配链路重新开局）。
