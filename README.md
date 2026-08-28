# atlas-sdk-go

Atlas 帧协议的 Go 客户端 SDK（v0.1 最小内核）。协议规范见
atlas 仓库 `docs/superpowers/specs/2026-08-28-client-sdk-multilang-design.md`。

## 当前能力

- **帧协议**：与服务端 `transport/frame` 对齐的分帧编解码（16B 头 + opLen + payload），
  校验失败/截断/超限按协议语义分类（protocol / network）。
- **Invoke**：`(epoch, seq)` 请求-响应匹配、超时取消（先到者胜出，恰好一次投递）、
  迟到响应静默丢弃；业务拒绝还原为 `*BusinessError`。
- **Notify**：按 operation 分发服务端推送，handler 独立 goroutine + panic 隔离，可退订。
- **心跳**：周期 Ping 传输保活（默认 30s，连续 3 次失败判定死链）；
  **不续租业务会话**（会话续租由业务层调度业务 Heartbeat，规范 §5.2 双层心跳）。
- **错误模型**：规范 §7 四分类——`*BusinessError`（按 `Reason` 分支，`IsBusinessError` 统一判断）、
  `NetworkError`、`TimeoutError`、`ProtocolError`（`errors.Is/As` 判定）。
- **golden vectors**：`testdata/golden/` 为协议一致性字节级用例（四语言同源），
  manifest 锁定 atlas 基线 commit（`feat/actor`）并含逐文件 sha256；
  `go test ./frame -update` 重新生成。

## 快速开始

```go
c, err := client.Dial("127.0.0.1:9001",
    client.WithHeartbeatInterval(30*time.Second),
    client.WithInvokeTimeout(10*time.Second),
)
if err != nil { panic(err) }
defer c.Close()

off := c.On("/gateway.v1.GatewayAuth/Notify", func(op string, payload []byte) {
    // 业务侧自行解码 payload（protojson camelCase）
})
defer off()

var resp LoginReply
err = c.Invoke(ctx, "/gateway.v1.GatewayAuth/Login", &LoginRequest{...}, &resp)
```

## 路线（规范 §9）

v0.1（当前）：TCP 通道 + Invoke + 心跳 + Notify + golden vectors。
后续：断线重连与多通道编排（dual/single 形态）、WS/KCP/UDP 通道、
`atlas sdk gen` DTO 生成器接入。

## 开发

```bash
make test        # 单测 + golden vectors（-count=1）
make update      # 重新生成 golden vectors（协议用例变更时）
make lint        # gofmt + go vet
```
