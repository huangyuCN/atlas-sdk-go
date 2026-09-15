# atlas-sdk-go 项目开发指引

## 项目概述

Atlas 帧协议的 Go 客户端 SDK：供游戏客户端、机器人与压测脚本连接 Atlas 游戏服务端，
提供开箱即用的长连接能力。目录结构、命令与发布流程见 `README.md` 与 `Makefile`。

---

# Agent 约定（全局）

## 开发规范（全局）

- **代码注释语言**：所有手写代码注释必须使用中文，包括 Go doc 注释、行内注释、复杂逻辑说明和测试意图说明。允许保留英文的情况仅限专有协议字段、外部标准名、错误码、指标名、trace attribute 名、第三方 API 原文，以及 protobuf/OpenAPI/工具生成文件中的生成注释。
- **枚举优先（杜绝魔法值）**：协议与代码中语义有限的值（状态/原因/类型/级别等）**必须**定义为 proto enum 或 Go 常量集合，杜绝散落的魔法字符串/数字——客户端拿到的是自解释的枚举名（protojson 默认下发枚举名）。协议新增 reason/状态/类型字段一律用 enum 类型；发现存量魔法值，在改造到该处时一并枚举化。自由文本（如操作备注、错误描述）不在此列；纯路由键（如推送 operation 的消息完整名）是协议寻址键，不属魔法值。
- **Go 注释格式（IDEA 无波浪线）**：doc 注释首句**必须以被声明的东西的名字开头**（官方 go.dev/doc/comment 规范），否则 GoLand 的 `GoCommentStart`（Comment of exported element starts with the incorrect name）与 `GoExportedElementShouldHaveComment`（Missing comment）会产生下波浪线警告。规则速查（详细规范与正反示例见 `docs/go-comments.md`）：
  1. **包注释**：必须以 `Package <包名>` 开头（`// Package ledger 实现 …`）。
  2. **顶层类型/函数/单条常量/变量注释**：首词必须是**类型名/函数名/标识符名**（`// Store Mongo 客户端…`、`// New 连接…`、`// DBName 平台数据库名…`）。
  3. **方法注释**：首词必须是**方法名**，**不带接收者类型前缀**（写 `// Coll 返回集合…`，不要写 `// Store.Coll …`）。
  4. **结构体/接口导出字段注释**：**不要求**以字段名开头（官方明确建议字段注释简短说明、不必完整句）。
  5. **注释与声明之间不能有空行**（否则不算 doc 注释，触发 Missing comment）；首句以句号结尾。
  6. 行内注释、`//go:` 指令注释不受「首词」限制；非导出声明不强制 doc 注释。
  7. 新增/修改代码时必须自查，code review 必查项：注释首词是否等于声明名。
  8. 全仓自动检查：`make comment-lint`（`scripts/go-comment-lint`，违规时以退出码 1 失败）；`make lint` 已包含该检查。
