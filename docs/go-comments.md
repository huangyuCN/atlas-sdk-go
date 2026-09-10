# Go 注释规范（IDEA / GoLand 无波浪线）

> 目标：本项目所有手写 Go **doc 注释**必须通过 GoLand/IDEA 的注释检查，不出现下波浪线警告。
>
> 依据来源（按权威性排序）：
> 1. **go.dev/doc/comment**（官方「Doc Comments」规范）
> 2. **golint** 规则（已 frozen/deprecated，规则被 IDEA 与 revive 继承）
> 3. **JetBrains Inspectopedia**：`GoCommentStart`（「Comment of exported element starts with the incorrect name」）与 `GoExportedElementShouldHaveComment`（「Missing comment」）
> 4. **Effective Go / CodeReviewComments**（golint 的设计依据）

## 一、总原则

> The first sentence of a doc comment should be a one-sentence summary that **starts with the name being declared**.

（官方原文大意：doc 注释的首句应是以「被声明的东西的名字」开头的一句话摘要。）

本项目注释正文使用**中文**，但**首词必须是声明名**（标识符本身是英文），形如：

```go
// GetBatch 返回权威批次记录；不存在时返回错误。
func (l *Ledger) GetBatch(...) ...
```

## 二、核心规则表：什么时候带什么名字

| 声明位置 | 注释首词必须是 | 示例 |
|---|---|---|
| **包**（`package` 上方） | `Package` + **包名** | `// Package ledger 实现 ETL 的 batch.Ledger（Mongo 持久化）。` |
| **顶层类型**（`type`） | **类型名** | `// Store Mongo 客户端与集合访问器。` |
| **顶层函数**（`func`） | **函数名**（允许带 `(...)`） | `// New 连接 Mongo；URI 为空立即失败。` |
| **方法**（`func (r T) M(...)`） | **方法名**（不带接收者类型前缀） | `// Coll 返回指定集合。`（不要写 `// Store.Coll ...`） |
| **顶层常量/变量（单条）** | **标识符名** | `// DBName 平台数据库名（manager/worker 共库）。` |
| **常量/变量组**（`const (...)`） | 组注释以组内名字或统一语义开头；组内的每个导出名也应可单独读懂 | `// 批次状态。`；组内：`// StateOpen 接受 AddFiles`（逐条见下节） |
| **结构体/接口的导出字段** | **不要求**以字段名开头 | `// 目标数据库（SourceCleaner/Loader 使用；空=由上层推导）。` |
| **非导出声明** | 不强制 doc 注释；若有注释同样中文、语义完整 | `// 清洗后是否保留临时文件。` |

### 关键细节

1. **方法注释不带接收者类型前缀**：GoLand 的 `GoCommentStart` 检查取「声明名」——方法的声明名就是**方法名**（`Coll`），不是 `Store.Coll`。写 `// Store.Coll ...` 会被判为错误首词（波浪线）。golint 传统建议的 `Type.Method` 形式**不要用**（本项目以「无波浪线」为准，统一方法名形式）。

2. **结构体/接口的导出字段注释不以字段名开头**：官方明确——"Unlike the doc comments for …most declarations, field comments usually do **not** begin with the field name"。字段注释是简短说明、不必是完整句，如：

   ```go
   type Store struct {
       client *mongo.Client // 底层 Mongo 客户端（连接生命周期由上层负责）
   }
   ```
   注意：字段注释以 `// ` 开头即可；不要写成 `// client 底层客户端`（首词非导出也没什么问题，只是风格上不推荐；**不要**写 `// Client ...` 与字段名同义的冗余首词）。

3. **常量/变量组**：`const (...)` 块若有组注释（以组首名字或统一语义开头）即满足「有注释」；块内每个导出的名字若要单独文档化，则以各自名字开头。GoLand 的 Missing comment 对「单条导出声明」才强制；组声明有组注释即不报警。

4. **完整句子 + 句号**：doc 注释首句应是完整的一句话、以句号结尾（官方建议；GoLand 不强查句点，按官方惯例统一写结尾句号，中文可用「。」）。

5. **注释与声明之间不能有空行**：注释与声明之间空行会使注释脱离声明、不再被视为 doc 注释 → 触发 Missing comment（波浪线）。

6. **非 doc 注释不受限**：行内注释、函数体内的说明性注释无「首词」要求，仍遵守中文注释约定。

7. **指令注释不是 doc 注释**：`//go:embed`、`//go:generate` 等 `//go:` 前缀指令注释不参与 doc 注释检查。

8. **首词的判定**：以注释文本中**第一个空白分隔的 token** 为准。`// 123 ...`、`//- xxx`、`// 中文描述 ...`（未以声明名开头）都会被判为错误首词。

## 三、正反示例

### 正确（无波浪线）

```go
// Package ledger 实现 ETL 的 batch.Ledger（Mongo 持久化）。
package ledger

// Store Mongo 客户端与集合访问器。
//
// 复用 infra 提供的连接；本结构只做集合访问封装。
type Store struct {
	client *mongo.Client // 底层客户端（连接生命周期由上层负责）
	db     *mongo.Database
}

// New 基于 Store 构造 Ledger。
//
// 三个集合引用（batches/sources/loadunits）随 Store 固定，AppID 由调用参数区分。
func New(s *store.Store) *Ledger {
	...
}

// Coll 返回指定名称的集合。
func (s *Store) Coll(name string) *mongo.Collection {
	return s.db.Collection(name)
}
```

### 错误（GoLand 会产生下波浪线）

```go
// mongo 客户端与集合访问器。          // ❌ 首词 mongo ≠ Store
type Store struct{ ... }

// 基于 Store 构造 Ledger。            // ❌ 首词「基于」≠ New
func New(s *store.Store) *Ledger { ... }

// store.Store.Coll 返回集合。         // ❌ 方法注释带了接收者前缀（首词 ≠ Coll）
func (s *Store) Coll(name string) *mongo.Collection { ... }
```

## 四、对应的 GoLand 检查（出现波浪线时排查）

| 检查名（Inspectopedia） | 触发条件 | 修复 |
|---|---|---|
| **GoCommentStart** —— Comment of exported element starts with the incorrect name | doc 注释首词 ≠ 声明名（或包注释不以 `Package` 开头） | 按上表改首词 |
| **GoExportedElementShouldHaveComment** —— Missing comment | 导出的包/类型/函数/方法/常量/变量没有 doc 注释 | 补 doc 注释（首词=声明名）；字段不在此列 |

IDE 位置：`Settings → Editor → Inspections → Go → Comment & Documentation`（两条检查默认开启；若被手动关闭，按此恢复）。

## 五、快速自查

```bash
# 抽查：找出导出的顶层声明中「紧跟其上的注释首词 ≠ 声明名」的位置（启发式，作参考）
# 方法/函数/类型/常量/变量均可由此人工复核
```

建议在 code review 中把「注释首词 = 声明名」作为必查项；实现者写完即自查，避免把违规带到评审。