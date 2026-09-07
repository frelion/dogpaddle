# DogPaddle 算子与执行内核路线图

本文定义 DogPaddle 算子体系和执行内核的演进阶段、语义边界、交付物与退出标准。
它是实施路线；阶段 0/1、阶段 3 的 Distinct 和阶段 4 的 grouped Aggregate 最小纵向切片已完成，
后续候选算子或用户接口仍不表示已经交付。当前精确能力以根目录 [`README.md`](README.md) 和各产品
crate 的 README 为准。

## 产品方向

DogPaddle 的核心产品不是某一种查询语言，而是一套嵌入式、持久化、可恢复、可组合的
数据流算子与运行内核。用户可以通过不同上层接口构造同一套底层 Flow，例如：

- 直接 Rust Builder；
- 已交付基础范围的 SQL；
- 面向常见数据任务的声明式 Pipeline API；
- DataFrame 风格 API；
- 由其他应用或语言编译得到的持久化计划。

这些接口都只是上层适配器，不进入 Change、Store、Operation 或 Flow 的核心语义。SQL v1 已用无状态算子、
Distinct 和 grouped Aggregate 证明这条分层路径，其余接口仍是候选。路线首先回答：

1. 一组算子是否拥有精确、可组合、可持久恢复的行为；
2. 状态算子能否正确解释有序、带正负 diff 的变化流；
3. 失败、背压、重放和进程重开是否保持同一业务结果；
4. Scan、Transform 和 Sink 是否足以承载真实数据闭环；
5. 上层 API 能否只做解析、类型检查和 lowering，而不补救底层语义。

## 当前基线

现有内核已经解决了后续算子最难事后补救的公共问题：

- 完整、精确 logical Arrow Schema 的全图传播、绑定和运行期守卫；
- Operation 状态、output、input cursor、active input 与 reclaim 的同事务提交；
- 背压、Operation 错误、commit 失败和 reopen 后的输入身份保持与完整重放；
- 保留重复、事件顺序和非零正负 diff 的 `Change`；
- 每个 Change 一个完整、自描述 Arrow IPC Stream 的稳定持久化边界；
- Definition、Operation data class、资源布局和 reopen 的确定性装配；
- 静态 DAG、fan-out、多输入端口与确定性有界调度。

当前内建算子为：

| 类别 | 算子 | 当前角色 | 路线判断 |
| --- | --- | --- | --- |
| Scan | SequenceScan | 生成连续 `u64` 测试/系统事件 | 保留，但不代表通用 ingress |
| Scan | PostgresCdcScan | 固定 Schema 单表 WAL CDC，checkpoint/output 同事务与 commit 后 ACK | 已有具体试点；snapshot、TLS/fencing 与发布门仍待实施 |
| Transform | RunningEventCount | 运行事件计数器 | 已明确为事件观测，不是关系 Aggregate |
| Transform | Distinct | 按完整记录的当前正权重维护存在性 | 已完成首个持久状态关系算子 |
| Transform | Aggregate | 按非空 group key 持续维护多个聚合结果 | 已完成阶段 4 最小纵向切片 |
| Transform | Project | 严格递增顶层索引的零拷贝删列 | 保留为结构/物理优化算子 |
| Transform | Filter | DataFusion Boolean Expr 行过滤 | 保留为基础无状态算子 |
| Transform | Extend | 保留输入并追加一个表达式列 | 保留为基础无状态算子 |
| Transform | Select | 有序多表达式完整输出 | 保留为基础无状态算子 |
| Transform | SchemaAlign | 显式完整 Schema 重塑 | 已完成基础结构对齐；不做隐式 coercion |
| Transform | UnionAll | exact Schema 多输入原样合并 | 保留为基础多输入算子 |
| Sink | SqliteSink | 将差分流幂等物化到独占的 SQLite 表 | 已完成首个本地外部副作用 Sink |
| Sink | PostgresSink | 将单输入 exact relation 幂等物化到独占的固定 Schema PostgreSQL 表 | 已有远端试点；TLS、在线演进与发布门仍待实施 |
| Sink | Discard | 无副作用地完成输入 | 保留为测试和显式丢弃终点 |

当前十四个内建算子已进入统一能力/conformance 表；覆盖度仍小，但已实现行为的可靠性边界值得
继续保留。后续工作重点是扩展算子族和公共
conformance，而不是让某个上层 API 反向定义运行内核。

当前 `dogpaddle-sql` 接受一条直接的 `INSERT INTO sqlite/postgres/discard(...) Query`，Scan 直接写成
`FROM sequence/postgres_cdc(...)`。它用 DataFusion 完成解析、类型分析和 coercion，把 TableScan、
Projection、Filter、非递归 CTE、`SELECT DISTINCT`、`UNION ALL` 与非空 `GROUP BY` lowering 为现有 Definition DAG。
SQL 聚合支持 `COUNT/SUM/AVG/MIN/MAX` 和纯分组；SQL 不建立 DogPaddle Table、View、catalog、独立状态或执行层。
Join、global aggregate、grouping sets、聚合 UDF/修饰符、普通 `UNION`、`DISTINCT ON`、Sort、Limit 和 Window
必须在建库前拒绝。`SqlProgram::build` 持久化 canonical Flow Definition，`open` 继续以
这份磁盘 Definition 为恢复真相，SQL 变更要求新路径或显式重建。

## 目标分层

```text
可选用户接口层
├── Rust Builder
├── SQL（基础范围已交付）
├── Pipeline DSL
├── DataFrame API
└── 其他语言或应用适配器
          │
          │ 解析、类型检查、优化、lowering
          ▼
算子组合层
├── Scan Definitions
├── Transform Definitions
├── Sink Definitions
└── exact Schema / data declarations
          │
          ▼
Flow 运行层
├── persistent DAG
├── build/open
├── deterministic bounded scheduling
├── backpressure
├── claim/cursor/replay
└── transaction coordination
          │
          ├──► Change：Arrow records + ordered diffs + IPC
          └──► Store：typed collections + transactions + recovery
```

依赖继续保持单向：

- Operation 不依赖 Flow 或任何用户接口；
- Flow 不枚举具体算子，也不知道 SQL、DataFrame 或 Pipeline DSL；
- Store 不依赖 Arrow、Change、Operation 或 Flow；
- 上层接口只依赖公共组合能力，把自己的计划 lowering 为 Operation Definition DAG；
- 不为某个接口在底层增加无法被其他接口复用的旁路状态或执行语义。

## 什么叫“一个算子做好了”

一个算子只有同时完成下列契约，才视为产品能力，而不是原型。

### 1. 结构契约

- 明确声明 `Scan`、`Transform(nonzero arity)` 或 `Sink(nonzero arity)`；
- 明确是否有 output；
- 明确每个输入端口的含义和跨端口顺序契约；
- 明确全部持久化 data class、逻辑名称、collection、codec 和 Size；
- runtime Operation 不保存 Definition、Store 或事务启动能力。

### 2. Schema 契约

- Definition 对有序、精确 input Schemas 纯绑定；
- 输出字段顺序、名称、类型、nullability 和 metadata 唯一确定；
- 相同 tag、payload 和 input Schemas 得到相同 binding；
- binding 失败发生在资源创建或打开前；
- runtime input/output Schema drift 被守卫并回滚。

### 3. Change 契约

- 明确记录顺序是否保持；
- 明确重复记录如何处理；
- 明确输入 diff 如何映射为输出 diff；
- 明确是否维护关系权重，以及谁验证负权重前缀；
- 不把 AppendLog offset 或物理 batch 边界当作业务 event ID；
- 不隐式排序、抵消、consolidation 或拆分业务事件。

### 4. 重批与 continuation 契约

- 稳定拆分或合并物理 Change 后，展平输入事件序列不变；
- 单输入算子的展平 output 和最终状态符合既定重批不变量；
- 多输入算子至少保持每端口事件子序列和最终关系状态；
- `Commit` continuation 存入算子声明的持久状态，不存入 Station；
- 同一个未完成 Change 被完整重放时不丢失、不多应用。

### 5. 事务与失败契约

- `Turn::Idle` 不开启写事务，prepared `Action::Idle` 回滚本 turn 全部写入；
- `Commit` 只提交 continuation 与可选 output，不完成输入；
- `Complete` 原子提交状态、可选 output、cursor、active rotation 和 reclaim；
- output capacity 拒绝回滚整个 turn 并保持输入 identity；
- codec、overflow、Schema、Store 或 commit 错误不留下部分业务状态；
- 外部确认只在本地提交后通过 `AfterCommit` 执行；必须提前发生的外部副作用另行定义持久幂等协议。

### 6. 持久化契约

- 唯一稳定 tag 和 canonical payload；
- golden bytes、truncation、malformed 和 no-panic 证据；
- build/open/reopen 物化相同 collection 和语义；
- 开发期不兼容格式变更直接更新当前基线，并要求删除旧数据库后重建；
- 不承诺、猜测或迁移旧 payload 的兼容行为，也不为旧库建立测试；未来稳定格式的版本政策另行定义。

### 7. 验证与性能契约

- 公共行为优先由公共 API 测试；
- 状态算子有独立 model/oracle；
- corruption 和失败后检查完整持久状态；
- correctness 完成后才加入 benchmark；
- benchmark 包含固定 fixture、seed、校验和原始样本，不替代正确性测试。

## 共同设计原则

### exact Schema 是防线，不是负担

字段对齐、类型转换和 nullability 放宽必须由显式算子或上层 lowering 完成。UnionAll、Join 等
消费算子继续接收 exact Schema，不在运行时猜测列的含义。

### Change 是有序变化流，不是无序集合批次

- 行位置属于语义；
- 重复事件合法；
- diff 可以大于一或小于负一；
- Change 自身不携带应用前关系状态；
- 维护关系状态的算子负责验证应用前权重加已处理前缀累计 diff 不为负；
- 物理重批不能悄悄改变业务输出。

### 无状态算子尽量共享 Arrow buffer

Project、Select/SchemaAlign 的直接列引用、Extend 的输入列、UnionAll 的转发和 Filter 全选路径继续尽量共享
Arrow buffer。优化不能改变 Schema、diff、顺序或失败边界。

### 状态属于算子，协调属于 Flow

Operation 只通过声明的 Cell/OrderedMap 等具体 data class 持有业务状态；Station/Flow 只协调输入
identity、事务、output、cursor 和 retention。不能把 Aggregate、Join 或 Window continuation 隐藏在
Station state。

### 上层 API 不成为持久化真相

Rust Builder、SQL 或其他接口可以保存自己的 Scan 描述，用于解释、重新编译和诊断；SQL v1 直接以
`FROM postgres_cdc(...)` 声明 Scan，并以 `INSERT INTO postgres/sqlite(...)` 声明 Sink。运行时恢复仍
基于 canonical Flow/Operation Definition。若接口版本或 lowering 规则变化导致语义不兼容，明确要求
重建，不让运行层猜测。

## 阶段总览

| 阶段 | 主目标 | 核心交付物 | 完成后得到什么 |
| --- | --- | --- | --- |
| 0（已完成） | 固化算子产品契约 | RunningEventCount 命名、分类、conformance、能力矩阵 | 现有算子成为明确基线 |
| 1（已完成基础范围） | 完成基础无状态/结构算子族 | SchemaAlign、Date/Timestamp/Decimal 传输、表达式状态矩阵 | 上层可可靠表达常见逐行变换 |
| 2（进行中） | 打通真实 Scan/Sink | PostgresCdcScan、SqliteSink、PostgresSink、ResultLog、Materialize | 不依赖测试 Scan/Sink 的真实数据闭环 |
| 3（已完成最小切片） | 建立精确行权重状态 | crate 私有 row digest、collision bucket、Distinct | 首个持久状态关系算子 |
| 4（已完成最小切片） | 完成 Aggregate 与多重集算子 | grouped COUNT/SUM/AVG/MIN/MAX；global/set ops/UDF 待续 | 可持续维护首个分组聚合关系 |
| 5 | 完成 Join 算子族 | Inner、Semi/Anti、Outer Join | 可组合的多关系增量计算 |
| 6 | 引入有界、顺序与时间语义 | Barrier、TopK、Window、watermark | 明确承载完成、排序和时间计算 |
| 7 | 完成运行产品化与上层 API 就绪 | lifecycle、连接器协议、observability、capability catalog | 多种用户 API 可稳定构建同一内核 |

阶段编号表示依赖顺序，不是严格发布版本；可以在不破坏前置语义的前提下并行开发独立证据。

## 阶段 0：固化现有算子契约

**状态：本轮完成。** 阶段 0/1 的九个基础算子及后续内建算子的同结构能力表、公共证据索引和新增算子 checklist 位于
[`crates/operation/README.md`](crates/operation/README.md)。

### 目标

把原有 8 个内建算子从“已有实现”提升为后续所有算子的模板，消除命名和能力理解歧义；阶段 1
加入 SchemaAlign 后，统一矩阵现覆盖 9 个算子。

### 已完成工作项

1. 为每个算子维护统一规格：
   - kind、arity、input/output Schema；
   - diff、顺序、重复和重批语义；
   - data declarations；
   - Action 行为；
   - overflow/error；
   - reopen ABI；
   - buffer sharing；
   - benchmark workload。
2. 将原 `Count` 的公共 API 定名为 `RunningEventCount`。它每观察一行事件加一，不根据 diff 维护
   关系 cardinality。当前 tag 为 `2`，output 字段为 `count`，逻辑 data 名为
   `running_event_count.count`。不保留旧 API alias、资源路径 fallback 或迁移；不承诺或测试旧库行为，旧数据库
   直接删除并按当前基线重建。
3. 固化算子分类和命名规则，避免未来出现名称相同但关系语义不同的算子。
4. 建立新增算子清单模板，要求实现者逐项回答“什么叫做好了”的七类契约。
5. 建立能力矩阵，而不是用目录或上层语法推断能力。
6. 审核现有算子测试是否全部覆盖各自声明的 Schema、diff、reopen、重批和错误契约；只补真实缺口，
   不复制更弱测试。

### 非目标

- 不引入某一种用户查询语言；
- 不把 RunningEventCount 改造成关系 Aggregate；
- 不增加兼容旧 API 的过渡层；
- 不为当前文档矩阵增加代码级 capability registry 或让 Flow 枚举具体算子；公共 introspection 仍属于阶段 7；
- 不改变 Station 的事务职责。

### 已完成结果

- 每个内建算子都有同结构的产品规格和公共验证索引；
- 名称不再暗示未实现的关系语义；
- decoder registry、tag 唯一性、golden、build/open/reopen 和 runtime traces 全部通过；
- `cargo xtask check`、Criterion test mode 和 owner-specific smoke benchmark 通过；
- 后续新增算子可以复制流程和证据模板，而不复制某个具体算子实现。

## 阶段 1：基础无状态与结构算子族

**状态：本轮完成基础范围。** SchemaAlign、Change 的 Date32/Timestamp/Decimal128 稳定传输、
Operation/Flow 的受限 temporal/decimal 纵向路径和三态表达式证据矩阵已落成；LargeUtf8 等类型、
额外结构算子与更广 DataFusion operator/type 组合明确留待后续真实 workload，不属于本轮完成定义。

### 目标

完成可以被任何上层 API 复用的逐行、逐列和 Schema 变换能力。该阶段不维护跨事件关系状态。

### 保留并加固现有算子

- Filter：Boolean/Kleene 过滤，false/null 删除，records/diffs 同步选择；
- Project：严格递增顶层索引的零拷贝列裁剪；
- Extend：保留输入并追加一个表达式字段；
- Select：基于同一个原始输入计算有序完整输出；
- UnionAll：所有端口 exact Schema，端口内顺序保持，跨端口无序；
- RunningEventCount：作为事件观测/诊断变换，不作为关系 Aggregate。

### 新增 SchemaAlign

已新增显式、可持久化的 `SchemaAlign`。它服务所有上层 API，而不是只服务 SQL。Definition 的
tag 为 `9`，不声明 Operation data；每个字段保存 `name + Expr + target nullability + Field metadata`，
Definition 另存 Schema metadata。Field/Schema metadata 的输入顺序不影响按 key 排序的 canonical
编码，重复 key 在构造期结构化拒绝，不采用 silent last-wins。

允许的变换：

- 字段选择、改名和重排；
- 由 Expr 中 [`cast`/`try_cast`](crates/operation/README.md#operationtransformschemaalign)
  明确声明的 cast；字段类型只从绑定表达式推导，不保存第二份 `DataType`；
- non-null 向 nullable 放宽；
- 规范化 Schema/Field metadata；
- 保持行序和 diff。

禁止的变换：

- 未验证的 nullable 向 non-null 收窄；
- 静默截断或由运行时猜测 cast；
- 修改 diff；
- 排序、去重或 consolidation。

所有表达式绑定同一个原始 input Schema，不能引用同一 SchemaAlign 新建的名称；空字段定义合法并
保留行数与 diff。直接列引用和 diff 共享 Arrow buffer，派生表达式按 DataFusion 语义分配。
UnionAll 和未来 Join 继续只接收 exact Schema；上层通过 SchemaAlign 显式构造公共输入结构。

### 类型能力

Change v1 现支持 Boolean、定宽整数、Float32/64、Utf8、Binary、Date32、Timestamp、Decimal128、
List 和 Struct。Timestamp 支持 Second、Millisecond、Microsecond、Nanosecond 四种 unit；timezone
可以缺省或为非空字符串，空字符串因 IPC 不能与缺省稳定区分而拒绝，Change 不解释 timezone 内容。
Decimal128 precision 为 `1..=38`，正 scale 不超过 precision，负 scale 按 Arrow 类型保留；Change
构造、全量解码和被选择字段的投影解码还会递归验证每个 non-null slot 的
`|unscaled| < 10^precision`。祖先 List/Struct null 不豁免物理存在的 non-null child；未选择字段不
读取或验证 value。该约束只保证 Decimal128 值可由声明 precision 表示，不定义 Decimal 算术或舍入。

本轮明确非目标为 `LargeUtf8`、`LargeBinary`、`FixedSizeBinary` 及其他 Arrow 类型。它们仍由统一
Schema guard 拒绝，只有真实 workload 和完整持久化证据出现后才扩展。

Operation 层已针对 Date32、无 timezone 的 Millisecond Timestamp 和 `Decimal128(10, 2)` 建立三个
公共纵向测试：Project/Select/Extend direct-copy，SchemaAlign 的 nullability 放宽及 Date32 → Int32、
Timestamp(ms) → Int64、Decimal128 `(10, 2) → (12, 3)` 显式 cast，以及 Filter 对三类同类型 literal
的组合比较。它们全部经过 `encode → decode → re-encode → bind → materialize → turn`，并检查
buffer/diff/顺序。Flow 再覆盖 SequenceScan → SchemaAlign → Project → Select → Extend → Filter →
RunningEventCount → Discard 的 build、运行与两次 reopen，最终 count 为 `3`。

这组证据只承诺上述 operator/type 组合，不承诺其他 Timestamp unit/timezone、时间运算、Decimal
算术/舍入或跨类型 cast。

类型扩展属于 Change、表达式和相关算子的共同持久化边界。每种类型都必须同步覆盖：

- Schema validation；
- 完整与选择性 Change IPC；
- standard Arrow reader interop；
- Decimal128 顶层/嵌套 value invariant，以及 unselected projection 不读/不验 value 的边界；
- 结构算子的 exact Schema 传递；表达式算术、comparison 和 cast 必须另有 Operation 级证据，不能
  从 Change 传输能力推导；当前只纳入上面列出的纵向组合；
- build/open/reopen；
- malformed 和 truncation。

### 表达式能力矩阵

已在 [`crates/operation/README.md`](crates/operation/README.md#表达式能力状态) 维护显式 DataFusion
Expr 三态矩阵：

- 已有 canonical Definition roundtrip、binding、evaluate 和 reopen 证据的 operator/type 组合，
  包括上面明确列出的 temporal/decimal 纵向切片；
- DataFusion 当前可规划但 DogPaddle 尚未承诺的组合；
- 因非 canonical payload、字段/类型/physical planning、隐式 coercion、Filter 类型或 SchemaAlign
  nullability 收窄而明确拒绝的表达式。

不要用“DataFusion 支持”代替 DogPaddle 产品证据。易变函数、UDF、session variable 和依赖外部
registry 的表达式在拥有确定性持久语义前不进入已承诺集合；当前无法 canonical 编码或 bind 的直接
拒绝，即使固定版本 DataFusion 碰巧可规划也只属于“未承诺”，直到显式准入与恢复证据完成。

### 可选后续结构算子

根据真实调用需求再决定是否加入：

- `Explode` / `Unnest`：一行产生多行；
- `Zip` 或字段组合：仅在 Select/Struct 表达式不足时；
- `Rename`：只有独立于 SchemaAlign 后仍有清晰价值时；
- `Cast`：只有独立算子能提供比 Select/SchemaAlign 更强证据时。

不为 API 便利创建与现有算子语义重叠的薄包装 Operation。

### 已完成结果

- 常见一对一、零或一行过滤、列裁剪、派生列、完整重塑和多输入合并可组合表达；
- 类型/metadata/nullability 对齐是显式算子，不削弱消费者 exact Schema；
- 每个列为“已承诺”的精确 operator/type 组合都有 golden、bind、evaluate、Flow reopen 和错误证据；
- buffer sharing 优化有身份或底层 buffer 证据；
- 无状态算子对稳定重批保持声明的展平输出。

## 阶段 2：真实 Scan 与 Sink

### 目标

消除只能依靠 SequenceScan 和 Discard 验证 Flow 的限制，用真实 Arrow Change 打穿输入、变换、
结果订阅和当前关系查询。

### 已完成：SqliteSink

`SqliteSink` 是首个本地外部副作用 Sink：它为精确 input Schema 创建并独占一个 SQLite `STRICT`
表，以 MDBX 中的版本化具体 mutation 批次覆盖 SQLite commit 与 MDBX commit 之间的失败窗口。
它不引入 SQLite 元数据表；在目标表未被外部修改、数据库文件未被替换或恢复的约束下，重放保持
最终结果恰好一次。通用 ingress、结果订阅与关系 snapshot 仍属于本阶段后续工作。

### 已有试点：PostgresCdcScan

首个外部输入是具体的固定 Schema 单表 PostgreSQL CDC Scan，不提前抽象公共 IngressScan。
运行协议已经落在所有 Operation 共用的唯一入口上：

```text
turn(None)
├─ Idle → 直接返回，不开事务
└─ Ready(prepared) → 开事务 → apply
   ├─ Action::Idle / 错误 / 背压 → 丢弃事务与 AfterCommit
   └─ Action::Commit + output 已接纳 → commit → AfterCommit
```

零输入 Scan 返回 `Action::Complete` 仍是协议错误。上述事务协调只由 Station 执行，不成为应用 API。
Flow 继续唯一持有写事务启动能力，连接器不得绕过 Operation/Station 打开第二个 writer。
PostgresCdcScan 在事务外 `turn(None)` 中以零超时 poll 并转换；`apply` 保存 checkpoint，
通过 `Action::Commit` 返回可选 Change，由 Station 在同一事务中追加 output。
只有该事务成功后才通过 `AfterCommit` ACK；rollback、背压或 commit 失败不推进 checkpoint/output，
只丢弃 completion。零超时只表示 poll 不等待数据，connector 启动和 ACK 仍同步且有界。
不新增 `Flow::ingest`、Scan 专用 hook 或外部 coordinator。这一提交边界的具体 D0–D7 实施顺序见
[`DEBEZIUM_ROADMAP.md`](DEBEZIUM_ROADMAP.md)。

当前边界：

- 不可变 exact logical Schema；
- 每个 delivery 转换为一个完整、非空 Change，或仅推进 checkpoint 而不伪造空 Change；
- 唯一 `postgres_cdc_scan.checkpoint: Cell<Vec<u8>>` 直接保存 D2 opaque checkpoint bytes，
  不加额外 envelope，也不保存 pending；
- 有界 delivery 和 Station output capacity；背压时不 ACK，由 D2 保留并重投；
- checkpoint 与 output 原子提交，ACK 不确定则 fail-stop/reopen，不把 checkpoint 当 delivery ID；
- Schema mismatch、编码或 commit 失败零部分写入；
- reopen 从已提交 checkpoint 继续接收。

原 `postgres_source.state` 的 pending 布局属于未发布的开发期格式；旧 Flow 必须重建，不提供
alias、兼容读取或迁移。未来本地输入 API 应按真实需求单独确定幂等身份，不反向扩展当前 Scan 协议。

### 已有试点：PostgresSink

`PostgresSink`（tag `12`）把一个单输入 exact relation 物化到独占的固定 Schema PostgreSQL 目标，
沿用 `FlowFactory::resource` 与 `Flow::advance`，不增加 Flow 专用方法。Definition 只保存 canonical、
非敏感 `PostgresTargetSpec`；宿主在 build/open 时注入具体 `PostgresSinkConfig`。当前配置只接受 numeric
IPv4/IPv6，连接握手和每个数据库工作单元都有 5 秒 client deadline。target discovery 是 build 前
显式的只读 catalog 操作，Definition/bind/materialize 与 Flow build/open 本身不访问 PG。

SQLite 与 PG 共用 `relation_sink.state: Cell<Vec<u8>>` 和固定 ID 的批次协议。Ready turn 在
MDBX 事务外批量匹配关系行、规划至多 1024 个具体 mutation；apply 先持久化 Prepared，
`AfterCommit` 才在一个目标事务中先 insert-ignore、再按 ID delete。下一 turn 结算 Ready：
有 continuation 时 `Commit`，该 Change 结束时 `Complete`。目标已提交但结算前崩溃时原样重投
同一组 ID；无需 receipt、delivery sequence 或 digest，也不重投已结算的旧批次。

该试点要求 Sink 独占其目标表、索引与约束，Schema 固定，外部不得改表、改数据或替换/
恢复数据库，同一 target spec 不得被其他 Flow 接管或共享。远端 marker 只标识 ownership/layout
版本，精确 logical Schema 由 Flow binding 与运行时 guard 保证；当前没有 TLS 或在线 Schema evolution。普通 Cargo gate 离线，显式本机
`system-tests/postgres/check_sink.py` 覆盖初始化、大批 insert/delete、混合插删重放、宽 Schema、
1000 条不同记录的交错更新与“PG 已提交/MDBX 仍 Prepared”窗口的进程重开。SQL 次数证据见
`TESTING.md`；尚无 Sink 独立吞吐或长稳 benchmark。

两种 Definition 直接装配共享运行内核，数据库适配只负责目标检查、初始化、精确匹配和原子写入。
没有公共通用 Sink trait、backend enum、plugin registry 或 ORM 抽象。旧 runtime/state 与兼容出口
直接删除；旧 Flow 和目标重建。

### 有限 Scan

根据测试和嵌入式任务需求，可增加显式结束的 `ValuesScan` 或 bounded Scan。结束必须是独立、
可恢复的协议事实，不能用暂时没有输入的 `Idle` 代替。SequenceScan 继续作为简单生成器；若要承担
范围生成，应显式增加终点而不是依赖 `u64::MAX`。

### ResultLogSink

持久保存输出 Change，并为客户端提供独立 consumer cursor：

```rust,ignore
let page = flow.result_log("result")?.read_from(cursor, limit)?;
```

它用于订阅变化、调试 diff、查询间转发和完整序列验证。动态 consumer 的注册、retention 和过期
策略需要明确归属，不能绕过 producer 的完整 consumer frontier。

### MaterializeSink

维护当前关系状态并提供 snapshot 分页。它必须：

- 按完整记录等价关系累计整数权重；
- 权重归零时删除物理状态；
- 验证负权重前缀；
- codec、overflow、capacity 和 commit 失败时回滚完整 turn；
- reopen 后恢复同一关系；
- 明确权重大于一时 snapshot 如何表达重复；
- 提供稳定、有界、可继续的只读 snapshot。

### 其他外部副作用 Sink

`SqliteSink` 与 `PostgresSink` 已用同一固定 ID Prepared 批次覆盖目标/MDBX 提交窗口。
后续网络、文件和数据库连接器仍须先选择 outbox、
幂等 key 或明确的两阶段协议；Operation `turn` 内不得留下无法由该协议重放或验证的可观察副作用。

远端数据库接入遵守以下最小边界，不提前建立通用 SQL Sink 框架：

- Definition 只持久化逻辑 destination key 和非敏感行为配置，不持久化密码、token 或完整 secret DSN；
- 当前 `PostgresSink` 由宿主在 build/open 的 materialize 边界显式注入具体 `PostgresSinkConfig`；
  Schema bind 继续保持纯函数，Operation 不读取全局环境变量或进程级单例；
- 当前同步的 `turn → apply → AfterCommit` 全程独占该 Operation：事务外准备属于 `turn`，
  本地提交后的幂等远端 delivery 属于 `AfterCommit`。连接和请求必须有明确超时；若真实 workload 证明需要异步执行，
  先扩展调度协议，不能把后台任务或第二套状态机藏进具体 Sink；
- SQLite 与 PostgreSQL 只共享已经由两个实现证明相同的 crate 私有 relation mechanics；各 backend
  保留专用 DDL、DML、锁和重放实现。当前不建立公共 Sink trait、backend registry 或 ORM。

### 退出标准

公共 API 使用真实订单 Change 完成：

```text
PostgresCdcScan
→ Filter/Extend/Select
→ ResultLogSink
→ MaterializeSink
→ drop/reopen
→ result/snapshot verification
```

必须覆盖插入、用旧记录 `-1` 加新记录 `+1` 表达的更新、删除、未 ACK delivery 重投、背压、
Schema drift、负权重前缀、fan-out 慢消费者和 reopen。完整端到端测试不使用 SequenceScan 或
Discard。

## 阶段 3：精确行权重与 Distinct

**状态：最小纵向切片已完成。**

### 目标

只实现 Distinct 真正需要的持久状态：按完整 canonical row 维护当前正权重。这里不增加 Store
collection，不发布通用 relation trait，也不预先设计 Aggregate/Join 的 arrangement 或 continuation。

### 私有持久状态

`distinct.weights` 是
`OrderedMap<RowDigest, CollisionBucket, Large>`。256-bit BLAKE3 digest 只定位 bucket；完整
canonical row bytes 才定义记录身份，所以不同 row 即使 digest 冲突也能共存并被精确比较。bucket
以稳定格式保存唯一的 `(row, positive u64 weight)`；weight 归零时删除 row，bucket 归空时删除
map entry。

canonical row 编码由现有关系 Sink 与 Distinct 共用，digest、bucket 和权重更新都留在 operation
crate 私有模块。后续算子只复用真实证明相同的部分。

### Distinct

`Distinct` 是单输入、exact-Schema-preserving Transform，逐事件按输入行序更新权重：

```text
0 → positive        输出 record, diff=+1
positive → positive 无输出
positive → 0        输出 record, diff=-1
```

它不先合并同一 Change 中的重复记录。负前缀或 overflow 使整个 turn 回滚；状态、output 与 input
completion 在同一事务提交，背压和 reopen 保持同一输入语义。

### 已完成结果

- `Distinct` 以 tag `13`、空 payload 和唯一 `distinct.weights` 资源进入统一 bind/materialize/turn 路径；
- collision bucket 使用完整 row 做最终比较，不把 hash 当记录身份；
- codec、边界变化、负前缀/overflow、背压与 reopen 有对应 owner 证据；
- SQL 只新增 `SELECT DISTINCT` lowering；普通 `UNION` 仍未支持；
- Aggregate 已在阶段 4 建立自己的 group/admission/index state；Join 的专用状态仍按其语义另行设计。

## 阶段 4：Aggregate 与多重集算子

**状态：grouped Aggregate 最小纵向切片已完成；global aggregate、多重集集合运算、聚合修饰符与 UDF
仍未完成。**

### 目标

实现真正按输入 diff 维护关系结果的 Aggregate。RunningEventCount 不参与这一算子族。

### 已交付范围

- tag `14` 的单输入 Aggregate 要求非空 group expression，允许零个或多个 call；所有 group 和 call
  在一个 Operation 中按声明顺序绑定、更新并共同构造一行输出。
- 内建函数为 `COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX`。COUNT 输出 non-null `Int64`；
  SUM 只接受 `Int64/UInt64` 并保持类型；AVG 对 `Int64/UInt64` 用 `i128/u128` 精确累计并输出 nullable
  `Float64`；MIN/MAX 接受 non-float flat scalar 并输出 nullable 同类型。
- group key 拒绝 Float32/Float64；global aggregate、grouping sets、aggregate
  `DISTINCT/FILTER/ORDER BY/null treatment`、UDF 及上述范围外类型在创建 Flow 前拒绝。

### 状态模型

Aggregate 只声明三个资源：

```text
aggregate.groups   GroupDigest → full group + group ID + weight + call states
aggregate.entries  (layout, group ID, tuple digest) → full tuple + weight
aggregate.control  next group ID
```

`entries` 的 layout `0` 保存每个完整 canonical input row，是撤回前的 exact admission；其余 layout
保存 Indexed reduction 的 canonical argument tuple。相同持久表达式 tuple 共享一个 layout，不按函数复制
multiset。digest 只定位 collision bucket，完整 bytes 才定义 identity。

私有静态 descriptor 唯一声明 function tag、arity、binding 与 reduction 形态：`Fold` 只操作每组有界小
state，`Indexed` 只操作 argument tuple 和自己的小候选 state，二者都不接收 Store。COUNT/SUM/AVG 是 Fold；
MIN/MAX 是 Indexed。当前极值撤回时，runtime 只扫描该 layout + group，按一次一个 digest collision bucket
分页并只保留当前候选，不把整组 materialize 到内存。

### 输出变化

每个输入事件按行序处理。当 group 结果从 `old_row` 变为 `new_row` 时输出：

```text
old_row, diff=-1
new_row, diff=+1
```

group 消失时只撤回旧行，首次出现时只插入新行，结果未变不输出。null 和非单位 diff 由各 descriptor
按同一 exact admission 解释；负 row 前缀或 arithmetic overflow 回滚整个 turn。

同一 Change 内一个 group 更新多次时逐事件发出上述变化，不按物理 batch 合并。group/call/index 状态、
output 与 input completion 在同一事务提交；错误、背压和 reopen 保持同一完整输入。

### Min/Max 的特殊状态

MIN/MAX 为每个 argument tuple 维护正权重；当前值撤回到零才触发上述分页重扫。null 不进入候选，
zero-weight tuple 立即清理；浮点、List 和 Struct 暂不进入 extrema index。

### 最小切片证据与剩余工作

- owner correctness 已覆盖 tag/payload、三资源、bind/materialize、COUNT/SUM/AVG/MIN/MAX 的有序变化、
  exact admission 整 turn rollback、极值不变不冗余输出和 reopen 后重扫；SQL 有跨 drop/open 的最终关系 witness、
  纯分组 witness 及拒绝路径无目录副作用证据。
- 尚需 global aggregate 的空关系语义、UnionDistinct/Intersect/Except、aggregate DISTINCT/FILTER/ORDER、
  UDF 接入、更多类型，以及大 group cardinality/高更新频率 benchmark；这些不由当前最小切片暗示支持。

## 阶段 5：Join 算子族

### 目标

在阶段 3 的 arrangement、weight invariant 和 materialized oracle 上实现多输入增量 Join。

### 实现顺序

1. 单 key Inner Equi-Join；
2. 多 key Equi-Join；
3. equi key 后的 residual predicate；
4. Semi Join；
5. Anti Join；
6. Left Outer Join；
7. Right/Full Outer Join。

### 状态和 diff

双边状态概念为：

```text
left key  → left rows + weights
right key → right rows + weights
```

一侧变化时查询另一侧状态；输出 diff 是输入 diff 与匹配记录权重的乘积，并在写状态前完整检查
乘法和累计 overflow。

Outer Join 还要维护匹配数量：

- 第一个匹配出现时撤回 null-extended row；
- 最后一个匹配消失时重新插入 null-extended row；
- 另一侧重复记录的权重变化正确更新 match count 和输出。

### 多输入契约

- 保持每个端口内部事件顺序；
- 不依赖跨端口的物理交织；
- 合法端口交织得到同一最终关系；
- left/right identity 和 key Schema 持久化稳定；
- Join state、output、当前输入 completion 和 reclaim 同事务提交；
- 未完成输入完整重放不重复加入 Join state。

### 退出标准

系统排列并验证：

- left then right、right then left 和交错输入；
- 不同物理分批；
- 重复记录和非单位 diff；
- 双侧插入、撤回和更新；
- unmatched/matched 状态转换；
- backpressure、reopen、overflow 和 corruption；
- Inner、Semi/Anti、Outer 各自的独立关系 oracle。

## 阶段 6：有界、顺序与时间算子

### 目标

只在显式信号和状态语义下引入完成、排序、TopK、Window 和时间相关计算，避免给无界变化流添加
含糊的 batch 算子。

### End-of-input 与 Barrier

`Idle` 只表示当前没有进展。有限输入完成、snapshot 边界和一致性切面需要独立、可持久恢复的协议：

- end-of-input；
- barrier identity；
- 各输入端口 barrier 对齐；
- barrier 前状态和 output 的提交边界；
- reopen 后 barrier progress；
- barrier 与 backpressure 的交互。

在这些信号进入 Operation input protocol 前，先评估是否需要从 `Option<OperationInput>` 扩展为明确的
data/control input，同时保持普通 Change 路径简单。

### Sort、Limit 与 TopK

- 完整 Sort 只对有限作用域有确定结果；
- 无界流中的 `Limit` 必须说明达到数量后是否永久停止消费；
- 持续 TopK 维护当前前 K 个关系项，输入变化时撤回旧成员并插入新成员；
- 排序 key、null order、稳定 tie-break 和 Float NaN 必须确定；
- TopK 状态和输出必须理解 diff，不能把当前物理 batch 当成全集。

### Window 与时间

引入窗口前必须选择并定义：

- event time、processing time 或两者；
- timestamp 字段和 timezone；
- watermark；
- late data；
- allowed lateness；
- window close/reopen；
- state cleanup；
- tumbling、hopping、session window 的 identity；
- 窗口 Aggregate/Join 与普通 Aggregate/Join 的复用边界。

时间或随机表达式不能通过默认 ExecutionProps 获得隐式非确定语义；时间必须来自输入、持久化的
执行上下文或显式 control event。

### 退出标准

- 有限 Scan 拥有真正 completion，不依赖 Idle；
- barrier 多输入对齐、reopen 和 backpressure 有独立状态模型；
- TopK/Window 输出在声明的比较域内对重批稳定；
- window cleanup 与 output/cursor 同事务或拥有明确的可恢复协议；
- 时间、timezone、late data 和不兼容版本行为文档化并有公共证据。

## 阶段 7：运行产品化与上层 API 就绪

### 目标

使算子内核具备稳定地承载多个用户接口和真实连接器的能力。该阶段仍不选择唯一用户入口。

### 生命周期

候选运行状态：

```text
Created
Running
Idle
Backpressured
Stopping
Stopped
Completed
Failed
RebuildRequired
```

增加 start、cancel、graceful stop、status、bounded completion 和可恢复删除。状态机必须区分“当前无
输入”“输出受压”“用户停止”“有限任务完成”和“不可恢复失败”。

### 外部 Scan/Sink 协议

已有 `PostgresCdcScan`、`SqliteSink` 与 `PostgresSink` 试点；后续候选包括：

1. 本地 API/AppendLog ingress；
2. 文件 snapshot；
3. Kafka；
4. 其他数据库 CDC；
5. 其他外部副作用 Sink。

外部 Scan 明确 external checkpoint 与 committed Change 的原子提交边界；只有来源确实提供
独立重试 identity 时才另行定义其幂等协议，不以 checkpoint 冒充 identity。外部 Sink 使用
outbox、幂等 key 或明确的两阶段提交协议。`SqliteSink` 与 `PostgresSink` 已共用固定 ID 的持久化
Prepared 批次与目标原子事务覆盖提交空隙。其他连接器不能把对应空隙留给具体 Sink 自行解释。

### 可观测性

至少暴露：

- Flow/Station/Operation 状态与错误；
- input cursor、active input、output head/tail；
- retained bytes、capacity 和 backlog；
- backpressure 来源；
- turn、commit、decode、evaluate、encode 和 reclaim 指标；
- Definition/tag/Schema/version；
- Store 磁盘使用和 materialized state 大小。

### 资源治理

- 已停止 Flow 的可恢复删除；
- 孤立或 incomplete Store 检测；
- 文件系统硬配额；
- result consumer lease/expiration；
- state/output retention；
- catalog 中 flow identity 与 path 的映射；
- 大状态 reopen 和后台维护的明确事务边界。

现有 output capacity 是 per-output soft high watermark，不是磁盘或内存硬配额。

### 性能与并行

先用 reference benchmark 找到真实瓶颈，再考虑 Partition、Exchange 和 Merge。保留 Flow 对唯一 writer
的控制，不让 Station 或连接器自行开始 writer。至少测量：

- Change batch size；
- 表达式数量和记录宽度；
- group/join cardinality；
- 状态和 backlog 大小；
- fan-out 与慢 consumer；
- materialization；
- reopen 和 backlog recovery；
- 长稳文件大小与 tail latency。

### 算子能力目录

为上层 lowering 提供只读、稳定的能力描述，而不是暴露具体 runtime Operation：

- kind 和 arity；
- Schema binding 结果；
- 是否有 output；
- 是否有持久状态；
- 是否保持顺序、diff 和行数；
- 所需 control signal；
- 支持的类型/表达式类别；
- Definition/version identity。

能力目录不能成为第二套可绕过 `OperationDefinition::bind` 的校验入口；最终真相仍是 Definition 的
统一 binding。

### 上层 API

| 接口 | 状态与主要价值 | 与内核的关系 |
| --- | --- | --- |
| Rust Builder | 最直接、类型化、最早可交付 | 直接组装 Definition DAG |
| SQL | 基础 v1 已交付；单文件表达常见逐行变换 | DataFusion logical plan lowering 为同一 DAG |
| Pipeline DSL | 面向固定数据任务，配置友好 | 编译为同一 DAG |
| DataFrame API | 适合程序化关系变换 | 解析表达式并 lowering |
| 其他语言绑定 | 扩大嵌入范围 | 调用稳定 plan/build/run API |

任何接口都不得：

- 在接口层另存一套运行状态；
- 绕过 exact Schema binding；
- 依赖未声明的 Store collection；
- 用自己的 retry 规则改变 Operation Action 语义；
- 把物理 batch、AppendLog offset 或 Station ID 暴露为业务事件 identity。

### 退出标准

- 至少两个不同风格的上层适配器能构造并 reopen 同一语义的 Flow；
- 上层错误能定位到用户计划节点，运行错误能映射回该节点；
- Definition 和 capability/version 足以判断 reopen 或 `RebuildRequired`；
- 外部 Scan/Sink crash/retry 有端到端证据；
- lifecycle、observability、资源删除和磁盘压力行为可预测；
- correctness、benchmark smoke、reference 和 endurance 均通过既定协议。

## 跨阶段统一门禁

每个新增或修改的持久化算子至少满足：

1. **公共 API 行为**：调用者可观察的承诺由公共 correctness 证明。
2. **独立语义 oracle**：状态关系算子不复用生产算法计算 expected。
3. **Definition golden**：tag、payload、truncation 和 canonical decode 有稳定证据。
4. **Schema binding**：成功和每种合法但不兼容输入都有结构化结果。
5. **纯失败无副作用**：binding、声明或拓扑失败不创建 Store 路径。
6. **data layout**：资源名、collection、codec、Size、create/open/reopen 精确。
7. **runtime Schema guard**：错误 input 不安装 Claim，错误 output 回滚 turn。
8. **稳定重批**：展平 input/output 和最终状态满足声明契约。
9. **完整重放**：Idle、Commit、错误、背压、commit 失败和 reopen 不多应用或跳过输入。
10. **事务原子性**：Operation state、output、cursor、active input 和 reclaim 全旧或全新。
11. **关系权重**：维护关系的算子拒绝非法负权重前缀并完整回滚。
12. **损坏拒绝**：malformed Definition、Change、state 无 panic、无部分写入。
13. **互操作**：Change 输出保持标准 Arrow IPC Stream；新增类型同步验证标准 reader。
14. **性能证据**：correctness 后增加真实 workload benchmark，不用微基准代替语义证据。
15. **文档同步**：算子语义、持久边界、使用方式和验证命令进入 operation/flow README 与 Rustdoc。

具体测试所有权、最低持久化证据和 benchmark 协议继续遵守 [`TESTING.md`](TESTING.md)。

## 最近三个实施里程碑

### 里程碑 A：现有算子成为模板（已完成）

```text
统一规格
→ RunningEventCount 语义命名纠正
→ conformance checklist
→ 缺口测试
→ capability matrix
```

目标不是增加数量，而是确保后续每个算子都沿同一个 Definition、binding、materialize、turn、reopen
和验证路径进入产品。

### 里程碑 B：真实数据闭环

```text
PostgresCdcScan
→ Filter/Extend/Select
→ SqliteSink / PostgresSink
→ crash / reopen
```

真实 Scan 到专用关系 Sink 的闭环已经存在：`PostgresCdcScan` 提供固定 Schema 单表持续 CDC，
`SqliteSink` 与 `PostgresSink` 提供可查询终点。初始全量、发布加固、ResultLog/Materialize 和应用可消费的
通用结果边界仍待实施。复杂算子仍主要依靠
测试 fixture 自证，上层用户 API 也尚未形成完整闭环。

### 里程碑 C：状态关系算子

```text
exact-row weights（已完成）
→ Distinct（已完成）
→ Group Aggregate：COUNT/SUM/AVG/MIN/MAX（最小切片已完成）
→ Global Aggregate / multiset set ops / aggregate UDF
→ Inner Join
```

Distinct 提供共享的 canonical row、collision bucket 和 checked weight 原语；Aggregate 在其上新增私有
group ID、exact admission、Fold/Indexed descriptor 和 argument-tuple layout。Join 的 keyed arrangement、fan-out
和 continuation 仍按 Join 语义另行设计。

## 开放决策

以下尚未解决的问题必须在对应阶段开始前关闭，不能由单个算子临时决定。RunningEventCount 的
命名，以及 Date32/Timestamp/Decimal128 的第一版 Change 边界，已经在阶段 0/1 关闭：

- 未来本地输入 API 的幂等 identity 作用域是 input、Flow 还是全局？这不要求把 connector checkpoint 当作 identity。
- ResultLog consumer 是 Definition 的静态一部分，还是运行期动态注册？
- Materialize 如何稳定编码完整 Record key、weight 和分页 continuation？
- Join 的 keyed arrangement、fan-out 和有界 continuation 应该如何持久化？
- Consolidate 的显式作用域是一个 Change、barrier 区间还是完整关系？
- Date/Timestamp/Decimal 上哪些额外 DataFusion operator/type 组合值得补齐证据并加入已承诺集合？
- 时间和随机表达式来自输入、持久执行上下文还是 control signal？
- end-of-input/barrier 如何进入统一 Operation input protocol？
- bounded Sort、持续 TopK 和 Window 各自的完成及 retention 边界是什么？
- 多个 Flow 是否共享输入日志或 arrangement；若共享，由哪个组合根拥有 retention？
- 何时引入 partition/exchange，而不破坏唯一 writer 和确定性提交？
- SQL 在 Join、global Aggregate、aggregate UDF、Window 等底层能力完成后扩展到哪些语法，以及何时需要只读
  capability/introspection？

## 内核稳定准入定义

只有同时满足以下条件，算子与执行内核才进入稳定接口评估：

- 基础无状态、结构、真实 Scan/Sink、Materialize、Distinct、Aggregate 和至少 Inner Join 有完整证据；
- Change 的 diff、顺序、重复和重批语义在所有算子族中一致；
- 关系状态统一处理 weight、负前缀、overflow、zero cleanup 和 reopen；
- exact Schema 对齐、实用 Date/Timestamp/Decimal 类型和表达式能力矩阵可用；
- Scan checkpoint 与外部 Sink 幂等提交边界可用；
- Flow start/cancel/stop/status/reopen/delete 生命周期完整；
- corruption、crash、backpressure、磁盘压力和不兼容升级行为可预测；
- 代表性 workload 有 smoke、reference 和 endurance 证据；
- 至少两个不同的用户接口候选只通过公共 Definition/Flow 能力构建同一内核；
- 所有跨版本不兼容都被明确拒绝或拥有经过测试的迁移路径。

在此之前，内核可以持续增加算子和承载基础或实验性的上层接口；也不能让 SQL v1、DataFrame 或
任何单一 API 反向冻结不合适的内核抽象。
