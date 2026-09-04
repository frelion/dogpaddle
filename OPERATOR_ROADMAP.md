# DogPaddle 算子与执行内核路线图

本文定义 DogPaddle 算子体系和执行内核的演进阶段、语义边界、交付物与退出标准。
它是实施路线；阶段 0/1 已完成的范围在本文明确标记，后续阶段中的候选算子或用户接口仍不表示
已经交付。当前精确能力以根目录 [`README.md`](README.md) 和各产品 crate 的 README 为准。

## 产品方向

DogPaddle 的核心产品不是某一种查询语言，而是一套嵌入式、持久化、可恢复、可组合的
数据流算子与运行内核。未来用户可以通过不同上层接口构造同一套底层 Flow，例如：

- 直接 Rust Builder；
- 面向常见数据任务的声明式 Pipeline API；
- DataFrame 风格 API；
- SQL；
- 由其他应用或语言编译得到的持久化计划。

这些接口都是候选适配器，不进入 Change、Store、Operation 或 Flow 的核心语义。路线首先回答：

1. 一组算子是否拥有精确、可组合、可持久恢复的行为；
2. 状态算子能否正确解释有序、带正负 diff 的变化流；
3. 失败、背压、重放和进程重开是否保持同一业务结果；
4. Source、Transform 和 Sink 是否足以承载真实数据闭环；
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
| Source | SequenceSource | 生成连续 `u64` 测试/系统事件 | 保留，但不代表通用 ingress |
| Transform | RunningEventCount | 运行事件计数器 | 已明确为事件观测，不是关系 Aggregate |
| Transform | Project | 严格递增顶层索引的零拷贝删列 | 保留为结构/物理优化算子 |
| Transform | Filter | DataFusion Boolean Expr 行过滤 | 保留为基础无状态算子 |
| Transform | Extend | 保留输入并追加一个表达式列 | 保留为基础无状态算子 |
| Transform | Select | 有序多表达式完整输出 | 保留为基础无状态算子 |
| Transform | SchemaAlign | 显式完整 Schema 重塑 | 已完成基础结构对齐；不做隐式 coercion |
| Transform | UnionAll | exact Schema 多输入原样合并 | 保留为基础多输入算子 |
| Sink | SqliteSink | 将差分流幂等物化到独占的 SQLite 表 | 已完成首个本地外部副作用 Sink |
| Sink | Discard | 无副作用地完成输入 | 保留为测试和显式丢弃终点 |

当前十个内建算子已进入统一能力/conformance 表；覆盖度仍小，但已实现行为的可靠性边界值得
继续保留。后续工作重点是扩展算子族和公共
conformance，而不是让某个上层 API 反向定义运行内核。

## 目标分层

```text
可选用户接口层
├── Rust Builder
├── Pipeline DSL
├── DataFrame API
├── SQL
└── 其他语言或应用适配器
          │
          │ 解析、类型检查、优化、lowering
          ▼
算子组合层
├── Source Definitions
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

- 明确声明 `Source`、`Transform(nonzero arity)` 或 `Sink(nonzero arity)`；
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

- `Idle` 回滚本 turn 全部写入；
- `Commit` 只提交 continuation 与可选 output，不完成输入；
- `Complete` 原子提交状态、可选 output、cursor、active rotation 和 reclaim；
- output capacity 拒绝回滚整个 turn 并保持输入 identity；
- codec、overflow、Schema、Store 或 commit 错误不留下部分业务状态；
- 外部副作用只通过另行定义的幂等提交协议执行。

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

Rust Builder、SQL 或其他接口可以保存自己的源描述，用于解释、重新编译和诊断；运行时恢复仍基于
canonical Flow/Operation Definition。若接口版本、catalog 或 lowering 规则变化导致语义不兼容，
明确要求重建，不让运行层猜测。

## 阶段总览

| 阶段 | 主目标 | 核心交付物 | 完成后得到什么 |
| --- | --- | --- | --- |
| 0（已完成） | 固化算子产品契约 | RunningEventCount 命名、分类、conformance、能力矩阵 | 现有算子成为明确基线 |
| 1（已完成基础范围） | 完成基础无状态/结构算子族 | SchemaAlign、Date/Timestamp/Decimal 传输、表达式状态矩阵 | 上层可可靠表达常见逐行变换 |
| 2（进行中） | 打通真实 Source/Sink | SqliteSink、Ingress、ResultLog、Materialize | 不依赖测试 Source/Sink 的真实数据闭环 |
| 3 | 建立关系状态原语 | relation state、arrangement、Consolidate、Distinct | 后续状态关系算子的共同基座 |
| 4 | 完成 Aggregate 与多重集算子 | Count/Sum/Min/Max、Group、集合运算 | 可持续维护聚合关系 |
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
- `cargo xtask check`、`cargo xtask bench-plan-check` 和相关 smoke benchmark 通过；
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
buffer/diff/顺序。Flow 再覆盖 source → SchemaAlign → Project → Select → Extend → Filter →
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

## 阶段 2：真实 Source 与 Sink

### 目标

消除只能依靠 SequenceSource 和 Discard 验证 Flow 的限制，用真实 Arrow Change 打穿输入、变换、
结果订阅和当前关系查询。

### 已完成：SqliteSink

`SqliteSink` 是首个本地外部副作用 Sink：它为精确 input Schema 创建并独占一个 SQLite `STRICT`
表，以 MDBX 中的版本化具体 mutation 批次覆盖 SQLite commit 与 MDBX commit 之间的失败窗口。
它不引入 SQLite 元数据表；在目标表未被外部修改、数据库文件未被替换或恢复的约束下，重放保持
最终结果恰好一次。通用 ingress、结果订阅与关系 snapshot 仍属于本阶段后续工作。

### IngressSource

新增引擎受控的通用输入算子。候选运行 API：

```rust,ignore
flow.ingest("orders", ingestion_id, change)?;
```

Flow 当前唯一持有写事务启动能力，连接器不得绕过 Flow 打开第二个 writer。ingest 应由 Flow 协调，
原子完成 input append、幂等 identity 和必要的 checkpoint 状态。

Ingress 至少定义：

- 不可变 exact logical Schema；
- 每次提交一个完整、非空 Change；
- retained-byte capacity 和 backpressure；
- 可持久、可重试的 ingestion identity；
- 重复 identity 的幂等结果；
- Schema mismatch、编码或 commit 失败零部分写入；
- reopen 后继续接收；
- 未来 external offset 与 ingestion identity 的映射位置。

第一版只需要可靠本地 ingress API，不直接耦合 Kafka、数据库或文件连接器。

### 有限 Source

根据测试和嵌入式任务需求，可增加显式结束的 `ValuesSource` 或 bounded source。结束必须是独立、
可恢复的协议事实，不能用暂时没有输入的 `Idle` 代替。SequenceSource 继续作为简单生成源；若要承担
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

`SqliteSink` 已用持久化具体 mutation 批次定义了第一个专用幂等提交边界。后续网络、文件和远程
数据库连接器仍须先选择 outbox、幂等 key 或明确的两阶段协议；Operation `turn` 内不得留下无法
由该协议重放或验证的可观察副作用。

远端数据库接入遵守以下最小边界，不提前建立通用 SQL Sink 框架：

- Definition 只持久化逻辑 destination key 和非敏感行为配置，不持久化密码、token 或完整 secret DSN；
- 第一个远端 Sink 落地时，由宿主在 build/open 的 materialize 边界显式注入 destination resolver；
  Schema bind 继续保持纯函数，Operation 不读取全局环境变量或进程级单例；
- 当前同步 `turn(&mut self)` 会独占该 Operation，并让外部提交发生在 Flow 的 MDBX 写事务期间；
  第一版远端客户端必须有明确的连接和请求超时。若真实 workload 证明需要异步或事务外 I/O，先扩展
  Flow 的提交协议，不能把后台任务或第二套状态机藏进具体 Sink；
- SQLite、PostgreSQL 与 MySQL 各自保留专用 DDL、DML、锁和重放实现。只有第二个实现证明行身份或
  配置注入语义完全相同时，才提取对应的小型公共组件。

### 退出标准

公共 API 使用真实订单 Change 完成：

```text
IngressSource
→ Filter/Extend/Select
→ ResultLogSink
→ MaterializeSink
→ drop/reopen
→ result/snapshot verification
```

必须覆盖插入、用旧记录 `-1` 加新记录 `+1` 表达的更新、删除、重复 ingestion identity、背压、
Schema drift、负权重前缀、fan-out 慢消费者和 reopen。完整端到端测试不使用 SequenceSource 或
Discard。

## 阶段 3：关系状态原语

### 目标

为 Materialize、Distinct、Aggregate、Join 和后续 TopK 建立一个共享的关系状态设计，而不是让每个
算子重新发明 Record key、weight、overflow 和负前缀规则。

### Relation state

概念模型为：

```text
Record 或 Key → rows / integer weights / operator-specific state
```

Store 继续不依赖 Arrow。Operation 层定义稳定的 Record/Key/state codec，并使用具体的 Cell 或
`OrderedMap<K, V, SIZE>`。只抽取多个真实算子都需要且语义完全一致的公共实现，不建立万能 Store trait
或第二套动态 collection 系统。

### Arrangement / Index

为后续按 key 查找的算子提供持久 arrangement：

- 一个 key 对应多个完整记录及各自权重；
- exact key Schema 和 row Schema；
- point lookup、有界 scan 和稳定 continuation；
- 重复记录与非单位 diff；
- zero-weight cleanup；
- codec/version、reopen 和 corruption；
- 独立模型验证。

Arrangement 是否成为公共用户可见算子，应由两个以上实际消费者证明；它可以先作为 Operation 内部
共享实现，不能为了未来可能复用而过早暴露公共抽象。

### Consolidate

显式合并等价记录的 diff，但不能偷偷改变普通 Change 或其他算子的语义。必须定义 consolidation
作用域：一个输入 Change、显式 barrier 之间，还是持久关系的当前状态。不同作用域应是不同能力，
不能共用含糊名称。

### Distinct

按记录当前总权重实现：

```text
0 → positive        输出 record, diff=+1
positive → positive 无输出
positive → 0        输出 record, diff=-1
```

导致负权重的输入报错并回滚。Distinct 的输出和状态必须对稳定重批、完整重放、背压及 reopen 保持
契约。

### 退出标准

- Materialize 和 Distinct 共享经过证明的 Record/weight 基础语义；
- arrangement 支持 Aggregate/Join 所需的真实 key/row 访问模式；
- 每个公共或内部稳定格式都有 codec golden 和 reopen；
- 独立 multiset model 覆盖非单位 diff、重复、归零、负前缀和 overflow；
- 任何关系状态更新都与 output 和 input completion 保持同事务原子性。

## 阶段 4：Aggregate 与多重集算子

### 目标

实现真正按输入 diff 维护关系结果的 Aggregate。当前 RunningEventCount 不参与这一算子族。

### 实现顺序

1. 无分组 `CountAggregate`；
2. 按 key 分组的 Count；
3. Sum；
4. Min/Max；
5. Average，由 Sum/Count 状态或独立 accumulator 明确定义；
6. 多个 aggregate expression 共享同一 group state；
7. 必要的多重集集合算子，如 UnionDistinct、Intersect、Except。

### 状态模型

一个 Aggregate Operation 应在同一 Station 中共同更新一个 group 的多个 accumulator：

```text
GroupKey → {
    input_weight,
    count_state,
    sum_state,
    min/max multiset state,
    ...
}
```

不要默认每个 aggregate 函数成为独立 Station，因为它们需要观察完全相同的输入序列并共同构造一行
输出。

### 输出变化

当 group 结果从 `old_row` 变为 `new_row` 时，变化流至少需要表达：

```text
old_row, diff=-1
new_row, diff=+1
```

group 消失时撤回旧行；首次出现时插入新行。空输入、null、非单位 diff、overflow 和 accumulator
类型都必须独立定义。

同一 Change 内一个 group 可能更新多次。实现前必须固定展平 output 规则，不能简单按物理 batch
只输出一个最终结果，否则稳定拆批或合批会改变业务事件序列。若希望引入 consolidation，需要使用
阶段 3 的显式作用域或未来 barrier，而不是依赖 Change 边界。

### Min/Max 的特殊状态

Min/Max 不能只保存一个标量；当前最值被撤回后需要找到下一项。必须维护带权重的有序 multiset，
并验证：

- 相同值重复插入和逐次撤回；
- null 规则；
- Float NaN 和排序规则；
- zero-weight cleanup；
- 大 group 的状态布局和 scan 成本。

### 退出标准

- Aggregate 对正负和非单位 diff 产生正确关系变化；
- 分组和无分组的空关系/null 语义明确；
- 同一输入事件序列的稳定重批得到契约一致的展平输出和最终状态；
- replay、backpressure、overflow、corruption 和 reopen 均无部分状态；
- 独立 multiset/aggregate oracle 不复用生产实现；
- 大 group cardinality 和高更新频率拥有对应 benchmark。

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

- 有限 source 拥有真正 completion，不依赖 Idle；
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

### 外部 Source/Sink 协议

建议连接器顺序：

1. 本地 API/AppendLog ingress；
2. 文件 snapshot；
3. Kafka；
4. 数据库 CDC；
5. 外部副作用 Sink。

外部 Source 明确 external offset、ingestion identity 和 committed Change 的映射；外部 Sink 使用
outbox、幂等 key 或明确的两阶段提交协议。`SqliteSink` 已用持久化 mutation 批次覆盖本地 SQLite
commit 与 MDBX commit 的空隙；其他连接器不能把对应空隙留给具体 Sink 自行解释。

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

### 上层 API 候选

| 候选接口 | 主要价值 | 与内核的关系 |
| --- | --- | --- |
| Rust Builder | 最直接、类型化、最早可交付 | 直接组装 Definition DAG |
| Pipeline DSL | 面向固定数据任务，配置友好 | 编译为同一 DAG |
| DataFrame API | 适合程序化关系变换 | 解析表达式并 lowering |
| SQL | 适合熟悉关系查询的用户 | parser/catalog/planner 后 lowering |
| 其他语言绑定 | 扩大嵌入范围 | 调用稳定 plan/build/run API |

任何候选接口都不得：

- 在接口层另存一套运行状态；
- 绕过 exact Schema binding；
- 依赖未声明的 Store collection；
- 用自己的 retry 规则改变 Operation Action 语义；
- 把物理 batch、AppendLog offset 或 Station ID 暴露为业务事件 identity。

### 退出标准

- 至少两个不同风格的上层适配器能构造并 reopen 同一语义的 Flow；
- 上层错误能定位到用户计划节点，运行错误能映射回该节点；
- Definition 和 capability/version 足以判断 reopen 或 `RebuildRequired`；
- 外部 Source/Sink crash/retry 有端到端证据；
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
IngressSource
→ Filter/Extend/Select
→ ResultLogSink
→ MaterializeSink
→ snapshot / reopen
```

这是最优先的新能力。虽然 `SqliteSink` 已提供可查询终点，但没有真实 Source 和应用可消费的通用
结果边界，复杂算子仍主要依靠测试 fixture 自证，上层用户 API 也无法形成完整闭环。

### 里程碑 C：关系状态到 Aggregate

```text
Relation state
→ Arrangement
→ Distinct
→ Count/Sum Aggregate
→ Group Aggregate
```

先完成 Record key、weight、负前缀和 materialized oracle，再进入 Join；不要同时引入 Aggregate 和
Join 的全部状态问题。

## 开放决策

以下尚未解决的问题必须在对应阶段开始前关闭，不能由单个算子临时决定。RunningEventCount 的
命名，以及 Date32/Timestamp/Decimal128 的第一版 Change 边界，已经在阶段 0/1 关闭：

- Ingress identity 的作用域是 input、Flow、connector partition 还是全局？
- ResultLog consumer 是 Definition 的静态一部分，还是运行期动态注册？
- Materialize 如何稳定编码完整 Record key、weight 和分页 continuation？
- Relation state/arrangement 哪些能力属于内部共享实现，哪些值得成为公共算子？
- Consolidate 的显式作用域是一个 Change、barrier 区间还是完整关系？
- Aggregate 在同一事件序列中如何输出旧值撤回和新值插入，才能保持重批契约？
- Date/Timestamp/Decimal 上哪些额外 DataFusion operator/type 组合值得补齐证据并加入已承诺集合？
- 时间和随机表达式来自输入、持久执行上下文还是 control signal？
- end-of-input/barrier 如何进入统一 Operation input protocol？
- bounded Sort、持续 TopK 和 Window 各自的完成及 retention 边界是什么？
- 多个 Flow 是否共享输入日志或 arrangement；若共享，由哪个组合根拥有 retention？
- 何时引入 partition/exchange，而不破坏唯一 writer 和确定性提交？
- 哪个上层 API 最先产品化，以及它需要哪些只读 capability/introspection？

## 内核稳定准入定义

只有同时满足以下条件，算子与执行内核才进入稳定接口评估：

- 基础无状态、结构、真实 Source/Sink、Materialize、Distinct、Aggregate 和至少 Inner Join 有完整证据；
- Change 的 diff、顺序、重复和重批语义在所有算子族中一致；
- 关系状态统一处理 weight、负前缀、overflow、zero cleanup 和 reopen；
- exact Schema 对齐、实用 Date/Timestamp/Decimal 类型和表达式能力矩阵可用；
- Source checkpoint 与外部 Sink 幂等提交边界可用；
- Flow start/cancel/stop/status/reopen/delete 生命周期完整；
- corruption、crash、backpressure、磁盘压力和不兼容升级行为可预测；
- 代表性 workload 有 smoke、reference 和 endurance 证据；
- 至少两个不同的用户接口候选只通过公共 Definition/Flow 能力构建同一内核；
- 所有跨版本不兼容都被明确拒绝或拥有经过测试的迁移路径。

在此之前，内核可以持续增加算子和承载实验性上层接口，但不为 SQL、DataFrame 或任何单一 API
提前冻结不合适的抽象。
