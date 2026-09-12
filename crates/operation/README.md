# dogpaddle-operation

`dogpaddle-operation` 提供具体、强类型的 Operation Definition、持久化 Data 和运行实例。
Definition 是无副作用、可持久化的数据；它声明所需数据对象的稳定逻辑名、collection 类型
与键值类型，也把有序、精确的输入 logical Arrow Schema 纯绑定为一个
一次性的编译结果。Flow 在 `build/open` 阶段先绑定完整拓扑，再创建或打开类型化对象，并消费
binding 装配运行实例。方向严格单向：`Definition → OperationBinding → runtime Operation`；运行实例
只保存执行参数和具体持久化 collection，不保留 Definition 或 binding。具体算子不接触 `Store`
或 `DataHandle`。

## 数据边界

共享的 Arrow Schema、批量差分模型和“每个 Change 一个自描述 IPC Stream”的编码属于独立的
`dogpaddle-change` crate。Operation 的运行接口以内存中的 `Change` 为输入输出；Operation 只
负责数据变换和自己声明的持久化状态，不读取边日志，也不决定物理
batch 的合并与 flush。Change 的行位置是事件顺序；Operation 必须依次观察输入事件，并按
其声明的语义产生有序输出，不能把未 consolidation 的输入当作可交换集合。除非将来接收到
独立定义的窗口、barrier 或 flush 信号，Operation 的展平输出事件序列和最终业务状态必须在稳定
合并或切分 Change 后保持不变，也不能因同一个 Change 被分成多少个 `Commit` turn 而改变。显式声明
跨端口无序的多输入关系算子只保持每个端口的事件子序列和最终关系状态；`UnionAll` 与
`InnerEquiJoin` 的跨端口交织由 Station 调度，可能随分批变化。除此之外，物理 Change 边界和 turn 边界都不能被算子当成
业务事件。这个比较域要求每种分批的输入和对应输出都能
由其声明的 Arrow 类型物理表示；例如不能要求 `Utf8` offset 已溢出的单个 `RecordBatch` 成功构造。

外部 Scan 返回普通 `Change`，由 Station 完成 Schema/capacity 校验与日志追加，不能绕过
Station 直接访问 output。稳态 CDC 把 checkpoint 与 output 放在同一事务；PostgreSQL/MySQL
初始快照则先把完整 IPC 写入各自的私有持久 spool，封口后再通过同一 Station
output 逐条原子发布。这是两个具体 Scan 的启动状态，不引入通用 ingress 协议。

## Schema 绑定

这里的 Schema 是一个端口承载记录的完整、精确 logical Arrow Schema，不包含物理 IPC 中固定的
`$dogpaddle.diff` 字段。字段名称、顺序、类型、nullability、嵌套结构以及 Schema/Field metadata
都属于匹配内容；它不是“需要哪些列”的局部约束，也不是每个 Change 可以变化的动态类型。

[`OperationDefinition`] 的统一 `bind` 入口接收按端口顺序排列的 `SchemaRef`：Scan 收到空 slice，Transform
和 Sink 收到恰好由 [`OperationKind`] 声明的数量。绑定先验证每个输入都是合法 `DogPaddle` logical
Schema，再由具体 Definition 接受或拒绝，并为 Scan/Transform 返回唯一、完整的 output Schema；
Sink 必须没有 output。一个 Definition 可以在不同 Flow 中绑定不同输入，但同一次 Flow build/open
完成后，每条 output 只对应一个精确 Schema。

绑定必须是纯且确定的：相同持久化 tag、payload 和有序 input Schemas 必须得到相同语义。结果是
短生命周期、只能消费一次的 `OperationBinding`，可携带 Schema 相关的已编译执行信息及最终
materialize closure；它不写 Store，也不进入持久化格式或运行态对象。目前 `SequenceScan` 固定
输出 `{ value: UInt64 non-null }`；`RunningEventCount` 接受任意合法的单一输入并固定输出
`{ count: UInt64 non-null }`；`Distinct` 接受一个任意合法的 exact input Schema 并原样作为 output；
`Aggregate` 把非空 `GROUP BY` 表达式与一组聚合调用绑定为一个完整 output Schema；
Project 按稳定顶层字段索引绑定输入，拒绝越界、重复或重排，
并以选中字段的完整 Schema 作为 output；Filter 用绑定后的 Boolean 表达式保持 input Schema；
Extend 由绑定表达式唯一推导一个新增字段的类型和 nullability；Select 从同一个原始输入计算有序的完整输出列；
`SchemaAlign` 从同一个原始输入计算有序字段，并显式声明名称、目标 nullability、Field metadata
和 Schema metadata；`UnionAll` 要求所有输入 Schema 完全相同并原样转发 Change；`InnerEquiJoin`
分别绑定两个输入上的非空等值键，并固定输出左侧全部字段后接右侧全部字段；Discard 接受任意
合法的单一输入且没有 output；`SqliteSink` 还把合法输入编译为确定的 `STRICT`
表布局、绑定 SQL 和无损行编码；`PostgresSink` 把单一 exact relation 输入绑定为固定的 `PostgreSQL`
表布局与参数化语句。无需额外的
`Any/Exact` 约束 DSL、Schema registry 或 fingerprint。

## 原子 Transform

[`operation::AtomicOperation`] 表示能在调用方的一次 Store 事务中完整消费一个 Change 的 Transform。
它可以读写自己声明的持久 Data、增加输出行或改变 diff，但不能保留跨 turn continuation、执行外部
副作用或产生 `AfterCommit`。同一 Station 中后续 Operation、最终 output 或 commit 失败时，当前
Operation 的全部重放相关变化必须能随事务回滚。

需要多次 turn 才能完成一个 Change、但能从未变化的 durable state 安全重放未提交 turn 的 Transform
显式声明为 `TurnTransform`。它实现完整的 `TurnOperation` 协议，只能位于 Station 首项，但可以让
后续单输入 Atomic 在同一事务中消费本 turn 的输出。continuation、首项状态、全部尾项状态、最终
output 与适用的输入完成共同提交；尾项错误或背压会回滚本 turn，并丢弃首项的 `AfterCommit`。

`ExclusiveTransform` 同样使用完整 turn 协议，但要求在其输出之后先形成独立持久化边界，因此必须
独占 Station。它用于未承诺从相同 durable state 重算同一输出的 Transform；例如含 `Volatile` 或
placeholder 表达式的现有表达式算子仍声明为 Exclusive，避免下游失败把其结果和下游状态一起重算。

资格由具体 Definition instance 显式写入 [`OperationKind`]。Project、`RunningEventCount`、Distinct、
`UnionAll` 恒为 `AtomicTransform`；Filter、Extend、Select、`SchemaAlign` 与 Aggregate 还检查全部
持久表达式。表达式树只接受 row-local scalar 构造和 `Immutable` scalar function；`Stable`、
`Volatile`、placeholder、subquery、aggregate/window、unnest、外部引用等实例声明为
`ExclusiveTransform`，继续使用相同 Schema binding 和执行 kernel，但必须独占 Station。
`InnerEquiJoin` 显式声明为两输入 `TurnTransform`，用持久 continuation 将一个完整输入拆成可重放的
验证与输出页，并允许后接单输入 Atomic tail。

这里没有第二套 Definition、binding 或 codec。能完整消费 Change 的 Transform 只产生一种 atomic
kernel；统一 `OperationDefinition::bind` 根据 kind 保留为 `Operation::Atomic`，或把需要独占边界的
实例包装为 `Operation::Turn`。`TurnTransform` 直接绑定同一个 `TurnOperation` 运行接口。因此
eligibility、持久化字节和运行语义都只有一个来源。

Filter、Extend、Select、`SchemaAlign` 与 `Aggregate` 的公共入口直接接收 `DataFusion` [`Expr`]；`dogpaddle_operation` 在 crate 根级重导出
[`Expr`]、[`col`]、[`ident`]、[`lit`]、[`cast`]、[`try_cast`] 和 [`ScalarValue`]，调用方不再学习另一套表达式
builder。需要按 Arrow 字段名逐字引用时使用 [`ident`]；[`col`] 保留 `DataFusion` 自身的大小写正规化和
multipart identifier 解析规则。Definition 的 `try_new` 立即使用 `datafusion-proto` 编码 `Expr`，无法编码时返回构造错误；
公开 getter 从同一表达式定义返回 `Expr`，不引入 `DogPaddle` 自有 AST。

Definition payload 直接保存 `DataFusion` Expr protobuf，不保存 `PhysicalExpr`。Schema bind 将完整 input
Schema 交给 `DataFusion` `create_physical_expr`；表达式的字段解析、type、nullability、cast 与
运行期 `evaluate` 全部由 `DataFusion` 定义。该 API 假定 logical coercion 已完成，而本 crate 不运行
logical/SQL planner，因此不会额外插入隐式 cast；混合类型表达式需要调用方显式 [`cast`]。binding 只保存 exact input Schema、physical expression 和
派生 output 属性；open 从 protobuf 还原 `Expr` 后重新完成同一过程。`DogPaddle` 继续负责完整 Schema
guard、Filter/Extend/Select/SchemaAlign 的 output Schema 约束，以及 records/diffs 的 Change 语义。

这份 protobuf 是版本绑定的持久格式，不承诺跨 `DataFusion` 版本兼容。工作区精确 pin 相互匹配的
`DataFusion`、`datafusion-proto` 与 Arrow；升级必须审查 proto roundtrip、physical planning 和执行语义。
当前仍是开发期格式；升级依赖后只维护新的 canonical payload 与证据，旧数据库直接删除并重建，
不承诺兼容、猜测或迁移旧表达式。当前版本拒绝 Expr 自身携带的非空 metadata，包括 Alias、
Literal、Cast、TryCast、Placeholder 和嵌套 Arrow Field metadata，因为 `DataFusion` protobuf 用
无序 map 编码这些值，不能形成稳定的 Definition bytes。SchemaAlign 明确声明的 target
Field/Schema metadata 不受此限制。DataFusion 的采用不等于引入 SQL 层。

### Arrow 类型边界

Change v1 的稳定 Schema/IPC 传输集合现为 Null、Boolean、全部 8/16/32/64 位整数、Float32/64、
Utf8、Binary、Date32、Timestamp、Decimal128、List 和 Struct。Timestamp 支持 Second、Millisecond、
Microsecond、Nanosecond 四种单位与可选非空 timezone；Decimal128 precision 为 `1..=38`，正 scale
不超过 precision，负 scale 按 Arrow 类型保留。`Change::try_new`、全量解码和被选择字段的投影解码
还会递归要求每个 Decimal128 non-null slot 满足 `|unscaled| < 10^precision`；祖先 List/Struct null
不豁免物理 non-null child，未选择字段不读取或验证 value。Project、UnionAll 及表达式直接列路径继续按 exact
Schema 搬运这些字段，不能据此推导任意 `DataFusion` kernel 都已成为产品能力。

Date32、Timestamp 与 Decimal128 在 Change 层拥有 Schema validation、完整/选择性 IPC、标准 Arrow
reader 互操作、嵌套/投影和损坏拒绝。Operation 层进一步承诺一个精确纵向切片：Date32、无 timezone
的 Millisecond Timestamp、`Decimal128(10, 2)` 可经 Project、Select 和 Extend 直接复制；
`SchemaAlign` 覆盖这些直接列、nullability 放宽、Date32 → Int32、Timestamp(Millisecond) → Int64 和
`Decimal128(10, 2)` → `Decimal128(12, 3)` 的显式 cast；Filter 覆盖三类字段与同类型 literal 的组合
比较。三组公共测试都经过 Definition `encode → decode → re-encode → bind → materialize → turn`，
并检查 buffer/diff/顺序。Flow 还覆盖
`SequenceScan → SchemaAlign → Project → Select → Extend → Filter → RunningEventCount → Discard`
的 build、运行和两次 reopen。

上述范围不承诺其他 Timestamp unit/timezone、跨类型转换、时间运算、Decimal 算术或舍入。
LargeUtf8、LargeBinary、FixedSizeBinary 等尚未进入 Change v1，因而在统一 Schema guard 被明确拒绝，
留给后续基于真实 workload 扩展。

### 表达式能力状态

能力按证据而不是按 `DataFusion` API 面积划分。这里的“已承诺”要求一个精确 operator/type 组合能
canonical protobuf roundtrip、针对 exact input Schema bind、完成 scalar/array evaluate，并进入真实
Operation 与 Flow 的 build/open/reopen 纵向证据；它不自动扩展到同一 operator 的其他 Arrow 类型组合。

| 状态 | 当前范围 | 调用者应如何理解 |
| --- | --- | --- |
| 已承诺 | exact 列引用；Boolean 列作为 Filter predicate；`UInt64` 列与同类型 literal 的 equality；`UInt64 → Utf8` 显式 cast；以及上节精确列出的 Date32/Timestamp(Millisecond, no timezone)/Decimal128 direct-copy、同类型比较与 `SchemaAlign` cast 组合 | 只依赖这些已走通持久 Flow 的精确组合；混合类型仍由调用方显式 cast |
| `DataFusion` 可规划、`DogPaddle` 未承诺 | 已有 Operation 级执行证据但尚无对应完整 Flow 纵向证据的 Boolean `and/or/not`、`is_null`、代表性 scalar/array `eq/not_eq`、整数加法与 `Utf8 → Int64` `try_cast`；只有 protobuf roundtrip 证据的 `is_not_null`；其他算术/比较、`between`/alias、内建函数、复杂嵌套表达式；未列出的 Timestamp unit/timezone、时间/Decimal 运算和其他 temporal/decimal cast | 当前 pin 上能构造、bind 甚至执行仍不构成持久产品契约；补齐精确 Flow build/open/reopen 证据后才能进入上一行 |
| 明确拒绝 | 不能逐字 canonical protobuf roundtrip 的 Expr；缺失/歧义字段或 `DataFusion` 无法 physical-plan 的表达式；Filter 的非 Boolean 结果；隐式类型 coercion；`SchemaAlign` 的 nullable → non-null 收窄；运行期 input Schema 漂移 | 分别在 Definition 构造、纯 bind 或 turn 边界返回结构化错误，不创建资源或提交部分进展 |

时间、随机、UDF、session variable 或外部 registry 依赖目前没有确定、可恢复的执行上下文，因此不在
已承诺集合。当前实现若不能编码或规划会按上表拒绝；即使某个表达式碰巧能由固定版本 `DataFusion`
规划，也仍属于“未承诺”，直到增加显式准入规则和完整持久化证据。

```rust
use std::num::NonZeroU32;
use arrow_schema::DataType;
use dogpaddle_operation::{ScalarValue, cast, col, ident, lit, try_cast};
use dogpaddle_operation::operation::transform::{
    ExtendDefinition, FilterDefinition, SchemaAlignDefinition, SchemaAlignField,
    SelectDefinition, UnionAllDefinition,
};

let is_seven = col("value").eq(lit(7_u64));
let extend = ExtendDefinition::try_new("is_seven", is_seven.clone()).unwrap();
let filter = FilterDefinition::try_new(is_seven).unwrap();
let select = SelectDefinition::try_new([("value", col("value"))]).unwrap();
let align = SchemaAlignDefinition::try_new([
    SchemaAlignField::try_new("id", cast(col("value"), DataType::Int64), true).unwrap(),
]).unwrap();
let union = UnionAllDefinition::new(NonZeroU32::new(2).unwrap());
assert_eq!(extend.field_name(), "is_seven");
assert_eq!(filter.predicate(), extend.expression());
assert_eq!(select.fields().len(), 1);
assert_eq!(align.fields().len(), 1);
assert_eq!(union.input_count().get(), 2);

let typed_null = lit(ScalarValue::Utf8(None));
let strict_text = cast(col("value"), DataType::Utf8);
let nullable_text = try_cast(col("value"), DataType::Utf8);
let exact_arrow_name = ident("Case.Sensitive");
assert!(ExtendDefinition::try_new("missing", typed_null).is_ok());
assert!(ExtendDefinition::try_new("strict_text", strict_text).is_ok());
assert!(ExtendDefinition::try_new("nullable_text", nullable_text).is_ok());
assert!(ExtendDefinition::try_new("copy", exact_arrow_name).is_ok());
```

## Operation 运行协议

运行资源与持久 Data 分开装配：`OperationBinding::materialize(data, resource)` 消费一个可选的
[`RuntimeResource`]。普通算子传 `RuntimeResource::none()`；`PostgreSQL`/`MySQL` Scan 与
`PostgreSQL` Sink 分别传拥有型配置。
binding 先验证其精确 Rust 类型，Flow 在创建 Store 前完成全图检查。这里没有全局 registry、
connector enum 或启动回调；资源只在 materialize 时 move 进 Operation，外部初始化仍由 turn 完成。

运行时 [`operation::Operation`] 是带执行能力的 enum：`Atomic` 保存
`Box<dyn AtomicOperation>`，`Turn` 保存 `Box<dyn TurnOperation>`。Scan、`TurnTransform`、Sink 与独占 Transform 使用
完整 `TurnOperation::turn` 协议；Atomic Transform 直接在 Station 已开启的事务中调用
`AtomicOperation::apply`。当 Atomic Transform 独占 Station 或位于首位时，`Operation::turn` 将它适配
为 `Action::Complete`。Operation 不接收 Subscription offset、Transaction 或事务启动能力。

一次 turn 明确分成三个线性阶段：

1. `TurnOperation::turn` 或 `Operation::turn` 在没有活动写事务时运行。它可以检查内存状态、惰性初始化资源或执行一次有界
   poll，但不能确认外部工作或提前推进任何影响重放的事实。返回 [`operation::Turn::Idle`] 时调用方
   不开启事务；返回 `Turn::Ready` 时得到一个只能消费一次的 [`operation::PreparedTurn`]。
2. `PreparedTurn::apply` 只在调用方持有的 Store 写事务内运行，只收到不能提交的
   `TransactionAccess`，并返回 [`operation::Action`] 与 [`operation::AfterCommit`]。这一阶段的写入
   必须能随事务完整回滚。
3. 调用方完成 output、input 与 Operation state 的原子提交后才消费 `AfterCommit`；其他所有路径
   只丢弃它。外部 delivery ACK 等不可回滚动作只能放在这里，绝不能放进 `Drop`。

`turn` 函数体现在执行；`Turn::ready` 和 `AfterCommit::new` 只是保存闭包，分别等到事务内和提交后
再执行。`apply(self)` 与 `run(self)` 消费各自的值，保证每个闭包至多执行一次；执行时机由 Station
保证。它们不自动提供外部系统的恰好一次语义，恢复仍依赖已提交的持久状态与连接器的重放契约。

`Action::Idle` 表示没有可提交进展，调用方必须回滚 prepared turn 的全部写入；`Commit` 提交
Operation 状态和可选 output，但不完成当前输入，下一 turn 仍收到同一端口、同一日志 offset 和
逐字节相同的完整 Change。零输入 Scan 也用 `Commit` 表示一次成功 turn。`Complete` 才在同一
事务中提交 Operation 状态、可选 output 和当前输入完成。两种提交动作都至多产生一个 owned
output Change；filter 或 Sink 可以使用 `None`。

跨 turn continuation 必须放在 Operation 自己通过 Definition 声明的持久化 Store 状态中，不能
隐藏在 Station；运行实例可以保存由该持久状态重建的临时资源。`turn` 或 `apply` 的提交前错误统一
擦除为 [`operation::OperationError`]，提交后的 callback 错误则使用独立的
[`operation::PostCommitError`]，明确表示本地事务已经无法回滚。Flow 遇到后者会停止该运行态
Station，要求 reopen 后从已提交状态恢复。`SqliteSink` 与 `PostgresSink` 共用固定 ID 幂等批次，
都先在 prepared turn 持久化工作，再在 `AfterCommit` 写入目标，最后由下一 turn 结算输入。

因为当前只有 post-commit error 携带“必须 reopen”的语义，`turn` 或 `apply` 返回普通
`OperationError` 时，同一个运行实例必须仍可从未改变的 durable state 重试。若 poll 或其他准备工作
发现临时 driver 已 poisoned，Operation 必须在返回错误前重置它，或把自身切换为下一 turn 会重建
driver 的内存状态，不能把隐藏的 needs-reopen 要求留给 Flow 猜测。

| action | 本 turn 写入与 output | 当前输入 |
| --- | --- | --- |
| `Idle` | 全部回滚 | 有输入时保持不变 |
| `Commit(output)` | 提交 | 有输入时保留，下一 turn 完整重放 |
| `Complete(output)` | 提交 | 完成，调用方才可推进 |

### 完整例子：从队列拉取并恢复

先读 [`QueueScan`](examples/support/queue_scan.rs) 的 `turn`：`client: None` 时在事务中读取
checkpoint，提交后建立临时 client；之后在事务外 poll，在事务中保存 checkpoint 和返回 output，
提交后才 ACK。算子本身不持有事务启动能力。

再读 [`queue_scan` 的调用代码](examples/queue_scan.rs)，或直接运行：

```sh
cargo run -p dogpaddle-operation --example queue_scan
```

示例先提交 `10`，关闭 Store 和 Operation，重新打开后继续提交 `20、30`。它用固定队列模拟可按
checkpoint 恢复的外部服务；独立调用代码把 output IPC 与 checkpoint 原子写入 Store。生产 Flow
由 Station 负责事务、Schema guard、容量和输入进展。示例没有可装入 Flow 的 Definition，也不是
已交付的 Debezium Scan。

`correctness/protocol.rs` 直接复用同一份算子代码，覆盖初始化回滚、未 ACK 重放，以及第二条记录
在本地提交前或提交后丢失运行态，再 reopen 的完整输出序列。测试中的 Drop 用于模拟这些恢复边界，
不代替未来真实连接器的进程崩溃验收。

下文所说的 data class 指一个完整的 Rust 持久化数据类型，包括 collection、键与值类型，例如
`Cell<u64>`、`OrderedMap<u64, String>` 或 `PartitionedMultiset<Group, Value>`。

## 内建算子能力与 conformance

下表是当前十六个内建算子的产品契约索引。`任意` 指任意合法且已由 Change 支持的精确 logical
Schema，不表示运行期动态 Schema；`共享` 只表示有公开 pointer/buffer 证据的路径。表中未列出的
`DataFusion` 表达式或 Arrow 类型不能由“底层依赖碰巧支持”推导为 `DogPaddle` 承诺。这是文档与测试
索引，不是代码级 capability registry；Flow 仍不枚举具体算子。

| 算子（tag） | kind / arity | bind 后的 Schema | 行、diff 与 action | Operation data | buffer 行为 | 公共证据 |
| --- | --- | --- | --- | --- | --- | --- |
| `SequenceScan` (`1`) | Scan / 0 | 固定 `value: UInt64 non-null` | 每 turn 一行、diff `+1`、`Commit`；耗尽后 `Action::Idle` | `sequence_scan.position: Cell<u64>` | 新建 output | golden、bind、末值、rollback、reopen |
| `PostgresCdcScan` (`11`) | Scan / 0 | 固定单表受支持列 | 私有捕获初始快照与 heartbeat 前 WAL，封口后原子发布，再持续 CDC | `phase` + `checkpoint` + `bootstrap_spool: Queue<Vec<u8>>` | 捕获期每个完整 Change 编码一次；发布期逐条解码 | tag11 golden、三资源/容量、捕获/封口/回滚/reset/reopen；显式真实 PG 快照→CDC gate |
| `MySqlCdcScan` (`15`) | Scan / 0 | 固定单表受支持列 | `initial_only` 私有捕获初始快照，封口后原子发布，再以 `recovery` 持续 CDC | `phase` + `checkpoint` + `bootstrap_spool: Queue<Vec<u8>>` | 捕获期每个完整 Change 编码一次；发布期逐条解码 | tag15 golden、三资源/容量、捕获/封口/回滚/reset/reopen、Connect JSON schema-control/type 转换；bundle 与真实 `MySQL` 验收显式运行 |
| `RunningEventCount` (`2`) | `AtomicTransform` / 1 | 任意 → `count: UInt64 non-null` | 按输入行序每行加一，忽略输入 diff 数值，输出 diff `+1` | `running_event_count.count: Cell<u64>` | 新建 count，保持行序 | tag `2` golden、bind、overflow、rollback、reopen、重批 |
| `Distinct` (`13`) | `AtomicTransform` / 1 | output exact input | 按行序更新完整记录权重；只在 `0 ↔ positive` 时输出 `+1/-1` | `distinct.weights: OrderedMultiset<Vec<u8>>` | 按输入行序选择边界事件并重建 diff | tag、完整 row key、边界、rollback、reopen、重批 |
| `Aggregate` (`14`) | `Atomic/ExclusiveTransform` / 1 | 非空 group fields 后接 calls；保留 Schema metadata 及 group Expr metadata | 按行序更新；新增/删除组输出 `+1/-1`，已有组结果变化输出旧 `-1`、新 `+1` | `aggregate.groups: OrderedMap`、`aggregate.entries: PartitionedMultiset`、`aggregate.control: Cell<u64>` | 按真实 output 一次建列；MIN/MAX 直接读取分区首尾 | tag14 golden、分区/排序、组合函数、rollback、reopen、非单位 diff/重批 |
| Project (`4`) | `AtomicTransform` / 1 | 严格递增顶层索引；保留所选 Field 与 Schema metadata | 行序和 diff 不变 | 无 | 所选列与 diff 共享 | golden、合法/拒绝 bind、空投影、runtime/reopen/重批、temporal/decimal 直接列；Definition codec，无独立 turn benchmark |
| Filter (`5`) | `Atomic/ExclusiveTransform` / 1 | Boolean Expr；output exact input | 仅保留 non-null true，records/diffs 同步筛选；全删返回 `None` | 无 | 全选共享；部分选择由 Arrow filter 分配 | Expr golden、bind/evaluate、null/Kleene、全部 layout family、Date32/Timestamp(ms)/Decimal 同类型组合比较、reopen/重批；Definition codec，无独立 turn benchmark |
| Extend (`6`) | `Atomic/ExclusiveTransform` / 1 | 保留 input，追加一个由 Expr 推导的 Field | 行序和 diff 不变 | 无 | input 列和 diff 共享；派生列按需分配 | Expr golden、bind/evaluate、名称拒绝、temporal/decimal 直接列、reopen/重批；Definition codec，无独立 turn benchmark |
| Select (`7`) | `Atomic/ExclusiveTransform` / 1 | 同一原始 input 上的有序 `name + Expr` 完整输出 | 行序和 diff 不变；空 Select 保留行数 | 无 | 直接列和 diff 共享；派生列按需分配 | Expr golden、bind/evaluate、空/非空 runtime Schema guard、别名隔离、temporal/decimal 选择/重排、reopen/重批；Definition codec，无独立 turn benchmark |
| `UnionAll` (`8`) | `AtomicTransform` / N，N > 0 | 所有输入必须 exact 相同，原样输出 | 保持每端口行序/diff；跨端口无序 | 无 | 整个 Change 原样共享 | golden、arity/bind 与 runtime exact-Schema 拒绝、多端口 runtime/reopen/重批；Definition codec，无独立 turn benchmark |
| `InnerEquiJoin` (`16`) | `TurnTransform` / 2 | 非空、同类型 flat non-float 等值键；output 固定为重命名后的 left 全字段 + right 全字段 | 按输入行序及 opposite canonical row 顺序输出，diff 为输入 diff × opposite multiplicity；NULL key 不匹配；分页 `Commit` 后 `Complete` | `inner_join.left_rows/right_rows: PartitionedMultiset<Vec<u8>, Vec<u8>>`、`inner_join.continuation: Cell` | 每个 output page 从 canonical opposite rows 重建 | tag16 golden、bind、两端更新、NULL/composite key、负前缀/overflow、rollback、分页/reopen、大行 fanout |
| `SchemaAlign` (`9`) | `Atomic/ExclusiveTransform` / 1 | 有序 `name + Expr + target nullable + Field metadata`，另有 Schema metadata | 行序和 diff 不变；空定义保留行数 | 无 | 直接列和 diff 共享；表达式结果按需分配 | golden/canonical metadata 与重复 key 构造拒绝、bind/收窄拒绝、空/非空 runtime Schema guard、temporal/decimal 精确 cast、runtime/reopen；Definition codec，无独立 turn benchmark |
| Discard (`3`) | Sink / 1 | 接受任意，无 output | 完成完整输入，`Complete(None)` | 无 | 不产生 output | golden、bind、runtime、rollback、reopen |
| `SqliteSink` (`10`) | Sink / 1 | 校验 `SQLite` 列名与列数；无 output | 共享固定 ID 批次协议，每批至多 1024 操作，目标提交后结算 continuation 或 `Complete` | `relation_sink.state: Cell<Vec<u8>>` | 共享 canonical/hash，绑定 `SQLite` 值 | tag/payload、state/hash golden、全部 v1 类型、批界、非负前缀、rollback/reopen；无独立 benchmark |
| `PostgresSink` (`12`) | Sink / 1 | 校验 `PostgreSQL` 列名、系统列与列数；无 output | 同一共享协议，批量匹配、insert-ignore 与 delete | `relation_sink.state: Cell<Vec<u8>>` | 共享 canonical/hash，绑定 PG 参数 | tag12 canonical JSON、资源/Schema/布局；普通 gate 离线，真实批量与恢复见 `system-tests/postgres/check_sink.py` |

所有十六个算子共用同一条 `Definition → exact Schema binding → materialize → turn` 路径。每个算子在
`tests/correctness/<operation>.rs` 垂直拥有自己的 literal golden、kind、data declaration、bind、
materialize、runtime 和 reopen 证据；`definition_codec`、`expression`、`protocol`、`atomic` 与 `metamorphic`
只保留跨算子契约；`correctness/atomic.rs` 证明实例级 kind、全部表达式 owner 的资格归纳和直接 atomic
执行。完整 Flow 的纯失败无建库副作用、资源名、build/open/reopen、运行期 Schema guard
和事务重放由 `crates/flow/tests/correctness` 所有。Operation 不建立 release benchmark；组合性能由
真正拥有 workload 的 Flow、Store 或 Change + Store target 证明。

tag `1..=10`、tag `13`、tag `14` 与 tag `16` 的稳定字节入口位于 `tests/fixtures/v1/`；tag11、tag12 与 tag15 的完整
canonical JSON golden 分别由 `tests/correctness/postgres_cdc_scan.rs`、`tests/correctness/postgres_sink.rs` 与
`tests/correctness/mysql_cdc_scan.rs` 拥有。其中事件计数、对齐、
`SQLite` Sink 与 Distinct 的 fixture 分别为 `running_event_count_definition.hex`、`schema_align_explicit.hex`、
`sqlite_sink_output_events.hex`、`distinct_definition.hex`、`aggregate_department.hex` 与
`inner_equi_join_id.hex`，冻结 tag `2`、`9`、`10`、`13`、`14` 与 `16`。每个算子文件会自行完成 decode、bind、
materialize 与运行证据。Flow manifest 的端到端基线为
`crates/flow/tests/fixtures/v1/sequence_scan_running_event_count_discard.hex`。这些文件名只帮助定位
证据；契约仍由公共测试断言和上表语义定义。

运行实例及具体算子统一组织在 `operation` 模块中，其下按语义分为三个公共模块：`scan`
保存零输入且拥有 output 的 Scan 算子，`transform` 保存消费并产生记录的转换算子，`sink` 保存只消费记录
的终点算子。当前 `scan` 包含 `SequenceScan`、`PostgresCdcScan` 与 `MySqlCdcScan`，`transform` 包含 RunningEventCount、Distinct、Aggregate、Project、Filter、
Extend、Select、SchemaAlign、`UnionAll` 与 `InnerEquiJoin`，`sink` 包含
Discard、`SqliteSink` 与 `PostgresSink`。目录分类不作为运行时类型系统；每个 Definition 必须通过
[`OperationDefinition::kind`] 显式声明包含输入数量的结构类型。

Scan 是结构角色，不要求底层一定存在一张可遍历的表。当前协议不表达 EOS：Scan 可以暂时
`Turn::Idle`，耗尽后也可以永远 `Turn::Idle`；需要流完成语义时应另行扩展协议。

## 状态关系算子：Distinct

`Distinct` 用 `distinct.weights: OrderedMultiset<Vec<u8>>` 维护完整 canonical row 的正权重。完整行
直接作为有序集合 key；缺失 key 表示权重 `0`，归零即删除。
权重从零变为正数时输出 `+1`，从正数归零时输出 `-1`，其余变化不输出；负前缀和 overflow 使整个
turn 回滚。

更新严格遵循输入行序，不先合并同一 Change 内的事件，并对稳定重批和 reopen 保持相同语义。
canonical row 保留浮点原始位模式，所以 `-0.0` 与 `+0.0` 是不同记录。

## 分组聚合：Aggregate

`AggregateDefinition` 把非空、有序的 `GROUP BY name + Expr` 与有序的
`name + AggregateCall` 一次绑定成一个单输入 Operation；调用列表可以为空，此时就是按 group key
分组去重。`COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX` 都通过 `AggregateCall`
构造器进入同一条 Definition codec。函数 tag 与 binding 集中在一张 crate 私有 descriptor 表里，
每个函数只选择可增量维护的 `Fold`，或由有序分区直接求首尾的 `Extrema`。函数实现不接触 Store。

持久状态只有三个具名对象：

- `aggregate.groups: OrderedMap<Vec<u8>, GroupState>` 以完整 canonical group 为 key；value 保存单调
  group ID、正 group weight 和各 Fold 调用的小状态。
- `aggregate.entries: PartitionedMultiset<EntryPartition, Vec<u8>>`。layout `0` 的分区按完整 canonical
  input row 做精确准入；其余分区按 `(layout ID, group ID)` 隔离一个 extrema expression 的有序
  argument key。相同持久表达式共享 layout；NULL 不进入 extrema 分区。
- `aggregate.control` 只分配单调、不复用的 group ID。

一次事件的 input admission、group weight、
函数小状态、索引和 output 全在同一写事务中更新；负前缀、weight/result overflow 或其他错误回滚整个 turn。
新组输出 `+1`，消失组输出 `-1`；已有组的结果确实变化时才按顺序输出旧行 `-1`、新行 `+1`，结果不变不输出。
group output 保留 input Schema metadata 以及 `DataFusion` `Expr::to_field` 推导的 `Field` metadata，并使用定义给出的名称。

`COUNT` 输出 non-null `Int64`。`SUM` 当前只接受 `Int64/UInt64`；`AVG` 对这两种整数用
`i128/u128` 精确累计，最终一次转换为 nullable `Float64`。`MIN/MAX` 接受当前 flat、非浮点 scalar
类型；数值用保持逻辑顺序的定长编码，Utf8/Binary 直接使用其字节。每次输出从对应分区读取首个或
末个 key，因此删除当前极值不需要扫描整组，也不在 group state 中缓存第二份极值。

当前明确拒绝全局聚合、浮点 group key、浮点 `SUM/AVG/MIN/MAX`、Decimal `SUM/AVG` 和嵌套
`MIN/MAX`。这避免把 bit identity、NaN order 或历史相关的浮点累计误称为 SQL 语义；后续增加对应实现时再连同
精确语义和持久证据一起开放。

## 两输入关系算子：InnerEquiJoin

`InnerEquiJoinDefinition` 接收非空、有序的 `(left Expr, right Expr)` 等值键和完整 output names。
每个表达式只绑定自己的输入 Schema；同一对键必须具有相同 exact type，并且 v1 只接受 flat、
non-float scalar。output 固定包含 left 全字段后接 right 全字段，字段只按 output names 改名并保留
source Field metadata，Schema metadata 固定为空。选择、重排或对齐继续交给后续普通 Transform。

两个输入关系分别保存在 `inner_join.left_rows` 与 `inner_join.right_rows`：partition 是完整 canonical
join key，partition 内的 key 是完整 canonical input row，value 是正 multiplicity。NULL key 仍进入
自己的关系状态以支持精确 retract，但永远不 probe 对侧。每个匹配的 opposite distinct row 只产生一个
output 事件，其 diff 是 input diff 与 opposite multiplicity 的 checked product，不按 multiplicity 展开。

Join 用 `inner_join.continuation` 将一个 pinned Change 分成两段。没有 continuation 时先对完整 Claim 的
own-row 权重变化做无写入准入，并在同一事务中直接开始 `Probe`；它分页遍历全部匹配并提前验证
persisted row 与 output diff，随后 `Emit`
按相同 canonical 顺序提交 output pages。当前 input row 只在它的最后一个 emit page 调整 own relation，
最后一行同时清除 continuation 并返回 `Complete`。因此页级背压、tail 错误或事务失败会一起回滚该页，
reopen 从 durable cursor 继续，不需要 Station 保存第二份进度。page item 数还会按 driving canonical row
大小收紧，避免一个大 own row 被固定 fanout 数重复后放大单页内存。同一 turn 在固定的 item 与
canonical byte 预算内跨过多个 input row，`Emit` 把这些 row 的小匹配分区按原顺序聚合成一个 output；
出现分页 cursor、耗尽任一预算或完成输入时才形成边界，单个超预算 row 仍允许独立推进。

## Definition 与持久化

具体 Definition 统一实现 sealed [`OperationDefinition`] trait。trait 要求每个具体算子手动返回
[`OperationKind`]，并以 `{ 逻辑名: data class }` 的形式向 Flow 声明完整数据 schema。
`OperationKind::Scan` 固定为零输入，`AtomicTransform`、`TurnTransform`、`ExclusiveTransform` 与 Sink variant 携带非零 `u32` 输入数量，因此类别、
input arity 和 output 属性不会形成非法组合。kind 不是从拓扑位置推断：Scan、Transform 和 Sink
分别声明自己在数据流中的结构语义；Station 读取所包裹 Definition 的 kind，再向 Flow 提供自己的
Scan/Sink 角色与 output 属性。Flow 负责生成完整
资源名，并调用声明携带的类型化 create/open 能力；得到的实例按逻辑名组成集合，再交给
此前 Schema bind 产生的 `OperationBinding::materialize`。binding 只按名称取得已经创建或打开的
`Cell<T>`、`OrderedMap<K, V>`、`OrderedMultiset<K>`、`PartitionedMultiset<P, K>` 或 `Queue<T>`，
声明顺序不参与绑定，也不接收 Store；物化会消费整组实例，
并拒绝缺失、类型错误或未被 binding 取走的多余资源。

collection 类型及其键值 codec 是持久 schema。Flow 只解释声明，不枚举具体算子或 collection 类型；
Store 负责资源的物理实现。

```rust
use dogpaddle_operation::{
    OperationDefinition, OperationKind, decode_definition, encode_definition,
    operation::scan::SequenceScanDefinition,
};

let scan = SequenceScanDefinition::new(10);
assert_eq!(scan.kind(), OperationKind::Scan);
assert_eq!(scan.kind().input_count(), 0);

let encoded = encode_definition(&scan);
let decoded = decode_definition(&encoded).unwrap();
assert_eq!(encode_definition(decoded.as_ref()), encoded);
```

Definition 集合在本 crate 内保持封闭，但不再使用公共 enum。稳定 decoder 由一张私有
`tag → decode function` 静态表选择；每个具体算子模块拥有自己的 tag、payload codec、
数据声明和物化逻辑。Flow 不枚举具体算子，也不解析 Operation payload。

为穿过 object-safe 的 [`OperationDefinition`] 边界，数据实例仅在一次性的 build/open 装配
过程中进行私有类型擦除；具名声明在 binding 的 `materialize` 中将其安全恢复为精确 data class。
类型不匹配会返回错误而不是 panic。类型擦除不会进入运行实例、事务访问路径或持久化格式。

物化结果统一装入 [`operation::Operation`] enum：完整消费一个 Change 的 kernel 保存为
`Atomic(Box<dyn operation::AtomicOperation>)`，拥有 turn/continuation/AfterCommit 协议的实例保存为
`Turn(Box<dyn operation::TurnOperation>)`。Flow 通过同一个可变 `turn` 入口分派，enum 会把 Atomic head 适配成
一次 `Complete` turn；运行实例只要求 `Send`，调度方
在从准备到提交后 completion 结束的整个 turn 期间持有其独占可变访问。Schema binding 只在 build/open
期间连接 Definition 与实例；运行 trait 和具体运行类型都不反向保存或暴露 Definition，Flow
已经从持久 Definition 获得 kind、资源声明和端口 Schema。
Operation 本身可以在外部实现，但 Flow 只从 sealed Definition 物化运行实例；开放可注入 Flow 的
第三方算子仍需另行设计 tag 分配、decoder 注册和运行错误边界。

## `PostgreSQL` CDC Scan 试点

[`operation::scan::PostgresCdcScanDefinition`]（tag `11`）只描述一个数据库中的一张固定 Schema 表。
先用 [`operation::scan::PostgresCdcScanConfig::discover`] 显式查询 catalog，再把得到的
`PostgresCdcScanSpec` 与必填的 `NonZeroU64 bootstrap_spool_bytes` 交给
[`operation::scan::PostgresCdcScanDefinition::try_new`]。build/open/bind 不连 PG、不启动 JVM。
配置由宿主构造并在每次打开时重新装配，不自动读取环境变量、配置文件或全局 secret registry。

持久 Definition 保存 engine/topic 名、数据库/表/slot/publication 身份、cluster system identifier、
database/table OID、有序列声明和 spool 容量，不含密码、用户名、host 或 runtime payload 路径。
payload 是固定字段顺序的 canonical JSON；未知字段、重复字段、非 canonical 字节与超过 1 MiB 的 payload 被拒绝。
试点 engine/schema/table/slot/publication 名仅允许 1–63 个小写 ASCII 字母、数字和下划线。

算子只声明 `postgres_cdc_scan.phase: Cell<u32>`、
`postgres_cdc_scan.checkpoint: Cell<Vec<u8>>` 和
`postgres_cdc_scan.bootstrap_spool: Queue<Vec<u8>>` 三个资源。checkpoint 原样保存 D2 opaque bytes，
不加 envelope，也不充当 delivery ID。spool 每个 entry 保存一个完整自描述 Change IPC Stream。
`bootstrap_spool_bytes` 是硬逻辑上限，下一条必须满足
`queued_bytes + 8-byte private sequence + encoded IPC bytes <= bootstrap_spool_bytes`；空 Queue 也不放行
超限首条。它不是 Store、JVM/Rust 内存或 WAL 磁盘配额。

此前开发期的单 checkpoint 布局已删除；旧 Flow 必须重建，不提供 alias、迁移或兼容读取。

同一个 `turn(None)` 按 `Fresh → Capturing → Publishing → Streaming` 推进：

1. `Fresh` 先在本地事务中发布 `Capturing`，再在事务外校验 source identity 并以
   `snapshot.mode=initial` 单线程启动快照。该阶段不产生公开 output。
2. `Capturing` 每次取一个完整 delivery，将其所有记录按顺序转为一个 Change。有数据时将完整
   IPC `try_push` 到私有 Queue，并将整个 delivery 的 candidate checkpoint 一起提交；仅提交后 ACK。
3. Debezium initial-snapshot `COMPLETED` notification 作为普通有序 delivery record 将快照封口，
   其 checkpoint 成为 `Q`。非空表之前必须观察到 `snapshot=last`；空表可直接封口。`last` 与
   completion notification 之间可出现跨 delivery 的 insert/update/delete；terminal delivery 的
   完整记录序列也保留在 spool。
4. `Publishing` 先停止 snapshot connector，然后每个 turn 在同一个 Store 事务中读取并
   `pop_front` 一条 spool Change、向 Station 追加 output。背压、Schema 失配或 commit 失败同时回滚
   dequeue 和 output。
5. spool 排空与 `Streaming` phase 同事务提交。后续以 `snapshot.mode=no_data` 从 `Q` 恢复同一
   slot，稳态 delivery 仍以 checkpoint + 可选 output 同事务提交，提交后才 ACK。

`Capturing` 中断时不从部分 checkpoint 恢复。reopen 或捕获错误必须先停止 connector，再在 Store 事务外
仅删除兼容、非 active 的 source-owned slot，进入 `Resetting` 逐条清除 spool，最后清除 checkpoint/phase
并回到 `Fresh`。不兼容或 active slot 会拒绝 reset。`Publishing` 和 `Streaming` 不删 slot、不重做快照。

容量不足时当前 delivery 不提交、不 ACK，spool 不变；必须使用更大 `bootstrap_spool_bytes` 和新 state
目录重建。容量必须容纳完整表快照与 completion notification 前的 WAL 重叠。普通 poll 错误会重建临时
connector；ACK error/panic 由 Station fail-stop，必须 reopen。checkpoint-only heartbeat 不制造空 Change。
零超时 poll 只表示不等待数据，connector 启动、停止及 ACK 仍是有界同步调用。

转换移出 JSON 行并借用其中的文本构建 Arrow，避免整行深拷贝和中间 String 副本；JSON 解析、
完整 Schema/值校验及必要的 Arrow buffer 写入仍保留。普通测试证明行为，不宣称 CDC 吞吐基线。

insert 输出 `+after`，delete 输出 `-before`，update 按顺序输出 `-before, +after`。只接受完整旧行、
精确列 Schema 和正确 Debezium metadata/topic；不排序、不抵消，不把 Debezium delivery 当成 `PostgreSQL` 事务边界。
不承诺一个捕获事务的所有行作为单个 Change 原子可见。

| `PostgreSQL` | Arrow | 试点约束 |
| --- | --- | --- |
| boolean | Boolean | 保留 nullability |
| smallint / integer / bigint | Int16 / Int32 / Int64 | 范围检查，不经浮点转换 |
| real / double precision | Float32 / Float64 | 包括 Connect 非有限值表示 |
| text / varchar | Utf8 | 逐字保留合法文本；`REPLICA IDENTITY FULL` 保证完整 TOAST 值 |
| bytea | Binary | 解码 Connect base64；`REPLICA IDENTITY FULL` 保证完整 TOAST 值 |
| date | Date32 | 有限且可表示的日期 |
| timestamp | Timestamp(Microsecond, None) | 固定 microseconds 模式；拒绝 infinity |
| timestamptz | Timestamp(Microsecond, UTC) | 有限、可解析的 RFC3339，拒绝亚微秒截断 |
| numeric(p,s) | Decimal128(p,s) | `1 ≤ p ≤ 38`、`0 ≤ s ≤ p`；拒绝无约束 numeric/NaN |

catalog discovery 至少需要 `PostgreSQL` 15；当前本机端到端证据使用 17.10，不能据此声明其他版本
已通过同等验收。连接入口特意叫 `new_unencrypted`：当前仅供受信本地网络或独立加密隧道，discovery 和 JDBC 都禁用
TLS；不把这个试点称为完整安全部署方案。使用专属 CDC 角色，需要正常 replication/catalog 权限，
以及显式授予 `pg_control_system()` 的 EXECUTE 权限；无需因为该查询让业务角色成为 superuser。

表必须是 permanent、非 partition 的普通表，`REPLICA IDENTITY FULL`，无 generated 列。
publication 由用户预先创建，必须发布全部列与全部 insert/update/delete/truncate，不能有 row filter。
slot 名在 discovery 和首次 snapshot 前必须不存在；随后由该 Flow/Scan 创建并独占，不能接管或共享已有 slot。
`initial` 快照复制已有行，并从同一切点继续 WAL；无需空表或业务写入准入栅栏。TRUNCATE 明确拒绝。
重启校验 system/database/table identity、logical Schema、publication 与 slot；运行中 DDL、publication/slot 修改或数据库替换不受支持。

不支持未列出的 PG 类型、多表路由、在线 Schema evolution、TLS、跨实例 fencing、旧布局迁移或 graceful stop API。

完整宿主在 `system-tests/postgres/hosts/src/bin/postgres_cdc.rs`；普通 Cargo 测试无需 Java/PG，真实端到端与
进程恢复由 `system-tests/postgres/check_cdc.py` 显式验收，见根目录 TESTING.md。

## `MySQL` CDC Scan 试点

[`operation::scan::MySqlCdcScanDefinition`]（tag `15`）是一个数据库中一张固定 Schema `InnoDB`
表的完整初始快照 + 连续 binlog Scan。先用 [`operation::scan::MySqlCdcScanConfig::discover`] 读取 catalog
并获取 `MySqlCdcScanSpec`，再与必填 `NonZeroU64 bootstrap_spool_bytes` 交给
[`operation::scan::MySqlCdcScanDefinition::try_new`]。SQL build 完成 discovery；`FlowFactory::build/open`、构造和 bind
不连 MySQL、不打开 JVM。runtime bundle、主机、凭据和唯一 replication client ID 只作为运行资源注入。

它只声明 `mysql_cdc_scan.phase: Cell<u32>`、`mysql_cdc_scan.checkpoint: Cell<Vec<u8>>` 和
`mysql_cdc_scan.bootstrap_spool: Queue<Vec<u8>>` 三个资源。Definition 另持久 spool 容量。checkpoint 是 D2
opaque bytes，spool 每条是完整 Change IPC。开发期旧 Definition/单 checkpoint 布局必须重建，不迁移。

状态为 `Fresh → Capturing → Publishing → Streaming`。`Fresh` 先持久化 `Capturing`，再以
`snapshot.mode=initial_only`、`snapshot.locking.mode=minimal`、单线程启动一致全表快照。快照的 `r` 事件
只写私有 spool，不产生公开 output。每个 delivery 的可选完整 Change IPC 和 candidate checkpoint 同事务提交，
之后才 ACK。Debezium initial-snapshot `COMPLETED` notification 作为普通有序 delivery record，
将 checkpoint `Q` 与 `Publishing` 一起封口。
`Publishing` 停止 snapshot connector，每个 turn 将一条 spool Change 的 dequeue 与 Station output append 同事务提交；
背压或提交失败同时回滚两者。spool 排空后进入 `Streaming`，以 `snapshot.mode=recovery` 和
`MemorySchemaHistory` 从 `Q` 继续 binlog，稳态仍以 checkpoint/output 同事务 + 提交后 ACK 运行。

`Capturing` 期 checkpoint 不是部分快照 resume token。错误或 reopen 中断捕获后，必须停止 connector，进入
`Resetting` 逐条清理 spool/checkpoint，再从 `Fresh` 重做完整快照。`Publishing`/`Streaming` reopen 不重做快照。
`bootstrap_spool_bytes` 是 `queued bytes + 8-byte private sequence + IPC` 的硬逻辑上限；超限 delivery 不提交、不 ACK，
必须使用更大容量和新 state 目录重建。

`minimal` locking 先以短时 global read lock 捕获 binlog 切点和 Schema，然后在 `InnoDB` consistent snapshot 中扫描行，
普通写入可继续。部署角色应具有这个短锁所需权限，但不得授予 `LOCK TABLES`：如果 global lock 失败，
Debezium 会在长表锁 fallback 之前失败。

`MySQL` 必须保留足以覆盖全表快照、私有 spool 排空、公开 output 背压和 recovery 追平的 binlog。过早
`PURGE BINARY LOGS` 使 `Q` 失效时必须 fail closed，不会重做新快照或选择更晚起点。`MemorySchemaHistory`
在 recovery 时从 catalog 重建，因此 Schema 在整个可恢复期间必须固定；DDL/TRUNCATE 拒绝且不 ACK。
不支持在线 Schema evolution、多表路由、TLS、跨实例 fencing、旧格式迁移或 graceful stop API。

catalog discovery 的验收版本为 `MySQL` 8.4，需要 `log_bin=ON`、ROW binlog、FULL row image、`lower_case_table_names=0`、单个非分区
`InnoDB` base table，且需要读取 `INFORMATION_SCHEMA.INNODB_TABLES` 的权限以冻结 table identity。只支持
signed `tinyint`/`smallint`→`Int16`、`mediumint`/`int`→`Int32`、`bigint`→`Int64`、`double`→`Float64`、字符
文本→`Utf8`、binary/blob→`Binary` 和 `decimal(p,s)`→`Decimal128`（`1 ≤ p ≤ 38`、`0 ≤ s ≤ p`）；unsigned、
float、时间、JSON、enum/set、bit、空间、generated 与 invisible 列均在 discovery 拒绝。`new_unencrypted`
对 catalog 与 Debezium 连接都强制关闭 TLS，只适用于受信网络或独立加密隧道，不是完整安全部署方案。

离线正确性证据位于 `tests/correctness/mysql_cdc_scan.rs` 与 `MySQL` Scan 模块的 Connect JSON conversion
测试。普通 Cargo gate 不启动 JDK、Debezium bundle 或 `MySQL`；部署前须显式构建 bundle，并在真实单表上验收
已有行快照、快照中并发写入、spool 背压、recovery 重开和 insert/update/delete。

## `operation::scan::SequenceScan`

[`operation::scan::SequenceScanDefinition`] 是零输入 Scan，记录首个 `u64` 值。物化后的
[`operation::scan::SequenceScanOperation`] 只持有复制出的 `start: u64` 和直接的
`Cell<u64>` position；首次产生 `start`，随后根据最后一次已提交的值逐一递增。每个 turn 产生一行，输出固定为一个
non-null `UInt64` `value` 字段，所有 diff 都是 `+1`。包含 `u64::MAX` 的最后一批可以成功提交，
后续 turn 的 `apply` 返回 `Action::Idle`，不再写 position 或产生 output，使 Flow 仍能调度 consumers 并排空已经提交的
Change。每次产生值的 turn 返回 `Action::Commit(Some(_))`；Station 不为
Scan 建立另一套 outcome 或事务路径。它声明自己产生输出。

Schema bind 不接收输入，并固定完整 output Schema 为一个 non-null `UInt64` `value` 字段。

它声明一个逻辑数据名 `sequence_scan.position`，由 Flow 解析为稳定 Station 资源名。

## `operation::transform::RunningEventCount`

[`operation::transform::RunningEventCountDefinition`] 要求一个输入。每成功推进一次，
[`operation::transform::RunningEventCountOperation`] 按输入行序计算事件数量：每一行恰好令直接持有的
唯一字段 `Cell<u64>` 加一，输入 diff 的符号和数值不改变“一个有序事件”的计数。每个已处理输入行输出
一个 non-null `UInt64` `count`，diff 固定为 `+1`；因此它是插入式的运行计数事件流，不是维护
单例关系的 cardinality aggregate。未写入的 count Cell 解释为 `0`，溢出返回
[`operation::transform::RunningEventCountError::Overflow`]。`RunningEventCount` 显式声明为携带一个输入的
[`OperationKind::AtomicTransform`]；拓扑位置不会把它隐式变成 Sink，因此完整 Flow 必须把它连接到
一个 Sink。

`RunningEventCount` 只声明 `running_event_count.count: Cell<u64>`，当前 Definition tag 为 `2`，output
字段为 `count`；公共 Rust API、逻辑 data 名与 Flow 路径同时采用清晰名称，资源为
`station/{index:08x}/operation/{operation_index:08x}/running_event_count.count`。不提供旧名称 alias、旧资源 fallback 或
迁移逻辑；旧版本创建的数据库直接删除并按当前 Definition 重建，不承诺或测试旧 manifest 的兼容
行为。当前实现每次 `apply` 处理完整 Change，并返回 `Some(_)`；它在写状态前预检整批行数，若最终值无法用 `u64` 表示，则返回 overflow，
整个 turn 不产生部分进展。协议允许其他 Operation 用声明的持久化状态在多个 `Commit` turn 中处理
同一 Change，这不是 `RunningEventCount` 必须采用的实现策略。

Schema bind 接受任意合法的精确单一输入，并固定完整 output Schema 为一个 non-null `UInt64`
`count` 字段。

## `operation::transform::Project`

[`operation::transform::ProjectDefinition`] 要求一个输入，并用稳定的 zero-based `u32` 顶层字段
索引描述投影。Schema bind 把这些索引编译为绑定精确 input Schema 的 `ChangeProjection`；索引必须
严格递增且都存在，空投影合法。output Schema 完整保留所选字段及 Schema/Field metadata，嵌套字段
只按完整子树选择，隐式 diff 始终保留。越界、重复或重排在 Flow 创建任何 Store 资源前以结构化
[`operation::transform::ProjectSchemaError`] 拒绝。

[`operation::transform::ProjectOperation`] 不声明 Store data，只保留 binding 编译出的
`ChangeProjection`。每次 `apply` 对完整 Change 做保持行序和 diff 的顶层投影，并返回 `Some(_)`；所选 Arrow Array buffer 与输入共享，不复制列数据。Definition 的字段
索引数量和每个索引使用稳定 big-endian `u32` 编码，tag/payload 与 input Schema 一起决定 reopen 后
重建的精确 output Schema。

## `operation::transform::Filter`

[`operation::transform::FilterDefinition`] 通过 fallible `try_new` 接收 `DataFusion` [`Expr`]，要求一个输入，
并持久化 `DataFusion` Expr protobuf。Schema bind 通过 `DataFusion` 把表达式编译到精确 input Schema；
最终类型不是 Boolean 或 `DataFusion` 无法规划
都会在 Flow 创建 Store 前以结构化 [`operation::transform::FilterSchemaError`] 拒绝。output Schema
与 input 完全相同。

[`operation::transform::FilterOperation`] 不声明 Store data。每次 `apply` 只保留 predicate 为 non-null
`true` 的行；`false` 和 null 都删除，同一个 Arrow filter predicate 同时筛选 records 与 diff，因而
相对事件顺序和每个保留事件的 diff 不变。没有行被选中时返回 `None`，因为空
Change 不可表示；全部选中时直接 clone Change 并共享全部 buffer。部分筛选前只把可能的第三方 Arrow
Array wrapper 通过 `to_data/make_array` 规范为标准 Array class（底层 buffer 仍共享），避免 Arrow
kernel 对自定义 concrete type panic，随后才进行一次向量筛选。

## `operation::transform::Extend`

[`operation::transform::ExtendDefinition`] 通过 fallible `try_new` 接收 `field_name + Expr`，要求一个输入，
并只持久化 field name 与 `DataFusion` Expr protobuf。
Schema bind 从表达式唯一推导新增字段的 `DataType` 和 nullability；调用者不重复声明 Field/type，避免两套
真相。output 依次保留所有 input `FieldRef` 与 Schema metadata，再追加一个 metadata 为空的新 Field；
重复名称、`$dogpaddle.` 保留名称和非法派生 Schema 仍由统一 output Schema 校验拒绝。一次只追加一列，
多列通过串联多个 Extend 明确表达，不引入同一算子内部的列依赖顺序。

[`operation::transform::ExtendOperation`] 不声明 Store data，只保存 exact-Schema-bound private plan 和
最终 output Schema。每次 `apply` 共享全部 input `ArrayRef` 与 diff buffer，只为真正计算出的列分配数据；
若表达式只是 Column，新列本身也与源列共享同一 ArrayRef。结果保持行序并返回
`Some(_)`。

## `operation::transform::Select`

[`operation::transform::SelectDefinition`] 通过 fallible `try_new` 接收有序的 `name + Expr` 集合。
每个表达式都独立绑定到同一个原始 input Schema，不能引用同一 Select 中新建的别名；输出只包含声明的列，
顺序、类型和 nullability 由声明与 `DataFusion` 唯一决定，并保留 input Schema metadata。空 Select 合法，
仍保留输入行数和 diff。

[`operation::transform::SelectOperation`] 不声明 Store data，只保存 binding 的 exact input/output
Schema 和编译后的表达式，并在任何表达式求值前检查 runtime input；因此空 Select
也会以 [`operation::transform::SelectError::InputSchemaMismatch`] 拒绝 Schema drift，不会产生 output
或持久写入。每次合法 `apply` 一次求值所有列并返回 `Some(_)`；直接列引用和 diff
与输入共享 Arrow buffer。

## `operation::transform::SchemaAlign`

[`operation::transform::SchemaAlignDefinition`] 是显式、可持久化的完整 Schema 重塑算子，要求一个
输入。它接收有序 [`operation::transform::SchemaAlignField`]；每个目标字段独立声明名称、`Expr`、
目标 nullability 与可选 Field metadata，Definition 另行声明完整 Schema metadata。每个表达式都
绑定到同一个原始 input Schema，因此选择、改名和重排由字段顺序与列引用表达；空字段列表合法并
保留输入行数和 diff。Field/Schema metadata 的输入顺序不影响按 key 排序的 canonical 编码；重复
key 在构造期返回结构化错误，不采用 silent last-wins。tag 固定为 `9`。

目标字段类型只由绑定后的表达式推导，不再保存第二份 `DataType` 真相。需要类型转换时，调用方必须
在 Expr 中显式使用 [`cast`] 或 [`try_cast`]；`SchemaAlign` 不猜测转换，也不插入隐式 coercion。
目标 nullability 可以等于表达式推导值，也可以把 non-null 显式放宽为 nullable；将 nullable 表达式
声明为 non-null 会在纯 bind 阶段以
[`operation::transform::SchemaAlignSchemaError::NullabilityNarrowing`] 拒绝。重复/保留字段名、保留
metadata key 和其他非法 output Schema 继续由统一 `DogPaddle` Schema guard 拒绝。

[`operation::transform::SchemaAlignOperation`] 不声明 Store data，只保存 exact-Schema-bound 表达式与
input/output Schema。它在任何表达式求值前检查 runtime input，所以空 `SchemaAlign` 同样会以
[`operation::transform::SchemaAlignError::InputSchemaMismatch`] 拒绝 Schema drift，不产生 output 或
持久写入。每个合法 turn 计算完整 output，保持行序并共享 diff；直接列引用继续共享原
`ArrayRef`，cast 等派生结果按 `DataFusion` 语义分配。它不排序、不去重、不 consolidation，也不修改
diff。`UnionAll` 与 `InnerEquiJoin` 仍然只接受 exact Schema；所有上层 API 若需要共同结构，都应显式插入
`SchemaAlign` 或生成等价的已声明变换。

## `operation::transform::UnionAll`

[`operation::transform::UnionAllDefinition`] 只接收非零 input count。bind 要求所有有序输入与 input 0
具有完全相同的 logical Schema，并以该 Schema 作为 output；不做 cast、对齐或字段名推断。
[`operation::transform::UnionAllOperation`] 不声明 Store data，只保存 input count 与 binding 得到的
exact common Schema；每个 turn 在转发前校验 runtime input，并以包含 port、expected 和 actual Schema
的 [`operation::transform::UnionAllError::InputSchemaMismatch`] 拒绝漂移，不产生 output 或持久写入。
合法输入按收到的端口原样 clone 完整 Change，
因此保持该端口的行序、diff 和 Arrow buffer。它与 SQL `UNION ALL` 一样不定义跨输入顺序；端口间
交织由 Station 统一调度，可随各 input 的分批和可用性变化。需要业务级总序时应另建显式排序或 barrier 语义。

## `operation::sink::Discard`

[`operation::sink::DiscardDefinition`] 显式声明为携带一个输入的 [`OperationKind::Sink`]，
不声明 Operation data，也没有 output。物化后的 [`operation::sink::DiscardOperation`] 是零状态
unit struct；它接受端口零上的完整 Change，并返回 `Action::Complete(None)`。Station 在同一事务中
确认输入 Subscription；失败或回滚不会丢失输入。Discard 只提供一个无外部副作用的显式 Flow 终点，外部
Sink 仍需各自设计与目标系统匹配的幂等提交协议。

Schema bind 接受任意合法的精确单一输入，并返回无 output 的 binding。

## 关系 Sink：一份协议，两种目标

`SQLite` 与 `PostgreSQL` 直接装配同一个 crate 私有关系内核。公共入口仍是各自的 Definition；
没有公共 Sink 框架、ORM、backend enum 或具体运行类型的兼容别名。数据库适配只负责新目标检查、
初始化、批量精确匹配和原子写批次，不接触 Store。

每个目标都有两个技术列：`$dogpaddle.id` 是此持久化 Sink 在所有输入中分配的递增 ID，删除后也不复用；
`$dogpaddle.hash` 是 `BLAKE3("dogpaddle.relation-row.v1\\0" || canonical_row)[..16]`。
hash 仅过滤候选，撤回仍精确比较完整逻辑值、优先选择最小 ID。SQL 参数保留 NULL、NUL、
浮点原始位和嵌套值的区别；hash 碰撞不会误删。

两种 Definition 都只声明 `relation_sink.state: Cell<Vec<u8>>`，保存 Initialize、Ready 或 Prepared。
Prepared 最多包含 1024 个具体操作：insert 是原 Change 的行索引与固定 ID，delete 是固定 ID，
另外只保存 next ID 和 continuation；不复制完整行、完整 Change 或 Station claim，不保存回执或摘要。

运行路径只有三步：

1. `turn` 在 Store 事务外按原事件顺序规划、批量匹配撤回，`apply` 把固定批次持久化为 Prepared。
2. 本地 commit 后，`AfterCommit` 在一个目标事务中先批量 insert-ignore，再批量按 ID delete。
   只忽略 technical-ID 主键冲突，其他约束错误仍失败；不存在的删除是正常重放。
3. 下一 turn 才结算 Ready；还有输入时 `Commit` continuation，否则 `Complete` 输入。

目标已提交但第 3 步未提交时，reopen 原样重投当前 Prepared。固定 ID 的重复插入和删除幂等；
即使同一批先插入再删除同一个 ID，重复执行后仍为空。只有结算成功才能准备下一批，
从不重投已结算的旧批次。目标 I/O 不占用 Store 写事务；`AfterCommit` 不确定时 fail-stop/reopen。

首次 turn 只恢复本地状态。新目标检查发生在发布 Initialize 之前；Initialize 提交后才建表，
初始化重放只接受同布局的空目标。目标布局在初始化或重新连接时检查，不逐 turn 扫表、
查询 MIN/MAX，也不向目标查询 ID 分配事实。

规划保留非负前缀：后续插入不能掩盖更早的非法撤回。大额负事件在首部分落地前校验完整数量，
后续依赖 durable continuation，不反复全量计数；正事件在首部分落地前校验完整 ID 区间。
每批返回的具体 ID 和具体操作均有 1024 上限，但完整匹配计数、输入 Change、单行大小不受此数值限制。

目标由 Sink 独占，不能外部改表/改数据、替换或恢复数据库，也不能添加业务唯一约束、trigger 或 FK。
目标事务允许先插后删，只承诺每批提交后的精确关系，**不承诺目标 WAL 事件顺序**，也不保证源事务
整体在目标原子可见。此共享 state、hash 与目标布局取代开发期旧格式；旧 Flow 和目标必须重建，
没有兼容读取、别名或迁移。

## `operation::sink::SqliteSink`

[`operation::sink::SqliteSinkDefinition`]（tag `10`）接收绝对 UTF-8 文件路径和新表名。
拒绝相对路径、内存库、NUL、空表名与 `sqlite_` 前缀；payload 是两个 `u32` big-endian
长度及 UTF-8 bytes。bind 只编译精确 Schema、`STRICT` 布局与行映射，不打开 `SQLite` 文件。

最多 1998 个顶层逻辑字段，允许零字段与空字段名；名称不得含 NUL、大小写不敏感重名或与技术列冲突。
目标是 `INTEGER PRIMARY KEY` ID、16-byte hash、逻辑列和 hash 索引，不增加元数据表或整行副本。
Boolean、有符号整数、`UInt8/16/32`、Date32、Timestamp 使用带约束 INTEGER；
`UInt64`、浮点、Decimal128 使用原始位 BLOB；Utf8 使用 `TEXT COLLATE BINARY`；
Binary、List、Struct 使用 BLOB；Null 始终为 NULL。所有 v1 类型无损映射。

撤回使用缓存的参数化精确查询；写入在一个 `BEGIN IMMEDIATE` 事务中完成。
连接使用 5 秒 busy timeout 与 `synchronous=FULL`，不修改 journal/WAL 模式。
数据库文件可以预先存在，但目标表必须新建。

## `operation::sink::PostgresSink`

[`operation::sink::PostgresSinkDefinition`]（tag `12`）是单输入、无 output 的 exact relation Sink。
调用方先用 [`operation::sink::PostgresSinkConfig::discover_target`] 检查目标，再把非敏感
[`operation::sink::PostgresTargetSpec`] 固化为 canonical JSON。spec 只含 sink ID、
database/schema/table 与 cluster/database identity；host、port、user、password 留在拥有型
`PostgresSinkConfig`，每次 build/open 经 `FlowFactory::resource` 注入。
Definition、bind、materialize 与 Flow build/open 均不联网，无 `PostgreSQL` 专用 Flow 方法。

一个 spec 只属于一个持久化 Flow/Sink，不可接管或共享已有目标。对象 comment 中的 marker
只标识 ownership/layout version；精确 Schema 由 Flow binding 与运行时 guard 保证。
目标只包含数据表、主键索引与 hash 索引，不创建回执表。

不同撤回行通过一条参数化批量查询匹配，返回本批所需 ID；插入使用多行 INSERT，删除使用 ID 数组。
宽 Schema 按 65,535 参数上限拆分语句，仍在同一目标事务内。运行配置只接受 numeric IPv4/IPv6，
直接使用 `hostaddr` 避开不可取消的系统 DNS；完整连接握手、discovery、身份校验和每个数据库工作单元
都有 5 秒 client deadline。失败或超时会丢弃整个临时 session，下一 turn 从不变的 durable state 重试。

最多 1598 个逻辑字段，字段名须能逐字表示，不能与技术列或精确小写系统列冲突。
列数上限不保证任意宽行均可写入，仍受 PG tuple/page 限制。Boolean 与可无损表示的整数使用
带约束标量，Date32/Timestamp 使用原始整数；Utf8（含 NUL）、Binary、List、Struct 使用 `bytea`，
`UInt64`、浮点、Decimal128 使用定长 `bytea`。这是无损关系存储，不是输入表 DDL 镜像；
普通不含 NUL 的文本可用 `convert_from(column, 'UTF8')` 查询。无隐式转换、额外视图、TLS
或在线 Schema evolution。

普通 Cargo gate 离线；真实 PG 批量匹配/写入、宽 Schema、精确类型与进程崩溃恢复由
`system-tests/postgres/check_sink.py` 配合已经构建的两个 host 验证，
完整范围见根目录 `TESTING.md`。

## 扩展约束

新增内建 Operation 时，在 `operation/scan`、`operation/transform` 或 `operation/sink`
模块中加入 Definition 和运行实例，实现 sealed `OperationDefinition` 以及运行态
`AtomicOperation` 或 `TurnOperation`，手动声明包含精确输入数量的 [`OperationKind`]，并声明唯一稳定 tag、逻辑资源名、
类型化 collection class、payload codec、纯 Schema bind 与一次性物化逻辑；公共 decoder 表只增加一条
`tag → decode function` 记录。运行实例可以保存执行参数、已装配 collection 与可由持久状态重建的
临时运行资源，但不能保存 Definition、Transaction 或事务启动能力，也不能提供回到 Definition 的
getter；不再为每个算子增加只包裹字段的 `OperationData` 类型。需要事务外工作的算子直接实现
`TurnOperation::turn` 并返回线性 `PreparedTurn`；完整处理一个 Change 的 Transform 实现
`AtomicOperation::apply`。Flow 的 build/open 不应出现具体算子分支。

分类模块只负责容纳多个具体算子并重导出它们的公共类型，不拥有或重导出分类级的单一 tag
或 decoder。tag 与 decoder 始终属于具体算子模块，decoder 表按具体模块路径注册，因此同一
分类内增加任意数量的算子都不会产生注册名称冲突。

一个 Operation 的 tag、payload、显式 kind、有序 port 语义以及“有序 input Schemas → binding”规则，
逻辑数据名称、类型化 collection 和 codec 共同决定持久化 schema。
derived input/output Schemas 不单独持久化；因此改变同一 tag/payload 对同一输入的绑定结果，或改变
Schema 相关状态的 codec，仍是持久化 ABI 变化。Flow 根据声明创建实例，binding materialize 再按逻辑名
取出；实例集合拒绝重复、缺失、错误 class 或未消费的资源。当前仍是开发期 v1，可以直接调整
当前 tag 对应的 schema，但必须同步更新当前 decoder、黄金字节、资源布局和 reopen 测试。旧数据库
直接删除并重建，不维护旧版专用 decoder、迁移或兼容分支，也不测试旧库行为；未来若明确发布稳定格式，再另行
定义版本政策。编码 tag 与 decoder 表必须复用具体模块中的同一个 tag 常量。

声明使用普通静态 Rust 值表达，不引入 Slot、Assembler、Factory registry 或位置 ABI。只有在
出现稳定且机械的声明样板后，才考虑用很薄的 `macro_rules!` 生成声明常量；宏不得生成算子
主体、Schema bind、materialize、codec 或运行逻辑。

### 新增算子 checklist

一个新算子只有逐项关闭下面七类契约，才进入上面的能力表；“DataFusion/Arrow 已支持”或存在一个
happy-path 单测都不能替代这些答案。

- **语义**：写清 kind/arity、逐事件规则、diff/重复/顺序、跨端口顺序、稳定重批和同一 Change
  跨 `Commit` 重放的不变量；维护关系状态时另行定义 weight、负前缀、overflow 与 zero cleanup。
- **Schema**：定义有序 exact inputs 到唯一 output 的纯映射，覆盖每一种合法但不兼容输入的结构化
  拒绝，以及 output/input 运行期 Schema drift 的整 turn 回滚。
- **持久化**：分配唯一 tag，冻结 canonical payload/golden/truncation；声明完整逻辑 data 名、collection
  与 codec，证明 build/open/reopen 的精确资源布局。开发期破坏性变更直接更新当前
  基线，删除旧数据库并重建，不留旧 API、旧版专用 decoder、资源名兼容分支或旧库行为测试。
- **turn 协议与事务**：明确 `Turn::Idle`、prepared `Action::{Idle, Commit, Complete}` 与可选
  `AfterCommit`，证明 Operation state、output、Subscription acknowledgement 和多输入 active 全旧或全新；错误、背压、
  commit 失败和 reopen 都不能多应用或跳过输入，提交前任何路径不得运行 completion。
- **内存与类型**：声明哪些列/diff 共享 buffer、哪些 kernel 分配；新增 Arrow 类型同步覆盖 Change
  validation、full/projected IPC、标准 reader、malformed 与相关表达式/算子。
- **公共证据**：在单一 `correctness` target 中提供 Definition roundtrip、bind/materialize/turn、错误、
  rollback、重批和 reopen；Flow 组合根拥有纯失败无建库副作用、runtime guard 与资源装配证据。
- **性能与文档**：只有真实 workload 需要独立 benchmark 时才增加 owner 自有 target；否则接入现有组合
  workload。同步 Rustdoc、crate README、根能力边界、`TESTING.md` 和路线图。

## 测试与性能

私有 decoder registry 和类型擦除不变量由源码白盒测试拥有；全部公开行为合并在单一
`correctness` target。`definition_codec`、`expression`、`protocol`、`metamorphic` 只拥有横切契约，
其余文件按每个具体算子纵向覆盖 literal golden、kind/data、bind、materialize、turn 与 reopen。
`protocol` 直接验证上述队列例子的恢复状态机和 borrowed delivery 提交时序。Definition v1 使用版本化黄金字节约束，
各算子 Schema 证据覆盖十五个 built-in 的精确传播、decoded golden 再绑定、错误 arity、非法 logical Schema，
以及 Project、Filter、Extend、Select、SchemaAlign、UnionAll 对合法但不兼容 Schema 的结构化拒绝；
`SchemaAlign` 还覆盖 canonical metadata、显式 cast、nullability 放宽/收窄和空 output；空
SchemaAlign/Select 都覆盖没有表达式可代为检查时的 runtime input Schema drift 拒绝，非空路径继续
覆盖相同 guard 与既有 evaluate 语义；表达式测试覆盖 `DataFusion` protobuf
编码失败、roundtrip 与精确版本 golden。各算子 runtime 证据覆盖完整 turn、commit、rollback、
reopen、固定 output Schema/diff、Project/Extend/Select 零拷贝、UnionAll 多端口原样转发、Filter 的空/全量选择及覆盖
Null/bitmap/fixed/variable/List/Struct 全部既有 layout family 的部分选择、DataFusion
`create_physical_expr` 的 type/nullability、scalar/array evaluate 与 null 传播、
携带混合 diff 的稳定重批和 Store 错误。Expression golden
会经过 `decode → bind → materialize → turn` 检查 protobuf 到执行语义，而不仅是重编码。
Date32、无 timezone 的 Millisecond Timestamp 与 `Decimal128(10, 2)` 另有三个公共纵向测试：结构
direct-copy、`SchemaAlign` 精确 cast/nullability 和 Filter 组合比较都先执行
`encode → decode → re-encode → bind → materialize → turn`，再断言 buffer、diff 与行序；这不扩大为
其他 temporal/decimal 运算承诺。关系 Sink 另外覆盖 Definition/state/hash golden、全部当前 v1
类型（含 Date32、全部 Timestamp 单位/timezone 与 Decimal128）及嵌套值、列边界与标识符、1024 批边界、
multiplicity/ID 预检、主键重复与其他完整性错误的区分，以及 `SQLite` 已提交但 Store
transaction 丢失后的初始化、insert、delete 和整批 reopen 重放。`PostgresSink` 的离线公共证据覆盖
tag12 canonical/non-secret Definition、精确 runtime resource、唯一 state Cell、Schema/spec 拒绝，以及
首 turn rollback 或丢弃 completion 不连接目标；真实批量读写与 crash recovery witness 由显式脚本所有。完整目录所有权、
测试矩阵和 fixture 规则见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。

Operation 通常不为 Definition codec 或一行算子 body 建立独立 benchmark，因为它们不能代表真实事务、
调度或持久化成本；相关性能由 Flow、Store 和跨 crate seam 的 owner workload 测量。Aggregate extrema
是例外：它以完整 turn、同步 Store commit 和状态恢复为 owner workload，专门测量分区首尾读取成本。
完整性能所有权见根目录 [`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。

## 验证命令

```bash
cargo test -p dogpaddle-operation
cargo test -p dogpaddle-operation --test correctness
cargo clippy -p dogpaddle-operation --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-operation --no-deps
```

### Aggregate extrema benchmark

`DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench aggregate_extrema`
运行同组非极值的高 multiplicity 更新、最小值撤回/恢复和八组重复 MIN/MAX。每次迭代
包含两次完整 Operation turn/apply、同步 Store commit 与 AfterCommit；fixture、seed、
输出 oracle 和 teardown 不计时。输入始终恢复到同一逻辑关系，输出在每次迭代后校验。
reference 使用同一 workload、更长采样窗口，并要求绝对 `DOGPADDLE_PERF_ROOT`；
结果目录保留 Criterion raw samples 和主机、提交及 workload context。此 target 不测
Flow 调度，也不把 smoke 数字当作性能基线。
