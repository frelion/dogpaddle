# dogpaddle-operation

`dogpaddle-operation` 提供具体、强类型的 Operation Definition、持久化 Data 和运行实例。
Definition 是无副作用、可持久化的数据；它声明所需数据对象的稳定逻辑名、collection 类型
及该 collection 真正需要的布局参数，也把有序、精确的输入 logical Arrow Schema 纯绑定为一个
一次性的编译结果。Flow 在 `build/open` 阶段先绑定完整拓扑，再创建或打开类型化对象，并消费
binding 装配运行实例。方向严格单向：`Definition → OperationBinding → runtime Operation`；运行实例
只保存执行参数和具体持久化 collection，不保留 Definition 或 binding。具体算子不接触 `Store`、
`DataHandle` 或物理放置策略。

## 数据边界

共享的 Arrow Schema、批量差分模型和“每个 Change 一个自描述 IPC Stream”的编码属于独立的
`dogpaddle-change` crate。Operation 的运行接口以内存中的 `Change` 为输入输出；Operation 只
负责数据变换和自己声明的持久化状态，不读取边日志，也不决定物理
batch 的合并与 flush。Change 的行位置是事件顺序；Operation 必须依次观察输入事件，并按
其声明的语义产生有序输出，不能把未 consolidation 的输入当作可交换集合。除非将来接收到
独立定义的窗口、barrier 或 flush 信号，Operation 的展平输出事件序列和最终业务状态必须在稳定
合并或切分 Change 后保持不变，也不能因同一个 Change 被分成多少个 `Commit` turn 而改变。显式声明
跨端口无序的多输入关系算子只保持每个端口的事件子序列和最终关系状态；当前 `UnionAll` 的跨端口
交织由 Station 调度，可能随分批变化。除此之外，物理 Change 边界和 turn 边界都不能被算子当成
业务事件。这个比较域要求每种分批的输入和对应输出都能
由其声明的 Arrow 类型物理表示；例如不能要求 `Utf8` offset 已溢出的单个 `RecordBatch` 成功构造。

外部 Source 返回普通 `Change`，由 Station 完成 Schema/capacity 校验与日志追加；自身 checkpoint
与 output 同事务提交即可，不需要另设持久 payload 中转，也不能绕过 Station 直接访问 output。

## Schema 绑定

这里的 Schema 是一个端口承载记录的完整、精确 logical Arrow Schema，不包含物理 IPC 中固定的
`$dogpaddle.diff` 字段。字段名称、顺序、类型、nullability、嵌套结构以及 Schema/Field metadata
都属于匹配内容；它不是“需要哪些列”的局部约束，也不是每个 Change 可以变化的动态类型。

[`OperationDefinition`] 的统一 `bind` 入口接收按端口顺序排列的 `SchemaRef`：Source 收到空 slice，Transform
和 Sink 收到恰好由 [`OperationKind`] 声明的数量。绑定先验证每个输入都是合法 `DogPaddle` logical
Schema，再由具体 Definition 接受或拒绝，并为 Source/Transform 返回唯一、完整的 output Schema；
Sink 必须没有 output。一个 Definition 可以在不同 Flow 中绑定不同输入，但同一次 Flow build/open
完成后，每条 output 只对应一个精确 Schema。

绑定必须是纯且确定的：相同持久化 tag、payload 和有序 input Schemas 必须得到相同语义。结果是
短生命周期、只能消费一次的 `OperationBinding`，可携带 Schema 相关的已编译执行信息及最终
materialize closure；它不写 Store，也不进入持久化格式或运行态对象。目前 `SequenceSource` 固定
输出 `{ value: UInt64 non-null }`；`RunningEventCount` 接受任意合法的单一输入并固定输出
`{ count: UInt64 non-null }`；Project 按稳定顶层字段索引绑定输入，拒绝越界、重复或重排，
并以选中字段的完整 Schema 作为 output；Filter 用绑定后的 Boolean 表达式保持 input Schema；
Extend 由绑定表达式唯一推导一个新增字段的类型和 nullability；Select 从同一个原始输入计算有序的完整输出列；
`SchemaAlign` 从同一个原始输入计算有序字段，并显式声明名称、目标 nullability、Field metadata
和 Schema metadata；`UnionAll` 要求所有输入 Schema 完全相同并原样转发 Change；Discard 接受任意
合法的单一输入且没有 output；`SqliteSink` 还把合法输入编译为确定的 `STRICT`
表布局、绑定 SQL 和无损行编码；`PostgresSink` 把单一 exact relation 输入绑定为固定的 `PostgreSQL`
表布局与参数化语句。无需额外的
`Any/Exact` 约束 DSL、Schema registry 或 fingerprint。

Filter、Extend、Select 与 `SchemaAlign` 的公共入口直接接收 `DataFusion` [`Expr`]；`dogpaddle_operation` 在 crate 根级重导出
[`Expr`]、[`col`]、[`ident`]、[`lit`]、[`cast`]、[`try_cast`] 和 [`ScalarValue`]，调用方不再学习另一套表达式
builder。需要按 Arrow 字段名逐字引用时使用 [`ident`]；[`col`] 保留 `DataFusion` 自身的大小写正规化和
multipart identifier 解析规则。Definition 的 `try_new` 立即使用 `datafusion-proto` 编码 `Expr`，无法编码时返回构造错误；
公开 getter 从同一表达式定义返回 `Expr`，不引入 `DogPaddle` 自有 AST。

Definition payload 直接保存 `DataFusion` Expr protobuf，不保存 `PhysicalExpr`。Schema bind 将完整 input
Schema 交给 `DataFusion` `create_physical_expr`；表达式的字段解析、type、nullability、cast 与
运行期 `evaluate` 全部由 `DataFusion` 定义。该 API 假定 logical coercion 已完成，而本 crate 不运行
logical/SQL planner，因此不会额外插入隐式 cast；混合类型表达式需要调用方显式 [`cast`]。binding 只保存 exact input Schema、physical expression 和
派生 output 属性；open 从 protobuf 还原 `Expr` 后重新完成同一过程。DogPaddle 继续负责完整 Schema
guard、Filter/Extend/Select/SchemaAlign 的 output Schema 约束，以及 records/diffs 的 Change 语义。

这份 protobuf 是版本绑定的持久格式，不承诺跨 `DataFusion` 版本兼容。工作区精确 pin 相互匹配的
`DataFusion`、`datafusion-proto` 与 Arrow；升级必须审查 proto roundtrip、physical planning 和执行语义。
当前仍是开发期格式；升级依赖后只维护新的 canonical payload 与证据，旧数据库直接删除并重建，
不承诺兼容、猜测或迁移旧表达式。DataFusion 的采用不等于引入 SQL 层。

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
`SequenceSource → SchemaAlign → Project → Select → Extend → Filter → RunningEventCount → Discard`
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
| `DataFusion` 可规划、DogPaddle 未承诺 | 已有 Operation 级执行证据但尚无对应完整 Flow 纵向证据的 Boolean `and/or/not`、`is_null`、代表性 scalar/array `eq/not_eq`、整数加法与 `Utf8 → Int64` `try_cast`；只有 protobuf roundtrip 证据的 `is_not_null`；其他算术/比较、`between`/alias、内建函数、复杂嵌套表达式；未列出的 Timestamp unit/timezone、时间/Decimal 运算和其他 temporal/decimal cast | 当前 pin 上能构造、bind 甚至执行仍不构成持久产品契约；补齐精确 Flow build/open/reopen 证据后才能进入上一行 |
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
[`RuntimeResource`]。普通算子传 `RuntimeResource::none()`；`PostgreSQL` Source 与 Sink 分别传拥有型配置。
binding 先验证其精确 Rust 类型，Flow 在创建 Store 前完成全图检查。这里没有全局 registry、
connector enum 或启动回调；资源只在 materialize 时 move 进 Operation，外部初始化仍由 turn 完成。

运行时 [`operation::Operation`] trait 只有一个统一、object-safe 的 `turn`。零输入 Source 与其他
Operation 走同一个协议，只是收到 `None`；Transform 与 Sink 每次收到一个完整 borrowed Change，
以及它在 Definition 有序输入中的 `usize` 端口序号。Operation 不接收 `AppendLog` offset、
Transaction 或事务启动能力。

一次 turn 明确分成三个线性阶段：

1. `Operation::turn` 在没有活动写事务时运行。它可以检查内存状态、惰性初始化资源或执行一次有界
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
逐字节相同的完整 Change。零输入 Source 也用 `Commit` 表示一次成功 turn。`Complete` 才在同一
事务中提交 Operation 状态、可选 output 和当前输入完成。两种提交动作都至多产生一个 owned
output Change；filter 或 Sink 可以使用 `None`。

跨 turn continuation 必须放在 Operation 自己通过 Definition 声明的持久化 Store 状态中，不能
隐藏在 Station；运行实例可以保存由该持久状态重建的临时资源。`turn` 或 `apply` 的提交前错误统一
擦除为 [`operation::OperationError`]，提交后的 callback 错误则使用独立的
[`operation::PostCommitError`]，明确表示本地事务已经无法回滚。Flow 遇到后者会停止该运行态
Station，要求 reopen 后从已提交状态恢复。`SqliteSink` 已有的 `SQLite`→`MDBX` 幂等协议仍保留在其
prepared turn 内，不为统一形式强行改成提交后写 `SQLite`。

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

先读 [`QueueSource`](examples/support/queue_source.rs) 的 `turn`：`client: None` 时在事务中读取
checkpoint，提交后建立临时 client；之后在事务外 poll，在事务中保存 checkpoint 和返回 output，
提交后才 ACK。算子本身不持有事务启动能力。

再读 [`queue_source` 的调用代码](examples/queue_source.rs)，或直接运行：

```sh
cargo run -p dogpaddle-operation --example queue_source
```

示例先提交 `10`，关闭 Store 和 Operation，重新打开后继续提交 `20、30`。它用固定队列模拟可按
checkpoint 恢复的外部服务；独立调用代码把 output IPC 与 checkpoint 原子写入 Store。生产 Flow
由 Station 负责事务、Schema guard、容量和输入进展。示例没有可装入 Flow 的 Definition，也不是
已交付的 Debezium Source。

`correctness/protocol.rs` 直接复用同一份算子代码，覆盖初始化回滚、未 ACK 重放，以及第二条记录
在本地提交前或提交后丢失运行态，再 reopen 的完整输出序列。测试中的 Drop 用于模拟这些恢复边界，
不代替未来真实连接器的进程崩溃验收。

下文所说的 data class 指一个完整的 Rust 持久化数据类型，包括 collection、值类型，以及该
collection 存在选择时的 `SIZE`，例如 `Cell<u64>` 或 `OrderedMap<u64, String, Large>`。

## 内建算子能力与 conformance

下表是当前十二个内建算子的产品契约索引。`任意` 指任意合法且已由 Change 支持的精确 logical
Schema，不表示运行期动态 Schema；`共享` 只表示有公开 pointer/buffer 证据的路径。表中未列出的
`DataFusion` 表达式或 Arrow 类型不能由“底层依赖碰巧支持”推导为 `DogPaddle` 承诺。这是文档与测试
索引，不是代码级 capability registry；Flow 仍不枚举具体算子。

| 算子（tag） | kind / arity | bind 后的 Schema | 行、diff 与 action | Operation data | buffer 行为 | 公共证据与性能 workload |
| --- | --- | --- | --- | --- | --- | --- |
| `SequenceSource` (`1`) | Source / 0 | 固定 `value: UInt64 non-null` | 每 turn 一行、diff `+1`、`Commit`；耗尽后 `Action::Idle` | `sequence_source.position: Cell<u64>` | 新建 output | golden、bind、末值/rollback/reopen；`operation_core` source body/commit |
| `PostgresSource` (`11`) | Source / 0 | 固定单表受支持列 | 事务外 poll，checkpoint 与 output 同事务提交后 ACK | `postgres_source.checkpoint: Cell<Vec<u8>>` | 移出 JSON 行、借用文本构建 Arrow；Source 不做 IPC 中转 | tag11 golden、纯资源/Schema 校验、初始化/回滚/reopen；显式真实 PG→SQLite 与进程恢复 gate |
| `RunningEventCount` (`2`) | Transform / 1 | 任意 → `count: UInt64 non-null` | 按输入行序每行加一，忽略输入 diff 数值，输出 diff `+1`，`Complete` | `running_event_count.count: Cell<u64>` | 新建 count，保持行序 | tag `2` golden、bind、overflow/rollback/reopen/重批；`operation_core` `RunningEventCount` body/commit、`flow_runtime` chain |
| Project (`4`) | Transform / 1 | 严格递增顶层索引；保留所选 Field 与 Schema metadata | 行序和 diff 不变，`Complete` | 无 | 所选列与 diff 共享 | golden、合法/拒绝 bind、空投影、runtime/reopen/重批、temporal/decimal 直接列；Definition codec，无独立 turn benchmark |
| Filter (`5`) | Transform / 1 | Boolean Expr；output exact input | 仅保留 non-null true，records/diffs 同步筛选；全删 `Complete(None)` | 无 | 全选共享；部分选择由 Arrow filter 分配 | Expr golden、bind/evaluate、null/Kleene、全部 layout family、Date32/Timestamp(ms)/Decimal 同类型组合比较、reopen/重批；Definition codec，无独立 turn benchmark |
| Extend (`6`) | Transform / 1 | 保留 input，追加一个由 Expr 推导的 Field | 行序和 diff 不变，`Complete` | 无 | input 列和 diff 共享；派生列按需分配 | Expr golden、bind/evaluate、名称拒绝、temporal/decimal 直接列、reopen/重批；Definition codec，无独立 turn benchmark |
| Select (`7`) | Transform / 1 | 同一原始 input 上的有序 `name + Expr` 完整输出 | 行序和 diff 不变；空 Select 保留行数；`Complete` | 无 | 直接列和 diff 共享；派生列按需分配 | Expr golden、bind/evaluate、空/非空 runtime Schema guard、别名隔离、temporal/decimal 选择/重排、reopen/重批；Definition codec，无独立 turn benchmark |
| `UnionAll` (`8`) | Transform / N，N > 0 | 所有输入必须 exact 相同，原样输出 | 保持每端口行序/diff；跨端口无序；`Complete` | 无 | 整个 Change 原样共享 | golden、arity/bind 与 runtime exact-Schema 拒绝、多端口 runtime/reopen/重批；Definition codec，无独立 turn benchmark |
| `SchemaAlign` (`9`) | Transform / 1 | 有序 `name + Expr + target nullable + Field metadata`，另有 Schema metadata | 行序和 diff 不变；空定义保留行数；`Complete` | 无 | 直接列和 diff 共享；表达式结果按需分配 | golden/canonical metadata 与重复 key 构造拒绝、bind/收窄拒绝、空/非空 runtime Schema guard、temporal/decimal 精确 cast、runtime/reopen；Definition codec，无独立 turn benchmark |
| Discard (`3`) | Sink / 1 | 接受任意，无 output | 完成完整输入，`Complete(None)` | 无 | 不产生 output | golden、bind、runtime/rollback/reopen；`operation_core` Definition codec、Flow sink workload |
| `SqliteSink` (`10`) | Sink / 1 | 接受任意合法 Schema，另校验 `SQLite` 列名与列数；无 output | 按行序展开 diff multiplicity，每批至多 1024 个 mutation；中间 `Commit`，最终 `Complete(None)` | `sqlite_sink.next_id: Cell<u64>`、`sqlite_sink.pending: Cell<Vec<u8>>` | 不产生 output；按行生成 canonical/hash 和 `SQLite` 绑定值 | tag/payload、pending/canonical/hash golden，全部 v1 类型、批界、multiplicity/ID 预检、冲突/锁错误及 `SQLite` commit 后 MDBX rollback/reopen 重放；无独立 benchmark |
| `PostgresSink` (`12`) | Sink / 1 | 单一 exact relation；校验 `PostgreSQL` 列名、系统列与列数；无 output | 每批至多 1024 个 mutation；持久 `Prepared` 后由 `AfterCommit` 原子提交 receipt + mutations，下一 turn `Commit` continuation 或 `Complete(None)` | `postgres_sink.state: Cell<Vec<u8>>` | 不产生 output；按行生成 canonical/hash 和参数值 | tag12 canonical JSON、非敏感 Definition、资源/Schema/布局、state codec；普通 gate 离线，真实 PG 恢复由 `tools/check_postgres_sink.py` 验收 |

所有十二个算子共用同一条 `Definition → exact Schema binding → materialize → turn` 路径，并由
`tests/correctness/{codec,definition,postgres,postgres_sink,protocol,runtime}.rs` 作为 Operation 公共证据入口；完整 Flow 的纯失败
无建库副作用、资源名、build/open/reopen、运行期 Schema guard 和事务重放由
`crates/flow/tests/correctness` 所有。`operation_core` 的 Definition codec 当前覆盖除 `SqliteSink`、`PostgresSource`
与 `PostgresSink` 外的九个算子；直接
turn body/durable commit 只测 `SequenceSource` 与 `RunningEventCount`。`flow_runtime` 测
source/sink、RunningEventCount chain、fan-out 和 capacity pressure。其他算子没有独立计时场景，
不因此获得虚构的微基准。

前十个 tag 的稳定字节入口位于 `tests/fixtures/v1/`；tag11 与 tag12 的完整 canonical JSON golden 分别内联在
`tests/correctness/postgres.rs` 与 `tests/correctness/postgres_sink.rs`。其中事件计数、对齐与 `SQLite` Sink 的 fixture 分别为
`running_event_count_definition.hex`、`schema_align_explicit.hex` 与 `sqlite_sink_output_events.hex`；它们分别冻结
tag `2`、`9` 与 `10`。codec 分区十个 decoded golden 都会重新 bind，两个 postgres 分区独立覆盖 tag11/tag12；Filter、Extend、Select、UnionAll 与
`SchemaAlign` 的 golden 还会
materialize/turn，其余算子的执行证据由 definition/runtime 分区独立覆盖。Flow manifest 的端到端基线为
`crates/flow/tests/fixtures/v1/sequence_source_running_event_count_discard.hex`。这些文件名只帮助定位
证据；契约仍由公共测试断言和上表语义定义。

运行实例及具体算子统一组织在 `operation` 模块中，其下按语义分为三个公共模块：`source`
保存无上游输入的源算子，`transform` 保存消费并产生记录的转换算子，`sink` 保存只消费记录
的终点算子。当前 `source` 包含 `SequenceSource` 与 `PostgresSource`，`transform` 包含 RunningEventCount、Project、Filter、
Extend、Select、SchemaAlign 和 `UnionAll`，`sink` 包含
Discard、`SqliteSink` 与 `PostgresSink`。目录分类不作为运行时类型系统；每个 Definition 必须通过
[`OperationDefinition::kind`] 显式声明包含输入数量的结构类型。

## Definition 与持久化

具体 Definition 统一实现 sealed [`OperationDefinition`] trait。trait 要求每个具体算子手动返回
[`OperationKind`]，并以 `{ 逻辑名: data class }` 的形式向 Flow 声明完整数据 schema。
`OperationKind::Source` 固定为零输入，Transform 与 Sink variant 携带非零 `u32` 输入数量，因此类别、
input arity 和 output 属性不会形成非法组合。kind 不是从拓扑位置推断：Source、Transform 和 Sink
分别声明自己在数据流中的结构语义；Station 读取所包裹 Definition 的 kind，再向 Flow 提供自己的
source/sink 与 output 属性。Flow 负责生成完整
资源名，并调用声明携带的类型化 create/open 能力；得到的实例按逻辑名组成集合，再交给
此前 Schema bind 产生的 `OperationBinding::materialize`。binding 只按名称取得已经创建或打开的
`Cell<T>` 或 `OrderedMap<K, V, SIZE>`，声明顺序不参与绑定，也不接收 Store；物化会消费整组实例，
并拒绝缺失、类型错误或未被 binding 取走的多余资源。

collection 只暴露真实存在的布局选择：`Cell<T>` 永远使用共享空间；`OrderedMap` 的 `Small`
形式共享底层物理空间，`Large` 形式拥有独立物理空间。Size 属于支持选择的具名数据对象的
静态 schema，而不是由 Flow 根据数据量猜测。Flow 只解释声明，不枚举具体算子或 collection
类型。

```rust
use dogpaddle_operation::{
    OperationDefinition, OperationKind, decode_definition, encode_definition,
    operation::source::SequenceSourceDefinition,
};

let source = SequenceSourceDefinition::new(10);
assert_eq!(source.kind(), OperationKind::Source);
assert_eq!(source.kind().input_count(), 0);

let encoded = encode_definition(&source);
let decoded = decode_definition(&encoded).unwrap();
assert_eq!(encode_definition(decoded.as_ref()), encoded);
```

Definition 集合在本 crate 内保持封闭，但不再使用公共 enum。稳定 decoder 由一张私有
`tag → decode function` 静态表选择；每个具体算子模块拥有自己的 tag、payload codec、
数据声明和物化逻辑。Flow 不枚举具体算子，也不解析 Operation payload。

为穿过 object-safe 的 [`OperationDefinition`] 边界，数据实例仅在一次性的 build/open 装配
过程中进行私有类型擦除；具名声明在 binding 的 `materialize` 中将其安全恢复为精确 data class。
类型不匹配会返回错误而不是 panic。类型擦除不会进入运行实例、事务访问路径或持久化格式。

物化后的具体实例统一实现 [`operation::Operation`] trait。Flow 将异构实例保存为
`Box<dyn operation::Operation>`，并通过同一个可变 `turn` 分派运行；运行实例只要求 `Send`，调度方
在从准备到提交后 completion 结束的整个 turn 期间持有其独占可变访问。Schema binding 只在 build/open
期间连接 Definition 与实例；运行 trait 和具体运行类型都不反向保存或暴露 Definition，Flow
已经从持久 Definition 获得 kind、资源声明和端口 Schema。
Operation 本身可以在外部实现，但 Flow 只从 sealed Definition 物化运行实例；开放可注入 Flow 的
第三方算子仍需另行设计 tag 分配、decoder 注册和运行错误边界。

## `PostgreSQL` Source 试点

[`operation::source::PostgresSourceDefinition`]（tag `11`）只描述一个数据库中的一张固定 Schema 表。
先用 [`operation::source::PostgresSourceConfig::discover`] 显式查询 catalog，再把得到的
`PostgresSourceSpec` 固化成 Definition；build/open/bind 不连 PG、不启动 JVM。配置由宿主构造并在
每次打开时重新装配，不自动读取环境变量、配置文件或全局 secret registry。

持久 Definition 保存 engine/topic 名、数据库/表/slot/publication 身份、cluster system identifier、
database/table OID 和有序列声明，不含密码、用户名、host 或 runtime payload 路径。payload 是固定
字段顺序的 canonical JSON；未知字段、重复字段、非 canonical 字节与超过 1 MiB 的 payload 被拒绝。
试点 engine/schema/table/slot/publication 名仅允许 1–63 个小写 ASCII 字母、数字和下划线。

算子只声明 `postgres_source.checkpoint: Cell<Vec<u8>>`，原样保存 D2 opaque checkpoint bytes；
其版本、校验和与边界校验由 D2 拥有，不增加 Source envelope。Cell 缺值表示首次运行，空或损坏的
bytes 是错误。没有 pending payload，数据只由 Station 编码一次并写入 output。单次 encoded
delivery 最多 16 MiB；这不是 JVM/Rust 总内存或 WAL 磁盘硬配额。

此前开发期 `postgres_source.state` 的 pending 布局已删除，已有试点 Flow 必须重建，不提供迁移
或兼容读取；缺少新 checkpoint 资源的旧 Flow 会拒绝 open，不会当作首次运行。

同一个 `turn(None)` 根据自己的状态推进：

1. 初次 turn 在短事务中读取 checkpoint，提交后发布可丢弃的内存缓存。
2. 后续 turn 在事务外校验 PG 身份、惰性启动 connector，以零超时 poll 检查当前 delivery。
   转换后返回 prepared turn；apply 写 checkpoint 并返回 `Commit(Some(change))`，由 Station
   在同一事务追加 output。仅在 `AfterCommit` 中更新恢复位置缓存并消费 Delivery ACK。
3. Schema/容量/commit 失败同时回滚 checkpoint 与 output，不 ACK；下一 turn 由 D2 重投该批。
4. 无数据返回 `Turn::Idle`。普通 poll 错误会使下一 turn 重建临时 connector；ACK error/panic 则由
   Station fail-stop，必须 reopen。

不增加 delivery receipt 或用 checkpoint 充当批次 ID：回滚没有 ACK，D2 原样重投 outstanding；
checkpoint/output 已提交但 ACK 不确定时禁止复用旧运行态，从已提交 checkpoint 启动 fresh Engine；
已经落盘的 output 由正常下游路径继续消费。checkpoint-only heartbeat 返回 `Commit(None)`，不制造
空 Change。零超时 poll 只表示不等待数据，connector 启动及 ACK 仍是有界同步调用；宿主应在
整轮 Idle 或持续 Backpressured 时安排等待，避免忙轮询。

转换移出 JSON 行并借用其中的文本构建 Arrow，避免整行深拷贝和中间 String 副本；JSON 解析、
完整 Schema/值校验及必要的 Arrow buffer 写入仍保留。普通测试证明行为，不宣称 CDC 吞吐基线。

insert 输出 `+after`，delete 输出 `-before`，update 按顺序输出 `-before, +after`。只接受完整旧行、
精确列 Schema 和正确 source/topic；不排序、不抵消，不把 Debezium delivery 当成 `PostgreSQL` 事务边界。
不承诺一个源事务的所有行在下游原子可见。

| `PostgreSQL` | Arrow | 试点约束 |
| --- | --- | --- |
| boolean | Boolean | 保留 nullability |
| smallint / integer / bigint | Int16 / Int32 / Int64 | 范围检查，不经浮点转换 |
| real / double precision | Float32 / Float64 | 包括 Connect 非有限值表示 |
| text / varchar | Utf8 | 不包含 Debezium 缺值占位符 |
| bytea | Binary | 解码 Connect base64；不包含缺值占位符 |
| date | Date32 | 有限且可表示的日期 |
| timestamp | Timestamp(Microsecond, None) | 固定 microseconds 模式；拒绝 infinity |
| timestamptz | Timestamp(Microsecond, UTC) | 有限、可解析的 RFC3339，拒绝亚微秒截断 |
| numeric(p,s) | Decimal128(p,s) | `1 ≤ p ≤ 38`、`0 ≤ s ≤ p`；拒绝无约束 numeric/NaN |

catalog discovery 至少需要 `PostgreSQL` 15；当前本机端到端证据使用 17.10，不能据此声明其他版本
已通过同等验收。连接入口特意叫 `new_unencrypted`：当前仅供受信本地网络或独立加密隧道，discovery 和 JDBC 都禁用
TLS；不把这个试点称为完整安全部署方案。使用专属 CDC 角色，需要正常 replication/catalog 权限，
以及显式授予 `pg_control_system()` 的 EXECUTE 权限；无需因为该查询让业务角色成为 superuser。

表必须是 permanent、非 partition 的普通表，`REPLICA IDENTITY FULL`，无 generated 列。
publication/pgoutput slot 由用户预先创建并独占，不自动创建、更改或删除；publication 必须发布
全部列与全部 insert/update/delete/truncate，不能有 row filter。TRUNCATE 会被明确拒绝，而非跳过。
重新启动时校验 system/database/table identity、logical Schema、publication 与 slot 可用性。
运行中的外部 DDL、publication/slot 修改、数据库恢复或替换不受支持；不是 DDL 监控器。

当前 `snapshot.mode=no_data`，没有初始全量。关系物化应在空表上建立匹配的 slot 起点，开始捕获后
再写业务数据；不能把既有非空表直接接到空 `SQLite` 表并期望完整镜像。缺失旧值不能回查当前表猜测。
字面值 `__debezium_unavailable_value` 在 text/bytea 中暂作保留值并拒绝，以免将缺失 TOAST 当作真实值。
不支持数组/domain/JSON/UUID 等未列出的 PG 类型，也不支持多表路由、在线 Schema evolution、初始
snapshot、TLS 配置、跨实例 fencing、自动变更外部资源或 graceful stop API；这些不由额外抽象提前实现。

完整宿主示例在 `crates/flow/examples/postgres_cdc.rs`；普通 Cargo 测试无需 Java/PG，真实端到端与
进程恢复由 `tools/check_postgres_cdc.py` 显式验收，见根目录 TESTING.md。

## `operation::source::SequenceSource`

[`operation::source::SequenceSourceDefinition`] 是零输入源，记录首个 `u64` 值。物化后的
[`operation::source::SequenceSourceOperation`] 只持有复制出的 `start: u64` 和直接的
`Cell<u64>` position；首次产生 `start`，随后根据最后一次已提交的值逐一递增。每个 turn 产生一行，输出固定为一个
non-null `UInt64` `value` 字段，所有 diff 都是 `+1`。包含 `u64::MAX` 的最后一批可以成功提交，
后续 turn 的 `apply` 返回 `Action::Idle`，不再写 position 或产生 output，使 Flow 仍能调度下游并排空已经提交的
Change。每次产生值的 turn 返回 `Action::Commit(Some(_))`；Station 不为
Source 建立另一套 outcome 或事务路径。它声明自己产生输出。

Schema bind 不接收输入，并固定完整 output Schema 为一个 non-null `UInt64` `value` 字段。

它声明一个逻辑数据名 `sequence_source.position`，由 Flow 解析为稳定 Station 资源名。

## `operation::transform::RunningEventCount`

[`operation::transform::RunningEventCountDefinition`] 要求一个输入。每成功推进一次，
[`operation::transform::RunningEventCountOperation`] 按输入行序计算事件数量：每一行恰好令直接持有的
唯一字段 `Cell<u64>` 加一，输入 diff 的符号和数值不改变“一个有序事件”的计数。每个已处理输入行输出
一个 non-null `UInt64` `count`，diff 固定为 `+1`；因此它是插入式的运行计数事件流，不是维护
单例关系的 cardinality aggregate。未写入的 count Cell 解释为 `0`，溢出返回
[`operation::transform::RunningEventCountError::Overflow`]。`RunningEventCount` 显式声明为携带一个输入的
[`OperationKind::Transform`]；拓扑位置不会把它隐式变成 Sink，因此完整 Flow 必须把它连接到
一个下游 Sink。

`RunningEventCount` 只声明 `running_event_count.count: Cell<u64>`，当前 Definition tag 为 `2`，output
字段为 `count`；公共 Rust API、逻辑 data 名与 Flow 路径同时采用清晰名称，资源为
`station/{index:08x}/operation/running_event_count.count`。不提供旧名称 alias、旧资源 fallback 或
迁移逻辑；旧版本创建的数据库直接删除并按当前 Definition 重建，不承诺或测试旧 manifest 的兼容
行为。当前实现每个 turn 一次处理完整 Change，并返回
`Action::Complete(Some(_))`；它在写状态前预检整批行数，若最终值无法用 `u64` 表示，则返回 overflow，
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
`ChangeProjection`。每个 turn 对完整 Change 做保持行序和 diff 的顶层投影，并返回
`Action::Complete(Some(_))`；所选 Arrow Array buffer 与输入共享，不复制列数据。Definition 的字段
索引数量和每个索引使用稳定 big-endian `u32` 编码，tag/payload 与 input Schema 一起决定 reopen 后
重建的精确 output Schema。

## `operation::transform::Filter`

[`operation::transform::FilterDefinition`] 通过 fallible `try_new` 接收 `DataFusion` [`Expr`]，要求一个输入，
并持久化 `DataFusion` Expr protobuf。Schema bind 通过 `DataFusion` 把表达式编译到精确 input Schema；
最终类型不是 Boolean 或 `DataFusion` 无法规划
都会在 Flow 创建 Store 前以结构化 [`operation::transform::FilterSchemaError`] 拒绝。output Schema
与 input 完全相同。

[`operation::transform::FilterOperation`] 不声明 Store data。每个 turn 只保留 predicate 为 non-null
`true` 的行；`false` 和 null 都删除，同一个 Arrow filter predicate 同时筛选 records 与 diff，因而
相对事件顺序和每个保留事件的 diff 不变。没有行被选中时返回 `Action::Complete(None)`，因为空
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
最终 output Schema。每个 turn 共享全部 input `ArrayRef` 与 diff buffer，只为真正计算出的列分配数据；
若表达式只是 Column，新列本身也与源列共享同一 ArrayRef。结果保持行序并返回
`Action::Complete(Some(_))`。

## `operation::transform::Select`

[`operation::transform::SelectDefinition`] 通过 fallible `try_new` 接收有序的 `name + Expr` 集合。
每个表达式都独立绑定到同一个原始 input Schema，不能引用同一 Select 中新建的别名；输出只包含声明的列，
顺序、类型和 nullability 由声明与 `DataFusion` 唯一决定，并保留 input Schema metadata。空 Select 合法，
仍保留输入行数和 diff。

[`operation::transform::SelectOperation`] 不声明 Store data，只保存 binding 的 exact input/output
Schema 和编译后的表达式，并在任何表达式求值前检查 runtime input；因此空 Select
也会以 [`operation::transform::SelectError::InputSchemaMismatch`] 拒绝 Schema drift，不会产生 output
或持久写入。每个合法 turn 一次求值所有列并返回 `Action::Complete(Some(_))`；直接列引用和 diff
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
diff。`UnionAll` 与未来 Join 仍然只接受 exact Schema；所有上层 API 若需要共同结构，都应显式插入
`SchemaAlign` 或生成等价的已声明变换。

## `operation::transform::UnionAll`

[`operation::transform::UnionAllDefinition`] 只接收非零 input count。bind 要求所有有序输入与 input 0
具有完全相同的 logical Schema，并以该 Schema 作为 output；不做 cast、对齐或字段名推断。
[`operation::transform::UnionAllOperation`] 不声明 Store data，只保存 input count 与 binding 得到的
exact common Schema；每个 turn 在转发前校验 runtime input，并以包含 port、expected 和 actual Schema
的 [`operation::transform::UnionAllError::InputSchemaMismatch`] 拒绝漂移，不产生 output 或持久写入。
合法输入按收到的端口原样 clone 完整 Change，
因此保持该端口的行序、diff 和 Arrow buffer。它与 SQL `UNION ALL` 一样不定义跨输入顺序；端口间
交织由 Station 统一调度，可随上游分批和可用性变化。需要业务级总序时应另建显式排序或 barrier 语义。

## `operation::sink::Discard`

[`operation::sink::DiscardDefinition`] 显式声明为携带一个输入的 [`OperationKind::Sink`]，
不声明 Operation data，也没有 output。物化后的 [`operation::sink::DiscardOperation`] 是零状态
unit struct；它接受端口零上的完整 Change，并返回 `Action::Complete(None)`。输入完成仍由 Station 在同一事务中
持久化 cursor；失败或回滚不会丢失输入。Discard 只提供一个无外部副作用的显式 Flow 终点，外部
Sink 仍需各自设计与目标系统匹配的幂等提交协议。

Schema bind 接受任意合法的精确单一输入，并返回无 output 的 binding。

## `operation::sink::SqliteSink`

[`operation::sink::SqliteSinkDefinition`] 用 fallible `try_new` 接收绝对 UTF-8 `SQLite` 文件路径和目标
表名。它拒绝相对路径、内存库、NUL、空表名与 `SQLite` 保留的 `sqlite_` 前缀；稳定 tag 为 `10`，
payload 依次保存两个 `u32` big-endian 长度及对应 UTF-8 bytes。Definition 声明
`sqlite_sink.next_id: Cell<u64>` 与 `sqlite_sink.pending: Cell<Vec<u8>>` 两项 Store 状态。bind 只针对
精确 input Schema 编译表布局、SQL 和行编码器，不打开 `SQLite` 文件，也不创建表；连接延迟到物化
Operation 的首个 turn。

Schema 最多包含 1998 个顶层逻辑字段，允许零字段与空字段名；顶层名称不得包含 NUL，不得在 `SQLite`
的 ASCII 大小写不敏感规则下重名或与 `$dogpaddle.id`、`$dogpaddle.hash` 冲突。目标表由 Sink 创建，
包含递增且永不复用的 `INTEGER PRIMARY KEY` technical ID、16-byte BLAKE3 行 hash，以及全部逻辑列。
表和 hash index 都使用确定的双引号转义标识符，所有数据值使用绑定参数。`SQLite` 映射无损保留当前
`DogPaddle` v1 全部类型的原始语义：Boolean、有符号整数、`UInt8/16/32` 与 Date32 使用带范围
约束的 `INTEGER`；Timestamp 使用 `INTEGER`；`UInt64`、`Float32/64` 与 Decimal128 分别使用
big-endian 整数 bytes、IEEE 原始位与 128-bit two's-complement BLOB；Utf8 使用
`TEXT COLLATE BINARY`；Binary、List、Struct 使用 BLOB；Null 字段始终写 NULL。非 nullable
字段除 Null 外都有 `NOT NULL`。Timestamp 的 unit/timezone 与 Decimal128 的 precision/scale 属于
已绑定 Schema，行值保留其底层原始值。

[`operation::sink::SqliteSinkOperation`] 以 canonical 递归行编码计算
`BLAKE3("dogpaddle.sqlite-row.v1\\0" || row)[..16]`。删除先按 hash 与 ID 升序读取候选，再在 Rust 中逐字段
精确比较，所以 hash 碰撞不影响正确性，重复行固定删除最小 ID。每个持久批次最多包含 1024 个具体
insert/delete，可跨越多个 Change 行；批内 overlay 保留事件顺序，因而同批先插入再删除与稳定重批
具有相同结果。开始负 diff 前会验证完整剩余 multiplicity 可删除，开始正 diff 前会验证完整 ID 区间，
包括 `i64::MIN` 和 ID 耗尽边界，不会先写出部分非法事件。

初始化与每个 Apply 都使用版本化 `pending` 状态跨越 MDBX turn；Apply 在 `BEGIN IMMEDIATE` `SQLite`
事务中严格按列表顺序执行，并在 `SQLite` commit 前把下一 continuation 写入尚未提交的 MDBX transaction。
若 `SQLite` 已提交而外层 MDBX commit 丢失，重放会用同一组 technical ID 验证或补齐结果，因此最终
效果恰好一次。Sink 不创建 `SQLite` 元数据表，也不保存整行 canonical bytes；首次初始化只接受不存在
的目标表，初始化重放只接受完全相同且为空的布局，ready 状态发现表缺失或布局变化会报错。数据库
文件本身可以预先存在。连接使用 5 秒 busy timeout 与 `synchronous=FULL`，不修改 journal/WAL 模式。
该协议假定目标表只有此 Sink 写入，不支持外部修改 schema/数据、替换文件或恢复旧备份。

## `operation::sink::PostgresSink`

[`operation::sink::PostgresSinkDefinition`]（tag `12`）是单输入、无 output 的 exact relation Sink。
调用方先用 [`operation::sink::PostgresSinkConfig::discover_target`] 检查目标 schema 和待创建对象，
再把返回的非敏感 [`operation::sink::PostgresTargetSpec`] 固化进 canonical JSON Definition。spec 只含
sink ID、database/schema/table 与 cluster/database identity；host、port、user、password 留在拥有型
`PostgresSinkConfig`，每次 build/open 通过既有 `FlowFactory::resource` 路径注入。Definition 构造、
bind、materialize 与 Flow build/open 均不联网；`Flow::advance` 才执行远端工作，且没有 `PostgreSQL`
专用 Flow 方法。

一个 `PostgresTargetSpec` 只能归属一个持久化 Flow/Sink；公共 `try_new` 只是离线构造入口，不表示可以
接管或共享已有目标。PG object comment 中的 marker 只是 ownership/layout-version 标记，不是另一份
Arrow Schema fingerprint，也不承诺检测契约外的任意 catalog 篡改。精确 logical Schema 仍由持久化
Flow Definition 的 binding 与每次运行时 Schema guard 保证。

Definition 只声明 `postgres_sink.state: Cell<Vec<u8>>`。这个版本化 Cell 保存 Initialize、Ready 或一个
完整 Prepared intent：delivery sequence、payload digest、分配前 technical-ID frontier、Change 内位置、
continuation 与至多 1024 个具体 insert/delete mutation。它不保存连接、ORM entity、完整行副本或
另一套 Station input 状态。

运行时按同一个 `turn → apply → AfterCommit` 协议推进：

1. 首次 turn 只从 Cell 恢复并持久化 Initialize；本地 commit 后的 `AfterCommit` 才创建或验证空目标，
   下一 turn 再把 Ready 持久化。
2. Ready 的 `turn` 在 MDBX 事务外验证目标、匹配撤回行并按输入顺序规划至多 1024 个 mutation；
   `apply` 只把 Prepared intent 写入 Cell 并返回 `Commit(None)`。
3. Prepared 已随 MDBX 提交后，`AfterCommit` 在一个 `PostgreSQL` 事务中写入该 delivery 的唯一 receipt
   行和全部 mutation。receipt 保存 sequence、digest 与 mutation count；相同 intent 重投只验证这行，
   不重复修改目标。
4. 下一 turn 才把 durable state 结算回 Ready：仍有 continuation 时返回 `Commit(None)` 并保留同一
   Change，批次结束时返回 `Complete(None)`。若进程在 PG commit 后、结算前退出，reopen 从 Prepared
   重投。能够准备下一批，已证明上一批在 MDBX 完成结算；因此新批次的 PG 事务同时删除旧 receipt，
   始终只保留最后一批的确认记录，不另设清理任务或保留历史流水。

连续同类 mutation 使用参数化多行 INSERT 或 DELETE USING VALUES，按原事件顺序执行；不会把整批
先按 insert/delete 重排。宽 Schema 按 PG 的 65,535 参数上限进一步拆分语句，仍在同一个 PG 事务内。
撤回只返回本批需要的至多 1024 个 ID，并按最小 technical ID 选择。大额负 diff 在第一部分落地前
由服务端计数校验完整数量，后续从已有 durable continuation 继续，不重复扫描剩余总量。
计数仍可能扫描大量匹配行，受 statement timeout 约束；1024 是 mutation/返回 ID 数量界限，不是
整条 SQL、输入 Change、单行大小或内存总量的硬上限。

普通目标查询失败会丢弃临时 client 并允许同一 durable state 重试；`AfterCommit` 失败发生在本地
commit 之后，因而由 Flow fail-stop 并要求 reopen。远端查询和提交都不占用 MDBX 写事务。

存储映射优先保留 Arrow 原始语义，而不是伪装成自然 SQL model：Boolean 与可无损表示的整数使用
带约束标量，Date32/Timestamp 使用原始整数；Utf8（包括 NUL）、Binary、List 与 Struct 使用变长
`bytea`，`UInt64`、浮点与 Decimal128 使用定长 `bytea` 保存精确位模式。
因此目标是无损关系存储，不是源表 DDL 的原样镜像；不能直接按原生 text/numeric/timestamp 使用
这些列。普通无 NUL 文本可用 `convert_from(column, 'UTF8')` 查询（含 NUL 时 PG text 无法表示）。
当前不提供隐式类型转换或额外可查询视图。

目标表、receipt 表、索引与约束由该 Sink 独占并保持固定的生成式 PG 存储布局。Schema 最多 1598 个顶层
字段；字段名须能被 `PostgreSQL` 逐字表示，且不得与 Sink technical columns 或精确小写系统列冲突。
列数上限不保证任意宽行都能写入，仍受 PG 本身的 tuple/page 和字段大小限制。
当前不支持 TLS、在线 Schema evolution、外部改表/改数据、数据库替换或恢复；运行中的这些变化不
属于恢复协议。实现保持 `PostgreSQL` 专用，不提供公共通用 Sink trait、backend enum、插件 registry
或 ORM 层。它与 `SqliteSink` 只共享 crate 私有的 relation position、technical-ID、continuation 与
批次校验机械；DDL、DML、state codec 和恢复协议分别实现。

普通 Cargo gate 完全离线，覆盖 tag12、Schema/resource/layout、state codec，以及初始化 completion
被提交或丢弃的协议边界；真实 `PostgreSQL` 大批 insert/delete、精确值匹配、参数上限、receipt 回收和提交前后恢复由
`python3 tools/check_postgres_sink.py --postgres-bin /absolute/path/to/postgresql/bin` 显式验收，详见根目录 `TESTING.md`。

## 扩展约束

新增内建 Operation 时，在 `operation/source`、`operation/transform` 或 `operation/sink`
模块中加入 Definition 和运行实例，实现 sealed `OperationDefinition` 和运行态
`Operation`，手动声明包含精确输入数量的 [`OperationKind`]，并声明唯一稳定 tag、逻辑资源名、
类型化 collection class、payload codec、纯 Schema bind 与一次性物化逻辑；公共 decoder 表只增加一条
`tag → decode function` 记录。运行实例可以保存执行参数、已装配 collection 与可由持久状态重建的
临时运行资源，但不能保存 Definition、Transaction 或事务启动能力，也不能提供回到 Definition 的
getter；不再为每个算子增加只包裹字段的 `OperationData` 类型。需要事务外工作的算子直接实现
`Operation::turn` 并返回线性 `PreparedTurn`；完全事务型的内建算子共用 crate 内部的零额外分配
适配路径。Flow 的 build/open 不应出现具体算子分支。

分类模块只负责容纳多个具体算子并重导出它们的公共类型，不拥有或重导出分类级的单一 tag
或 decoder。tag 与 decoder 始终属于具体算子模块，decoder 表按具体模块路径注册，因此同一
分类内增加任意数量的算子都不会产生注册名称冲突。

一个 Operation 的 tag、payload、显式 kind、有序 port 语义以及“有序 input Schemas → binding”规则，
逻辑数据名称、类型化 collection、codec 和适用时的 `SIZE` 共同决定持久化 schema。
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
- **持久化**：分配唯一 tag，冻结 canonical payload/golden/truncation；声明完整逻辑 data 名、collection、
  codec 与 `Small`/`Large`，证明 build/open/reopen 的精确资源布局。开发期破坏性变更直接更新当前
  基线，删除旧数据库并重建，不留旧 API、旧版专用 decoder、资源名兼容分支或旧库行为测试。
- **turn 协议与事务**：明确 `Turn::Idle`、prepared `Action::{Idle, Commit, Complete}` 与可选
  `AfterCommit`，证明 Operation state、output、cursor、active 和 reclaim 全旧或全新；错误、背压、
  commit 失败和 reopen 都不能多应用或跳过输入，提交前任何路径不得运行 completion。
- **内存与类型**：声明哪些列/diff 共享 buffer、哪些 kernel 分配；新增 Arrow 类型同步覆盖 Change
  validation、full/projected IPC、标准 reader、malformed 与相关表达式/算子。
- **公共证据**：在单一 `correctness` target 中提供 Definition roundtrip、bind/materialize/turn、错误、
  rollback、重批和 reopen；Flow 组合根拥有纯失败无建库副作用、runtime guard 与资源装配证据。
- **性能与文档**：只有真实 workload 需要独立 benchmark 时才增加稳定 Plan；否则接入现有组合
  workload。同步 Rustdoc、crate README、根能力边界、`TESTING.md` 和路线图。

## 测试与性能

私有 decoder registry 和类型擦除不变量由源码白盒测试拥有；全部公开行为合并在单一
`correctness` target，按 `codec`、`definition`、`postgres`、`postgres_sink`、`protocol` 与 `runtime` 分区。protocol 直接验证上述队列
例子的恢复状态机，runtime 覆盖内建算子与 borrowed delivery 的提交时序。Definition v1 使用版本化黄金字节约束，
Schema 测试覆盖十二个 built-in 的精确传播、decoded golden 再绑定、错误 arity、非法 logical Schema，
以及 Project、Filter、Extend、Select、SchemaAlign、UnionAll 对合法但不兼容 Schema 的结构化拒绝；
`SchemaAlign` 还覆盖 canonical metadata、显式 cast、nullability 放宽/收窄和空 output；空
SchemaAlign/Select 都覆盖没有表达式可代为检查时的 runtime input Schema drift 拒绝，非空路径继续
覆盖相同 guard 与既有 evaluate 语义；表达式测试覆盖 `DataFusion` protobuf
编码失败、roundtrip 与精确版本 golden。runtime trace 统一覆盖完整 turn、commit、rollback、
reopen、固定 output Schema/diff、Project/Extend/Select 零拷贝、UnionAll 多端口原样转发、Filter 的空/全量选择及覆盖
Null/bitmap/fixed/variable/List/Struct 全部既有 layout family 的部分选择、DataFusion
`create_physical_expr` 的 type/nullability、scalar/array evaluate 与 null 传播、
携带混合 diff 的稳定重批和 Store 错误。Expression golden
会经过 `decode → bind → materialize → turn` 检查 protobuf 到执行语义，而不仅是重编码。
Date32、无 timezone 的 Millisecond Timestamp 与 `Decimal128(10, 2)` 另有三个公共纵向测试：结构
direct-copy、`SchemaAlign` 精确 cast/nullability 和 Filter 组合比较都先执行
`encode → decode → re-encode → bind → materialize → turn`，再断言 buffer、diff 与行序；这不扩大为
其他 temporal/decimal 运算承诺。`SQLite` Sink 另外覆盖 Definition/pending/hash golden、全部当前 v1
类型（含 Date32、全部 Timestamp 单位/timezone 与 Decimal128）及嵌套值、列边界与标识符、1024 批边界、
multiplicity/ID 预检、`SQLite` 锁与 ID/完整性冲突，以及 `SQLite` 已提交但 MDBX
transaction 丢失后的初始化、insert、delete 和整批 reopen 重放。`PostgresSink` 的离线公共证据覆盖
tag12 canonical/non-secret Definition、精确 runtime resource、唯一 state Cell、Schema/spec 拒绝，以及
首 turn rollback 或丢弃 completion 不连接目标；真实批量读写、receipt 与 crash recovery witness 由显式脚本所有。完整目录所有权、
测试矩阵和 fixture 规则见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。

`operation_core` 是本 crate 唯一的 release benchmark：Definition encode/decode 当前覆盖除
`SqliteSink`、`PostgresSource`、`PostgresSink` 外的九个内建算子；一行事务型 `turn + apply` body，以及包含 begin、turn、apply 和
durable commit 的完整事务，只直接测 `SequenceSource` 与 `RunningEventCount`。这两个 case 利用
crate 内部完全事务型适配器的结构性空 completion 保留既有 turns-per-transaction 口径，不适用于
带事务外准备或 `AfterCommit` 的 Operation。固定大小 Cell 的长稳
归 Store 所有，因此当前不设置 Operation endurance。
benchmark 使用工作区的 `dogpaddle-bench-protocol` 严格解析配置、采集主机指纹，在 run plan 中声明
稳定 series，并让 raw sample 只引用紧凑 case ID；统一 artifact 派生 `operations/s` 等人类统计，machine
consumer 可由 raw `elapsed_ns / operations` 无损派生 per-operation 耗时，不重复保存。Operation 本地 support 仍拥有 workload 字段、计时/oracle 和
`SampleStore`；`SampleStore` 在所属场景或 durable 样本校验后立即释放，
不积累到 run root 最终 drop。
相邻 `operation_core.plan.json` 由 `cargo xtask bench-plan-check` 在不创建 Store fixture 的情况下
验证 smoke/reference 两档冻结 Plan；当前分别为 22 和 30 个 case。
smoke 默认使用临时目录，正式回归必须选择 `reference` profile 并显式指定固定文件系统目录；
环境变量和 typed JSONL 输出协议以根目录
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md) 为准。

## 验证命令

```bash
cargo test -p dogpaddle-operation
cargo test -p dogpaddle-operation --test correctness
cargo clippy -p dogpaddle-operation --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-operation --no-deps
cargo bench -p dogpaddle-operation --bench operation_core

# PR benchmark protocol smoke
cargo xtask bench-smoke
```
