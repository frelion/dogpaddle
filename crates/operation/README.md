# dogpaddle-operation

这个 crate 定义 `DogPaddle` 的算子：数据从哪里来、如何变化、最后写到哪里。

第一次读代码时，可以先把一次查询看成下面这条链：

```text
Scan                 Transform                         Sink
产生 Change  ──────>  读取并产生 Change  ────────────>  消费 Change
Postgres CDC          Filter / Aggregate / Join         SQLite
```

一个算子只负责自己的计算和状态。它不读取 Flow 的边日志，不选择下一个 Station，也不创建或
提交 Store 事务。Flow 负责把算子连起来，Station 负责执行，Store 负责持久化。

理解这个 crate 最重要的是两条线：

```text
构建/恢复：Definition + exact Schemas + scoped DataScope + RuntimeResource
            ── checked construct ──> Runtime Operation + output Schema
运行时：输入 Change ──> Operation ──> 状态更新 + 可选的输出 Change
```

普通算子开发从 [`Project`](src/operation/transform/project.rs) 开始，再读
[`RunningEventCount`](src/operation/transform/running_event_count.rs) 及其
[correctness 测试](tests/correctness/running_event_count.rs)。日常开发只需要掌握 Definition、
Schema 绑定、自己的类型化状态与 `AtomicOperation::apply`；Station 提交、订阅确认和恢复调度由 Flow 负责。
跨 turn 工作和外部 I/O 再使用 `TurnOperation` 与 AfterCommit，详见下文。

公共入口只提供具体 Definition、必要的参数与错误类型，以及统一运行协议。
具体 `XxxOperation` 和它们的构造函数都是 crate 内部实现；调用方一律通过 Definition 的 checked
`construct` 取得统一 `Operation`。该入口校验 Schema、执行能力和资源类型，再取得 typed handles
并构造运行实例，不执行外部 I/O、事务或状态读取。

定义/表达式的精确维护规则见 [定义契约](docs/definitions.md)；Station 的事务与确认由
[Flow 运行契约](../flow/docs/runtime.md) 唯一规定。本文提供使用与阅读顺序。

## 先认识 Definition 和运行实例

同一个算子有两种形态。

**Definition 是计划。** 例如 Filter Definition 保存谓词，Aggregate Definition 保存分组表达式和
聚合函数。它是纯数据，可以稳定编码进 Flow Definition。Definition 不持有数据库句柄、连接、
密码或正在执行到哪一步。

`OperationDefinition` 是 sealed trait，下游 crate 不能实现。新增内建算子必须修改这个 crate，并在
统一 decoder 表中注册稳定 tag；这样磁盘中的 Definition 不会在运行时落入未知实现。其纯
`output_schema(inputs)` 路径复用具体算子的同一 Schema 编译规则，供 SQL 等上层在接触 Store 前取得
权威输出 Schema；它不声明状态或构造 runtime，Sink 返回 `None`。

**Runtime Operation 是正在工作的实例。** 它保存已经按输入 Schema 编译好的表达式、Flow 为它
打开的类型化状态，以及必要的临时客户端。它不再保存 Definition，也不知道自己的稳定资源路径。

中间只有一个 checked construction path：

1. 在接触 Store 前，对全部 Definition 调用 `validate_resource(&resource)`，预检运行资源是否存在且为
   精确 Rust 类型。Flow 会先对全图完成这一步，因此错误不会留下目录或部分 catalog。
2. `construct` 接收每个输入端口的完整 Arrow Schema、已限定资源名范围的短期 `DataScope` 和拥有型
   `RuntimeResource`，统一检查输入数量、DogPaddle Schema 与资源 presence/type。
3. sealed 具体 Definition 只在本地编译表达式/算法布局，并用 `DataScope::data` 声明或查找固定逻辑名
   的 typed collections；同一代码同时服务新建与恢复。
4. 统一入口复核 output Schema 和 `Atomic`/`Turn` 执行能力，规范化 Exclusive Atomic 为 Turn，返回
   `ConstructedOperation`。调用方用 `into_parts()` 一次性取出最终 `Operation` 和 output Schema。

下面的无状态 Filter 展示完整的新建和恢复生命周期。实际 Flow 会先对全图做 Schema 传播和
`validate_resource` preflight，再创建 `StoreSetup`；这里的 `commit(path, init)` 空初始化闭包只因为
Filter 没有需要写入初值的状态：

```rust
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use dogpaddle_operation::operation::transform::FilterDefinition;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col, decode_definition, encode_definition, lit,
};
use dogpaddle_store::{Store, StoreSetup};

let input = Arc::new(Schema::new(vec![Field::new(
    "value",
    DataType::UInt64,
    false,
)]));
let definition = FilterDefinition::try_new(col("value").eq(lit(7_u64)))?;
let encoded = encode_definition(&definition);
let definition = decode_definition(&encoded)?;
let fixture = tempfile::tempdir()?;
let path = fixture.path().join("state");

// Preflight every runtime resource before creating or opening Store state.
let resource = RuntimeResource::none();
definition.validate_resource(&resource)?;

// New state: the scope declares the concrete Definition's typed data.
let mut setup = StoreSetup::new();
let constructed = {
    let mut data = setup.data_scope();
    definition.construct(&[Arc::clone(&input)], &mut data.scoped("operation"), resource)?
};
assert_eq!(constructed.output_schema(), Some(&input));
let (_operation, output_schema) = constructed.into_parts();
assert_eq!(output_schema.as_ref(), Some(&input));
let transactions = setup.commit(&path, |_init| Ok(()))?;
drop(transactions);

// Existing state: the same constructor looks up exactly the same typed data.
let definition = decode_definition(&encoded)?;
let resource = RuntimeResource::none();
definition.validate_resource(&resource)?;
let store = Store::open(&path)?;
let constructed = {
    let mut data = store.data_scope();
    definition.construct(&[Arc::clone(&input)], &mut data.scoped("operation"), resource)?
};
let (_operation, output_schema) = constructed.into_parts();
assert_eq!(output_schema.as_ref(), Some(&input));
let _transactions = store.into_transactions();
# Ok::<(), Box<dyn std::error::Error>>(())
```

`StoreSetup::new()` 只建立内存 draft，不做文件系统 I/O；`setup.data_scope()` 只声明新名称。
`Store::data_scope()` 则只查找现有名称，并拒绝缺失资源或 collection kind 不匹配。新建路径最终必须
消费 setup 调用 `commit(path, init)`，在一笔事务中发布 Store marker、完整 catalog 和各算子初值；
恢复路径不再次初始化，而是在 construction 完成后消费 `Store` 获得运行期事务能力。

正常使用时不需要手工执行这套装配；`FlowFactory::build/open` 会完成它。旧的绑定阶段和双路 setup
入口没有兼容 API，也不会为旧调用方式保留 alias、fallback 或迁移路径。

## Schema 在这里意味着什么

端口 Schema 是记录列的完整 logical Arrow Schema，不包含 `Change` 编码中的
`$dogpaddle.diff`。字段名、顺序、类型、nullability、嵌套结构和 metadata 都必须精确匹配。

不同算子在 checked `construct` 时做不同检查：

- Filter 要求谓词输出 Boolean，并保持输入 Schema。
- Select 从同一个输入计算一组有序输出列。
- `UnionAll` 要求所有输入 Schema 完全相同。
- `EquiJoin` 分别绑定左右键，要求每对键具有相同类型；可选 residual 在精确的
  `left.* + right.*` candidate Schema 上绑定，具体 kind 决定输出列和 outer nullability。
- `AsOfJoin` 要求左右 equality/order 表达式成对同类型，order 至少一对；nearest 和
  tolerance 额外要求唯一可计算距离的 order，tie-break 只针对右侧绑定，可选
  residual 与 `EquiJoin` 一样使用 `left.* + right.*` qualifier。
- Sink 检查目标系统能否无损表示全部输入列，并且没有输出 Schema。

运行时收到的 `Change` 仍会与绑定时 Schema 比较。这样，磁盘 Definition、编译好的表达式和真实
输入不会在 Schema 漂移后悄悄错位。

## 三类业务角色，五种执行能力

Scan、Transform、Sink 是容易理解的业务角色；`OperationKind` 进一步告诉 Flow 输入数量，以及
这个实例能否和相邻算子放进同一个 Station。

| kind | 输入 / 输出 | 如何执行 | Station 装配 |
| --- | --- | --- | --- |
| `Scan` | 0 / 有输出 | 主动拉取或生成数据 | 可作为首项，后接单输入 Atomic |
| `AtomicTransform(N)` | N / 有输出 | 一笔事务完整消费一个 Change | 可作为首项；单输入时也可作为尾项 |
| `TurnTransform(N)` | N / 有输出 | 一个 Change 可以分成多个有界 turn | 只能作为首项，可后接单输入 Atomic |
| `ExclusiveTransform(N)` | N / 有输出 | 需要自己的持久输出边界 | 必须独占 Station |
| `Sink(N)` | N / 无输出 | 消费数据并结束这条路径 | 必须独占 Station |

因此一个 Station 的程序始终是一条简单的线：

```text
首 Operation  ──>  Atomic  ──>  Atomic  ──> ...
```

首项可以是 Scan、AtomicTransform 或 TurnTransform；后面只能追加单输入 `AtomicTransform`。
Station 内没有第二张拓扑图，中间结果也不写日志。最后一个 Operation 的输出才进入 Station 的
持久日志。Exclusive 和 Sink 单独装配，所以外部副作用或必须固定结果的计算不会被错误地融合。

具体 Definition 实例自己声明 kind。Filter、Extend、Select、SchemaAlign 和 Aggregate 会根据表达式
分类：可重放的逐行 immutable 表达式可以成为 Atomic；仍受支持但需要单独边界的实例成为 Exclusive，
其他表达式会在 Definition 构造或 checked `construct` 时被拒绝。EquiJoin 的 key 和 residual 必须是 immutable；不满足时直接拒绝，
不会退化成 Exclusive。`EquiJoin` 是两输入 TurnTransform，可以分页完成一个输入，再把每一页
交给后面的 Atomic 算子。

## 一次 Station 是怎样运行的

假设 Station 是：

```text
EquiJoin ──> Filter ──> Project
```

运行时大致发生这些事：

1. Station 从一个输入端口取得一整个 `Change`。多输入 Station 会在这批数据完成前固定这个端口，
   因而 Join 处理左侧时右侧关系不会在中途变化。
2. 首 Operation 在没有写事务时准备一次有界工作。
3. Station 开启一笔 Store 写事务，执行首项本 turn 的状态变化。
4. 如果首项产生输出，Filter 和 Project 在同一事务中连续处理内存中的 `Change`。
5. Station 尝试把最终输出写入自己的持久日志；若输入已经完成，也在这笔事务中推进订阅位置。
6. 事务提交后，才执行外部 ACK 等不可回滚动作。

Atomic 尾项、Schema 检查、输出背压或其他提交前错误会确定回滚整个 turn，下一次可以从未改变的
持久状态重算。`Transaction::commit` 返回错误时，落盘结果可能不确定；Station 会停止继续运行，
调用方必须 reopen，再由持久状态决定从哪里恢复。

这就是算子融合带来的直接收益：Filter 和 Project 之间不再进行 Arrow IPC 编码、RocksDB 写入、
订阅读取和解码，同时仍共享一笔事务。持久边界只保留在 Station 之间。

## 两种运行接口

大多数 Transform 使用更简单的 `AtomicOperation::apply`。它接收完整输入和当前事务访问权，
一次返回 `Option<Change>`：

- `Some(change)`：产生一批输出。
- `None`：这批输入没有输出，例如 Filter 删除了全部行。
- 返回错误：Station 回滚事务；同一输入可以安全重试。

Atomic Operation 可以更新自己声明的 Store 状态，但不能保存跨 turn 进度、执行外部副作用或安排
提交后的动作。

Scan、Sink、Join 以及 Exclusive Transform 使用完整的 `TurnOperation::turn` 协议：

```text
没有写事务                         Store 写事务                      提交以后
turn(input) ──> PreparedTurn ──> prepared.apply(access) ──> commit ──> AfterCommit
        └────> Turn::Idle              └────> Action
```

`turn` 适合轮询外部来源、恢复临时客户端或准备一个有界页面。它不能提前 ACK，也不能推进任何影响
重放的事实。`None` 表示本轮没有 Claim：Scan 始终收到 `None`，输入 Operation 在上游暂时没有数据时
也会收到 `None`，从而可以继续处理自己的持久内部工作；没有这种工作时返回 `Turn::Idle`，Station
连事务都不需要开启。

`PreparedTurn::apply` 在事务内返回一个 `Action`：

| action | 本 turn 的写入和输出 | 当前输入 |
| --- | --- | --- |
| `Idle` | 全部回滚 | 保持原样 |
| `Commit(output)` | 提交 | 若有 Claim 则保留；无 Claim 时只提交内部进度 |
| `Complete(output)` | 提交 | 同事务完成并推进，要求本轮确实有 Claim |

没有输入的 Scan 和执行内部工作的输入 Operation 都用 `Commit` 表示成功。只有收到 Claim 的
Operation 可以返回 `Complete`。
Join 用 `Commit` 保存分页进度，最后一页才返回 `Complete`。

`AfterCommit` 只在事务真正提交后执行。CDC 的外部 delivery ACK、关系 Sink 的目标数据库写入都在
这个阶段。rollback、背压或 commit 失败只会丢弃它。AfterCommit 失败时，本地状态已经提交，当前
Station 会停止，必须 reopen 后从持久状态恢复。

可运行的最小协议例子在
[`examples/support/queue_scan.rs`](examples/support/queue_scan.rs)。它演示“事务外 poll、事务内同时
保存 checkpoint 和 output、提交后 ACK”，运行方式是：

```bash
cargo run -p dogpaddle-operation --example queue_scan
```

## 状态和外部资源为什么分开

Definition 通过稳定逻辑名称声明自己需要的持久数据，例如：

```text
sequence_scan.position: Cell<u64>
distinct.weights: OrderedMultiset<Vec<u8>>
equi_join.left_rows: PartitionedMultiset<Vec<u8>, Vec<u8>>
asof_join.left_rows: OrderedMap<Vec<u8>, RowWeight>
```

Flow 用 `station/{station}/operation/{operation}` 前缀限定 build/open 对应的 `DataScope`，
再将这个子 scope 交给 Operation。具体 Definition 不接收全局前缀，只用固定逻辑名和 codec 声明或查找
`Cell`、`OrderedMap` 等 handle。旧的 Data declaration、`DataInstances` 和 erased materializer 已不在
这条路径中，Flow 也不会枚举具体算子或解释其状态布局。

某些外部算子的密码、网络访问参数和临时客户端配置通过 `RuntimeResource` 传入。它只是拥有型
`Any` 擦除容器：checked construction path 先检查精确 Rust 类型，具体 Definition 再取回该值；它不承载持久状态、codec
或资源字典。资源每次 build/open 由调用方重新注入，不进入 Store；非敏感 source/target identity、固定
Schema 和 `SQLite` 路径等稳定信息仍保存在 Definition。普通算子必须收到空资源，且只有 Station 首项
可以获得运行资源。

## 四个有状态关系算子的直觉

| 算子 | 状态如何组织 | 一条事件如何改变结果 |
| --- | --- | --- |
| Distinct | 完整行 → 正权重 | 只在零与正权重之间跨越时输出 |
| Aggregate | 分组 → Fold 状态与极值缓存 | 旧结果撤回，再发布新结果 |
| `EquiJoin` | 连接键 → 左右完整行与权重 | 查另一侧同键记录，分页发布匹配和存在性修正 |
| `AsOfJoin` | 分区、排序键 → 左右完整行与权重 | 为每个左行选择至多一个右行；右侧变化修正历史选择 |

Aggregate 按「分组 + 调用参数」校验被跟踪权重，不保存完整输入行；其他三个算子按完整行身份记账。
Join 的 Probe 先验证整个 Claim，Emit 再分页发布，避免输入后段失败时已经发布了前段结果。
复杂算法的状态、输入类型、NULL、分页和恢复规则以 [关系算子契约](docs/relations.md) 为准。

## 内建算子索引

“精确输入”表示运行期 Schema 固定，并非动态 Schema。“持久状态”一列列出由具体算子代码拥有
逻辑名和 codec 的 typed collections；`无` 表示只用当前事务中的输入输出。

| 算子（tag） | kind / 输入数 | 核心行为 | 持久状态 |
| --- | --- | --- | --- |
| `SequenceScan` (1) | Scan / 0 | 从起始值连续产生 `UInt64`，diff 固定 `+1` | `sequence_scan.position: Cell<u64>` |
| `RunningEventCount` (2) | Atomic / 1 | 每观察一行计数加一；忽略输入 diff 值 | `running_event_count.count: Cell<u64>` |
| `Discard` (3) | Sink / 1 | 完成输入，不产生输出 | 无 |
| `Project` (4) | Atomic / 1 | 按严格递增索引保留顶层列 | 无 |
| `Filter` (5) | Atomic 或 Exclusive / 1 | 只保留谓词为 non-null true 的行 | 无 |
| `Extend` (6) | Atomic 或 Exclusive / 1 | 保留输入并追加一个表达式列 | 无 |
| `Select` (7) | Atomic 或 Exclusive / 1 | 从同一输入计算完整有序输出列 | 无 |
| `UnionAll` (8) | Atomic / N | 原样转发 Schema 完全相同的各端口 Change | 无 |
| `SchemaAlign` (9) | Atomic 或 Exclusive / 1 | 显式产生目标字段与 metadata | 无 |
| `SqliteSink` (10) | Sink / 1 | 把精确关系增量写入新的 `SQLite` STRICT 表 | `sink.control: Cell<Vec<u8>>`、`sink.buffer: OrderedMap<u64, Vec<u8>>` |
| `PostgresCdcScan` (11) | Scan / 0 | `PostgreSQL` 初始快照后持续 CDC | phase、checkpoint、bootstrap spool |
| `PostgresSink` (12) | Sink / 1 | 把精确关系增量幂等写入 `PostgreSQL` | `sink.control: Cell<Vec<u8>>`、`sink.buffer: OrderedMap<u64, Vec<u8>>` |
| `Distinct` (13) | Atomic / 1 | 把任意正权重关系变成集合边界变化 | `distinct.weights: OrderedMultiset` |
| `Aggregate` (14) | Atomic 或 Exclusive / 1 | 增量维护非空分组聚合 | groups、entries、control |
| `MySqlCdcScan` (15) | Scan / 0 | `MySQL` 初始快照后持续 CDC | phase、checkpoint、bootstrap spool |
| `EquiJoin` (16) | Turn / 2 | 增量维护带可选 residual 的 Inner、Left Semi/Anti、Left/Full Outer | left rows、right rows、continuation；非 Inner 使用 key counts 或逐行 match counts |
| `AsOfJoin` (17) | Turn / 2 | 按 equality partition 增量维护 backward/forward/nearest 的单候选 Inner、Left Outer/Semi/Anti | ordered left rows、ordered right rows、continuation |
| `DorisSink` (18) | Sink / 1 | 通过 Unique Key merge-on-write 表维护 Apache Doris 精确关系 | `sink.control: Cell<Vec<u8>>`、`sink.buffer: OrderedMap<u64, Vec<u8>>` |
| `ClickHouseSink` (19) | Sink / 1 | 通过 `ReplacingMergeTree` 与 `FINAL` view 维护 `ClickHouse` 精确关系 | `sink.control: Cell<Vec<u8>>`、`sink.buffer: OrderedMap<u64, Vec<u8>>` |

源码按业务角色放在 [`operation/scan/`](src/operation/scan/)、
[`operation/transform/`](src/operation/transform/) 和
[`operation/sink/`](src/operation/sink/)。目录只是帮助阅读；真正的输入数、输出属性和融合资格
始终来自每个 Definition 的 `OperationKind`。

## 表达式边界

`Select` 与 `SchemaAlign` 共用私有批量投影执行实现：绑定时共享一个 `DFSchema`，每个 Change 只做一次整组 exact Schema 校验，空投影也检查。各 Definition 继续独立决定字段、metadata、nullability 与稳定编码，运行错误保留具体字段序号。

Filter、Extend、Select、SchemaAlign、Aggregate、`EquiJoin` 和 `AsOfJoin` 直接接收 `DataFusion` `Expr`。
crate 根级重导出 `col`、`ident`、`lit`、`cast`、`try_cast` 和 `ScalarValue`。`ident` 按 Arrow
字段名逐字引用；`col` 使用 `DataFusion` 自己的 identifier 规则。

Definition 构造时立即把表达式编码并解码为 canonical protobuf；checked `construct` 再针对 exact input
Schema 生成 `PhysicalExpr`。类型、nullability、cast 和 evaluate 语义由固定版本的 `DataFusion` 提供。
Operation 层不运行 SQL planner，也不插入隐式 cast，调用者需要显式 `cast`。
`EquiJoin` residual 的两个输入固定使用 `left` 与 `right` qualifier；它绑定原始输入字段的类型、
nullability 和 metadata，而不是 Outer 已放宽或 Semi/Anti 已裁剪的输出 Schema。
`AsOfJoin` residual 使用同样的 qualifier；equality/order 分别针对自己的输入 Schema 绑定，
tie-break 只针对 right Schema 绑定。

当前产品证据覆盖以下纵向切片：

| 状态 | 能力 |
| --- | --- |
| 已承诺 | 精确列引用、Boolean predicate、`UInt64` 同类型 equality、`UInt64 → Utf8` 显式 cast |
| 已承诺的时间/Decimal 切片 | Date32、无 timezone 的 Millisecond Timestamp、`Decimal128(10,2)` 的直接复制、同类型比较，以及 `SchemaAlign` 中已测试的显式 cast |
| `DataFusion` 可能支持但 `DogPaddle` 尚未承诺 | 未经 Definition codec、checked construction、runtime 与 Flow reopen 全链验证的其他表达式和类型组合 |
| 明确拒绝 | 无法 canonical protobuf roundtrip、字段缺失或歧义、Filter 非 Boolean、隐式 coercion、运行时 Schema 漂移 |

只有逐行 immutable scalar 表达式可以融合。Stable、Volatile、placeholder、subquery、
aggregate/window、unnest 和外部引用等实例需要独立持久边界，或在 Definition 构造/checked `construct` 时被拒绝。

Expr protobuf 与精确 pin 的 `DataFusion` 版本绑定。升级 `DataFusion` 时必须审查 roundtrip、physical
planning 和执行语义；当前 v1 不读取或迁移旧 payload，状态库直接删除重建。

## 外部端点边界

CDC 先将初始快照放入私有 spool，封口后逐条发布，再进入持续捕获。PostgreSQL spool 还需要容纳
封口前的 WAL 重叠；MySQL 把并发变化留在 binlog。两者的阶段、事务、容量、重置和部署前提见
[CDC Scan 契约](docs/cdc.md)。它们保留具体 connector 实现，不建立通用 CDC 框架。

`PostgresCdcScanOptions` 为运行资源提供类型化调优，可调整 discovery 与 connector 的连接/查询
timeout、进入 polling 后的有限重试次数与最大等待、持续流 heartbeat 和初始 snapshot fetch size。默认显式固定
5 秒连接与查询 timeout、无限重试、300 毫秒初始/10 秒最大重试等待、1 秒持续流 heartbeat 和
10240 行 snapshot fetch。捕获阶段 heartbeat 始终为 1 毫秒。PostgreSQL JDBC 的连接 timeout 与
Debezium JDBC 的 query timeout 都以秒生效，因此 connector 值会向上取整；native discovery 仍使用
精确毫秒值。这些选项不进入 Definition 或持久状态，reopen 时需要重新提供。

`MySqlCdcScanOptions` 为运行资源提供类型化调优，并由 `MySqlCdcScanConfig` 翻译成固定版本的
Debezium properties。它可以同时调整 discovery 与 connector 的连接/查询 timeout、进入 polling 后的有限重试次数、
最大重试等待、持续流 heartbeat 和可选 snapshot fetch size。默认显式固定 Debezium 的 30 秒连接、
10 分钟查询、无限重试、300 毫秒初始/10 秒最大重试等待与 1 秒持续流 heartbeat；discovery 仍固定
5 秒。初始快照 heartbeat 始终为 1 毫秒。Debezium JDBC 的 query timeout 向上取整到整秒，discovery
的 socket timeout 保留精确毫秒值。MySQL 的 snapshot fetch 默认会完全省略 property，以保留
Connector/J 的特殊流式结果行为；显式 fetch size 也只注入初始 snapshot connector。这些选项不进入
Definition 或持久状态，reopen 时需要重新提供。这组重试参数不控制初始 task 启动，PostgreSQL 中也不控制 replication slot 创建。两类 connector 进入 polling 的总等待仍由
`dogpaddle-debezium` 固定为 60 秒，不由单次连接或查询 timeout 推导。

关系 Sink 先把完整输入提交到本地 buffer，再将固定 ID 的 mutation plan 持久化为 Prepared；目标提交后，
下一 Store turn 才结算本地进度。SQLite、PostgreSQL、Doris 和 `ClickHouse` 共用这套私有内核，具体目标
只实现布局、查询与幂等写入。容量口径、恢复校验、行身份和各目标限制见 [关系 Sink 契约](docs/sinks.md)。

## 持久化 ABI

`encode_definition` 的外层格式是：

```text
"dogpaddle.operation\0" + format version 1 + u16 operation tag + variant payload
```

tag、payload、表达式 protobuf、每个 Definition 的数据逻辑名和类型、canonical row/key 编码、
`GroupState`、`JoinContinuation`、`AsOfContinuation` 与 buffered Sink control codec、buffer 内完整
Change IPC、collection 的 key/value codec，以及 Flow 加上的 Station/Operation 序号路径共同构成
当前 v1 持久化边界。关系 Sink 使用的 16-byte row hash、固定 technical ID 和 Prepared mutation
codec 还是目标布局/恢复 ABI。decoder 表在
[`src/codec.rs`](src/codec.rs) 按具体算子注册，不存在分类级 decoder 或运行期 registry。

大部分 Definition 的固定字节位于 [`tests/fixtures/v1/`](tests/fixtures/v1/)；三个外部端点的
canonical JSON 由各自测试直接冻结。完整 Flow Definition 基线位于
[`crates/flow/tests/fixtures/v1/`](../flow/tests/fixtures/v1/)。

## 新增一个算子

建议先读最小的 [`Project`](src/operation/transform/project.rs)，再读带状态的
[`RunningEventCount`](src/operation/transform/running_event_count.rs)；需要分页时读
[`EquiJoin`](src/operation/transform/equi_join/) 和
[`AsOfJoin`](src/operation/transform/asof_join/)，需要外部恢复协议时读
[`queue_scan`](examples/support/queue_scan.rs)。

新增实现应依次完成：

1. 在 `scan/`、`transform/` 或 `sink/` 下建立具体模块。
2. Definition 显式声明唯一 tag、`OperationKind` 和 canonical payload。
3. 在 sealed `construct` 中编译 exact input Schema 语义，通过 `DataScope` 获取 typed handles，并产生最终 Operation 与唯一 output Schema。
4. 由算子代码固定逻辑资源名、collection 类型和 codec；新建与恢复使用同一 constructor。
5. 选择 `AtomicOperation` 或 `TurnOperation`，让所有重放相关写入服从调用方事务；需要临时配置时只从
   `RuntimeResource` 取回精确类型。
6. 在 [`src/codec.rs`](src/codec.rs) 注册具体 decoder。
7. 在 `tests/correctness/<operation>.rs` 覆盖 literal golden、kind、checked construct、typed data、turn、
   rollback 和适用的 reopen。
8. 只有引入新的通用执行机制时才增加 Flow witness；普通算子语义由自己的 correctness 文件拥有。

## 测试与性能

Operation 的公共测试集中在 [`tests/correctness/`](tests/correctness/)：

- 每个算子文件纵向覆盖 Definition、codec、checked construct、typed data、运行和 reopen。
- [`definition_codec.rs`](tests/correctness/definition_codec.rs) 验证共享外层格式。
- [`atomic.rs`](tests/correctness/atomic.rs) 验证实例级融合资格和 Atomic 执行。
- [`protocol.rs`](tests/correctness/protocol.rs) 验证 turn、rollback、ACK 与恢复边界。
- [`metamorphic.rs`](tests/correctness/metamorphic.rs) 验证稳定重批后的语义。
- Flow 的资源路径、Station program、build/open/reopen 和 Schema guard 由
  [`crates/flow/tests/correctness/`](../flow/tests/correctness/) 验证。

`Aggregate` 的 MIN/MAX、`EquiJoin` 的 match/presence transition、`AsOfJoin` 的 ordered lookup/
historical rematch 和 durable buffered `SQLite` Sink 各有 owner benchmark；其他组合性能由真正拥有
workload 的 Flow、Store 或 Change + Store target 负责。

`asof_join` Criterion 把两个使关系回到原状的完整 Claim 作为计时单位，覆盖多小 partition、
单大 partition、尾部小修正、历史全量修正、nearest+tolerance 和 residual 远候选回退。
`asof_join_resources` 为每个 case 启动新子进程：fixture、seed 与 input Arrow 在 profiler 前建立，
`dhat` 只覆盖一个完整 driving Claim；output Arrow bytes 和两个 ordered rows map 的 decoded
key+weight 逻辑大小分开报告。NULL-order left/right history 都使用 N/2N 对照，并自动要求 driving
Claim 的 turn、output 与 Rust heap 完全不随无关历史增长。Rust allocator、Arrow、Store logical bytes
都不是 RSS；runner 对 RSS 明确记为 unavailable。

```bash
cargo test -p dogpaddle-operation
cargo clippy -p dogpaddle-operation --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-operation --no-deps
cargo test -p dogpaddle-operation --benches
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench projection
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench aggregate_extrema
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench equi_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench buffered_sink
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench asof_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench asof_join_resources
```

全工作区测试所有权和性能口径见 [`TESTING.md`](../../TESTING.md)。
