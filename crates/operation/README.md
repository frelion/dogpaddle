# dogpaddle-operation

这个 crate 定义 DogPaddle 的算子：数据从哪里来、如何变化、最后写到哪里。

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
构建时：Definition ── bind(Schema) ──> Binding ── 接入状态和资源 ──> Runtime Operation
运行时：输入 Change ──> Operation ──> 状态更新 + 可选的输出 Change
```

前一条线让错误尽量在建库前暴露，后一条线保证状态、输出和输入进度可以在一个事务里前进。

## 先认识 Definition 和运行实例

同一个算子有两种形态。

**Definition 是计划。** 例如 Filter Definition 保存谓词，Aggregate Definition 保存分组表达式和
聚合函数。它是纯数据，可以稳定编码进 Flow Definition。Definition 不持有数据库句柄、连接、
密码或正在执行到哪一步。

`OperationDefinition` 是 sealed trait，下游 crate 不能实现。新增内建算子必须修改这个 crate，并在
统一 decoder 表中注册稳定 tag；这样磁盘中的 Definition 不会在运行时落入未知实现。

**Runtime Operation 是正在工作的实例。** 它保存已经按输入 Schema 编译好的表达式、Flow 为它
打开的类型化状态，以及必要的临时客户端。它不再保存 Definition，也不知道自己的稳定资源路径。

中间的 `bind` 和 materialize 只是把这两种形态安全地接起来：

1. `bind` 接收每个输入端口的完整 Arrow Schema，检查列、类型、输入数量和输出 Schema。它是纯
   计算，不访问 Store、网络、时间或随机数。
2. bind 成功后得到一次性的 `OperationBinding`。Flow 此时才创建或打开 Definition 声明的状态。
3. Flow 把这些状态和可选运行资源交给 binding，得到 Runtime Operation。

例如，下面的 Filter 可以在没有 Store 的情况下完成编码、解码和 Schema 检查：

```rust
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use dogpaddle_operation::{
    OperationDefinition, col, decode_definition, encode_definition, lit,
};
use dogpaddle_operation::operation::transform::FilterDefinition;

let input = Arc::new(Schema::new(vec![Field::new(
    "value",
    DataType::UInt64,
    false,
)]));

let definition = FilterDefinition::try_new(col("value").eq(lit(7_u64)))?;
let encoded = encode_definition(&definition);
let reopened = decode_definition(&encoded)?;
let binding = reopened.bind(&[Arc::clone(&input)])?;

assert_eq!(binding.output_schema(), Some(&input));
# Ok::<(), Box<dyn std::error::Error>>(())
```

正常使用时不需要手工 materialize；`FlowFactory::build/open` 会完成这条路径。分阶段的价值在于：
Schema 或运行资源不合法时，不会先创建一半 RocksDB 资源；reopen 也能从持久 Definition 和状态
重新得到同一个执行实例。

## Schema 在这里意味着什么

端口 Schema 是记录列的完整 logical Arrow Schema，不包含 `Change` 编码中的
`$dogpaddle.diff`。字段名、顺序、类型、nullability、嵌套结构和 metadata 都必须精确匹配。

不同算子在 bind 时做不同检查：

- Filter 要求谓词输出 Boolean，并保持输入 Schema。
- Select 从同一个输入计算一组有序输出列。
- UnionAll 要求所有输入 Schema 完全相同。
- InnerEquiJoin 分别绑定左右键，要求每对键具有相同类型。
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

首项可以是 Scan、AtomicTransform 或 TurnTransform；后面只能追加单输入 AtomicTransform。
Station 内没有第二张拓扑图，中间结果也不写日志。最后一个 Operation 的输出才进入 Station 的
持久日志。Exclusive 和 Sink 单独装配，所以外部副作用或必须固定结果的计算不会被错误地融合。

具体 Definition 实例自己声明 kind。Filter、Extend、Select、SchemaAlign 和 Aggregate 会根据表达式
分类：可重放的逐行 immutable 表达式可以成为 Atomic；仍受支持但需要单独边界的实例成为 Exclusive，
其他表达式会在构造或 bind 时被拒绝。InnerEquiJoin 的 key 必须是 immutable；不满足时直接拒绝，
不会退化成 Exclusive。`InnerEquiJoin` 是两输入 TurnTransform，可以分页完成一个输入，再把每一页
交给后面的 Atomic 算子。

## 一次 Station 是怎样运行的

假设 Station 是：

```text
InnerEquiJoin ──> Filter ──> Project
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
重放的事实。返回 `Turn::Idle` 时，Station 连事务都不需要开启。

`PreparedTurn::apply` 在事务内返回一个 `Action`：

| action | 本 turn 的写入和输出 | 当前输入 |
| --- | --- | --- |
| `Idle` | 全部回滚 | 保持原样 |
| `Commit(output)` | 提交 | 保留，下一 turn 再收到完整输入 |
| `Complete(output)` | 提交 | 同事务完成并推进 |

没有输入的 Scan 用 `Commit` 表示一次成功输出。只有消费输入的 Operation 可以返回 `Complete`。
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
inner_join.left_rows: PartitionedMultiset<Vec<u8>, Vec<u8>>
```

Flow 在路径中加入 Station 和 Operation 序号，然后统一创建或打开这些对象。具体算子只得到
`Cell`、`OrderedMap` 等类型化 handle，看不到 Store、RocksDB 句柄或物理 key。materialize 会拒绝
缺失、类型错误和多余的数据实例。

某些外部算子的密码、网络访问参数和临时客户端配置属于 `RuntimeResource`。它们每次 build/open 由
调用方重新注入，不进入 Store；非敏感 source/target identity、固定 Schema 和 SQLite 路径等稳定信息
仍保存在 Definition。资源使用精确 Rust 类型匹配；普通算子必须收到空资源。只有 Station 首项可以
获得运行资源，因此可融合的 Atomic 尾项始终是纯粹的本地计算。

## 三个有状态关系算子的直觉

### Distinct：完整行到账本

`Distinct` 把完整行的确定性字节编码（canonical row）当作 key，在 `OrderedMultiset` 中保存正权重：

- `0 → positive` 输出这行 `+1`；
- `positive → 0` 输出这行 `-1`；
- 其他权重变化不输出。

输入仍按行序应用。非法负前缀或整数溢出会回滚整个 Change。

### Aggregate：每组一个小状态

`Aggregate` 用完整分组键查找 group state。COUNT、SUM、AVG 保存可增量更新的小状态；
MIN/MAX 把候选值放进有序分区，直接读第一个或最后一个值。另一个精确行账本负责拒绝不存在的
撤回。

一条事件引起的准入、group state、极值索引和输出在同一事务更新。已有组结果改变时先输出旧行
`-1`，再输出新行 `+1`。

v1 要求至少一个分组表达式，且分组键不能包含浮点值。COUNT 可以统计受支持表达式的 non-null 值，
包括浮点列；SUM/AVG 只接受整数，MIN/MAX 只接受扁平非浮点值。不支持 global aggregate 和嵌套 MIN/MAX。

### InnerEquiJoin：左右各一本按连接键分类的账本

`InnerEquiJoin` 维护：

```text
left_rows[join key]  = 左侧完整行及各自权重
right_rows[join key] = 右侧完整行及各自权重
```

左侧来一行时，它查右侧同 key 的所有行，输出 diff 为“输入 diff × 对侧权重”的组合；右侧输入
完全对称。复合 key 任一分量为 NULL 时不匹配，但原行仍进入本侧账本，以便以后精确撤回。

热点 key 可能产生非常大的结果，所以 Join 用持久 continuation 分页：Probe 先验证这批输入的全部
匹配都能安全计算，Emit 再分页产生真正输出。一个输入 Change 完成前，Station 固定当前端口；reopen
可以从已提交页继续。分页限制单页工作量，但不限制整个 Join 关系的磁盘大小，也无法消除连接结果
本身的高 fan-out 成本。

## 内建算子索引

“精确输入”表示运行期 Schema 固定，并非动态 Schema。Data 一列列出算子自己拥有的持久状态；
`无` 表示只用当前事务中的输入输出。

| 算子（tag） | kind / 输入数 | 核心行为 | Data |
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
| `SqliteSink` (10) | Sink / 1 | 把精确关系增量写入新的 SQLite STRICT 表 | `relation_sink.state: Cell<Vec<u8>>` |
| `PostgresCdcScan` (11) | Scan / 0 | PostgreSQL 初始快照后持续 CDC | phase、checkpoint、bootstrap spool |
| `PostgresSink` (12) | Sink / 1 | 把精确关系增量幂等写入 PostgreSQL | `relation_sink.state: Cell<Vec<u8>>` |
| `Distinct` (13) | Atomic / 1 | 把任意正权重关系变成集合边界变化 | `distinct.weights: OrderedMultiset` |
| `Aggregate` (14) | Atomic 或 Exclusive / 1 | 增量维护非空分组聚合 | groups、entries、control |
| `MySqlCdcScan` (15) | Scan / 0 | MySQL 初始快照后持续 CDC | phase、checkpoint、bootstrap spool |
| `InnerEquiJoin` (16) | Turn / 2 | 增量维护两输入内等值连接 | left rows、right rows、continuation |

源码按业务角色放在 [`operation/scan/`](src/operation/scan/)、
[`operation/transform/`](src/operation/transform/) 和
[`operation/sink/`](src/operation/sink/)。目录只是帮助阅读；真正的输入数、输出属性和融合资格
始终来自每个 Definition 的 `OperationKind`。

## 表达式边界

Filter、Extend、Select、SchemaAlign、Aggregate 和 InnerEquiJoin 直接接收 DataFusion `Expr`。
crate 根级重导出 `col`、`ident`、`lit`、`cast`、`try_cast` 和 `ScalarValue`。`ident` 按 Arrow
字段名逐字引用；`col` 使用 DataFusion 自己的 identifier 规则。

Definition 构造时立即把表达式编码并解码为 canonical protobuf；bind 时再针对 exact input Schema
生成 `PhysicalExpr`。类型、nullability、cast 和 evaluate 语义由固定版本的 DataFusion 提供。
Operation 层不运行 SQL planner，也不插入隐式 cast，调用者需要显式 `cast`。

当前产品证据覆盖以下纵向切片：

| 状态 | 能力 |
| --- | --- |
| 已承诺 | 精确列引用、Boolean predicate、`UInt64` 同类型 equality、`UInt64 → Utf8` 显式 cast |
| 已承诺的时间/Decimal 切片 | Date32、无 timezone 的 Millisecond Timestamp、`Decimal128(10,2)` 的直接复制、同类型比较，以及 SchemaAlign 中已测试的显式 cast |
| DataFusion 可能支持但 DogPaddle 尚未承诺 | 未经 Definition codec、exact bind、runtime 与 Flow reopen 全链验证的其他表达式和类型组合 |
| 明确拒绝 | 无法 canonical protobuf roundtrip、字段缺失或歧义、Filter 非 Boolean、隐式 coercion、运行时 Schema 漂移 |

只有逐行 immutable scalar 表达式可以融合。Stable、Volatile、placeholder、subquery、
aggregate/window、unnest 和外部引用等实例需要独立持久边界，或在构造/bind 时被拒绝。

Expr protobuf 与精确 pin 的 DataFusion 版本绑定。升级 DataFusion 时必须审查 roundtrip、physical
planning 和执行语义；当前 v1 不读取或迁移旧 payload，状态库直接删除重建。

## 外部端点边界

PostgreSQL CDC Scan 会把初始快照和封口前观察到的 WAL 重叠写入私有
`bootstrap_spool: Queue<Vec<u8>>`，因此 spool 必须容纳两者。MySQL Scan 的 spool 只保存完整快照；
并发变化留在 binlog，binlog 必须覆盖快照、发布和追平阶段。封口后，两者都把 spool 逐条发布到
Station output，再进入持续流阶段。spool 容量是硬限制；超限的 delivery 不提交也不 ACK。

两个 CDC Scan 都固定单表 Schema，运行中不支持在线 DDL、TLS 或跨实例 fencing。捕获阶段 reopen
会清理未完成快照并从头再做，不从半个快照继续。

SQLite 与 PostgreSQL Sink 共用关系写入协议：先在 Store 中持久化至多 1024 个具体 mutation，
提交后在目标数据库的一个事务中按稳定 `$dogpaddle.id` 幂等执行，下一 turn 再结算输入。目标已经
提交而本地尚未结算时会重投当前批次。

SQLite Sink 只接受新的非保留目标表和绝对 UTF-8 文件路径。PostgreSQL Sink 要求调用方每次注入
连接配置，Definition 只保存 discovery 得到的非敏感 target spec；当前不支持 DNS endpoint、TLS、
共享目标或在线 Schema evolution。真实数据库限制和恢复证据见对应 correctness 与 system test。

## 持久化 ABI

`encode_definition` 的外层格式是：

```text
"dogpaddle.operation\0" + format version 1 + u16 operation tag + variant payload
```

tag、payload、表达式 protobuf、每个 Definition 的数据逻辑名和类型、canonical row/key 编码、
GroupState 与 JoinContinuation 等状态 codec、collection 的 key/value codec，以及 Flow 加上的
Station/Operation 序号路径共同构成当前 v1 持久化边界。关系 Sink 使用的 16-byte row hash 还是
远端布局 ABI。decoder 表在
[`src/codec.rs`](src/codec.rs) 按具体算子注册，不存在分类级 decoder 或运行期 registry。

这是开发期 v1。破坏性修改直接更新当前格式、golden 和布局测试；不增加旧版本 alias、fallback、
迁移或兼容分支。已有旧数据库删除后重建。

大部分 Definition 的固定字节位于 [`tests/fixtures/v1/`](tests/fixtures/v1/)；三个外部端点的
canonical JSON 由各自测试直接冻结。完整 Flow Definition 基线位于
[`crates/flow/tests/fixtures/v1/`](../flow/tests/fixtures/v1/)。

## 新增一个算子

建议先读最小的 [`Project`](src/operation/transform/project.rs)，再读带状态的
[`Distinct`](src/operation/transform/distinct.rs)；需要分页时读
[`InnerEquiJoin`](src/operation/transform/inner_join/)，需要外部恢复协议时读
[`queue_scan`](examples/support/queue_scan.rs)。

新增实现应依次完成：

1. 在 `scan/`、`transform/` 或 `sink/` 下建立具体模块。
2. Definition 显式声明唯一 tag、`OperationKind`、canonical payload 和完整 Data。
3. 在 sealed `bind_schemas` 中检查 exact input Schema，产生唯一 output Schema 和一次性 binding。
4. 选择 `AtomicOperation` 或 `TurnOperation`，让所有重放相关写入服从调用方事务。
5. 只通过 materialize 接收具名类型化 Data 和可选 `RuntimeResource`。
6. 在 [`src/codec.rs`](src/codec.rs) 注册具体 decoder。
7. 在 `tests/correctness/<operation>.rs` 覆盖 literal golden、kind、Data、bind、materialize、turn、
   rollback 和适用的 reopen。
8. 只有引入新的通用执行机制时才增加 Flow witness；普通算子语义由自己的 correctness 文件拥有。

## 测试与性能

Operation 的公共测试集中在 [`tests/correctness/`](tests/correctness/)：

- 每个算子文件纵向覆盖 Definition、codec、bind、materialize、运行和 reopen。
- [`definition_codec.rs`](tests/correctness/definition_codec.rs) 验证共享外层格式。
- [`atomic.rs`](tests/correctness/atomic.rs) 验证实例级融合资格和 Atomic 执行。
- [`protocol.rs`](tests/correctness/protocol.rs) 验证 turn、rollback、ACK 与恢复边界。
- [`metamorphic.rs`](tests/correctness/metamorphic.rs) 验证稳定重批后的语义。
- Flow 的资源路径、Station program、build/open/reopen 和 Schema guard 由
  [`crates/flow/tests/correctness/`](../flow/tests/correctness/) 验证。

`Aggregate` 的 MIN/MAX 有 owner benchmark；其他组合性能由真正拥有 workload 的 Flow、Store 或
Change + Store target 负责。

```bash
cargo test -p dogpaddle-operation
cargo clippy -p dogpaddle-operation --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-operation --no-deps
cargo test -p dogpaddle-operation --benches
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench aggregate_extrema
```

全工作区测试所有权和性能口径见 [`TESTING.md`](../../TESTING.md)。
