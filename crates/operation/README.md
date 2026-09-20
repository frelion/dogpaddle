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
构建/恢复：Definition + exact Schemas + DataScope + prefix + RuntimeResource
            ── checked construct ──> Runtime Operation + output Schema
运行时：输入 Change ──> Operation ──> 状态更新 + 可选的输出 Change
```

这不是给旧装配层换名字：旧的分阶段绑定对象、Data declaration、类型擦除的 data bag、materializer
和双路 setup 入口都已删除。唯一的 checked construction path 先统一校验输入数量、输入/输出
Schema、执行能力和运行资源 presence/type，再由 sealed 具体 Definition 本地编译语义、通过
`DataScope` 取得类型化 handle 并直接构造最终 Operation。构造不执行外部 I/O、事务或状态读取。

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
2. `construct` 接收每个输入端口的完整 Arrow Schema、短期 `DataScope`、稳定前缀和拥有型
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
    definition.construct(&[Arc::clone(&input)], &mut data, "operation", resource)?
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
    definition.construct(&[Arc::clone(&input)], &mut data, "operation", resource)?
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

Flow 只生成 `station/{station}/operation/{operation}` 前缀并提供 build/open 对应的 `DataScope`。具体 Definition 的同一个 constructor 用固定逻辑名和 codec 声明或查找
`Cell`、`OrderedMap` 等 handle。旧的 Data declaration、`DataInstances` 和 erased materializer 已不在
这条路径中，Flow 也不会枚举具体算子或解释其状态布局。

某些外部算子的密码、网络访问参数和临时客户端配置通过 `RuntimeResource` 传入。它只是拥有型
`Any` 擦除容器：checked construction path 先检查精确 Rust 类型，具体 Definition 再取回该值；它不承载持久状态、codec
或资源字典。资源每次 build/open 由调用方重新注入，不进入 Store；非敏感 source/target identity、固定
Schema 和 `SQLite` 路径等稳定信息仍保存在 Definition。普通算子必须收到空资源，且只有 Station 首项
可以获得运行资源。

## 四个有状态关系算子的直觉

### Distinct：完整行到账本

`Distinct` 把完整行的确定性字节编码（canonical row）当作 key，在 `OrderedMultiset` 中保存正权重：

- `0 → positive` 输出这行 `+1`；
- `positive → 0` 输出这行 `-1`；
- 其他权重变化不输出。

输入仍按行序应用。非法负前缀或整数溢出会回滚整个 Change。

### Aggregate：每组一个小状态

`Aggregate` 用完整分组键查找 group state。COUNT、SUM、AVG 保存可增量更新的小状态；MIN/MAX 把
候选值放进有序分区，并在 group state 里缓存每个「排序表达式 + 方向」的当前极值：插入时直接比较
更新，只有被撤回的正是当前极值时，才回分区重取第一个或最后一个值。缓存保存保序编码的 key，因而
会和对应分区短暂重复一份字节；这是按 bound extrema slot 数量限制的明确取舍，用可变 key 字节换取
正常输出路径不必逐行读取 `RocksDB` 分区。

它不保留完整输入行。算子只检查三类被跟踪的权重非负：分组行数、每个 COUNT(expr)/SUM/AVG 的非空
参数计数、每个极值参数的份数；此外从未出现过的分组遇到负 diff 直接失败。也就是说它按「分组 +
调用参数」校验，而不是按记录校验；需要记录级身份的算子（`Distinct`、`EquiJoin`、`AsOfJoin` 和
Sink）仍然按完整行记账。由于 NULL 参数不进入极值分区，宽松的参数级契约允许一个分组归零时仍有
不可达的旧极值键；归零路径会在同一事务中按 layout 清空这些残留。正常合法重放通常每个分区已经为空，
此时只做每个 layout 一次边界检查。

一条事件引起的 group state、极值索引、极值缓存和输出在同一事务更新。已有组结果改变时先输出旧行
`-1`，再输出新行 `+1`。

v1 要求至少一个分组表达式，且分组键不能包含浮点值。COUNT 可以统计受支持表达式的 non-null 值，
包括浮点列；SUM/AVG 只接受整数，MIN/MAX 只接受扁平非浮点值。不支持 global aggregate 和嵌套 MIN/MAX。

### EquiJoin：一套状态覆盖五种关系语义

`EquiJoin` 维护：

```text
left_rows[join key]  = 左侧完整行及各自权重
right_rows[join key] = 右侧完整行及各自权重
```

左侧来一行时，它查右侧同 key 的所有行，输出 diff 为“输入 diff × 对侧权重”的组合；右侧输入
完全对称。复合 key 任一分量为 NULL 时不匹配，但原行仍进入本侧账本，以便以后精确撤回。

Definition 用 `EquiJoinKind` 选择 `Inner`、`LeftSemi`、`LeftAnti`、`LeftOuter` 或 `FullOuter`。
Semi/Anti 只输出左侧字段；Outer 为可能补 NULL 的一侧放宽字段 nullability。可选 residual 对同 key 的
每个候选记录对求值，只有 non-null `true` 才匹配。没有 residual 时，非 Inner 继续使用紧凑的
`key_counts`，只记录每个 key 在左右各有多少种不同完整行；有 residual 时，非 Inner 改为维护逐完整行
的 `match_counts`，记录它有多少种满足整个 `ON` 条件的对侧记录。重复行权重仍只保存在左右 row state，
不会膨胀 presence 计数。Semi/Anti 中同一 exact row 仅改变 multiplicity 时不重扫对侧 bucket：right
变化不改变 support，left 变化直接读取该行已有的 actual/shadow match count。需要变化的 driving-row
count 按 qualifying page 合并，对侧每种 distinct row 的 count 仍分别更新。

热点 key 可能产生非常大的结果，所以 Join 用持久 continuation 分页：Probe 先验证这批输入的全部
匹配都能安全计算，Emit 再分页产生真正输出。Residual presence Join 的 Probe 只写隐藏的模拟计数，
随后由 `ClearShadow` 有界清理，再由 Emit 原子更新真实计数；因此后段 predicate、解码或 diff 错误不会
在输出前污染关系状态。一个输入 Change 完成前，Station 固定当前端口；reopen 可以从已提交页继续。

Probe/Emit 重复扫描和求值是有意的 whole-Claim failure-before-output 边界：整个 Claim 中后面的
predicate、存储行损坏或 output-diff overflow 不会在前面的结果已经发布后才暴露。当前不持久化
qualifying-pair spool 或 bitset，也不用跨阶段内存 cache 代替可重放的第二遍。

`PreparedClaim` 只为整批保留 canonical row、join key、diff 和 admission effect，不再保留每行的全量
`ScalarValue`；每个 turn 处理当前 row 时，仅在 predicate 或真实输出需要字段值时，才从 Station 固定的
`RecordBatch` 惰性物化一次短期 values。空 bucket、无输出存在性路径和稳定 Semi/Anti 右侧更新不会复制宽行。
Residual 候选的
常规单批上限是 256 行、1 MiB Store logical bytes 和 16,384 个 candidate scalar slots；实际行数还受
candidate 字段数及当前 turn 剩余预算约束。每批会完整解码候选并构造 Arrow candidate batch，但
LeftSemi/LeftAnti 的左侧 driving row 只保留 qualifying count，其他路径也只把 predicate 通过的候选
values 带入当前输出阶段。

`TURN_ITEMS` 和 `TURN_BYTES` 以 256 项和 4 MiB 限制常规单 turn 的逻辑扫描、ScalarValue slot、
输出和事务工作量。分区扫描按每个候选重复计算 partition frame、完整 join key、row key 与 multiplicity，
driving row 的持久访问也至少逐处理页计入；宽计算 key 或 LeftSemi/LeftAnti 的右侧宽行不会逃逸预算。
这些值不是进程 RSS 硬上限：Station 仍已持有完整 Change，Arrow/DataFusion 可以产生
额外中间分配，且空 turn 遇到单个超过批字节或 scalar-slot 界限的 Store row 时会单独处理它，以避免永久
停滞。因此峰值至少是 `O(Claim + candidate page)`，还有“单个 oversized row”的活性例外。逐行
`match_counts` 以完整 canonical row 为 key，持久状态与 tracked rows 的总宽度成正比；分页也不限制
整个 Join 关系的磁盘大小，无法消除连接结果本身的高 fan-out 成本。

### AsOfJoin：动态关系中的单候选最近匹配

`AsOfJoin` 固定把 port `0` 作为 probe/left，port `1` 作为 candidate/right。Equality
表达式先划分 partition，然后每个正权重 left exact row 在当前 right 关系中最多选一个候选：

- `Backward` 选最大的前驱，`Forward` 选最小的后继，两者都显式声明是否允许 exact match；
- `Nearest` 按唯一 distance-capable order 的绝对距离选择，并显式声明等距时选前驱还是后继；
- backward/forward 可以用非空 lexicographic order tuple；nearest 和 tolerance 只能用单个
  integer、Date32、Timestamp 或 Decimal128 order；
- tolerance 是该 order 物理单位上的包含边界上限。它只限制匹配，不是 watermark，也不允许删除历史状态。

Equality 键可为空，表示一个全局 partition。`Equal` 模式下任一分量为 NULL 就不匹配；
`NotDistinct` 让两侧 NULL 进入同一 partition。Order 的 NULL 永远不匹配。指定了 residual 时，
它在排名前对完整 `left + right` pair 求值；false 或 NULL 候选会被跳过，搜索继续到更远的
eligible candidate。

同 order 下的不同 right rows 先按有序 right-only tie-break 排名，每个 tie 都指定升/降序和
NULL first/last。若显式 tie 仍不唯一，Definition 必须选择拒绝歧义，或使用 canonical right row
的升/降序作最终决胜。同一 canonical right row 的 multiplicity 只决定 candidate 是否存在，
不会把一个 left row 的匹配输出再乘一次。

`AsOfJoinKind` 提供 `Inner`、`LeftOuter`、`LeftSemi` 和 `LeftAnti`。Right 候选的
`0 ↔ positive` presence transition 会重新计算已有 left rows，依次输出旧结果 `-weight` 和新结果
`+weight`；仅在正 multiplicity 之间变化不会改变被选 identity。两侧的插入和撤回都进入同一
ordered relation，不是“左流到达时查一次右表”的 processing-time lookup。

加权关系在没有 occurrence identity 时不能唯一决定 Right/Full ASOF 中哪个物理 right copy
已被使用。例如同一 left row 权重 2、同一 right row 权重 3 时，两个 probe copy 可以共用一个
candidate occurrence，也可以各用一个；joined value multiset 相同，但 unmatched right 权重不同。
`Change` 没有这种 occurrence identity，因而这四种 left-family 是输入关系能唯一决定的完整语义；
交换左右侧可以表达反向的 probe 问题，但那是另一个选择函数，不是 Right ASOF 的等价改写。

算子只声明三个持久资源：

```text
asof_join.left_rows: OrderedMap<Vec<u8>, RowWeight>
asof_join.right_rows: OrderedMap<Vec<u8>, RowWeight>
asof_join.continuation: Cell<AsOfContinuation>
```

两个 map 的 key 按 `partition + order + tie rank + canonical row` 排序，值只保存正 multiplicity；
continuation 保存当前输入行序号、Probe/Emit phase、外层 left cursor、候选 cursor、已找到的 before/after
winner 和歧义标记。Probe 先为整个 pinned Change 验证准入、候选、解码、residual、tie 与所有输出
diff 都可表示，Emit 再发布修正并更新关系；状态、output 和最后的 input completion 始终在调用方
事务中一起前进。

候选搜索和 right-side rematch 都以 Store 的 owned page 进行。常规候选页最多 64 项、1 MiB
logical Store bytes 和 16,384 个 `ScalarValue` slots；整个 turn 常规最多 256 项和 4 MiB 逻辑
工作量。当空 turn 的首个 Store item 本身超限时，为了活性会单独接受它。因此普通运行时峰值是
`O(pinned Claim + candidate page + turn output)`，而非整个 partition；单个 oversized row 仍是显式例外。
这些边界限制一次 turn 的内存和事务放大，不限制整个关系的磁盘状态。没有 watermark 时两侧历史都
必须保留。Residual 可以让最近候选不合格，所以当前正确性路径要分页扫描整个 right partition；
right presence transition 还要扫描该 partition 的全部 left rows，并对每个 left row 完成候选搜索。因而普通左侧
lookup 成本与候选 partition 大小成正比，最坏右侧历史修正是该 partition 左右状态的乘积；分页只保证
每个 turn 有界，不会隐藏总成本。候选 right scan 与 rematch left scan 都从索引内的
matchable-order marker 直接 seek，不会读取 order 为 NULL、因而永远不可能参与匹配的历史。

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

`PostgreSQL` CDC Scan 会把初始快照和封口前观察到的 WAL 重叠写入私有
`bootstrap_spool: Queue<Vec<u8>>`，因此 spool 必须容纳两者。MySQL Scan 的 spool 只保存完整快照；
并发变化留在 binlog，binlog 必须覆盖快照、发布和追平阶段。封口后，两者都把 spool 逐条发布到
Station output，再进入持续流阶段。spool 容量是硬限制；超限的 delivery 不提交也不 ACK。

两个 CDC Scan 都固定单表 Schema，运行中不支持在线 DDL、TLS 或跨实例 fencing。捕获阶段 reopen
会清理未完成快照并从头再做，不从半个快照继续。

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

`SQLite`、`PostgreSQL`、`ClickHouse` 与 `Doris` Sink 共用唯一的 durable buffered Sink 协议。完整输入 Change 先编码为一个
`sink.buffer` entry，并与 `sink.control` accounting、input `Complete` acknowledgement 在同一 Store
事务提交；因此调用方在目标数据库写入前就可以释放 Claim。连续小 Change 可以聚合；没有新 Claim、
待处理事件达到目标上限或 retained bytes 达到 delivery watermark 时，运行时才从 buffer 构造一个
有界批次。单个 buffered Change 的 canonical、uncompressed IPC 加 8-byte key 不得超过 8 MiB；编码前
先无拷贝预检 IPC body，避免超大 Change 在拒绝前形成完整临时 body。owned decode 在对齐合适时共享这份
受限 IPC backing，否则只做受 body 上限约束的局部对齐复制。整个 buffer 最多按 IPC+key 的逻辑口径
保留 64 MiB、1,048,576 个 relation events；这不是 Rust heap、RocksDB WAL 或磁盘硬配额。
每个目标批次最多 1024 个 mutation，完整 encoded 输入聚合受 8 MiB 上限；
target mutation 按 canonical row、技术字段和每列固定 framing 的逻辑口径计费，并受独立 8 MiB
上限。后者不是 driver heap、SQL/wire payload 或数据库事务资源的硬配额。
超出单项或 event 上限、或不能在剩余 technical-ID 区间内排空的 Claim 在 admission 前明确失败，不产生
ACK 或部分 buffer 写入。

批次先把 relation checkpoint、buffer settlement 和固定-ID mutation plan 持久化为 `Prepared`，Store
commit 后才在目标数据库的一个事务中执行；之后的独立 Store turn 删除完整消费的 entries 并发布
新的 `Ready`。进程在目标提交与本地 settle 之间退出时，reopen 从原 buffer 精确重建 Prepared 批次并
重投；Prepared 的 insert/delete 都绑定 delivery row index 与固定 `$dogpaddle.id`，目标事务在忽略
重复 insert 后仍核对该 ID 的完整逻辑行，再执行 delete，使原样重投幂等且拒绝 ID/行错配。外部提交结果不确定或 `AfterCommit` 失败
会使当前 runtime fail-stop，只有 reopen 可以继续。恢复在任何目标副作用前分页校验全部 retained
entries、连续 sequence、Schema、control accounting，以及 checkpoint 下剩余正事件的技术 ID 容量，
不能先交付损坏 buffer 的有效前缀。该检查覆盖结构损坏与正常 crash/replay；外部篡改 Store/目标为另一组
语义自洽状态不在恢复契约内，目标表仍必须由 Sink 独占。

`SQLite` Sink 只接受新的非保留目标表和绝对 UTF-8 文件路径。PostgreSQL Sink 要求调用方每次注入
连接配置，Definition 只保存 discovery 得到的非敏感 target spec；当前不支持 DNS endpoint、TLS、
共享目标或在线 Schema evolution。PostgreSQL 的 5 秒 work-unit deadline 包含宽 Schema 为遵守参数上限
产生的全部 SQL 分片往返，因此极宽目标需要低延迟连接。真实数据库限制和恢复证据见对应 correctness
与 system test。

`ClickHouse` Sink 使用无 TLS HTTP endpoint，持久化 Atomic database UUID，并独占一个
`ReplacingMergeTree(version)` 状态表和公开 `FINAL` view；delete version 高于 live version，旧 live 重放
不能复活 tombstone。Doris Sink 使用无 TLS `MySQL` endpoint，持久化唯一 cluster ID，并独占一个开启
merge-on-write 的 Unique Key 状态表和公开 view；delete marker 同时作为 sequence column。两者均只把
非敏感 target identity 写入 Definition，host、port、user 和 password 必须在每次 build/open 时重新注入。
两个状态表都为 row hash 建立后端原生索引，lookup 仍逐 logical row 做完整值核对。为了让提交结果不确定的旧写入永远不能复活已经删除的 technical ID，删除记录作为每个 ID 的终态保留；引擎 compaction 可合并同一 ID 的版本，但状态表物理基数仍随历史分配过的 technical ID 增长。当前没有安全的自动 GC，长期高 churn 部署必须监控目标容量并在维护窗口以新 state/target 重建。

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
[`Distinct`](src/operation/transform/distinct.rs)；需要分页时读
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
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench aggregate_extrema
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench equi_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench buffered_sink
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench asof_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench asof_join_resources
```

全工作区测试所有权和性能口径见 [`TESTING.md`](../../TESTING.md)。
