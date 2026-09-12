# EquiJoin 设计与实现说明

状态：常用 EquiJoin family 已实现

日期：2026-09-12

线性 Station 与 SQL 确定性装配已经直接落入当前 v1：一个 Station 保存一个非空、有序 Operation
列表，列表共享事务且只持久化最终输出。其通用设计见
[`station-pipelines-and-durable-boundaries.md`](station-pipelines-and-durable-boundaries.md)。本文只记录
EquiJoin 的公共语义、状态和恢复边界。

## 最终形状

Operation 层只有一个 Definition、一个 runtime 和五个 kind：

```rust
EquiJoinDefinition::try_new(
    EquiJoinKind::LeftOuter,
    [(col("customer_id"), col("id"))],
    ["order_id", "customer_id", "id", "name"],
)
```

`EquiJoinKind` 包含：

| kind | 输出关系 | 输出字段 |
| --- | --- | --- |
| `Inner` | 所有匹配的左右记录对 | left 后接 right |
| `LeftSemi` | 至少有一个右侧匹配的 left | 仅 left |
| `LeftAnti` | 没有右侧匹配的 left | 仅 left |
| `LeftOuter` | Inner 加未匹配 left 的 NULL 扩展行 | left 后接 nullable right |
| `FullOuter` | Inner 加两侧未匹配记录的 NULL 扩展行 | nullable left 后接 nullable right |

Right Outer/Semi/Anti 不增加 Operation kind。SQL lowering 交换左右输入和 key，复用对应 Left kind，
随后用同一个 Station 内的 `SchemaAlign` 恢复 SQL 的原字段顺序、名称、nullability 和 metadata。

这保持了两个简单边界：Operation 实现纯关系语义；SQL 负责语法方向和列身份。没有 per-kind runtime、
Join Station、adapter core 或第二张 Station 内拓扑。

## Key 和 Schema

Definition 持久化 kind、非空有序 key-expression pairs 和 output names。port `0` 固定为 left，port `1`
固定为 right。每个表达式只绑定自己的 exact input Schema，每对 key 类型必须完全相同，并且只能使用
可 canonical 编码的扁平非浮点类型。类型转换必须显式写进 DataFusion `Expr`。

复合 key 任一分量为 NULL 时不匹配。本侧完整记录仍进入自己的 row state，后续 retract 才能进行
exact admission。Semi/Anti 只要求 left output names；Inner/Outer 要求 left 后接 right 的全部 names。
Outer 在 bind 时只放宽可能被 NULL 填充的字段，不另存一份 output Schema。

## 私有状态

所有 kind 使用：

```text
equi_join.left_rows:
    PartitionedMultiset<CanonicalJoinKey, CanonicalLeftRow>
equi_join.right_rows:
    PartitionedMultiset<CanonicalJoinKey, CanonicalRightRow>
equi_join.continuation:
    Cell<JoinContinuation>
```

每个 partition key 是完整 composite key bytes；entry key 是完整 canonical row bytes；multiplicity 是正
`u64`，缺失表示零。Inner 只需要这三份资源，保持原来的匹配热路径。

Semi、Anti 和 Outer 另有：

```text
equi_join.key_counts:
    OrderedMap<CanonicalJoinKey, KeyCounts { left_distinct, right_distinct }>
```

这里统计的是每侧不同完整行的数量，不是 multiplicity 总和。同一行从权重 1 变成 100 不改变 presence；
另一种完整行第一次出现才加一。NULL key 不进入 counts，左右都为零的条目必须删除。它只回答“对侧
是否存在匹配”和“本侧是否发生第一个/最后一个 distinct row”这两个问题。

## 增量语义

输入 Change 按原行序执行。每个事件先在内存 shadow 中按本侧当前权重预检 exact admission；同一个
Claim 内的重复行和 first/last presence 往返也按这一顺序推导。任何负前缀、权重溢出或 key-count
溢出都会在发布该 Claim 的任何结果前失败。

Inner 和 Outer 的匹配记录 diff 为：

```text
checked_i64(input_diff × opposite_row_multiplicity)
```

Semi/Anti 在 left 变化时根据 right presence 决定是否原样输出 left diff；right 的第一个或最后一个
匹配 distinct row 会为该 key 下每种 left row 输出其完整 multiplicity。Outer 的 presence transition
遵循：

```text
第一个匹配出现：撤回对侧 NULL 扩展行，再发布匹配记录对
最后一个匹配消失：撤回匹配记录对，再补回对侧 NULL 扩展行
```

同一次 transition 的 NULL correction 与对应 pair 是一个不可拆的分页 work item，因此不会在已提交
页面之间暴露半个 outer transition。

## 有界 turn、事务与 reopen

热点 key 可以产生很大的 fan-out。一个 Claim 使用两阶段 continuation：

1. `Probe` 按 item/byte 预算扫描所有需要读取的对侧 rows，验证 row decode、typed NULL 构造和全部 output
   diff，不产生 output；
2. 全部 Probe 成功后，`Emit` 再按相同有序扫描分页发布结果，并在每个输入行的最后一页调整本侧 rows
   和 counts。

`JoinContinuation` 只保存 input port、`Probe | Emit`、row ordinal 和对侧排他 resume key。它不复制
Subscription identity、Change、join key 或 fingerprint。Station 在 Claim 完成前 durable-pin 当前输入
port，对侧 state 不会在 continuation 中途变化；reopen 仍由 Subscription 提供同一个完整 Claim，runtime
据此重建临时表达式、row 和 key cache。

每页的 Join state、Atomic 尾项状态、最终 output 和 continuation 在同一 Store transaction 中提交。
中间页返回 `Action::Commit`，最后一页清除 continuation 并返回 `Action::Complete`，由 Station 同事务
推进 input Subscription。背压、尾链错误和 commit failure 回滚整页；commit 结果不确定时沿用 Station
fail-stop/reopen 协议。

两遍扫描增加读放大，换来 whole-Claim 的确定失败边界：后段的 corrupt row 或 output-diff overflow 不会在
前段结果已经发布后才出现。`TURN_ITEMS` 与 `TURN_BYTES` 当前以 256 项和 4 MiB 约束常规单 turn 的扫描、
输出和事务工作；它们是软预算，首个 Store item 自身超限时允许由空 turn 单独处理。预算不覆盖完整
Claim 的瞬态内存：Station 已持有整个输入 Change，runtime 还会为整批建立 row、value 和 key 的
`PreparedClaim` 缓存，所以峰值至少是 `O(Claim)`，也可能超过 4 MiB。左右完整关系仍保存在 RocksDB；
普通无界 Join 的总状态与输入关系本身一样没有固定上限，结果 fan-out 也无法由实现消除。

## SQL lowering 与 Station 装配

SQL v1 支持：

- `JOIN` / `INNER JOIN`；
- `LEFT` / `RIGHT` / `FULL [OUTER] JOIN`；
- `LEFT` / `RIGHT SEMI JOIN`；
- `LEFT` / `RIGHT ANTI JOIN`。

每个 Join 至少包含一个跨左右输入的 equality key。DataFusion logical `DFSchema` 用 qualifier 解析同名
字段，lowering 按 ordinal 改写成当前唯一物理字段名。Inner 的其他 `ON` conjunct 变成 Join 后的
`Filter`，只有该 Filter 是 `AtomicTransform` 时才接受，从而保证 `EquiJoin → SchemaAlign → Filter`
仍在一个 Transform Station。Outer/Semi/Anti 的 residual 会影响“是否匹配”，不能外提，因此当前明确
拒绝。

Join 是二输入 `TurnTransform`，自然成为新 Station 的首 Operation；后续单输入 Atomic SchemaAlign、
Filter 或 Projection 按通用唯一消费边规则追加。这个规则不识别 Join tag，也没有 Join 专用装配接口。

Cross/Natural/Using、纯非等值 Join、`EXISTS` 和 `IN` 当前都在创建 Flow 前拒绝。

## 持久化 ABI 与证据

稳定 operation tag 仍为 `16`，payload 直接包含 kind byte；旧 Inner-only payload 不保留兼容读取。
资源从 `inner_join.*` 直接改为 `equi_join.*`。这是开发期 v1，已有旧状态目录删除后重建。

Operation owner evidence 包含：

- 五个 literal golden、kind payload、Inner 三资源和其余 kind 四资源；
- 独立朴素关系 oracle、NULL、重复 multiplicity、same-Claim presence transitions；
- exact Schema、Outer nullability、左右端口交织和稳定重批；
- Probe/Emit 分页、backpressure rollback、reopen、负前缀与正负 output overflow；
- Inner、presence-stable 与 first/last Semi、first/last Full Outer benchmark。

SQL evidence 包含全部 SQL Join 方向的最终 SQLite 关系与 reopen、Right 字段顺序/nullability、Inner
residual 同 Station，以及所有明确拒绝路径无状态目录副作用。

## 后续边界

Operation 当前保持纯 equi。Outer/Semi/Anti residual 需要把 predicate 纳入 matched-state 定义，不能用
现有下游 Filter 冒充；非等值 Join、ASOF、interval、temporal lookup 和 cross Join 也各有不同的索引、
时间或有界性合同，应作为后续独立算子设计。

共享 arrangement 只在至少两个真实消费者需要完全相同的 keyed state，且 benchmark 证明重复维护是
主要成本时再引入。匹配条件至少要包含 producer identity、ordered key expressions、NULL equality 和
key/row codec version；跨 Flow 共享还需要单独解决 owner、生命周期和失败域。

相关设计参考：[Materialize arrangements](https://materialize.com/docs/fundamentals/concepts/arrangements/)、
[Flink streaming joins](https://nightlies.apache.org/flink/flink-docs-master/docs/sql/reference/queries/joins/)、
[DBSP incremental operators](https://arxiv.org/html/2203.16684)。
