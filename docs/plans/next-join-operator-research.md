# DogPaddle ASOF Join 决策与实施记录

> 历史提案：记录当时方案与取舍，不作为当前实现约束。当前设计以根 [AGENTS.md](../../AGENTS.md) 指向的 owner 文档为准；不要据此恢复已删除的 API 或抽象。

## 结论

调研选择并已经完成的下一种新 Join 语义是完整的 **动态 `ASOF JOIN`**：

- port `0` 永远是 probe，port `1` 永远是候选；每个正权重 probe exact row 最多选择一个候选 identity；
- 自然关系族包含 Inner、Left Outer、Left Semi 与 Left Anti；
- 支持 backward、forward 与 nearest，四种 SQL 不等式、是否允许 exact match，以及 nearest 等距时显式选择前向或后向；
- 支持零组或多组 equality partition key、普通相等与 null-safe `NOT DISTINCT`；
- 支持非空 lexicographic order tuple、显式右侧 tie-break 与显式最终 tie policy；
- 支持在排序前判定候选资格的 immutable Boolean residual：较近候选为 false/NULL 时继续寻找更远的合格候选；
- 对有稳定距离的单 order 类型支持包含边界的 tolerance，但 tolerance 不授权状态回收；
- 两侧都是可插入、可撤回的动态关系，候选侧历史版本变化会修正既有结果；
- 不把处理时间、source timestamp 或 watermark 偷渡进语义。

这不是把一个 MVP 命名为 ASOF。DuckDB 的静态物理行模型确实支持 Right/Full
ASOF；这里没有把“业界不支持”当借口。DogPaddle 的边界来自关系语义而非排期：交换输入会得到另一个选择函数，而 exact-row multiplicity 下没有唯一的 Full ASOF physical-copy 语义。例如，同一 canonical left row 权重为 2、同一 canonical right row 权重为 3 时，两个 probe occurrence 可以选中同一个 right occurrence，也可以各选一个；两者产生相同的 joined value multiset，却分别留下权重 2 或 1 的 unmatched right。现有 `Change` 没有 occurrence identity，因此输入关系不能决定唯一结果。若未来增加 occurrence identity，或明确发明 canonical coverage 语义，可以另行加入 Right/Full；在当前模型中假装支持反而是不确定的。Residual 则相反：DuckDB 已经给出明确的 candidate-eligibility 语义，所以本次直接实现；它会使相邻 order cell 的窄 rematch 区间不再安全，runtime 必须按 partition 做有界分页候选搜索，不能用错误的优化缩小结果。

本次先完成了 `EquiJoin` 原生 residual predicate：全部 Inner/Outer/Semi/Anti Join 都能在等值 bucket 内正确判断额外 `ON` 条件；随后 J1/J2 已经完成动态 `ASOF JOIN` Operation 与原生 SQL lowering。它仍排在 event-time interval/window join 之前，后者需要 DogPaddle 先拥有明确的事件时间、watermark、迟到数据和状态清理契约。

当时如果只能选一个对外可见的新算子，决策就是 `AsOfJoin`。它比 `CrossJoin`、无约束 `ThetaJoin`、外部 `LookupJoin` 或 event-time `IntervalJoin` 更符合当前产品的 CDC、持久状态、顺序执行和确定性恢复边界。

## 当前基线

DogPaddle 已经拥有完整的普通等值 Join 家族：Inner、Left/Right/Full Outer、Left/Right Semi 和 Left/Right Anti，以及完整动态 ASOF 的 Operation 能力和 left-preserving 原生 SQL 接入。普通 Join 的 Operation 层只需要五种朝向固定的 `EquiJoinKind`，SQL 层通过交换输入实现 Right 家族。无 residual 时继续使用 equality-key presence counts 快路径；有 residual 时使用具体 canonical row 的 qualifying-match counts，并通过可恢复的 `Probe → ClearShadow → Emit` 分页状态机保证整份 pinned Change 在输出前完成校验。

这意味着“再增加一种左右保留组合”已经没有明显空白。原调研识别了三个缺口；条件能力缺口和
有序相关能力已经分别由 residual 与 ASOF 关闭，现在只剩一个结构性缺口：

1. **有界时间能力**：没有 event time、watermark 或 late-data contract，因此不能安全声称支持会自动淘汰状态的 window/interval join。

其中有界时间能力是硬边界。`Change` 明确没有事件时间、watermark、来源 offset 或物化关系；日志 offset 加行号也不能充当长期事件 ID。由此可知，任何依赖“时间已经过去，所以旧行永远不会再匹配”的算子都会引入跨 Change、Operation、Flow 和 source 的新协议，而不只是新增一个 Operation。

## 业界能力地图

流式系统里的“Join”至少分成五类，名字相近但状态与修正语义差别很大。

| 类别 | 典型语义 | 状态行为 | 对 DogPaddle 的意义 |
| --- | --- | --- | --- |
| Regular dynamic join | 任一侧变化都会修正完整结果 | 通常永久保留两侧历史 | 当前 EquiJoin 已覆盖主干 |
| Predicate/theta join | `ON` 是任意布尔条件 | 无索引时接近笛卡尔探测 | 应先限制在等值分区内 residual |
| ASOF/nearest join | 每个 probe 行选择有序维度上的一个最近匹配 | 输出不做多对多膨胀；动态 build 更新可能重配一段 probe 行 | 当前已实现 |
| Interval/window join | 只匹配时间范围内的事件 | watermark 推进后可回收旧状态 | 需要先设计时间与迟到语义 |
| Temporal/lookup join | probe 到达时读取维表某个版本或外部当前值 | 可少存状态，但依赖外部快照、缓存和 I/O 一致性 | 与当前确定性重放边界冲突较大 |

Flink 把 Regular、Interval、Event-time Temporal、Processing-time Temporal 和 Lookup Join 分开建模。它明确指出 Regular Join 需要永久保留两侧状态；Interval Join 依靠 time attribute 的准单调性清理状态；Event-time Temporal Join 由两侧 watermark 触发，并可能丢弃已晚于 watermark 的 probe 行。[^1] Spark Structured Streaming 同样要求通过两侧 watermark 加跨流时间约束，才能判断旧状态何时不再可能匹配；Outer Join 的 NULL 行还必须等到引擎能确认未来不会再有匹配时才输出。[^2]

这两套系统说明：**Interval Join 不是“EquiJoin 多一个 `<` 条件”**。只要产品承诺有界状态和正确的 unmatched 输出，它就是时间进度协议。

另一条路线是 ASOF。DuckDB 将它定义为：按等值条件分组，再用一个不等式有序列选择最近右行；每个左行最多匹配一个右行，Left ASOF 在无匹配时补 NULL。[^3] DuckDB 1.5 还允许 Inner/Left/Semi/Anti 在候选 pair 上使用 arbitrary predicate：较近候选不合格时继续寻找更远候选；其 Right/Full 依赖静态 build-row found bitmap，带 arbitrary predicate 时仍明确拒绝。[^10] RisingWave 也支持流式 ASOF，并要求至少一个 equality 条件和一个 inequality 条件；有序属性通常是时间，但不强制必须是时间类型。[^4] Apache DataFusion 当前文档采用 Snowflake 风格的 `MATCH_CONDITION`，同样覆盖四个方向，并把 `ON`/`USING` 用作等值分组。[^5]

ASOF 的关键优势不是语法新奇，而是**受控的输出基数**。普通 range/theta join 可能把一个输入变化扩散为整个范围内的所有配对；ASOF 对每个左行只保留最近的一个候选。这对 CDC 管道尤其有价值：订单关联“下单时有效的客户等级”、交易关联“当时最近报价”、设备事件关联“当时生效配置”、审计事件关联“当时组织归属”都是同一个模型。这里必须区分输出与状态：没有 watermark/retention 时，ASOF 仍要永久保留可能被撤回或被历史修正影响的两侧输入，因此它控制输出膨胀，但不承诺有界状态。

## 候选排序

评分采用 1–5，越高越好；“风险可控”高分表示风险较低。

| 候选 | 用户价值 | 当前架构契合 | 确定性/恢复 | 状态可控 | 风险可控 | 建议 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Dynamic ASOF Join | 5 | 4 | 5 | 3 | 3 | **已实现** |
| EquiJoin 内建 residual | 4 | 5 | 5 | 3 | 4 | **本次已完成** |
| Interval/Window Join | 5 | 2 | 2 | 5 | 2 | 等 watermark 契约 |
| External Lookup Join | 4 | 2 | 2 | 5 | 2 | 等外部快照/I/O 协议 |
| Mark / null-aware anti | 3 | 4 | 4 | 3 | 3 | 等子查询路线 |
| General Theta/Range Join | 3 | 3 | 4 | 1 | 2 | 不做无约束版本 |
| Cross Join | 1 | 3 | 5 | 1 | 2 | 不作为产品优先项 |
| Lateral Join | 3 | 1 | 3 | 2 | 1 | 它是 planner 能力，不是单一算子 |

### EquiJoin residual：本次已完成，但不算下一种新语义

改动前 SQL 只能把 Inner Join 的 residual 放到 Join 后 Filter，因为 Inner 的 `join(key) → filter(predicate)` 与在 Join 内判断等价。Outer/Semi/Anti 不成立：先生成 NULL padding 或 presence 结果再过滤，会把本应保留的行删掉，所以旧实现只能拒绝它。

现在 Join 自己保存并绑定一个完整的 immutable residual expression，只对同一 equality bucket 内的候选 pair 求值。Outer/Semi/Anti 的 match count 已从“每个 equality key 的对侧非空”升级为“每个具体保留行有多少 qualifying distinct rows”；无 residual 的实例仍保留原有 `key_counts` 快路径。实现复用了现有等值索引、事务和完整 Join kind 家族，并增加 shadow preflight 与分页 continuation，确保 overflow、underflow、回滚和 reopen 不会留下部分结果。

这个里程碑已经补齐标准 `ON key equality AND predicate` 的正确性，也为随后完成的 ASOF candidate predicate、未来 range specialization 和 Join 内减少中间 Change 打下基础。实现没有另造一个与 EquiJoin 大量重叠的公共 `PredicateJoin` 类型，而是演进 `EquiJoinDefinition` 的持久 payload、状态布局和当前 v1 golden。

### 为什么不是 Interval Join

Flink 的 Interval Join 要求 time attribute，并依赖时间进度删除旧状态；Spark 也只有在 watermark 和时间范围同时存在时才能确定状态可回收。[^1][^2] DogPaddle 现在没有任何一个必要概念。若只实现 `l.ts BETWEEN r.ts - Δ AND r.ts + Δ` 的关系语义而不承诺清理状态，它本质上只是一个带范围 predicate 的动态 Join；把它命名为 Interval Join 会让用户错误期待 watermark、迟到处理和有界状态。

可以在 ASOF 之后做一个纯关系 `RangeJoin` specialization，但应明确它永久保留两侧关系。真正的 event-time Interval Join 必须单独立项，先定义：

- event-time expression 属于 source、Change 还是 Operation Definition；
- watermark 如何持久化、合并、恢复和跨 edge 传播；
- idle input 是否阻塞全局进度；
- late row 是拒绝、丢弃、旁路还是修正；
- Outer unmatched 行何时输出；
- watermark 与同一 Change 的逐行顺序及原子提交如何交互。

在这些问题回答前实现算子，会把最难的语义藏进局部代码。

### 为什么不是外部 Lookup Join

Lookup Join 对 CDC enrichment 很诱人。Flink 用它在 probe 到达时查询 JDBC 等外部维表，并明确规定未来维表更新不会修改已经产生的结果；它还需要异步模式、容量、timeout、retry 和 cache 等执行策略。[^6] 这是一种 processing-time temporal 语义，不是普通动态关系 Join。

DogPaddle 的 TurnOperation 确实可以承载外部 I/O，但当前重放保证要求未提交 turn 从未变化的 durable state 重放。若 retry 时外部数据库已经变化，同一输入可能得到不同结果；若把 lookup 结果先写入 durable state，又需要设计结果快照、失效、凭据和 endpoint identity。它比 ASOF 多出一整套外部一致性协议，不宜作为下一步。

未来若做，应该叫 `LookupJoin`，并明确选择以下一种契约：

1. 外部系统提供可重读的 snapshot/version token；或
2. 第一次 lookup 结果先持久化，再与 output/input completion 原子推进；或
3. 明确承诺 at-least-once、非确定 processing-time enrichment。

第三种与当前产品审美最不一致。

### 为什么不是任意 Theta/Cross Join

PostgreSQL 对 Cross Join 的定义就是 N×M 个组合。[^7] 对持续变化的两张表，这既意味着输出爆炸，也意味着任意一侧更新都可能扫描整个对侧。Materialize 能提供很宽的 Join 语义，是因为它同时拥有 arrangements/index reuse、差分数据流和更完整的优化体系；其文档也明确提醒 Lateral 可能非常昂贵。[^8]

DogPaddle 当前是确定性顺序 Flow，没有成本优化器、统计信息、通用 arrangement registry 或并发调度。先暴露无约束 Theta/Cross Join，会把一个语法特性变成不可预测的磁盘扫描器。更合理的顺序是：

1. equality partition + residual；
2. ASOF 的单候选 ordered lookup；
3. 有明确边界的 range join；
4. 最后才考虑 planner 能证明有界或用户显式接受代价的 general theta join。

## 推荐的 ASOF 语义

### SQL 形状

建议跟随 DataFusion/Snowflake 形状，而不是再创造 DogPaddle 方言：

```sql
SELECT
    orders.order_id,
    prices.price
FROM orders
ASOF JOIN prices
MATCH_CONDITION (orders.ordered_at >= prices.valid_from)
ON orders.sku = prices.sku;
```

DataFusion/Snowflake 的 SQL surface 固定为 left-preserving，因此不再额外写
`LEFT` 关键字；Operation API 仍独立提供 Inner、Left Outer、Left Semi 与 Left Anti。

四个方向定义如下：

| 条件 | 选择的右侧 order value |
| --- | --- |
| `left >= right` | 小于等于 left 的最大 right |
| `left > right` | 严格小于 left 的最大 right |
| `left <= right` | 大于等于 left 的最小 right |
| `left < right` | 严格大于 left 的最小 right |

equality key 可以是零组；零组表示整个候选关系是一个全局 partition。实现仍会把大 partition 的 rematch 分页，并在性能证据中单列其最坏成本，不能通过删掉合法语义来隐藏 fan-out。

### 动态关系，而不是 processing-time lookup

建议采用**完全动态关系语义**：

- 左行插入：选择当时关系中最近的右行并输出；
- 左行撤回：撤回它当前应匹配的结果；
- 右行插入：如果它成为一段既有左行的新最近候选，撤回旧配对并插入新配对；
- 右行撤回：受影响左行退回到下一个最近候选，或在 Left ASOF 下变成 NULL padding；
- reopen：只从持久关系状态和 continuation 推导同一结果，不读取墙钟或 source metadata。

这比 processing-time temporal join 更贵，但语义与 DogPaddle 现有 `+/- diff`、错误回滚和 reopen 一致。RisingWave 文档明确区分两者：process-time temporal join 只在左侧变化时产出，右侧变化只影响未来 lookup；ASOF 则是有序最近匹配。[^4] DogPaddle 不应让同一个名字混合这两种行为。

### NULL、类型、tolerance 与 tie

完整合同是：

- equality、order 与 tie-break 都必须是 immutable DataFusion expressions；
- 左右对应 expression 必须具有完全相同类型；
- `Equal` equality key 的任一 NULL 不匹配，`NotDistinct` 则让两侧 NULL 进入同一 partition；
- NULL order value 永远不匹配；tie-break NULL 使用定义中显式的 first/last 次序；
- order type 只接受具有稳定全序和稳定编码的 flat non-floating 类型；Float32/Float64 的 NaN、signed zero 与 total-order ABI 不在 ASOF 中猜测；
- backward/forward 可使用 lexicographic order tuple；nearest/tolerance 只用于一个能定义 widened distance 的 integer、Date32、Timestamp 或 Decimal128 order；
- tolerance 非负且包含边界，候选选定后若距离超限就视为无匹配；它不代表 watermark，也不允许删除历史；
- 同 order 的 distinct right rows 先按显式 right-only tie-break 排序；完整 tie rank 仍相同时由 Definition 明确选择拒绝、canonical-row ascending 或 descending，绝不依赖 RocksDB 遍历碰巧稳定；
- 同一个 canonical right row 的 multiplicity 只表达 candidate presence，不乘入 pair output。
- optional residual 在 equality/order NULL 资格检查之后、候选排序之前对完整 `left + right` pair 求值；只有 non-null true 参与，较近 false/NULL 候选不会遮住更远 true 候选。

DuckDB/Calcite 对“最近”的定义并不自动解决并列候选的确定性；RisingWave 也提示 batch ASOF 在多个候选共享最近 ordered property 时结果可能变化。[^4] DogPaddle 因而把 tie 与 nearest 等距策略都放进持久 Definition，而不是留给运行时扫描顺序。

## 与现有架构的连接

### 为什么适配 TurnTransform

`AsOfJoin` 是二输入 `TurnTransform(2)`：一次右侧变化可能使大量既有左行改配，必须分页输出并把进度持久化。现有 EquiJoin 已经证明了以下机制：

- 首 Operation 独占输入 Claim；
- 在短写事务中更新状态、输出并完成输入；
- 背压时保存 continuation；
- reopen 后从未变化或已提交的 durable state 重放；
- Atomic 尾链可以与最终输出共享事务。

因此不需要新的 Flow 抽象、route、worker 或 background indexer。新增复杂性应全部落在 Operation 自己声明的数据和 continuation 中。

### 建议的持久状态

建议让 operation crate 私有 codec 拥有 `partition + order + canonical row` 的稳定、按字节有序编码，并用 Store 的 `OrderedMap` 范围扫描：

| 资源 | 作用 |
| --- | --- |
| `asof_join.left_rows` | 按 equality partition、left order、canonical row 排序，保存正 multiplicity |
| `asof_join.right_rows` | 按 equality partition、right order、canonical row 排序，保存正 multiplicity |
| `asof_join.continuation` | 当前 port、输入 row、probe/rematch phase、外层 left cursor、候选 cursor、分页 winner 与歧义标记 |

如果 profiling 证明右侧更新时反复计算当前匹配过贵，再以真实数据决定是否增加派生的
left-match cache；当前设计不同时持久化能从两个有序关系索引推导出的第三份事实。Continuation
只保存完成当前 bounded turn 后无法从 durable relation 与 pinned Change 重新推出的搜索进度，不充当第二份关系状态。

现有 Store 已有 owned、可分页、带 range 和 continuation 的 `OrderedMap::scan`，这正是 ASOF 所需的后向/前向最近查找与受影响区间扫描基础。现有 `PartitionedMultiset` 只有整分区方向扫描，没有 range 参数，因此不要为了复用 EquiJoin 的 `Rows` 类型而把 range 语义塞进上层过滤；用一个准确的新 key codec 或在真实调用压力证明后再增加窄 Store 结构。

### 增量算法轮廓

以最常见的 `left.order >= right.order` 为例：

1. **左行变化**：在相同 equality partition 中，反向查找 `right.order <= left.order` 的第一个右行；Inner 无匹配不输出，Left 无匹配输出 NULL padding。
2. **右行插入 r**：无 residual 时可用同分区的相邻右侧 order 推导受影响 left interval；有 residual 时这个区间不再充分，必须分页验证完整 partition，逐行撤旧增新。
3. **右行删除 r**：无 residual 时同一 left interval 退回 predecessor；有 residual 时逐个 left probe 重新分页寻找下一个合格 candidate。
4. **同一 Change 多行**：严格按输入行序观察，每个前缀都做负 multiplicity、overflow 和 tie 检查；所有状态与输出仍在一个 Change 的事务边界内。

其他三个不等式只是方向和端点开闭变化，不应复制四套 runtime。Definition 在 bind 时把它们正规化成“查 predecessor/successor + affected interval”的内部方向枚举。

## DataFusion 接入风险

原来的 DataFusion 55.0.0 release 与 sqlparser 0.62.0 只有 ASOF AST，没有 logical node。Compatibility spike 最终定位到 DataFusion 提交 `82335b426d8851db6a7b965f3d43053c585cabfd`：它同时包含 logical `AsOfJoin`、SQL lowering 与 plan proto，且仍使用同一 55.0.0/Arrow 59.3.0 类型族。工作区把所有 direct DataFusion crate 和 lock 中全部 transitive DataFusion crate 精确 pin 到这个单一 source/revision；`cargo tree -d` 必须证明不存在第二套 DataFusion、Arrow 或 sqlparser 类型。

这个原生 logical node 只表达 left-preserving、零或多组 ordinary equality 和一个 `< / <= / > / >=` match pair，不表达 nearest、tolerance、tie 或 residual。因此 SQL 层只降低它真实携带的能力；完整能力仍由 Operation API 提供，不能为了扩 SQL surface 在 DogPaddle 内另写表达式 analyzer。DataFusion 自己也把 ASOF/Range Join 视为专门 Join 能力，历史 issue 指出普通不等式 Join 容易退化为 nested loop。[^9]

## 实施状态与后续路线图

### J0：Join residual 完整性（已完成）

- `EquiJoinDefinition` 已增加可选 immutable residual；
- Inner/Outer/Semi/Anti 已统一在候选 pair 上求值，`false` 与 `NULL` 均不成匹配；
- residual presence state 已改为具体行的 qualifying-match count，并覆盖分页、回滚与 reopen；
- SQL 已放开 Outer/Semi/Anti residual，并正确重写 Right 家族交换后的字段朝向；
- owner benchmark 已同时保留无 residual control，并增加 Inner、LeftSemi、FullOuter residual workload。

这一步已经校验了“候选 pair predicate + per-row match transition”的通用正确性，并成为已完成 J1 的基础。

### J1：完整 AsOfJoin Operation（已完成）

- 已实现 `AsOfJoinKind::{Inner, LeftOuter, LeftSemi, LeftAnti}`；
- 已实现 backward/forward/nearest、exact control、equidistant policy；
- 已实现 `0..N` equality key pairs、`Equal/NotDistinct`，以及非空 lexicographic order tuple；
- 已实现显式 tie-break/fallback、单 metric order tolerance 和 optional candidate-eligibility residual；
- 已固定三个持久资源与 stable tag/golden，并覆盖两侧 insert/retract、multiplicity、NULL、tie、overflow、分页背压和 reopen；
- 已提供 operation owner benchmark，覆盖 probe lookup、build-side rematch、全局 partition 与 nearest 最坏范围。

### J2：SQL lowering（已完成）

- DataFusion 已精确 pin 到同时提供 ASOF AST、logical node、SQL lowering 与 plan proto 的 revision；
- lowering 只接受原生 `AsOfJoin` logical node，不在 DogPaddle 内重建 SQL analyzer；
- output Schema 已严格对齐 DataFusion，Program identity 覆盖会改变持久语义的查询与编译 ABI；
- SQL correctness 已有真实公共 `SequenceScan → AsOfJoin → SqliteSink` 最终关系 witness：覆盖四个不等式、`ON`/`USING`/全局分区、left-preserving 初始输出、右侧历史修正和 drop/start reopen。

这里的 Sequence→SQLite witness 不是 CDC 系统验收。仓库目前仍没有专门的真实
`PostgreSQL/MySQL CDC → ASOF → SQLite/PostgreSQL` system case；它是后续测试扩展，不能用现有
Sequence correctness 或普通 CDC system case 代替。

### J3：时间进度扩展

- ASOF tolerance 已属于 J1，但只限制匹配；
- 单独设计 event-time/watermark 后，才允许任何按时间回收 ASOF 历史状态的优化；
- 回收协议必须保持 late correction 与 reopen 语义，不得悄悄把 dynamic ASOF 改成 processing-time lookup。

### J4：后续候选

按需求证据选择其一：

- `MarkJoin` + subquery lowering，支持 `EXISTS/IN/NOT IN` 的三值逻辑；
- equality-partitioned `RangeJoin`，输出范围内全部匹配；
- snapshot-safe `LookupJoin`；
- multi-way Join arrangement reuse / join-order optimization。

## 验收标准

### Correctness

- 四个 comparison direction、nearest、exact control 与两种 equidistant preference；
- Inner、Left Outer、Left Semi 与 Left Anti；
- 零个/多个 equality keys、Equal/NotDistinct、单个/多个 expression order keys；
- NULL equality/order/tie-break；
- tolerance 的 0、恰好边界和越界；
- exact duplicate、显式 tie-break、Reject 与两个 canonical fallback；
- residual 的 nearest-false fallback、NULL、左右字段引用与整批错误回滚；
- right before first、between versions、after last；
- 左右 insert/retract 与同一 Change 内 update-before/update-after；
- right insertion 替换一段左行、right deletion 回退 predecessor；
- exact duplicate multiplicity transition；
- ambiguous tie 整批回滚；
- negative prefix、weight/output diff overflow；
- 小 output capacity 下多页 continuation；
- 每个 phase 的 drop/reopen；
- Atomic 尾链失败和 output backpressure 不留下部分状态；
- golden payload、资源布局、build/open/reopen。

### 性能

至少建立四组 owner benchmark：

1. equality partitions 多、每组版本少：典型维表；
2. 单 partition 版本多：压力测试 ordered lookup；
3. right insert 只影响少量 left：正常 rematch；
4. right insert 位于历史开头、影响大量 left：最坏分页与写放大。

主要指标不是只看 rows/s，而是：每输入行读写的 RocksDB logical bytes、scan pages、输出倍率、continuation 次数和 reopen 后完成代价。ASOF 的卖点是结果基数受控，不代表 build-side 历史修正一定便宜；最坏情况必须在文档中可预测。

## 最终建议

实施顺序应当是：

```text
EquiJoin residual correctness ✓
        ↓
完整 Dynamic ASOF Join ✓
        ↓
event time + watermark contract
        ↓
bounded Interval/Window Join
```

这个顺序保留了 DogPaddle 最有价值的特征：每个结果都来自明确的动态关系语义，状态、output 与 input completion 同事务提交，重试和 reopen 不依赖墙钟或外部世界碰巧没变。它也给产品带来一个用户能立即理解的新能力，而不是只增加语法糖。

## Sources

[^1]: Apache Flink, “[Joins](https://nightlies.apache.org/flink/flink-docs-master/docs/sql/reference/queries/joins/),” Flink SQL documentation. Regular, interval, temporal and lookup semantics; accessed 2026-09-14.
[^2]: Apache Spark, “[Structured Streaming Programming Guide — Join Operations](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html),” stream-stream state, watermarks and outer/semi completion semantics; accessed 2026-09-14.
[^3]: DuckDB, “[FROM and JOIN Clauses — As-Of Joins](https://duckdb.org/docs/stable/sql/query_syntax/from),” ASOF equality grouping, inequality direction and at-most-one match; accessed 2026-09-14.
[^4]: RisingWave, “[Joins](https://docs.risingwave.com/processing/sql/joins),” streaming ASOF and process-time temporal join semantics, requirements and tie caveat; accessed 2026-09-14.
[^5]: Apache DataFusion, “[SELECT syntax — ASOF JOIN](https://datafusion.apache.org/user-guide/sql/select.html),” Snowflake-style syntax and comparison-direction semantics; accessed 2026-09-14.
[^6]: Apache Flink, “[Lookup Join](https://nightlies.apache.org/flink/flink-docs-master/docs/sql/reference/queries/joins/#lookup-join),” external enrichment and processing-time behavior; accessed 2026-09-14.
[^7]: PostgreSQL Global Development Group, “[SELECT — CROSS JOIN](https://www.postgresql.org/docs/current/sql-select.html),” Cartesian-product semantics; accessed 2026-09-14.
[^8]: Materialize, “[JOIN](https://materialize.com/docs/sql/select/join/),” broad dynamic Join and LATERAL cost warning; and “[Optimization](https://materialize.com/docs/transform-data/optimization/),” arrangement/index reuse and delta joins; accessed 2026-09-14.
[^9]: Apache DataFusion, “[ASOF join support / Specialize Range Joins](https://github.com/apache/datafusion/issues/318)” and “[Range/inequality joins are slow](https://github.com/apache/datafusion/issues/8393),” specialized ordered Join motivation and nested-loop limitation; accessed 2026-09-14.
[^10]: DuckDB, “[ASOF join type tests](https://github.com/duckdb/duckdb/blob/main/test/sql/join/asof/test_asof_join_types.test),” Inner/Left/Right/Full/Semi/Anti coverage and the explicit Right/Full-with-arbitrary-predicate rejection; “[ASOF filter tests](https://github.com/duckdb/duckdb/blob/main/test/sql/join/asof/test_asof_join_filter_pushdown.test),” candidate predicate behavior; and “[integer ASOF tests](https://github.com/duckdb/duckdb/blob/main/test/sql/join/asof/test_asof_join_integers.test),” unmatched build-row output for Right/Full; accessed 2026-09-14.

### Repository evidence

- `crates/change/README.md`, lines 64–74: Change ordering and absence of event time/watermark/source offset.
- `crates/operation/src/operation/transform/equi_join/definition.rs`: current tag, residual payload, exact candidate-Schema binding and resource selection.
- `crates/operation/src/operation/transform/equi_join/runtime.rs`: pure-equality fast path and residual `Probe → ClearShadow → Emit` state machine.
- `crates/operation/src/operation/transform/asof_join/`: complete ASOF Definition, persistent state and dynamic runtime.
- `crates/operation/tests/correctness/asof_join.rs`: Operation owner semantics, failure, pagination and reopen coverage.
- `crates/sql/src/plan.rs`: equality extraction, native residual/ASOF lowering and Right Join orientation.
- `crates/sql/tests/correctness/execution.rs`: public Sequence→ASOF→SQLite final-relation and reopen witness; this is not a CDC system case.
- `crates/store/src/collections/ordered_map.rs`, lines 135–185: owned paged range scan with continuation.
- `Cargo.toml`, lines 36–42: every direct DataFusion dependency pinned to revision `82335b426d8851db6a7b965f3d43053c585cabfd`.
