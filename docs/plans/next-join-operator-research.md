# DogPaddle 下一种 Join 算子

## 结论

下一种真正的新 Join 语义应当是 **动态 `ASOF JOIN`**，第一版只做：

- `ASOF JOIN` 与 `ASOF LEFT JOIN`；
- 一组或多组等值分区键；
- 一对有方向的有序匹配表达式：`>=`、`>`、`<=` 或 `<`；
- 每个左行最多选择一个最近的右行；
- 两侧都是可插入、可撤回的动态关系，右侧历史版本变化会修正既有结果；
- 不把处理时间、source timestamp 或 watermark 偷渡进语义。

本次已经先完成 `EquiJoin` 原生 residual predicate：全部 Inner/Outer/Semi/Anti Join 都能在等值 bucket 内正确判断额外 `ON` 条件。所以下一个里程碑可以直接进入 `ASOF JOIN`；它仍应排在 event-time interval/window join 之前，后者需要 DogPaddle 先拥有明确的事件时间、watermark、迟到数据和状态清理契约。

如果只能选一个对外可见的新算子，选 `AsOfJoin`。它比 `CrossJoin`、无约束 `ThetaJoin`、外部 `LookupJoin` 或 event-time `IntervalJoin` 更符合当前产品的 CDC、持久状态、顺序执行和确定性恢复边界。

## 当前基线

DogPaddle 已经拥有完整的普通等值 Join 家族：Inner、Left/Right/Full Outer、Left/Right Semi 和 Left/Right Anti。Operation 层只需要五种朝向固定的 `EquiJoinKind`，SQL 层通过交换输入实现 Right 家族。无 residual 时继续使用 equality-key presence counts 快路径；有 residual 时使用具体 canonical row 的 qualifying-match counts，并通过可恢复的 `Probe → ClearShadow → Emit` 分页状态机保证整份 pinned Change 在输出前完成校验。

这意味着“再增加一种左右保留组合”已经没有明显空白。原调研识别了三个缺口，其中条件能力缺口
已经关闭，剩下两个结构性缺口：

1. **有序相关能力**：没有“取某一时刻之前最近版本”的算子。
2. **有界时间能力**：没有 event time、watermark 或 late-data contract，因此不能安全声称支持会自动淘汰状态的 window/interval join。

其中有界时间能力是硬边界。`Change` 明确没有事件时间、watermark、来源 offset 或物化关系；日志 offset 加行号也不能充当长期事件 ID。由此可知，任何依赖“时间已经过去，所以旧行永远不会再匹配”的算子都会引入跨 Change、Operation、Flow 和 source 的新协议，而不只是新增一个 Operation。

## 业界能力地图

流式系统里的“Join”至少分成五类，名字相近但状态与修正语义差别很大。

| 类别 | 典型语义 | 状态行为 | 对 DogPaddle 的意义 |
| --- | --- | --- | --- |
| Regular dynamic join | 任一侧变化都会修正完整结果 | 通常永久保留两侧历史 | 当前 EquiJoin 已覆盖主干 |
| Predicate/theta join | `ON` 是任意布尔条件 | 无索引时接近笛卡尔探测 | 应先限制在等值分区内 residual |
| ASOF/nearest join | 每个 probe 行选择有序维度上的一个最近匹配 | 输出不做多对多膨胀；动态 build 更新可能重配一段 probe 行 | 最适合当前下一步 |
| Interval/window join | 只匹配时间范围内的事件 | watermark 推进后可回收旧状态 | 需要先设计时间与迟到语义 |
| Temporal/lookup join | probe 到达时读取维表某个版本或外部当前值 | 可少存状态，但依赖外部快照、缓存和 I/O 一致性 | 与当前确定性重放边界冲突较大 |

Flink 把 Regular、Interval、Event-time Temporal、Processing-time Temporal 和 Lookup Join 分开建模。它明确指出 Regular Join 需要永久保留两侧状态；Interval Join 依靠 time attribute 的准单调性清理状态；Event-time Temporal Join 由两侧 watermark 触发，并可能丢弃已晚于 watermark 的 probe 行。[^1] Spark Structured Streaming 同样要求通过两侧 watermark 加跨流时间约束，才能判断旧状态何时不再可能匹配；Outer Join 的 NULL 行还必须等到引擎能确认未来不会再有匹配时才输出。[^2]

这两套系统说明：**Interval Join 不是“EquiJoin 多一个 `<` 条件”**。只要产品承诺有界状态和正确的 unmatched 输出，它就是时间进度协议。

另一条路线是 ASOF。DuckDB 将它定义为：按等值条件分组，再用一个不等式有序列选择最近右行；每个左行最多匹配一个右行，Left ASOF 在无匹配时补 NULL。[^3] RisingWave 也支持流式 ASOF，并要求至少一个 equality 条件和一个 inequality 条件；有序属性通常是时间，但不强制必须是时间类型。[^4] Apache DataFusion 当前文档采用 Snowflake 风格的 `MATCH_CONDITION`，同样覆盖四个方向，并把 `ON`/`USING` 用作等值分组。[^5]

ASOF 的关键优势不是语法新奇，而是**受控的输出基数**。普通 range/theta join 可能把一个输入变化扩散为整个范围内的所有配对；ASOF 对每个左行只保留最近的一个候选。这对 CDC 管道尤其有价值：订单关联“下单时有效的客户等级”、交易关联“当时最近报价”、设备事件关联“当时生效配置”、审计事件关联“当时组织归属”都是同一个模型。这里必须区分输出与状态：没有 watermark/retention 时，ASOF 仍要永久保留可能被撤回或被历史修正影响的两侧输入，因此它控制输出膨胀，但不承诺有界状态。

## 候选排序

评分采用 1–5，越高越好；“风险可控”高分表示风险较低。

| 候选 | 用户价值 | 当前架构契合 | 确定性/恢复 | 状态可控 | 风险可控 | 建议 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Dynamic ASOF Join | 5 | 4 | 5 | 3 | 3 | **下一种新算子** |
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

这个里程碑已经补齐标准 `ON key equality AND predicate` 的正确性，也为 ASOF 的候选 predicate、未来 range specialization 和 Join 内减少中间 Change 打下基础。实现没有另造一个与 EquiJoin 大量重叠的公共 `PredicateJoin` 类型，而是演进 `EquiJoinDefinition` 的持久 payload、状态布局和当前 v1 golden。

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
ASOF LEFT JOIN prices
MATCH_CONDITION (orders.ordered_at >= prices.valid_from)
ON orders.sku = prices.sku;
```

四个方向定义如下：

| 条件 | 选择的右侧 order value |
| --- | --- |
| `left >= right` | 小于等于 left 的最大 right |
| `left > right` | 严格小于 left 的最大 right |
| `left <= right` | 大于等于 left 的最小 right |
| `left < right` | 严格大于 left 的最小 right |

第一版建议要求至少一组 equality key。虽然纯全局 ASOF 在 SQL 上有意义，但强制分区键能限制 rematch fan-out，也贴合 CDC 中按业务实体/产品/设备做版本查找的主要场景。

### 动态关系，而不是 processing-time lookup

建议采用**完全动态关系语义**：

- 左行插入：选择当时关系中最近的右行并输出；
- 左行撤回：撤回它当前应匹配的结果；
- 右行插入：如果它成为一段既有左行的新最近候选，撤回旧配对并插入新配对；
- 右行撤回：受影响左行退回到下一个最近候选，或在 Left ASOF 下变成 NULL padding；
- reopen：只从持久关系状态和 continuation 推导同一结果，不读取墙钟或 source metadata。

这比 processing-time temporal join 更贵，但语义与 DogPaddle 现有 `+/- diff`、错误回滚和 reopen 一致。RisingWave 文档明确区分两者：process-time temporal join 只在左侧变化时产出，右侧变化只影响未来 lookup；ASOF 则是有序最近匹配。[^4] DogPaddle 不应让同一个名字混合这两种行为。

### NULL、类型与 tie

第一版应做窄而确定的合同：

- equality key 与 order expression 都必须是 immutable DataFusion expressions；
- 左右对应表达式必须具有完全相同类型；
- NULL equality key 或 NULL order value 不匹配；
- order type 只接受能提供稳定全序、且能稳定编码的 flat non-floating 类型；
- Float32/Float64 暂不支持，避免 NaN、signed zero 和 total ordering 规则成为持久 ABI；
- 同一 equality partition 内，若两个不同右行具有相同 order value，第一版运行时拒绝该前缀，整个 Change 回滚；相同 canonical right row 的 multiplicity 只表达 presence，不制造多个 ASOF 输出。

最后一条非常重要。DuckDB/Calcite 对“最近”的定义并不自动解决并列候选的确定性；RisingWave 也提示 batch ASOF 在多个右行共享最近 ordered property 时结果可能跨执行变化。[^4] DogPaddle 的恢复语义不能接受任意选择。长期可以增加显式 tie-break expression，但第一版拒绝歧义比暗含 canonical-row 次序更容易解释。

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
| `asof_join.continuation` | 当前 port、输入 row、probe/rematch phase、range cursor |

如果 profiling 证明右侧更新时反复计算当前匹配过贵，再增加一个派生的 left-match cache；第一版不应同时持久化能从两个有序关系索引推导出的第三份事实。

现有 Store 已有 owned、可分页、带 range 和 continuation 的 `OrderedMap::scan`，这正是 ASOF 所需的后向/前向最近查找与受影响区间扫描基础。现有 `PartitionedMultiset` 只有整分区方向扫描，没有 range 参数，因此不要为了复用 EquiJoin 的 `Rows` 类型而把 range 语义塞进上层过滤；用一个准确的新 key codec 或在真实调用压力证明后再增加窄 Store 结构。

### 增量算法轮廓

以最常见的 `left.order >= right.order` 为例：

1. **左行变化**：在相同 equality partition 中，反向查找 `right.order <= left.order` 的第一个右行；Inner 无匹配不输出，Left 无匹配输出 NULL padding。
2. **右行插入 r**：找到同分区中 r 的下一个更大右侧 order `successor(r)`；只有 `left.order ∈ [r.order, successor(r).order)` 的左行可能改配到 r。分页扫描这段 left range，逐行撤旧增新。
3. **右行删除 r**：同一 left 区间受影响，新的候选是 r 的 predecessor；没有 predecessor 时按 kind 删除结果或改成 NULL padding。
4. **同一 Change 多行**：严格按输入行序观察，每个前缀都做负 multiplicity、overflow 和 tie 检查；所有状态与输出仍在一个 Change 的事务边界内。

其他三个不等式只是方向和端点开闭变化，不应复制四套 runtime。Definition 在 bind 时把它们正规化成“查 predecessor/successor + affected interval”的内部方向枚举。

## DataFusion 接入风险

仓库精确 pin DataFusion 55.0.0 与 sqlparser 0.62.0。后者 AST 已有 Snowflake 风格 `JoinOperator::AsOf { match_condition, constraint }`；但当前本地 `datafusion-sql` 55.0.0 的 `SqlToRel` 没有 ASOF 分支，会明确返回 `Unsupported JOIN operator`，`datafusion-expr` 也没有公开 ASOF logical node。与此同时，DataFusion 在线文档已经描述 ASOF SQL。[^5] 这说明在线文档/开发分支的能力领先于仓库当前 pin，而不是 DogPaddle 已经能直接取得 ASOF logical plan。因此 SQL 接入前必须做一个很小的 compatibility spike，确认：

1. 可升级的 DataFusion 版本是否把 ASOF 表示为稳定 logical node；
2. 升级是否保持现有 Expr protobuf、physical-expression binding 和 analyzer 语义；
3. 新 logical node 是否保留 equality grouping、comparison direction 和精确 output Schema 所需的全部信息。

不要为了赶 SQL 语法而在 DogPaddle 内另写一套 ASOF expression analyzer。合理顺序是 Operation API 先落地并完成 correctness；SQL lowering 等 DataFusion 提供足够 logical representation，或在一次经过完整升级审查后接入。DataFusion 自己也把 ASOF/Range Join 视为专门 Join 能力，历史 issue 指出普通不等式 Join 容易退化为 nested loop。[^9]

## 建议路线图

### J0：Join residual 完整性（已完成）

- `EquiJoinDefinition` 已增加可选 immutable residual；
- Inner/Outer/Semi/Anti 已统一在候选 pair 上求值，`false` 与 `NULL` 均不成匹配；
- residual presence state 已改为具体行的 qualifying-match count，并覆盖分页、回滚与 reopen；
- SQL 已放开 Outer/Semi/Anti residual，并正确重写 Right 家族交换后的字段朝向；
- owner benchmark 已同时保留无 residual control，并增加 Inner、LeftSemi、FullOuter residual workload。

这一步已经校验了“候选 pair predicate + per-row match transition”的通用正确性；下一步直接进入 J1。

### J1：AsOfJoin Operation

- `AsOfJoinKind::{Inner, LeftOuter}`；
- equality key pairs + one ordered pair + direction；
- 三个持久资源与 stable tag/golden；
- 两侧 insert/retract、multiplicity、NULL、tie、overflow、分页背压、reopen；
- operation owner benchmark，覆盖 probe lookup 与 build-side rematch。

### J2：SQL lowering

- 完成 DataFusion compatibility spike/必要升级；
- 只接受明确的 ASOF AST/logical node；
- output Schema 严格对齐 DataFusion；
- Program identity 覆盖 direction、表达式与 kind；
- 端到端 PostgreSQL/MySQL CDC → ASOF → SQLite/PostgreSQL system case。

### J3：有界扩展

- 可选 tolerance，例如“最近但不超过 15 分钟”；
- 先作为普通 immutable predicate，不承诺状态 GC；
- 单独设计 event-time/watermark 后，才把 tolerance 升级为可回收状态的时间算子。

### J4：后续候选

按需求证据选择其一：

- `MarkJoin` + subquery lowering，支持 `EXISTS/IN/NOT IN` 的三值逻辑；
- equality-partitioned `RangeJoin`，输出范围内全部匹配；
- snapshot-safe `LookupJoin`；
- multi-way Join arrangement reuse / join-order optimization。

## 验收标准

### Correctness

- 四个 comparison direction；
- Inner 与 Left Outer；
- 多 equality keys 与表达式 order keys；
- NULL key/order；
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
Dynamic ASOF Join (Inner + Left)
        ↓
ASOF tolerance without GC
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

### Repository evidence

- `crates/change/README.md`, lines 64–74: Change ordering and absence of event time/watermark/source offset.
- `crates/operation/src/operation/transform/equi_join/definition.rs`: current tag, residual payload, exact candidate-Schema binding and resource selection.
- `crates/operation/src/operation/transform/equi_join/runtime.rs`: pure-equality fast path and residual `Probe → ClearShadow → Emit` state machine.
- `crates/sql/src/plan.rs`: equality extraction, native residual lowering and Right Join orientation.
- `crates/store/src/collections/ordered_map.rs`, lines 135–185: owned paged range scan with continuation.
- `Cargo.toml`, lines 36–42: exact DataFusion 55.0.0 pin.
