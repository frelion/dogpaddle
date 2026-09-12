# Join 设计与实现说明

状态：Inner Equi Join 首个切片已实现
日期：2026-09-12

线性 Station 与 SQL deterministic grouping 已经直接落入当前 v1：一个 Station 保存一个非空、有序的普通 Operation 列表，列表共享事务且只持久化最终输出。其 canonical 设计、事务语义、装配规则和验证证据统一记录在
[`station-pipelines-and-durable-boundaries.md`](station-pipelines-and-durable-boundaries.md)。本文记录已落地的 Inner Equi Join 语义、状态与后续 Join family 边界。

## 目标与决策

第一步实现二输入 `InnerEquiJoinDefinition`。它直接拥有左右 key expressions，因此：

```sql
... JOIN right ON normalize(left.a) = right.b
```

不需要先创建 helper Extend，也不产生只为 key 计算服务的中间持久日志。需要跨 turn continuation 的 Join 声明为多输入 `TurnTransform`，位于 Station 首项并可吸收后续单输入 Atomic；Join 的当前页、continuation、尾项状态和最终 output 在同一事务提交。

当前决策是：

- Join Definition 持久化非空、有序的左右 key pairs 和左后右的完整 output names；
- port `0` 永久表示 left，port `1` 永久表示 right；
- 第一版只实现 SQL `=` 语义的 Inner equi Join；
- Join 私有拥有左右两份 keyed relation state，不先建立共享 arrangement；
- 每个 Claim 先完整预检，再用 durable continuation 有界发布匹配结果；
- residual、Semi/Anti 和 Outer Join 在 Inner Join 正确性闭合后依次加入；
- Definition 和 state 直接属于实现时的当前 v1，状态路径变化时删除旧库重建。

表达式归属遵循一个统一规则：决定匹配、分组、排序或窗口语义的表达式，由对应关系算子的 Definition 持久化并针对 exact input Schema 绑定。Station fusion 只决定在哪里执行，不改变表达式的语义 owner。

## 相关研究

这些设计选择与成熟流处理系统的边界一致：

- Materialize 将 Join 的 keyed state 建模为 arrangement，并在 identity 完全一致时复用；Join key 因此属于 Join/arrangement，而不是独立持久投影。[plan operators](https://materialize.com/docs/sql/explain-plan-operators/)、[arrangements](https://materialize.com/docs/fundamentals/concepts/arrangements/)、
  [Shared Arrangements](https://arxiv.org/abs/1812.02639)
- Flink 的 regular streaming Join 会持续保留两侧状态；TTL 会影响迟到记录和 retract 的正确性，不能成为默认细节。[Join documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/sql/reference/queries/joins/)
- DBSP 对双线性算子的增量公式说明，一侧 delta 与对侧当前 relation 相乘即可；DogPaddle 的顺序端口提交使交叉 delta 只产生一次。[DBSP, Theorem 3.4](https://arxiv.org/html/2203.16684)
- Calcite 区分带 qualifier 的逻辑列身份和执行阶段的字段位置；DogPaddle 同样先解析逻辑身份，再生成唯一 Arrow 字段名。[algebra](https://calcite.apache.org/docs/algebra.html)

## Definition 与 Schema binding

当前稳定 tag 是 `16`：

```text
JoinKeyPair {
    left:  Expr,     # 只对 input 0 Schema bind
    right: Expr,     # 只对 input 1 Schema bind
}

InnerEquiJoinDefinition {
    keys: NonEmpty<JoinKeyPair>,
    output_names: left-all then right-all,
}
```

持久格式从一开始就允许多个 key pair。实现证据可以先覆盖单 key，再开启 multi-key，无需改变 Definition 形状。左右 expressions 继续使用当前精确 pin 的 DataFusion protobuf；类型 coercion 必须由上层显式写成 `cast`，Operation bind 不插入隐式转换。

bind 必须验证：

- Definition 恰好接收两个 ordered input Schemas；
- 每个 left expression 只绑定 input `0`，每个 right expression 只绑定 input `1`；
- 每对 key 在显式 coercion 后具有完全相同的类型；
- key 类型具有稳定、无碰撞的 canonical equality encoding；
- output names 数量等于左右字段总数且名称唯一，类型、nullability 与 metadata 按左后右完整保留；
- 每个表达式和最终 output 都通过现有 DogPaddle Schema guard。

SQL `=` 下，只要复合 key 的任一分量为 NULL，该行就不与对侧匹配。本侧记录仍须进入 state，确保以后 retract 可以做 exact admission。`IS NOT DISTINCT FROM` 的 NULL equality 是独立语义，不能复用同一 Definition。

## SQL 列身份与 lowering

DataFusion logical `DFSchema` 可以用 qualifier 区分 `left.id` 和 `right.id`，Arrow Schema 与 DogPaddle Change 则要求字段名唯一。Join lowering 不能依赖输入名字碰巧不冲突，也不能在运行时覆盖同名字段。

lowering 应携带类似以下私有信息：

```text
LoweredRelation { node, physical_schema }
```

DataFusion logical `DFSchema` 保留 qualifier；lowering 用它解出字段 ordinal，再按 `physical_schema` 同一 ordinal 改写为唯一物理字段名。不另存一张 column map。Join Definition 只保存左后右的全量 output names；Join 后的 projection 继续由 SchemaAlign 表达，并可由现有 grouping 追加到 Join 所在 Station。

对 `JOIN ... ON ...`，SQL lowering 按以下顺序工作：

1. 展平 `AND` conjunction；
2. 识别一侧只引用 left、另一侧只引用 right 的 equality；
3. 保留 Analyzer 已经插入的显式 cast；
4. 规范化 pair 方向为 `(left expression, right expression)`；
5. 把其余条件保留为 residual；
6. 在 residual 尚未实现时，于访问 Store 前明确拒绝。

函数仍服从现有 expression persistence 与 volatility 契约。Join key 只接受确定、可重放且能够从 protobuf 重新绑定的 scalar expression；时间、随机、session variable 和依赖外部 registry 的函数不进入第一版。

## 私有状态布局

Inner Join 的最小状态是两份私有 keyed multiset 和一个 continuation：

```text
inner_join.left_rows:  PartitionedMultiset<CanonicalJoinKey, CanonicalRow>
inner_join.right_rows: PartitionedMultiset<CanonicalJoinKey, CanonicalRow>
inner_join.continuation: Cell<JoinContinuation>
```

布局规则：

- partition key 保存完整 canonical composite key bytes，不使用可能碰撞的短 hash；
- entry key 保存完整 canonical input row bytes；
- multiplicity 由 Store 表示为正 `u64`，缺失表示零；
- canonical key/row codec 绑定 exact Schema，decode 后必须恢复全部当前 Change v1 类型；
- resource 名、key/value codec 和 continuation encoding 都是 Join state schema。

当前 relation 模块已有 canonical row encoder。Join 还需要 exact-Schema-bound decoder，从持久 row bytes 重建 probe 输出。第一版补齐 encoder/decoder roundtrip 和 corruption rejection，不为每行保存完整 Arrow IPC。

普通无界 Join 必须保留两侧所有正权重记录。若以后需要 TTL，它必须成为带明确 event-time、watermark、迟到和 retract 语义的独立能力。

## 增量更新语义

设本侧输入事件的 diff 为 `d`，对侧某个匹配 distinct row 的当前权重为 `w`：

```text
output_diff = checked_mul(d, w)
```

一个输入 Change 按行序处理。每行必须：

1. 编码本侧完整 row 与 join key；
2. 按现有 multiplicity 做 exact admission，拒绝负权重前缀；
3. 对侧按同一 key 分页扫描 distinct rows；
4. 对每个匹配 row 检查 `d × w` 的 signed overflow；
5. 以固定 left-then-right 字段顺序构造输出；
6. 提交本侧 adjustment、当前页 output 与 continuation。

若同一 Change 内同一行多次出现，admission 必须按事件顺序累计，不能先 consolidation 后掩盖负前缀。任何 key 编码、row decode、multiplicity 或 diff overflow 都属于业务错误，不能留下半个 state transition。

当前 Station 会在同一 Claim 完成前 durable-pin active input，因此 Join 处理 port `0` continuation 时，port `1` state 不会被同一 Station 的另一个 turn 改变。后一端口在前一 Claim 完成后看到已提交 state，从而只生成一次交叉 delta。

## 有界 fan-out 与 continuation

一个输入行可能匹配大量对侧 distinct rows。Join 不能在单次事务中扫描整个 partition 或构造无界 Change。首先把 `PartitionedMultiset::scan` 补成 owned page API：

```text
scan(direction, resume_after, ScanLimit { max_items, max_bytes })
    -> page { entries, continuation }
```

Store 在返回前完成 item/byte admission、整页复制和 decode；错误不返回半页。continuation 稳定表达下一页位置，并在同一 transaction snapshot 内保持严格顺序。

Join 对一个完整 Claim 使用两阶段协议：

1. 没有 continuation 时，先按行序检查完整 Change 的本侧 exact admission 和最终权重，不修改业务 state，并在同一事务内直接开始 `Probe`；只有遇到工作边界才持久化 continuation；
2. `Probe` 在固定 item/byte 工作预算内遍历全部对侧候选，空分区也计一个 work item；它检查 row decode 和 output diff，不产生 output；
3. `Emit` 只有在完整 Probe 成功后才再次遍历，同一 turn 可聚合多个小分区，提交本侧 state、output 和 continuation；最后一块工作完成输入。

第二遍 range read 保证当前 Claim 数据决定的业务错误在其任何 state/output 发布前出现。若真实性能数据证明读放大不可接受，再评估每个 key 的 weight summary。

`JoinContinuation` 只保存：

- 当前 input port；
- `Probe | Emit` phase；
- 当前 input row ordinal；
- 对侧 partition 的排他 resume key。

canonical key、input row、diff、Subscription position 和 Change fingerprint 都不重复持久化。reopen 后仍由 Subscription 提供同一完整 Claim，运行实例从该 Claim 重建临时 row/key cache；Station 的 durable active pin 保证 continuation 期间对侧状态不变，因此 resume key 也不需要额外存在性校验。

Emit 在当前 row 的最后一页才调整该 row 的本侧权重；一个 turn 可以按固定总预算完成并聚合多个小分区。预算不足时先提交已有工作，下一 turn 再处理当前 row；只有空 turn 可以单独推进一个超过 byte budget 的不可拆项。中间块使用 `Action::Commit`；最后一块原子提交最终 output、清除 continuation 并用 `Action::Complete` 完成输入。背压、尾链错误和 commit failure 一起回滚本块；reopen 从已提交 phase/row/resume key 继续。

## residual 与 Join family

扩展顺序固定为：

1. Inner equi Join，Definition 已支持非空 multi-key；
2. equi partition 内 residual predicate；
3. Semi/Anti Join；
4. Left Outer Join；
5. Right/Full Outer Join。

residual 是 Join Definition 的组成部分。Inner residual 有时可等价为下游 Filter，但 Outer、Semi 和 Anti Join 用它判断一行是否真正 matched，不能统一外提。

Outer Join 还要按 canonical row 维护通过 residual 的 match weight：第一个 match 出现时撤回 null-extended row，最后一个 match 消失时重新插入。对应侧 output nullability 在 bind 时显式放宽；每种 family 使用独立关系 oracle。

## Arrangement 的后续边界

第一版左右 `PartitionedMultiset` 只归当前 Join 所有。等至少两个真实消费者需要相同 keyed state，且 profile 证明重复维护显著，再考虑单 Flow 内复用：

```text
ArrangementSpec {
    producer_identity,
    ordered_key_exprs,
    value_projection,
    null_equality,
    key_codec_version,
    row_codec_version,
}
```

只有 `ArrangementSpec` 完全相同才能复用。跨 Flow 共享还涉及 owner、subscriber 生命周期、retention、删除、Schema/version、失败域和跨 Flow transaction，不进入当前 Join 工作。

## 实施里程碑

| 阶段 | 工作 | 退出标准 |
| --- | --- | --- |
| J0 Store/codec（已完成） | 给 `PartitionedMultiset` 增加 item/byte 分页和 continuation；补 canonical row decoder 与 composite key codec；验证 roundtrip、NULL、损坏和 resume | Join 只依赖公共 Store collection contract 即可有界扫描并恢复完整 row |
| J1 Inner Operation（已完成） | 新增 tag `16`、key pairs、三个资源、whole-Claim preflight、分页 Emit；覆盖端口交错、diff、NULL、retract、overflow、fault 和 reopen | 每块 state/output/continuation 同事务提交，最终块完成 Claim，恢复不重不漏 |
| J2 SQL lowering（已完成） | 用私有 `LoweredRelation` 保留物理 Schema；按 logical Schema ordinal 改写 qualified column；从 `ON` 提取 equi pairs 与显式 cast；先拒绝 residual；接入现有 arena、确定性分组 pass 和当前 v1 Flow Definition | `JOIN ON deterministic_func(left.a) = right.b` 没有 helper Station/log，build/open 保持相同 grouping 和 relation |
| J3 Join family | 加入 residual，再依次实现 Semi/Anti、Left Outer、Right/Full Outer | 每种语义有独立 multiset oracle 和 nullability witness |
| J4 优化 | 根据 profile 决定 weight summary、单 Flow arrangement reuse、join order 或成本模型 | 只引入由真实 workload 证明收益的状态与规则 |

## 验证与性能

Operation correctness 至少覆盖：

- tag 唯一性、literal golden、Definition roundtrip 和精确资源布局；
- 两侧 Schema bind、key 类型、output identity/nullability/metadata；
- materialize、turn、分页 continuation 和 reopen；
- 每种左右端口交织与不同 Change 重批下的最终 multiset；
- residual 前后、NULL key、重复 row、非单位正负 diff；
- 负前缀、所有 checked overflow、row corruption 和 continuation mismatch；
- output 背压、apply/commit fault 全部回滚，`AfterCommit` 不提前运行；
- catalog 中只有 Join 私有 state 和 Station 最终 output，没有 helper expression log。

SQL correctness 额外覆盖 qualifier/ordinal identity、同名字段、key expression/cast lowering、拒绝 residual 的无路径
副作用，以及 build/open 不重新分组。

owner benchmark 在 fixture 构造和结果校验位于计时外的前提下记录：

- probe partition cardinality 和输出 amplification；
- 每个 Claim 的 preflight/emit page 数与 transaction 数；
- item/byte page limit 下的峰值 Change bytes；
- 两遍 scan 的 read bytes、rows/s 和 p50/p95/p99；
- RocksDB write/WAL/compaction bytes。

普通 correctness 通过后再建立 reference profile 和回归阈值；最终工作区验收运行 `cargo xtask check`。

## 当前不做

- 不把 Join key 降低为 helper Extend；
- 不实现 cross Join、非等值无界 Join 或隐式 nested-loop fallback；
- 不给无界 relation 暗加 TTL；
- 不在第一版共享或跨 Flow 持有 arrangement；
- 不让 open 依据新统计或 planner 重新选择 physical grouping；
- 不开放没有确定性和 replay 契约的函数；
- 不把 Join 的多输入状态机与 Station 线性执行协议合并成第二套执行引擎。
