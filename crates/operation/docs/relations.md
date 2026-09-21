# 有状态关系算子契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## Distinct

`Distinct` 的 tag 是 13，是单输入、exact-Schema-preserving Transform，Definition payload 为空，只声明 `distinct.weights: OrderedMultiset<Vec<u8>>`。
key 是完整 canonical row bytes，multiplicity 是 Store 维护的正 `u64`；缺失表示零，checked signed adjustment 归零即删除。
输入按行序逐事件更新：负前缀和 overflow 回滚整个 turn，仅 `0 → positive` 输出 `+1`、`positive → 0` 输出 `-1`。
状态、output 和 input completion 同事务提交，背压与 reopen 保持同一输入语义。
SQL `SELECT DISTINCT` 复用这一 exact-row identity，包括按原始位模式区分浮点值。
canonical Arrow row 编码和 diff 语义留在 operation crate 私有 `relation` 模块；Store 只提供通用 multiplicity，不预建 Aggregate/Join 的关系框架。
现有关系 Sink 继续使用同一 canonical row 的 16-byte `row_hash` ABI。

## Aggregate

`Aggregate` 的 tag 是 14，是单输入的 grouped relational Transform；至少一个 group expression，aggregate call 可以为空。
Definition 保存有序命名 group expression 和有序 `AggregateCall`，输出固定为 group fields 后接 call fields。
它只声明 `aggregate.groups: OrderedMap<Vec<u8>, GroupState>`、`aggregate.entries: PartitionedMultiset<EntryPartition, Vec<u8>>` 和 `aggregate.control: Cell<u64>`：groups 以完整 canonical group 为 key，保存稳定 group ID、正 group weight、每个 Fold call 的小状态与每个极值 slot 的缓存极值；entries 的 partition 是 `layout + group ID`，每个 layout 对应一个不同的排序表达式，只维护有序的 extrema argument key，不保存完整输入行；control 只分配不复用的 group ID。
极值 slot 在绑定期按 `(layout, 方向)` 去重产生，`MIN(x), MAX(x)` 共用一个 layout、两个 slot，重复的同一调用复用一个 slot；group state 里的缓存是该 slot 当前极值键的保序字节，与 entries 在同一事务更新，只有被撤回的正是缓存极值时回分区重取 `first`/`last`，因此 NULL 参数既不进分区也不进缓存。
静态函数 descriptor 唯一声明 stable function tag、arity、binding 和 `Fold`/`Extrema` reduction；COUNT/SUM/AVG 使用每组定长 Fold state，MIN/MAX 从缓存极值取结果，函数实现只接收值或小状态，不接收 Store。
校验按「分组 + 调用参数」而不是按记录进行：只有从未出现过的分组遇到负 diff、分组行数减为负、某个 Fold call 的非空参数计数减为负、某个极值参数的份数减为负才报错；撤回一行而它的参数组合被其它行覆盖不再报错，需要记录级身份的算子继续按完整行记账。
每个输入事件按行序完成全部 call 更新和旧行 `-1`/新行 `+1`；组首次出现只输出 `+1`，消失只输出 `-1`，结果未变不输出，整个 Change 的状态、output 和 input completion 同事务提交，任何负权重、overflow 或背压均不留下部分状态。
COUNT 输出 non-null `Int64`；SUM 仅接受 `Int64/UInt64` 并保持类型；AVG 仅接受 `Int64/UInt64`，以 `i128/u128` 累计后输出 nullable `Float64`；MIN/MAX 接受 non-float flat scalar（Null、Boolean、整数、Utf8、Binary、Date32、Timestamp、Decimal128）并输出 nullable 同类型。
group key 不能包含 Float32/Float64；global aggregate、grouping sets、aggregate modifier、UDF、浮点 SUM/AVG/MIN/MAX、List/Struct MIN/MAX 均不属于 v1。

MIN/MAX 的 NULL 参数不进入 entries/cache。
参数级撤回允许组归零时存在不可达旧 extrema keys；归零必须在同一事务按 layout 清空，空分区仍执行边界检查。

## EquiJoin

`EquiJoin` 的 tag 是 16，是两输入 `TurnTransform`；port `0` 固定为 left，port `1` 固定为 right。
Definition 显式保存 `EquiJoinKind::{Inner,LeftSemi,LeftAnti,LeftOuter,FullOuter}`、非空有序 key-expression pairs、output names 和可选 residual；Semi/Anti 只输出 left fields，Inner/Outer 输出 left 后 right，Outer 自动把可能补 NULL 的 fields 放宽为 nullable。
它只接受可重放的 immutable expression、精确相同且可 canonical 编码的扁平非浮点 key；复合 key 任一分量为 NULL 时不匹配。
Residual 在原始 exact input fields 组成的 `left.* + right.*` candidate Schema 上绑定，必须返回 Boolean，只有 non-null true 匹配。
所有 kind 声明 `equi_join.left_rows` / `equi_join.right_rows: PartitionedMultiset<Vec<u8>, Vec<u8>>` 和 `equi_join.continuation: Cell<JoinContinuation>`：两侧按完整 canonical key 分区，以完整 canonical row 及正 `u64` multiplicity 表示关系。
无 residual 的非 Inner 另外声明 `equi_join.key_counts: OrderedMap<Vec<u8>, KeyCounts>`，值是该 key 左右两侧的正 distinct-row counts；NULL key 不进入 counts，zero/zero 必须删除。
带 residual 的非 Inner 改为声明 `equi_join.match_counts: OrderedMap<Vec<u8>, u64>`，按完整行记录 qualifying distinct opposite rows；FullOuter 跟踪两侧，其他 presence kind 只跟踪 left，真实零必须缺失。
当前 v1 match-count key 为单字节 port 加完整 canonical row；continuation 的 v1 codec 不含 phase。旧布局的数据库需重建，不提供格式识别、迁移或兼容路径。
每个 Claim 先按行序预检本侧 exact admission 和同 Claim 的 presence transitions，在发布输出前拒绝本侧负前缀和 `u64` multiplicity overflow。随后直接分页扫描对侧、求值 residual、更新真实 support 并构造输出，不预演整个 Claim，也不保存影子计数。
每页同事务提交真实状态、output 与 continuation；每个 outer presence transition 的 null correction 与对应 pair 作为同一分页 work item，当前输入行最后一页同事务调整本侧 rows/counts，最后一行清理 continuation 并 Complete。
Station durable active pin 保证 Claim 完成前对侧状态不变；continuation 只保存 port、row ordinal、本行 match marker 和排他 resume key，不复制 Subscription identity、Change 或 fingerprint。
Inner 保持三资源热路径；无 residual 不访问 match counts；五种语义共用一个 Definition/runtime，不建立 per-kind Operation、arrangement、Join Station 或第二套执行协议。

`EquiJoin` 的输入准备逐个求值并编码 key expression，释放当前 key array 后再处理下一个；全部 key 完成后才编码完整行。
整批 admission 在任何输出发布前完成，不增加输入硬上限。

错误边界是一笔 turn 事务。后续页面的 residual 求值、存储行解码、typed NULL output 或
`i64` output-diff overflow 可能在前面页面已经发布后失败；合法输入和合法的两侧 multiplicity
也可能触发计算错误。当前失败页全部回滚，之前提交的状态、输出与 continuation 保留，
下游及外部 Sink 可能已经看到部分结果。状态可能停在一个输入行处理到一半的位置，
不能把它解释为完整输入事件前缀的最终关系。只有 Complete 才确认整个 Claim。

reopen 从持久的排他游标继续，不重复已提交页，也不跳过失败页；确定性错误仍会在同处失败。
恢复不补偿已发布输出、不自动删除状态，也不提供 skip 或修复坏 Claim 的接口。
当前行的本侧 rows/counts 仅在最后一页更新，reopen 从当前行重建 admission；
已提交的 actual support 与 continuation 共同描述行内进度。PreparedClaim 不缓存可推进的分页游标。

成功执行时，对固定有序 `(port, Change)` 输入，展平后的有序 `(row, diff)` 输出不因分页变化而改变。
turn 数会影响多输入 Flow 的调度交错，因此不承诺全图差分轨迹、输出时间、IPC 字节或
RunningEventCount 结果不变；纯关系链在每源顺序相同且成功执行到静止后应得到相同最终关系。

`PreparedClaim` 只为整批保留 canonical row、join key、diff 和 admission effect，不再保留每行的全量
`ScalarValue`；每个 turn 处理当前 row 时，仅在 predicate 或真实输出需要字段值时，才从 Station 固定的
`RecordBatch` 惰性物化一次短期 values。
空 bucket、无输出存在性路径和稳定 Semi/Anti 右侧更新不会复制宽行。

Residual 候选的
常规单批上限是 256 行、1 MiB Store logical bytes 和 16,384 个 candidate scalar slots；实际行数还受
candidate 字段数及当前 turn 剩余预算约束。
每批会完整解码候选并构造 Arrow candidate batch，但
LeftSemi/LeftAnti 的左侧 driving row 只保留 qualifying count，其他路径也只把 predicate 通过的候选
values 带入当前输出阶段。

`TURN_ITEMS` 和 `TURN_BYTES` 以 256 项和 4 MiB 限制常规单 turn 的逻辑扫描、ScalarValue slot、
输出和事务工作量。
分区扫描按每个候选重复计算 partition frame、完整 join key、row key 与 multiplicity，
driving row 的持久访问也至少逐处理页计入；宽计算 key 或 LeftSemi/LeftAnti 的右侧宽行不会逃逸预算。

这些值不是进程 RSS 硬上限：Station 仍已持有完整 Change，Arrow/DataFusion 可以产生
额外中间分配，且空 turn 遇到单个超过批字节或 scalar-slot 界限的 Store row 时会单独处理它，以避免永久
停滞。
因此峰值至少是 `O(Claim + candidate page)`，还有“单个 oversized row”的活性例外。
逐行
`match_counts` 以完整 canonical row 为 key，持久状态与 tracked rows 的总宽度成正比；分页也不限制
整个 Join 关系的磁盘大小，无法消除连接结果本身的高 fan-out 成本。

Semi/Anti 同一 exact row 仅改变正 multiplicity 时不重扫对侧 bucket；right 更新不改变 support，left 更新直接读已有 actual count。
Driving-row count 按 qualifying page 合并，对侧 distinct-row count 分别更新。

## AsOfJoin

`AsOfJoin` 的 tag 是 17，是两输入 `TurnTransform`；port `0` 固定为 left/probe，port `1` 固定为 right/candidate。
Definition 保存 `AsOfJoinKind::{Inner,LeftOuter,LeftSemi,LeftAnti}`、零或多组 `Equal`/`NotDistinct` equality pairs、非空 lexicographic order pairs、`Backward`/`Forward`/`Nearest` direction 与 exactness、nearest 等距偏好、显式 right tie-break、canonical/reject fallback、可选 inclusive tolerance、output names 和可选 candidate residual。
Equality/order/tie 只接受左右精确同型且可稳定 canonical/order 编码的 flat non-float scalar；nearest/tolerance 只允许一个 distance-capable order，其类型为整数、Date32、Timestamp 或 Decimal128。
它声明 `asof_join.left_rows`、`asof_join.right_rows: OrderedMap<Vec<u8>, RowWeight>` 与 `asof_join.continuation: Cell<AsOfContinuation>`，完整索引 key 依次编码 equality partition、order、right rank 和 canonical row；缺失代表零 multiplicity，只有 RHS presence 的 `0 ↔ positive` 才触发历史 rematch。
每个 Claim 先整批 preflight 非负权重/overflow，再以 Probe/Emit 两阶段和持久的 outer/candidate cursors 分页；Probe 只验证全部选择、residual、ambiguity、decode 和输出 diff，Emit 才按事件顺序原子提交 right state、旧结果 `-left_weight`、新结果 `+left_weight` 与 continuation。
候选 right scan 和 RHS rematch 的 left outer scan 都必须从 matchable-order marker 精确 seek，不得重复读取同 partition 中永远不匹配的 NULL-order history。
运行期只缓存 pinned Claim 的 prepared rows、effects 与一份按 active row 增量维护的 before/after visibility overlay；rollback/reopen 时由 durable row/phase 重建，不能持久化第二套 input identity。
没有 watermark/retention 时两侧关系永久保留。
Right/Full ASOF 不属于当前 exact-row weighted relation：没有 occurrence identity 时 unmatched right copy 数量不能由输入关系唯一决定；不得用交换输入伪装成同一选择函数，也不得以任意 physical scan order 补定义。

候选搜索和 right-side rematch 都以 Store 的 owned page 进行。
常规候选页最多 64 项、1 MiB
logical Store bytes 和 16,384 个 `ScalarValue` slots；整个 turn 常规最多 256 项和 4 MiB 逻辑
工作量。
当空 turn 的首个 Store item 本身超限时，为了活性会单独接受它。
因此普通运行时峰值是
`O(pinned Claim + candidate page + turn output)`，而非整个 partition；单个 oversized row 仍是显式例外。

这些边界限制一次 turn 的内存和事务放大，不限制整个关系的磁盘状态。
没有 watermark 时两侧历史都
必须保留。
Residual 可以让最近候选不合格，所以当前正确性路径要分页扫描整个 right partition；
right presence transition 还要扫描该 partition 的全部 left rows，并对每个 left row 完成候选搜索。
因而普通左侧
lookup 成本与候选 partition 大小成正比，最坏右侧历史修正是该 partition 左右状态的乘积；分页只保证
每个 turn 有界，不会隐藏总成本。
候选 right scan 与 rematch left scan 都从索引内的
matchable-order marker 直接 seek，不会读取 order 为 NULL、因而永远不可能参与匹配的历史。

AsOfContinuation 保存当前行序号、Probe/Emit phase、outer/candidate cursor、已经找到的 before/after winner 与歧义标记。

ASOF 的 `Equal` 在任一 NULL equality 分量时不匹配，`NotDistinct` 允许 NULL 分区；NULL order 永远不匹配。

Residual 在候选排名前求值，false/NULL 跳过并继续找更远候选。
right-only tie-break 每项显式指定升降序和 NULL first/last；
最终 fallback 必须明确为歧义拒绝或 canonical right row 升/降序。
同一 right row 的正 multiplicity 只表示候选存在，不再次乘进左侧输出权重。

Tolerance 使用 order 的物理单位且包含端点，只限制匹配，不授权清理历史。


## EquiJoin 源码分工

`equi_join/runtime.rs` 拥有分页推进、continuation 和持久写入；私有子模块 `runtime/matches.rs` 拥有候选扫描、residual 与批次预算，`runtime/output.rs` 拥有结果构造、修正和 checked diff 验证。
这些模块仍操作同一个运行对象，不增加 Context、Engine、每种 Join kind 的对象或第二套执行协议。
