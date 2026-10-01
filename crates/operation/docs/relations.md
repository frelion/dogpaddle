# 有状态关系算子契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## Distinct

`Distinct` 是单输入、exact-Schema-preserving Transform，Definition 的 canonical JSON payload 为 `{}`，只声明 `distinct.weights: OrderedMap<Vec<u8>, NonZeroU64>`。
key 是完整 canonical row bytes，multiplicity 是 Store 维护的正 `u64`；缺失表示零，checked signed adjustment 归零即删除。
输入按行序逐事件更新：负前缀和 overflow 回滚当前 Atomic 页，仅 `0 → positive` 输出 `+1`、`positive → 0` 输出 `-1`。页内连续相同 key 只缓存一个 key 和其当前 `u64` 权重；仍逐事件校验及产出边界，key 切换或页结束时才写回最终权重，净变化为零时不写。无效前缀继续毒化 Store 事务。
每行先无拷贝检查完整 canonical 编码大小，并在分配 key 前扣共享预算；List 的 NULL children 也按实际 canonical marker 计费，不能用小 Arrow buffer 绕过页界。新 run 的固定 8-byte 权重读取和最终写回分别在访问前按完整 key 加权重计费；损坏权重仍由 Store 的严格正权重 codec 拒绝并毒化事务。预算不足回滚整页，包括已经写回的早先 run；部分输出的 Arrow filter 复制也先准入。
状态、output 和 input completion 同事务提交，背压与 reopen 保持同一输入语义。
SQL `SELECT DISTINCT` 复用这一 exact-row identity，包括按原始位模式区分浮点值。
canonical Arrow row 编码和 diff 语义留在 operation crate 私有 `relation` 模块；Store 只提供通用 multiplicity，不预建 Aggregate/Join 的关系框架。
现有关系 Sink 继续使用同一 canonical row 的 16-byte `row_hash` ABI。

## Aggregate

`Aggregate` 是单输入的 grouped relational Atomic Transform；至少一个 group expression，aggregate call 可以为空。Definition 保存有序命名 group expression 和有序 `AggregateCall`，输出固定为 group fields 后接 call fields。`AggregateCall<E = Expr>` 只有 `CountAll`、`Count(E)`、`Sum(E)`、`Avg(E)`、`Min(E)`、`Max(E)` 六个变体；持久参数使用同一 ADT 的 `StoredExpression`，未知函数和错误参数个数无法表示，不维护数字函数目录或 Descriptor。

它只声明 `aggregate.groups: OrderedMap<Vec<u8>, GroupState>`、`aggregate.entries: OrderedMap<PartitionKey<EntryPartition, Vec<u8>>, NonZeroU64>` 和 `aggregate.control: Cell<u64>`。groups 以完整 canonical group 为 key，保存稳定 group ID、正 group weight、每个不同参数的充分统计和每个极值 slot 的缓存；entries 的 partition 是 `layout + group ID`，只维护排序参数的 key 和正份数；control 分配不复用的 group ID，不保存完整输入行。

绑定按 canonical expression 去重参数，每个不同参数每页只求值一次。`COUNT(*)` 直接读 group weight；同参数 `COUNT(x)`、`SUM(x)`、`AVG(x)` 共用一个非空 count 和 signed `i128` 或 unsigned `u128` sum，只有 COUNT 的非数值参数保存 count。输出调用是统计的读出，无独立 Fold 字节状态。COUNT 输出 non-null `Int64`；SUM 仅接受 `Int64/UInt64` 并保持类型，每个输入事件都检查对应窄 sum（即使与 AVG 共用统计）；AVG 以宽 sum 累计后输出 nullable `Float64`。

`GroupState` 的当前开发期 v1 value 使用固定宽度 big-endian owner codec：标记 `1`、u64 id、u64 正 weight、u32 statistic 数、u32 extrema 数；每个 statistic 固定 32 字节（Count/Signed/Unsigned tag、u64 count、16-byte sum、零 padding），每个 extrema 是 u64 key 长度加 key，`u64::MAX` 单独表示 None。编码长度恰为逻辑状态计费加一字节，因此 Map 在复制/解码前的长度准入也限制充分统计与缓存的解码逻辑大小；集合数先与剩余最短 payload 检查再分配，不因损坏 count 预留大块内存。解码拒绝零 group weight、非法 tag/padding、截断、尾随字节与越界 key。旧 value 直接重建，不提供格式识别或迁移。

`MIN(x), MAX(x)` 共用一个排序 layout、两个 slot，重复方向复用一个 slot；同一个参数也复用上述求值结果。MIN/MAX 接受 non-float flat scalar（Null、Boolean、整数、Utf8、Binary、Date32、Timestamp、Decimal128）并输出 nullable 同类型。NULL 参数不进 entries/cache。相邻相同极值参数在事务内每 layout 暂存一个 key，仍逐事件校验份数；切换 key/group 或页结束才写回。撤回缓存极值时先 flush pending，再通过有界 first/last 刷新缓存，group weight 为零也必须刷新。净变化为零可省最终写入，不省中间校验和有序输出。

校验按分组与参数统计进行，不维护完整行身份；参数组合被其它行覆盖时允许撤回。group 归零时，所有统计必须 count=0、sum=0，所有 extrema cache 必须 None，否则当前页失败并回滚。合法归零由逐事件份数归零自然删除 entries，不执行无界分区清理。需要记录级身份的算子继续按完整行记账。

每个输入事件完成全部更新和旧行 `-1`/新行 `+1`；组首次出现只输出 `+1`，消失只输出 `-1`，结果未变不输出。Atomic kernel 只消费共享逻辑字节预算，不消费 head work items；Flow 在 head 分页后同事务提交该页状态、输出和 Resume，预算不足回滚并缩小 head 页。持久 group 和极值读取在复制、解码前由 Store admission 限制；状态、临时参数、缓存、写入和结果计入该页预算。此前已提交的页不会因后续页非法而撤销。

group key 不能包含 Float32/Float64；global aggregate、grouping sets、aggregate modifier、UDF、浮点 SUM/AVG/MIN/MAX、List/Struct MIN/MAX 均不属于 v1。

## EquiJoin

`EquiJoin` 是两输入 `PagedTransform`；port 0 为 left，port 1 为 right。
Definition 保留 `Inner/LeftSemi/LeftAnti/LeftOuter/FullOuter`、非空 equality pairs、output names 和可选 residual。
所有表达式 immutable；每对 key exact 同型、flat non-float，NULL 不匹配。
residual 在 `left.* + right.*` candidate Schema 上绑定，必须 Boolean，只有 non-null true qualifying。

两侧 rows 为 `OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, NonZeroU64>`，以完整 equality key 和 canonical row
维护正 multiplicity。无 residual 的非 Inner 另有 `key_counts: OrderedMap<Vec<u8>, KeyCounts>`；
有 residual 的非 Inner 用 `match_counts: OrderedMap<Vec<u8>, u64>` 按完整行保存 qualifying distinct opposite rows，
FullOuter 跟踪双侧，其他 presence kind 只跟踪 left。zero counts 必须缺失。
match-count key 为单字节 port 加 canonical row；不声明 operator continuation。

帧 Resume 唯一保存输入 ordinal 与私有 `found_match + exclusive opposite-row key`。
窗口只准备本预算能处理的输入事件；key expression 逐组批量求值，释放当前 array 后处理下一组。
每个事件在第一页从真实本侧权重准入，不保存全输入 RowEffect、影子关系或跨事务 prepared rows。

页内批量扫描候选、求值 residual，并立即更新真实 support。
outer null correction 与对应 pair 共同占一个 head work item，即使该项输出两行，也不二次扣 head 数量。
最后一页调整本侧 rows/key counts；More 持久位置与状态和输出同事务。
帧的 DFS 顺序保证当前输入处理完前对侧不被后续事件改变。

晚期负权重、residual、codec 或 output diff overflow 只回滚当前页，先前提交页与帧保留。
每次页事务后销毁运行实例，再从 Definition、Store 和 Resume 重构，必须得到相同下一页。

## AsOfJoin

`AsOfJoin` 是两输入 `PagedTransform`，固定 SQL left outer 输出。
Definition 只有 direction/exactness、SQL equality pairs、一个 order pair 和 left-then-right output names。
所有表达式 immutable、pair exact 同型、flat 可索引 non-float scalar；NULL equality/order 不匹配。
nearest、tolerance、lexicographic order、residual、tie-break、NotDistinct、canonical fallback 和额外 kind 退休。

两侧为 `OrderedMap<Vec<u8>, NonZeroU64>`；当前开发期 v1 index key 依次编码 equality partition、order、canonical row，
各部分使用零字节转义及终止符，order 首字节区分 NULL 与可匹配值。不保存 rank 或 operator continuation。
同一 exact RHS row 的多份数只表示一个候选；不同 row 在同一 selected time 时拒绝歧义。
没有受影响 left 时可以保留歧义 RHS bucket，之后探测或历史修改暴露它时确定性失败。

左事件直接按 direction/exactness 有界 seek 到最近的 order bucket；最多读两个 exact rows 判断歧义。
候选探测若因字节上限提前截断，不能将未读部分当作不存在；不足两个探测项，或 overlay bucket 不足三个项且仍有 continuation 时，整页回滚并报预算不足。
右事件只在 exact-row presence 的零/正边界变化时产生历史修正。
当前真实 RHS 在全部修正页完成前保持 before 状态；event overlay 仅描述当前行的 after presence，
只在最后一页将当前事件写入真实 RHS。Resume 只保存当前右事件最后已修正的 left key。

影响区间按严格相邻 RHS 时刻定义。Backward inclusive 为 `[t,next)`，strict 为 `(t,next]`；
Forward inclusive 为 `(prev,t]`，strict 为 `[prev,t)`，不存在邻居时该端延伸到 partition 边界。
一页按此区间扫描多个 left；所有 left 共享该事件固定的 before/after winner，不逐 left 重扫整个 RHS history。
每个 left 的 `-old,+new` 是同一修正原子，至少一次预算扣账；空区间和无输出也前进。

状态与 output/Resume 同事务；晚页歧义、权重或 diff overflow 保留早页和失败帧。
reopen 不补偿早页，也不自动修复已有关系或帧。

## 共享分页与验证

计算运行实例只能保存编译表达式、布局和 typed handles。
Resume 是唯一 ordinal 加强类型 cursor，严格 StoreValue codec 限制 control 为 64 KiB，拒绝截断、尾随字节、
非规范 varint、未知 variant 和超长 cursor。输入与 variant/Schema/port 的绑定由 kernel 校验。
正常续页的 cursor 绑定验证复用当前驱动行的批量准备，并计入同一 StepBudget；仅恢复检查拥有独立的有界只读额度。

一个 StepBudget 贯穿 head 与全部 Atomic tails：head 计输入/扫描/修正数量，所有项共享实际逻辑 bytes。
Store scans 在读前传 byte bound；已知 key/value/output writes 在写前计费。
候选扫描成功即计入已读字节，即使后续输出准入拒绝该页；canonical 编码失败前已复制的前缀仍计费。
需要构造完整行及索引副本时，先无复制检查 canonical 大小，再准入并编码，不能把失败分配留在重试预算之外。
canonical row 解码先无分配检查 framing，并将顶层及全部嵌套 scalar 槽位、已知 Arrow payload 计入同一预算；
List 声明的整个临时 scalar Vec，以及嵌套数组转换的 owned/borrowed array Vec 槽位，在遍历和分配前准入；
NULL Struct/List 的 Arrow shape 也计费。这是已知逻辑 scratch 与 payload 的准入，Arrow concat 的全部内部暂存不构成 RSS 硬界。
预算不足回滚整页，Flow 以同一 input 和 Resume 确定性减半 head 额度；最小工作项仍超限时返回 `BudgetExceeded`。
表达式输出仍可能额外分配；这些逻辑工作界不承诺进程 RSS 或执行时间硬界。

correctness 覆盖五种 EquiJoin 与四种 residual 配置的 independent bag oracle、weighted 插删、NULL、
逐页 rollback 和完整 runtime 重构、晚页负事件、fanout、4096 空 bucket 的批量推进。
ASOF 覆盖 Forward/Backward strict/inclusive independent bag、相邻时刻插删、重复同 row multiplicity、
NULL、空区间、歧义暴露和最後页 RHS 落账。Flow 负责 root/child/send/queue 的持久故障窗口和融合 tail 回滚。

EquiJoin 私有 `runtime/matches.rs` 维护候选扫描和 residual 批次，`runtime/output.rs` 维护结果构造和 checked diff。
ASOF 只有直接的 indexed kernel 与 index codec，不建立通用候选注册、排名层或共享增量框架。
