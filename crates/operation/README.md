# dogpaddle-operation

这个 crate 定义 `DogPaddle` 的计算和具体外部适配：Source 捕获输入，Transform 维护关系，Sink 交付结果。
Operation 不读取 Flow 调用栈、不选择下一个节点，也不创建或提交 Store 事务。
Flow 拥有图连接、持久帧和事务边界；Store 拥有 typed collections。

```text
Source                    Transform                         Sink
PostgreSQL / MySQL CDC -> Filter / Aggregate / Join -> SQLite / PostgreSQL / Doris / ClickHouse
```

构造和运行只有两条入口：

```text
Definition + exact Schemas + scoped DataScope + RuntimeResource
    -> checked construct -> Operation + output Schema

immutable input + opaque Resume + TransactionAccess + StepBudget
    -> Step { output, progress: More(Resume) | Done }
```

简单算子从 [`Select`](src/operation/transform/select.rs) 和
[`RunningEventCount`](src/operation/transform/running_event_count.rs) 开始阅读。
具体运行类型都是私有实现；调用方通过 `OperationDefinition::construct` 构造统一 `Operation`。
定义和表达式规则见 [定义契约](docs/definitions.md)，关系与分页规则见 [关系契约](docs/relations.md)。

## Definition 与 checked construction

Definition 是可持久化的纯计划，不持有数据库句柄、连接、密码或执行位置。
`OperationDefinition` 是全部内建算子的封闭 enum，也是唯一的稳定名称 JSON 计划表示；具体模块拥有纯业务验证与编译。Flow 直接嵌入该 Serde 计划并拥有唯一持久外壳；Operation 不另提供 encode/decode codec。反序列化证明结构、CDC 支持列域和表达式可重放性，`output_schema`/`construct` 在访问 Store 句柄前完成业务验证。
不保留旧 tag、格式识别、fallback、迁移或兼容入口。开发期 v1 布局变更后直接重建受影响的状态和目标。

构造过程按同一路径服务新建和 reopen：

1. 在接触 Store 前预检全部 `RuntimeResource` 的 presence 和精确 Rust 类型。
2. `construct` 校验输入数量、完整 `DogPaddle` Arrow Schema 和执行能力。
3. 具体 Definition 编译表达式，并在限定的 `DataScope` 内声明或查找固定名称的 typed handles。
4. 返回最终 `Operation` 与完整 output Schema；Sink 的 output Schema 为 `None`。

构造不读取业务状态、不打开事务，也不执行外部 I/O。
`StoreSetup::commit(path, init)` 原子发布新 catalog 和初值；`Store::open` 使用同一构造规则查找既有资源，
恢复失败不自动删除、重建或改写状态。资源归属稳定 Operation ID，不归属内存中的融合段。

`output_schema(inputs)` 复用同一纯 Schema 编译规则，供 SQL 在打开状态前取得权威 Schema。
字段名、顺序、类型、nullability、嵌套结构和 metadata 都必须精确匹配；logical Schema 不包含 Change 的 diff 列。
运行期仍检查每份输入的 Schema。

## 计算能力与分页

`OperationKind` 声明业务角色、输入 arity 和融合资格：

| kind | 输入 / 输出 | 执行能力 |
| --- | --- | --- |
| `Scan` | 0 / 有输出 | Source capture；已捕获数据的 identity 切片可作为计算 head |
| `AtomicTransform(N)` | N / 有输出 | `AtomicOperation::apply` 完整处理当前 slice |
| `PagedTransform(N)` | N / 有输出 | `PagedOperation::step` 处理有界扫描或修正页 |
| `Sink(N)` | N / 无输出 | 私有 outbox enqueue 与独立外部 drain |

计算 head 可以接单输入 Atomic 尾链；链的编译索引只在内存中存在。
尾项直接 `apply`，不创建自己的帧、队列、Resume 或生命周期。
普通 Atomic head 与 Source identity 在进入首个 Atomic 前先切 input window，再转换为同一 `Step`。

```rust,ignore
fn apply(
    &self,
    input: OperationInput<'_>,
    access: TransactionAccess<'_>,
    budget: &mut StepBudget,
) -> Result<Option<Change>, OperationError>;

fn step(
    &self,
    input: OperationInput<'_>,
    resume: &Resume,
    access: TransactionAccess<'_>,
    budget: &mut StepBudget,
) -> Result<Step, OperationError>;
```

两个接口都用 `&self`。常驻字段只有编译表达式、布局和 typed handles。
窗口求值、canonical rows、pending group 和候选批次都是单次调用的 scratch；回滚直接丢弃它们。
计算不存在 prepare callback、AfterCommit、operator continuation Cell、全输入 admission cache 或内存游标。

`Resume` 是唯一输入 ordinal 加封闭、私有的强类型 cursor。
其严格 `StoreValue` codec 检查版本、完整消费、canonical 编码与 64 KiB control 上限。
Flow 只保存、传回并校验 variant 与输入绑定，不解释具体 Join cursor。
`initial_resume` 是纯构造方法；`More` 必须前进，`Done` 表示完整输入用尽。
无输出扫描仍推进 Resume；最后一页的 `Done` 与输出共同保存，发送完成前仍有明确发送责任。
帧与发送的持久契约只由 [Flow runtime](../flow/docs/runtime.md) 规定。

每个 driving event 在第一页逐事件准入。一个 step 可以批量推进多个事件，候选和 residual 保持 Arrow 向量求值。
晚页负权重、表达式、codec 或 diff overflow 回滚当前页，早期已提交页保留。
**完整 Change 或 Delivery 不再是计算事务边界**；reopen 在保留的失败帧上重算同一失败位置。

`StepBudget` 贯穿 head、所有 Atomic tail、control 和 payload 写入。
head item 额度限制输入事件、扫描候选或修正原子；Atomic tail 只扣共享逻辑 bytes。
预算不足返回 `BudgetExceeded`，Flow 整页回滚并确定性减半 head item 额度，最小原子仍超限则失败。
预算描述逻辑事务工作量，Arrow/DataFusion 表达式的临时分配不构成严格 RSS 或执行时间保证。
可变大小索引读取使用 byte-bounded scan；已知编码写入在写前扣账。

## 内建计算算子

| 算子 | 能力 | 关系行为与状态 |
| --- | --- | --- |
| `RunningEventCount` | Atomic | 逐事件递增 count，忽略输入 diff；私有 `Cell<u64>` |
| Filter | Atomic | 只保留 non-null true；不声明 state |
| Select | Atomic | 有序投影与计算，可显式覆盖 metadata/nullability；纯列引用共享 arrays 和 diffs |
| `UnionAll` | Atomic | exact-Schema 输入按端口原样转发 |
| Distinct | Atomic | exact canonical row positive weights 的零/正边界 |
| Aggregate | Atomic | grouped COUNT/SUM/AVG/MIN/MAX，unique argument statistics 与 extrema indexes |
| `EquiJoin` | Paged / 2 | Inner、LeftSemi、LeftAnti、LeftOuter、FullOuter，保留可选 residual |
| `AsOfJoin` | Paged / 2 | SQL left outer、单 order、Forward/Backward、strict/inclusive、Reject ties |

关系计算共享私有 canonical row 和有序 scalar 编码，不在 Store 建立关系框架。
参数和状态的精确维护规则见 [关系契约](docs/relations.md)。

`EquiJoin` 分区按 equality key 组织 exact rows。NULL key 不匹配；residual 绑定到 `left.* + right.*`，
只有 non-null true qualifying。outer null correction 和对应 pair 是同一个分页工作项。
无 residual 的 presence 从两侧 Rows 分区推导，不另保存 key-count 缓存；residual qualifying support 仍是独立持久事实。
当前事件最后一页直接写回已 checked 的本侧 Rows after 权重，More 后重新从真实权重准入；帧按 DFS 运行保证处理该页期间对侧关系不被后续输入改变。

ASOF equality 使用 SQL NULL 规则，order 为精确同型可索引 scalar。
右侧 exact-row multiplicity 不影响单候选选择；distinct rows 在被选中的同一时刻形成歧义并拒绝。
历史右侧 presence 变化只修正两个邻接时刻定义的 left 区间，页内共享 before/after winner，
最后一页才将当前右事件记入真实 RHS index。索引只为 equality/order 分段，完整 canonical row 作为原始末段；winner 解码直接借用该 key 的后缀，不持有第二份 row。当前两侧资源为 `asof_join.left_index/right_index`，受影响的开发期旧布局直接重建。
nearest、tolerance、多 order、residual、tie-break、NotDistinct、canonical fallback 与额外 kind 不属于当前 API。

## Source 与 Sink 外部边界

Source 拥有一条 input Queue、phase、checkpoint 和真实 Delivery 的 ACK。快照封口前隐藏，封口后在原地可消费，不把 payload 搬到第二条队列。
`published` 只读返回 schema-bound front bytes；调用方用 exact Schema 解码，完成全部页和下游调用后以 `consume_published` 同事务删除前项。捕获只追加，不随 consumer 进度延迟 ACK。
捕获、恢复、容量及 `PostgreSQL` / `MySQL` 差异由 [CDC 契约](docs/cdc.md) 规定。
两种源以 Arrow `Fields` 直接声明完整源列，构造器和 binding 在编码前检查各自支持的类型、空 metadata 与名称；输出 Schema 共享字段，不保留第二套 Column/Type。
两种源的 Config 均接收 `CdcOptions` 的六项运行覆盖；未设置的项由具体源使用各自默认值，参数校验返回 `CdcOptionsError`。它们不进入 Definition 或持久状态。

Sink 拥有 outbox，事件位置同时确定消费进度和固定 occurrence IDs；Prepared 只保存边界与删除 IDs。enqueue 与独立 drain 的事务、
目标重投及负 diff 前缀验证由 [Sink 契约](docs/sinks.md) 规定。远端 Sink 直接共享 Arrow Schema，SQL 与目录类型从字段派生，不另保一份列布局。

## 验证与 benchmark

公共行为由显式 `correctness` target 验证。
Join owner tests 覆盖五种 `EquiJoin` 及 residual 的独立 bag oracle、weighted 事件、逐页回滚与 runtime 重构；
ASOF 覆盖四种方向/exactness、邻接历史插删、multiplicity、NULL、歧义暴露和 last-page RHS 落账。
预算、tail 失败、调用栈窗口、CDC seal/ACK、目标 commit gap 和系统组合由对应 owner 共同验证。

```text
cargo test -p dogpaddle-operation --test correctness
cargo test -p dogpaddle-operation --benches --locked
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench equi_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench asof_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-operation --bench asof_join_resources
```

ASOF resource target 在独立子进程中记录当前 SQL kernel 的 lookup history、历史修正区间、空影响区间与
NULL history 的 Rust allocator 和 encoded key/value bytes。输入和 fixture 在 dhat 计时前建立；
`RocksDB` native allocation、WAL 和 RSS 不混入这些指标。测试模式只验收可运行性，性能比较要求同 host、
rustc、profile、数据规格与 baseline epoch。Buffered Sink 对照见 [PERFORMANCE.md](PERFORMANCE.md)。完整 target 表与 gate 见 [TESTING.md](../../TESTING.md)。
