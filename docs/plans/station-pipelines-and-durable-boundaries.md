# Station 流水线与持久化边界实施计划

状态：核心实现与 SQL 自动装配已完成；physical explain、CDC system witness 和长期性能 gate 后续推进  
优先级：当前执行内核主线，先于 Join  
日期：2026-09-11

## 目标

DogPaddle 改造前把一个逻辑 Operation、一个运行 Station 和一条持久化输出边绑定在一起。这个模型拥有清晰的恢复语义，
但会让每个 Filter、Project、Extend、Select、SchemaAlign 都经过一次完整 Change IPC 编码、RocksDB 写入、WAL 同步提交、
下游读取和 IPC 解码。

本计划把最终架构固定为：

```text
durable input 0 → input inline pipeline 0 ┐
durable input 1 → input inline pipeline 1 ├→ one durability-group coordinator
...                                      ┘        (v1: one core Operation)
                                                       │
                                                       ▼
                                           output inline pipeline
                                                       │
                                                       ▼
                                                 durable output
```

一个 Station 是一个持久化协调域。它对外只有一个 Action、continuation 和 effect owner；v1 由恰好一个完整 core Operation
担任这个 coordinator，并可在每个输入端口和最终输出上包含纯 inline pipeline。当前本地执行模型中，只有 Station 之间存在
`SubscribedLog`/`Subscription`；Station 内部的 Change 只在内存中传递。

这个结构要同时满足：

- 消除普通逐行变换之间的 IPC、WAL 和持久队列；
- 保留现有 Claim、active input、事务、backpressure、reopen 和 `AfterCommit` 语义；
- Flow 不枚举具体算子；
- Store 不感知 Pipeline、Operation 或 Change；
- build 持久化最终物理分组，open 不重新优化；
- 以后增加算子时只声明是否支持 inline，不增加特例。

## 核心判断

### Station 是持久化与恢复域

需要分开理解三种身份：

| 身份 | 回答的问题 | 是否持久化 |
| --- | --- | --- |
| Logical Operation | 计算什么、Schema 如何变化 | Definition 持久化 |
| Station pipeline | 哪些步骤共享一个 turn、事务和恢复单元 | 物理分组持久化 |
| Durable edge | 上下游能否独立提交、缓冲和恢复 | `SubscribedLog` + `Subscription` |

逻辑算子保留自己的 Definition、tag、payload、Schema binding 和错误位置。融合只改变物理 Station 分组，不抹掉逻辑步骤。

### 非持久化连接必须共享提交

唯一必须始终成立的恢复规则是：

> 上游只有在下游已经在同一 Store 事务中吸收其结果，或者结果已经进入 durable output 后，才能完成当前输入或确认外部工作。

因此不增加“两个独立 Station 之间的 volatile edge”。如果 producer 已经提交而 consumer 尚未吸收，进程崩溃会丢数据；
如果 producer 一直等待 consumer 后再提交，两者实际属于同一个事务域，应表示为一个 Station。

另一种可行体系是全图 epoch/barrier/checkpoint：独立算子之间使用内存边，故障后整图回滚到同一 epoch。它需要 source rewind、
barrier 对齐、版本化 state snapshot 和全图恢复协调，不符合当前逐 Claim 原子提交模型，不进入本计划。

### Operation state 与 durable output 是两件事

Aggregate、Distinct 等 core 的业务 state 必须持久化，以便处理未来输入。它们的每一个下游逐行变换结果不必单独持久化。
例如 `Aggregate → SchemaAlign → Filter` 可以在一个事务中更新 aggregate state、完成输入，并只 append Filter 后的最终 Change。

第一版仍在两个各自拥有 Action、continuation 或 effect 的完整 Operations 之间保留 durable edge。这是通用组合机制的永久边界，
不是临时实现限制。未来一个专用 Operation 可以内部协调多个 state kernels，但它必须向 Station 呈现一个统一状态机和一个
group-level coordinator。

### 算子固有表达式仍属于算子

Pipeline 不能成为补齐算子表达能力的手段。`JOIN ON func(A) = B` 的 `func(A)` 应直接属于 Join key，
`GROUP BY func(A)` 应直接属于 Aggregate group expression，未来 TopK order key、Window assigner 和 residual predicate 也遵循同一规则。
只有逻辑计划中本来就独立存在的 Project、Filter、Extend、Select、SchemaAlign 才进入 inline pipeline。这样新增算子不会依赖隐藏的
helper column、临时 Schema 或额外持久边界。

## 最终不变量

1. **一个 Station 恰好一个 durability-group coordinator。** v1 coordinator 就是一个 core Operation；只有它可以 `turn`、返回 `Idle/Commit/Complete`、声明 Store data、接收 runtime resource 或产生 `AfterCommit`。
2. **每个 input port 至多一条有序纯 inline pipeline，core output 至多一条有序纯 inline pipeline。** 空 pipeline 合法。
3. **InlineTransform 不接触事务。** 它没有 `TransactionAccess`、Store data、runtime resource、continuation、外部副作用或 `AfterCommit`。
4. **Subscription 仍是唯一 durable input identity。** Inline pipeline 不保存 offset、Claim 或第二套 progress。
5. **只有最终 output 持久化。** capacity、writer、retained bytes 和 subscribers 只属于 Station 的最终 `SubscribedLog`。
6. **物理分组在 build 前确定并进入 canonical Flow Definition。** open 只恢复，不运行 grouping 或 optimizer。
7. **Schema 在 Station 内顺序纯绑定。** derived Schema 不另建 Cell、fingerprint 或 registry。
8. **每个绑定后的 inline 实例都可确定重放。** 相同 Definition、binding、exact input Schema 和逻辑事件序列必须得到相同结果或相同错误，且不能因物理重批而改变语义。
9. **两个完整 Operation 不做通用融合。** 需要共享提交的多状态计算实现为一个具体 coordinator Operation，由它统一拥有 Action、continuation 和 effect。
10. **Flow 拓扑仍只连接 Station。** 当前 Station 间 transport 仍是 `SubscribedLog`，不增加公共 Edge 类型、Station 内部 DAG 或内部持久队列。

## 为什么通用 Station 只能接受一个完整 Operation

现有完整 Operation 可以：

- `Commit` 自己的 continuation，但保留当前完整输入；
- 分多个 turn 产生 output；
- 修改自己的 Store data；
- 返回借用输入或运行资源的 `AfterCommit`；
- 在无写事务时进行 bounded external poll 或准备工作。

任意串联两个完整 Operation 没有统一、简单的协议：

- 第一个 `Commit` 后，第二个是否必须立刻完整消费临时 output；
- 第二个需要多个 turn 时，第一个 output 如何跨进程恢复；
- 第二个 backpressured 时，第一个 continuation 是否可以提交；
- 两个 `AfterCommit` 中第一个成功、第二个失败后从哪个 durable phase 恢复；
- 两个 Operation 对同一外部 Claim 的 `Complete` 应由谁决定。

通用解最终会在两者之间增加 durable queue 和独立 progress，这等价于把现有 `SubscribedLog` 搬进 Station，复杂度增加而持久化成本没有消失。
因此一个 group-level coordinator 才是最终架构边界。v1 保持 one-core；未来 multiway join、特殊聚合链等可以作为一个拥有统一
Definition、state、continuation 和 effect 协议的具体 core，并在内部使用多个私有 state kernels，但不建立通用 stateful fusion 框架。

## 最小核心类型

### Operation crate

保留现有 `Operation`、`Turn`、`PreparedTurn`、`Action` 和 `AfterCommit` 协议，增加一个窄能力：

```rust
pub(crate) trait InlineTransform: Send + 'static {
    fn apply(
        &mut self,
        input: &Change,
    ) -> Result<Option<Change>, OperationError>;
}
```

`InlineTransform` 的契约是：

- 单输入；
- 一次调用完成，不返回 `Idle` 或 `Commit`；
- 产生零个或一个完整 Change；
- 不增加行数，保留被选择行的相对顺序，并逐行原样携带对应 diff；
- 对逻辑串接满足重批同态：`F(C1 ⊕ C2) ≡ F(C1) ⊕ F(C2)`；这里 `⊕` 表示相同 exact Schema 下的
  row+diff 事件序列拼接，`None` 表示空序列，`≡` 比较展平事件流而不是物理 Change 边界；
- Definition 与 binding 的语义不可变，只有可丢弃、可重建的执行 cache 可以变化；
- 只缓存可由 Definition、binding 和输入重建的临时对象；
- 错误后可用相同输入重试；
- 不执行时间、随机、网络、文件或其他不可重放观察。

Definition 侧增加 sealed `InlineOperationDefinition` capability 和每次 build/open 重新生成的运行期 `InlineBinding`。只有 Operation crate 内明确实现该能力的
Definition 才能进入 pipeline，不能通过 `data().is_empty()` 推断。UnionAll 和 Discard 也可能没有普通 Operation data，但它们不符合
单输入纯变换协议。

类型擦除也由 Operation crate 完成：公开 builder 通过 sealed capability 把具体 Definition 转成 opaque `InlineDefinition`；
`encode_inline_definition`/`decode_inline_definition` 复用原 Operation tag 和 payload，但只返回这个已经验证过的 wrapper；
`InlineDefinition::bind` 只产生 `InlineBinding`。Flow 只保存和调用 wrapper，不能枚举 tag、向下转型或把
`Box<dyn OperationDefinition>` 猜成 inline。

概念接口为：

```rust
pub trait InlineOperationDefinition:
    OperationDefinition + private::InlineSealed
{
    fn try_into_inline(self)
        -> Result<InlineDefinition, InlineEligibilityError>;
}

pub struct InlineDefinition {
    erased: Box<dyn InlineOperationDefinition>,
}

impl InlineDefinition {
    pub fn bind(&self, input: SchemaRef) -> Result<InlineBinding, InlineBindError>;
}

pub fn encode_inline_definition(definition: &InlineDefinition) -> Vec<u8>;
pub fn decode_inline_definition(bytes: &[u8])
    -> Result<InlineDefinition, DefinitionCodecError>;
```

这些入口的实际可见性服从现有 crate API 风格；关键约束是 normal definition 与 inline definition 在擦除后仍是不同类型。
`try_into_inline` 检查 Definition-instance 级的可判定条件，例如表达式 volatility；exact-Schema 条件随后由 `bind` 检查。

non-expanding、保序、diff-preserving 和重批同态由每个 sealed concrete implementation 在构造上保证，并通过 conformance/metamorphic
tests 证明，binder 不尝试从 `PhysicalExpr` 推导这些代数性质。binder 只检查 exact Schema、表达式树和其他可判定条件；表达式只接受
能证明为 immutable 的函数，`Stable`、`Volatile` 或无法证明的 UDF 均拒绝 inline。

Project、Filter、Extend、Select、SchemaAlign 抽取一个共享执行 kernel：

- standalone 使用 adapter 把 kernel 的 `Some/None` 结果映射为 `Action::Complete`；
- inline 直接使用同一个 kernel；
- Definition codec、Schema 推导和运行语义只有一份实现。

### Flow crate

最终内部 Definition 形状为：

```rust
struct StationDefinition {
    id: String,
    core: Box<dyn OperationDefinition>,
    output_capacity_bytes: Option<NonZeroU64>,
    inputs: Vec<InputDefinition>,
    output_inline: Vec<InlineDefinition>,
}

struct InputDefinition {
    station_id: String,
    inline: Vec<InlineDefinition>,
}
```

output presence 在 canonical codec 中仍是一个显式 discriminator；纯校验保证 Sink 的 capacity 为 None 且
`output_inline` 为空，并保证 Scan/Transform 有非零 capacity。每个 inline definition 仍使用自己的既有
Operation tag 和 payload 编码，不增加 `CompositeOperation` tag。

运行态保持同样简单：

```rust
struct InlinePipeline {
    stages: Vec<InlineBinding>,
}

struct StationProgram {
    core: Box<dyn Operation>,
    inputs: Vec<InlinePipeline>,
    output: InlinePipeline,
}

struct Station {
    program: StationProgram,
    inbox: Inbox,
    output: Option<Arc<Output>>,
    // existing fail-stop and status fields
}
```

不增加公共 `Stage` enum、内部消息、内部 Claim、内部 Subscription 或另一套 scheduler。

## Schema binding

全图仍按唯一确定性拓扑 schedule 传播 Schema，但每个 Station 的绑定改为：

```text
for each input port:
    producer final output Schema
      → bind input inline stage 0
      → ...
      → bind input inline stage N
      → core input Schema for this port

ordered core input Schemas
      → bind core
      → core output Schema
      → bind output inline stage 0
      → ...
      → Station final output Schema
```

约束如下：

- input pipeline 数量必须等于 core input arity；
- Scan 没有 input pipeline；
- Sink 没有 output capacity 或 output pipeline；
- 每个 inline binding 必须产生 output Schema；
- 每一层继续验证完整 DogPaddle Schema；
- binding error 包含 `station_id`、`input(port)/output` 和 inline ordinal；
- 每个 sealed inline kernel 自行检查 exact input，input pipeline 的最终结果再与 core input Schema 匹配；
- 只有 Station final output Schema 进入 `Output`，不单独持久化中间 Schema。

## 运行协议

### 有输入的 core

```text
1. Inbox 取得或复用原始 owned Claim。
2. 在没有写事务时运行所选 port 的 input pipeline。
3. input pipeline 返回 None：
   a. 不调用 core；
   b. 开启短写事务；
   c. complete 原始 Claim，并更新适用的 active input；
   d. commit。
4. input pipeline 返回 Some(transformed)：
   a. 以原始 port 和 transformed Change 调用 core.turn；
   b. Turn::Idle 不开启事务并保留原始 Claim；
   c. Turn::Ready 后开启写事务并调用 core.apply；
   d. 对 Action 携带的 output 运行 output pipeline；
   e. 只 append 最终 Some(Change)；
   f. core 返回 Complete 时 complete 原始 Claim；
   g. commit；
   h. 运行 core 的 AfterCommit。
```

关键语义：

- core 返回 `Commit` 时保留原始 Claim；下一 turn 从原始 Change 确定性重算 input pipeline；
- 首版不把 transformed Change 加入 Claim 或任何 durable state；未来可以做可丢弃内存 cache；
- input pipeline 的 Filter 全删只完成原始 Claim，不产生 core state 或 output；
- output pipeline 全删只把 core Action 的 output 变为 `None`，不改变 `Commit/Complete`；
- output pipeline error 或最终 backpressure 会回滚**当前 turn**的 core writes、final output 和 input completion；
- commit failure 的 durable 结果可能不确定，当前 Station 必须 fail-stop，并在 reopen 后以 Store state 判定结果；
- 这些失败都会丢弃 core 的 `AfterCommit`；
- transformed input 必须一直存活到 `AfterCommit<'turn>` 执行完成，因为 completion 可能借用它；
- `AfterCommit` 只会在本地事务成功提交后运行，但跨崩溃可能重试；它代表 durable intent 的结算，不提供 exactly-once callback 保证；
- 外部 effect 必须由提交前保存的 intent 幂等执行或判重，`AfterCommit` 失败继续使用现有 fail-stop/reopen；
- 输入 core 若以 `Complete` 推进 Subscription，其 `AfterCommit` 必须能脱离已完成输入恢复；仍依赖输入 bytes 的 effect 必须先用
  `Commit` 持久化自包含 intent 并保留 Claim，完成 effect 后在后续 turn 才 `Complete`。

这里的原子性以 turn 为单位。core 若用多个 `Commit` 分页处理一个 Claim，前几页一旦提交就已经可见；第 N 页的 tail error 或
backpressure 会回滚第 N 页并由 durable continuation 重试；commit failure 则必须 reopen 后确定该页是全旧还是全新。不能承诺整个 Claim 的全有或全无。如果某个 core
需要在任何数据错误前都不发布该 Claim 的结果，它必须先预检完整 Claim，或者在可能失败的 tail 前保留 durable Station 边界。

### Scan core

Scan 没有 input pipeline。它的 `Commit(Some(Change))` 经过 output pipeline 后只 append 一次；output pipeline 全删时仍提交 Scan state，
并在本地 commit 后运行原 Scan 的外部 ACK。最终 output backpressure 会回滚当前 turn 的 Scan checkpoint 并丢弃 ACK，与当前协议一致。
ACK 在 crash/reopen 后也必须由持久 checkpoint 协议安全恢复，不能把一次 callback 调用当作 exactly-once 事实。

### Sink core

Sink 没有 output pipeline。每个 input port 可以有纯 input pipeline，因此 Sink 前的 Filter/Select 不需要独立 Station。
Sink continuation 每个 turn 都从原始 Claim 重算相同 transformed Change；其 row index 和幂等状态继续引用 transformed Change，
不改变关系 Sink 的既有 Prepared → effect → settlement 协议。Source ACK 与外部 Sink effect 不合并为同一个 coordinator；除非未来
引入端到端事务协议，它们之间始终保留 durable boundary。

## 持久化边界规则

用户或上层 planner 通过创建两个 Station 来显式保留边界，不增加单独的 `EdgePolicy`。两个 Station 之间的 `connect` 永远是 durable edge。

| 情况 | 物理安排 | 原因 |
| --- | --- | --- |
| 连续 Project/Filter/Extend/Select/SchemaAlign | 同一 inline pipeline | 纯确定变换，无独立恢复价值 |
| core 后的纯变换 | core 的 output pipeline | 在 core 事务中形成最终 output |
| durable fan-out 后每个分支自己的纯变换 | 对应 consumer 的 input pipeline | 每个分支语义不同，共享 producer log |
| 共同纯变换之后 fan-out | producer 的 output pipeline，最终 log 多 subscriber | 变换只计算一次，结果共享 |
| 两个 core Operations | 两个 Station | 各自拥有 continuation、state 或 effect 协议 |
| 外部 Sink/Scan 与另一个 core | Station 边界 | 外部 ACK、幂等 intent 和 fail-stop 独立 |
| 用户要求 materialize、独立 backlog 或容量隔离 | Station 边界 | 这是显式运行语义 |
| exchange/repartition/并行度变化 | Station 边界 | 需要新的 durable identity 与调度域 |
| 会展开多批、阻塞或缓存无界输入的步骤 | core 或 Station 边界 | 不能承诺一次纯变换完成 |

stateful 本身不要求 core 后立刻持久化；它要求所在 Station 只有一个 group-level coordinator。其纯 input/output pipeline 仍可融合。
这里把本地 durable edge 固定为现有 `SubscribedLog`。未来 exchange、barrier 或分布式 channel 是新的调度模型，应单独设计协议；当前不为
尚不存在的 transport 增加通用 Edge 抽象。

因此代表性链：

```text
Scan → Extend → Filter → Select → Aggregate → SchemaAlign → Sink
```

会稳定收敛为：

```text
Station Scan
  output: Extend → Filter → Select
  ↓ durable SubscribedLog
Station Aggregate
  output: SchemaAlign
  ↓ durable SubscribedLog
Station Sink
```

七个逻辑步骤仍全部存在，但只有三个 coordinator Stations 和两条 durable edges。

## 公共构建接口

`StationRef` 继续表示物理 Station。保持当前声明风格，增加两个明确入口：

```rust
let aggregate = factory.station("aggregate", aggregate_definition);
factory.connect([source], aggregate);
factory.inline_input(aggregate, 0, pre_filter)?;
factory.inline_output(aggregate, output_projection)?;
factory.output_capacity_bytes(aggregate, capacity);
```

规则固定为：

- `inline_input(station, port, definition)` 按调用顺序追加到该 port；
- `inline_output(station, definition)` 按调用顺序追加；
- 两者只接受 sealed `InlineOperationDefinition`，并在 Definition instance 不能转换为 `InlineDefinition` 时返回错误；
- foreign `StationRef`、非法 port、Sink output inline、Scan input inline 在 build 的纯校验阶段拒绝，且发生在 Store 创建前；
- 重复调用表示继续追加，不建立隐式覆盖；
- 单 Operation Station 仍通过 `station(id, definition)` 声明，空 pipeline 不需要新 wrapper；
- 显式使用独立 Station 仍是请求 durable boundary 的方式。

具体 Rust 方法名在实现时可以按文档风格微调，但上述语义作为最终 API 约束，不引入临时 `FusedOperationDefinition`。

## Canonical Definition 与资源布局

Flow Definition 仍使用开发期 format version `1`，本次直接把完整 pipeline 形状定义为 v1 的唯一布局并同步更新 golden。
decoder 只读取这一个当前 v1 布局；旧数据库直接删除重建，不保留旧布局 decoder、迁移器或兼容分支。

```text
magic:                         fixed bytes "dogpaddle.flow\0"
format version:                u16 big-endian = 1
station count:                 u32 big-endian
for each station:
  station id length + bytes:   u32 big-endian + UTF-8
  core blob length + bytes:    u32 big-endian + Operation Definition
  input count:                 u32 big-endian
  for each input port:
    producer id length + bytes: u32 big-endian + UTF-8
    input inline count:        u32 big-endian
    for each inline stage:
      blob length + bytes:     u32 big-endian + Inline Definition
  output presence:             u8, exactly 0 or 1
  when presence = 1:
    retained capacity bytes:   nonzero u64 big-endian
    output inline count:       u32 big-endian
    for each inline stage:
      blob length + bytes:     u32 big-endian + Inline Definition
checksum:                       u32 big-endian CRC32 of every preceding byte
```

当前格式仍在开发期，因此直接重定义 v1 字节布局。旧布局数据库不属于输入集合，必须删除重建；实现中不识别、猜测或迁移旧布局。

要求：

- 所有 length/count 在分配前检查 `u32`/`usize` 溢出和剩余字节，presence 的其他值一律拒绝；
- inline stage 顺序属于持久语义；
- decoder 对 unknown tag、非 inline-capable definition、truncation、trailing bytes 和非法结构 fail closed；
- build 继续先 encode，再 decode canonical Definition，只使用 decoded 结果完成 topology、Schema binding 和资源创建；
- open 只读取同一物理 grouping，不重新执行 fusion；
- 更新当前 v1 golden bytes/layout tests，旧数据库删除重建，不加 alias、fallback 或 migration。

只有 core 声明 Store data，物理名称继续是：

```text
station/{index:08x}/operation/{logical_name}
```

inline stages 按类型禁止 data/resource，因此不需要 stage resource namespace。多输入 active Cell、最终 output log、subscriber ID 派生和
schedule 继续使用当前 Station 机制。被融合掉的逻辑边不会创建 catalog entry、SubscribedLog 或 Subscription。

## SQL 的确定性物理分组

Flow 只提供机制，不枚举具体算子，也不自动解释 SQL。`dogpaddle-sql` 先把 DataFusion LogicalPlan lowering 为一个私有逻辑节点 arena，
计算完整 consumer count，再生成 Station Definition。

不能继续在递归 traversal 中看到一个 Filter 就立即创建 Station：共享 CTE、fan-out 和分支专属变换需要先看到全图。

编译固定为五步：

1. lowering 完整 logical arena，并传播每个节点的 exact logical Schema；
2. 对每个 unary candidate 调用 `try_into_inline`；成功者标记为 inline-capable，失败者保留为普通 core，不静默改写表达式；
3. 计算真实 node identity 的 consumer count，并标出显式 materialize/capacity/isolation boundaries；
4. 运行下面的确定性 partitioner，得到 Station programs 与 durable edges；
5. 生成 canonical Flow Definition；Flow 在 encode/decode 后重新 bind 全部 core 和 inline stages，失败即在创建 Store 前终止，
   open 只恢复已经持久化的分组。

arena identity 与输出顺序先固定：从唯一 sink root 做 depth-first postorder，按声明的 input port 顺序访问；同一个 lowering identity
只编号一次，重复 CTE/Scan 引用复用该 identity。禁止用指针地址、Definition 相等或表达式结构相等合并两个节点。Station 按 arena
topological order 声明；Scan 保留 `sql/scan/{index:08x}`，Sink 保留 `sql/sink`，其余 surviving core 按该顺序获得稠密的
`sql/transform/{index:08x}`。被 inline 的节点没有空壳 Station ID，以 `station + input/output + ordinal` 定位。

第一版 grouping 采用确定性的两向归属：

1. Scan、Sink 和非 inline-capable node 先成为 core Station；两个 core 永远不合并；
2. 从每个 core 向下游吸收 pure unary nodes：只要当前尾节点只有一个 consumer 且没有显式 boundary，就按顺序放进 core 的
   output pipeline；被吸收的末节点可以 fan-out，因此共同 prefix 只计算一次，最终 log 拥有多个 subscribers；
3. durable fan-out 后的 branch-specific pure chain 不得回推到 producer；若它线性通向下一个 core，就按逻辑顺序放入该 core 的
   对应 input pipeline；
4. 若 fan-out 后的 pure region 自身再次成为共享节点，不能复制执行或按结构做 CSE；选择该共享节点作为 standalone core adapter，
   其线性前缀进入 input pipeline、线性后缀进入 output pipeline；
5. 显式 materialize、独立 capacity/debug boundary 在归属前先切边；
6. 同一 logical graph 必须稳定产生相同 Station 声明顺序、inline 顺序、subscriber IDs 和 Definition bytes。

### 完整装配示例：共享 CTE fan-out

用户只提交逻辑 SQL：

```sql
INSERT INTO sqlite(
    path => env('RESULT_DB'),
    table => 'routed_numbers'
)
WITH numbers AS (
    SELECT value
    FROM sequence(start => 0)
)
SELECT value AS number
FROM numbers
WHERE value % 2 = 0
UNION ALL
SELECT value AS number
FROM numbers
WHERE value % 5 = 0;
```

lowering 得到的逻辑 DAG 是：

```text
SequenceScan → shared CTE projection ─┬→ Filter(even) → SchemaAlign(number) ─┐
                                      └→ Filter(%5)  → SchemaAlign(number) ─┤
                                                                             ▼
                                                                      UnionAll → SqliteSink
```

分类结果：SequenceScan、二输入 UnionAll 和外部 SqliteSink 是 coordinators；两个 Filter、两个 SchemaAlign 和公共 projection
是 inline-capable。公共 projection 位于 fan-out 前，归入 Scan output；两条分支变换位于 fan-out 后，分别归入 UnionAll 的 input ports。

planner 概念上生成：

```rust
let scan = factory.station("sql/scan/00000000", sequence_scan);
factory.inline_output(scan, shared_projection)?;
factory.output_capacity_bytes(scan, CAPACITY);

let union = factory.station("sql/transform/00000000", union_all_2);
factory.connect([scan, scan], union);
factory.inline_input(union, 0, even_filter)?;
factory.inline_input(union, 0, even_schema_align)?;
factory.inline_input(union, 1, multiple_of_five_filter)?;
factory.inline_input(union, 1, multiple_of_five_schema_align)?;
factory.output_capacity_bytes(union, CAPACITY);

let sink = factory.station("sql/sink", sqlite_sink);
factory.connect([union], sink);
```

最终 physical explain 为：

```text
station sql/scan/00000000
  core: SequenceScan
  output inline: Project(value)
  durable output: 2 subscribers

station sql/transform/00000000
  input 0 from scan/subscriber 0: Filter(even) → SchemaAlign(number)
  input 1 from scan/subscriber 1: Filter(%5)  → SchemaAlign(number)
  core: UnionAll(2)
  durable output: 1 subscriber

station sql/sink
  input 0 from union/subscriber 0
  core: SqliteSink
```

Store 只出现 `flow/definition`、Scan position、Scan output、Union active-input、Union output 和 relation Sink state。五个 inline
definitions 仍进入 canonical plan 和错误定位，但不创建 output log、Subscription、Store data 或 WAL commit。build 将这份 physical plan
持久化；open 只按原分组重装配，不重新判断如何融合。

第一版不运行成本模型，也不整体启用 DataFusion logical optimizer。projection pruning、predicate pushdown 和公共子表达式消除属于后续
语义优化；Station grouping 只改变物理持久化位置，不重写关系语义。

## 可观测性

融合会删除中间 backlog，因此需要显式展示物理计划：

```text
station "normalize-orders"
  input 0: station "orders", subscriber 0
    inline 0: Filter(tag=5)
  core: Aggregate(tag=14)
  output:
    inline 0: SchemaAlign(tag=9)
    inline 1: Filter(tag=5)
    capacity: 67108864 bytes
    subscribers: 2
  durable boundary reason: fan-out
```

第一阶段不为 inline stage 建持久指标或状态。错误必须包含 Station ID、input/output 位置、port 和 stage ordinal；`Flow::status()` 继续报告
真正存在的 Station、Subscription 和 output backlog。后续若性能分析需要 per-stage CPU 计数，可以作为可丢弃运行指标加入，不能形成
新的持久身份。

## 实施安排

### P0：基线与契约冻结

交付物：

- 本文及 Flow README 的最终不变量；
- 一条代表性 `Scan → Extend → Filter → Select → Aggregate → SchemaAlign → Sink` benchmark fixture；
- 记录 commits、WAL bytes、IPC encode/decode 次数与字节、吞吐、延迟和 transaction duration；
- catalog/layout snapshot，明确当前每条逻辑边创建的资源；
- 固定 public API 语义、Definition 字段顺序和 error location。

退出标准：无需实现 fusion，也能量化它将删除哪些成本；所有架构问题在进入 codec 修改前关闭。

### P1：Operation inline seam，行为不变

受影响区域：

- `crates/operation/src/definition.rs`：sealed capability、`InlineBinding`；
- `crates/operation/src/operation/mod.rs`：`InlineTransform` 和 standalone adapter；
- Project、Filter、Extend、Select、SchemaAlign：抽共享 kernel；
- `InlineDefinition` 擦除边界与专用 encode/decode；binder 检查 immutable expression，sealed implementation 承诺其余代数性质；
- operation correctness/metamorphic tests。

这一阶段不修改 Flow Definition，不启用任何融合。五个算子的 standalone public behavior、tag、payload 和资源布局保持完全一致。

退出标准：每个 kernel 的 standalone adapter 与原实现对 Schema、metadata、NULL、diff、行序、空结果和 error 完全等价；
非 inline Operation 无法取得 `InlineBinding`。

### P2：Station pipeline 最小完整纵向切片

一次性落最终 Definition 形状，同时实现 input/output pipeline；首个端到端算子使用 Filter。

为了控制改动面，P2 按三个可独立 review 和回退的合并单元执行。能力在 runtime 已经具备完整语义后才进入持久格式和公共 API：

1. **P2a runtime shell**：先让 binding、`StationParts` 和 `Station` 使用最终 program 容器，但所有 pipeline 为空；Flow Definition bytes、
   Store layout 和运行行为逐字不变；
2. **P2b private runtime slice**：通过 Flow 内部装配入口用真实 Filter 和真实 Scan/Transform/Sink witness 完成 input/output pipeline、
   全删 ack-only、backpressure、`Commit` 重算、active input 和 crash/commit/AfterCommit fault matrix；Definition/codec/public API 仍不变；
3. **P2c atomic exposure**：一次性开放最终 Definition、codec、Station 内 Schema propagation、inline binding 装配、资源拒绝以及
   `inline_input`/`inline_output` 公共 API；同时重定基准当前 Flow v1 布局，并在同一合并单元完成 malformed codec、build/open/reopen
   与 catalog layout 证据；旧库明确重建，不保留旧 decoder。

受影响区域：

- `flow/build/definition.rs`：core、per-port input pipeline、output pipeline；
- `flow/build/codec.rs`：新 canonical layout；
- `flow/build/schema.rs`：Station 内顺序 binding；
- `flow/build/mod.rs` 与 `build/open.rs`：只为 core 创建/open data 和 runtime resource；
- `assembly.rs`：装配 `InlinePipeline`，其拓扑和 subscriber 算法不变；
- `station/runtime.rs`：input transform、ack-only path、core、output transform；
- `FlowFactory`：`inline_input`/`inline_output`；
- Flow golden、malformed、binding、topology、status 和 runtime tests。

必须覆盖三个真实组合：

```text
SequenceScan core + output Filter
input Filter + RunningEventCount core
input Filter + Discard core
```

退出标准：

- build/open/reopen 得到相同 pipeline；
- filtered input 的 ack-only transaction 正确推进 Subscription；
- filtered output 不改变 core 的 Commit/Complete；
- 当前 turn 的 backpressure/提交前 error 不执行 AfterCommit 且不推进输入；commit failure 同样不执行 AfterCommit，
  当前实例 fail-stop，reopen 后以 durable state 恢复；
- catalog 只包含 core data、active Cell 和最终 output；
- 普通单 Operation Station 行为不变。

### P3：迁移完整纯算子族

- Project、Extend、Select、SchemaAlign 启用 inline；
- 建立所有 standalone/inline 差分等价测试；
- 覆盖空 Select、buffer sharing、Field/Schema metadata、cast、Filter all/none/partial；
- 增加多个 inline stages 的顺序、stage error attribution 和 Schema propagation 测试；
- 更新 Flow README、Operation capability/conformance 表和示例。

退出标准：任意合法五算子纯 pipeline 只创建一个 Station output，并在 reopen 后保持 exact Schema 和相同展平 Change。

### P4：SQL 私有 logical graph 与自动 grouping

- **P4a compiler seam**：把 `crates/sql/src/lower.rs` 当前遍历时立即创建 Station 的路径拆成私有 logical node arena、
  singleton partition 和 FlowFactory emitter；先保持 Station 数量、ID 和 Definition bytes 不变；
- **P4b deterministic partitioner**：计算 consumer count、共享 Scan/CTE 和分支，按固定规则生成 core Station、input pipeline 和
  output pipeline；同一个 PR 必须携带 P0 对照性能证据，通过 correctness 与性能门后直接切换 SQL 默认物理计划；
- 保持现有 SQL 关系语义和 endpoint 资源注入；
- 增加 physical plan explain；
- SQL build/open/reopen 与无目录副作用测试覆盖 fused plan。

退出标准：

- 现有 SQL 测试得到相同最终关系；
- 代表性纯链的 Station/output log/commit 数按预期下降；
- fan-out 只共享一个 producer log，每条分支拥有正确 input pipeline；
- open 不运行 grouping；
- 相同 SQL、endpoint discovery 和环境解析结果产生相同 physical Definition bytes；
- 被消除的 transform Station ID 不保留空壳；surviving Station ID 按最终 core 的确定性逻辑位置分配，修改后的 physical plan
  明确要求重建已有 SQL state path。

### P5：完整验收与持续回归门

- 对比 standalone durable graph、显式 pipeline 和 SQL 自动 grouping；
- 检查吞吐、p50/p95/p99、WAL/IPC bytes、transaction duration、峰值 Change bytes；
- 增加 CDC output pipeline 对 ACK latency 和最终 backpressure 传播的 system witness；
- 固化 P4 合并时使用的性能阈值，加入长期 benchmark regression gate；
- 运行完整 `cargo xtask check` 和 owner benchmark smoke。

退出标准：结构成本确定下降，correctness/recovery gate 全部通过，没有不可解释的长事务或外部 ACK 延迟退化。

## 合并顺序与依赖

```text
P0 baseline/spec
       │
       ▼
P1 inline kernel seam ───────────────┐
       │                             │
       ▼                             │
P2 Station pipeline vertical slice  │
       │                             │
       ▼                             │
P3 full pure operator family        │
       │                             │
       ▼                             │
P4 SQL physical grouping            │
       │                             │
       ▼                             │
P5 system/performance regression gate ◄─┘
```

每一步都必须能独立合并并通过工作区 gate。P1、P2a 和 P2b 只增加内部接缝且保持持久字节不变；P2c 是唯一一次 canonical format
rebaseline，并在同一合并单元开放已完整实现的 API；P4b 才改变 SQL 默认 lowering。这让任何问题都能定位到一个清楚边界，
同时不会留下需要未来删除的 CompositeOperation 或第二套执行协议。

建议实际按下表逐 PR 推进，不把相邻行压成一个大改：

| PR | 内容 | 合并前硬门 |
| --- | --- | --- |
| R0 | P0 基线、catalog snapshot、真实纯链 benchmark | 当前语义与成本数据可重复 |
| R1 | P1 `InlineDefinition`/`InlineBinding`/共享 kernel | Operation golden 不变，standalone/inline conformance 全过 |
| R2 | P2a Station program 空容器 | Flow Definition bytes、Store layout、全部现有行为不变 |
| R3 | P2b 私有 input/output runtime | 真实算子、turn 级 fault matrix、现有持久 bytes 不变 |
| R4 | P2c 当前 v1 布局重定基准 + build/open + 最终 public API | v1 golden/malformed/reopen/catalog/transaction 全过；无旧布局兼容代码 |
| R5 | P3 五个纯算子全部接入 | 全量差分、metadata、NULL、diff、重批与 buffer-sharing 全过 |
| R6 | P4a SQL logical arena + singleton emitter | SQL Station IDs、Definition bytes、结果和 reopen 与当前完全一致 |
| R7 | P4b deterministic grouping + 默认切换 | physical snapshot、fan-out/CTE、结果/reopen、P0 性能门全过 |
| R8 | P5 CDC system witness、长期回归门和文档收口 | `cargo xtask check`、bench test mode、owner smoke 与 system gate 全过 |

## 验证矩阵

### Definition 与 build/open

- 当前 format v1 canonical literal、checksum、truncation、unknown tag、trailing bytes，以及未知 format version fail closed；
- inline count/长度 overflow；
- 非 inline-capable definition 出现在 pipeline 时 fail closed；
- Scan input、Sink output、非法 port、capacity mismatch；
- stage binding error 的 Station/port/direction/ordinal；
- resource 缺失、错误类型、多余 resource 仍只针对 core；
- build 所有纯验证失败不创建目录；
- reopen 不增加、遗漏或重命名资源。

### 运行事务

- input pipeline 返回 Some、None、error；
- core 返回 Idle、Commit(None/Some)、Complete(None/Some)；
- output pipeline 每一 stage 返回 Some、None、error；
- 最终 output admitted/backpressured/codec error；
- Store commit success/failure；
- core AfterCommit none/success/failure/panic fail-stop；
- 在 core apply 后、每个 inline stage 之间和 final append 前注入失败；
- 本地 commit 后、effect 前，以及 effect 成功后、durable settlement 前 crash/reopen；
- 第 N 个 continuation page 的 tail error/backpressure 只重试当前页，先前页不重复也不丢失；
- `Complete + AfterCommit` 的恢复证据不依赖已经完成的 Claim；依赖输入的 effect 必须经过 durable intent + `Commit`；
- Claim cache 丢失和完整 Flow reopen；
- 多输入 active port pin、input pipeline drop 与 rotation；
- fan-out subscriber 独立 position 和日志回收；
- Scan、Transform、Sink 三类 core 的真实 witness。

### 等价性

对同一 fixture 同时构造：

```text
每个算子独立 Station 的 durable graph
同样逻辑步骤组成的 Station pipeline
```

比较：

- 展平 output records/diffs；
- 最终 core state；
- Sink 最终 relation；
- exact Schema、Field metadata 和 Schema metadata；
- failure injection 后 reopen 的最终结果；
- 不同合法物理 Change 重批；
- 非 inline-capable tag 与 volatile/未知 volatility 表达式在 decode/bind 时拒绝；每个已注册 implementation 用 conformance 和
  metamorphic fixtures 证明 non-expanding、保序、diff-preserving 与重批同态。

## 性能指标

不预设没有基线支持的百分比目标。长期记录：

- 每个源 Change 的 Store transaction/commit 数；
- `SubscribedLog` append、peek、ack 和 retained bytes；
- Arrow IPC encode/decode 次数和字节；
- RocksDB WAL bytes、write bytes 和 compaction bytes；
- Changes/s、rows/s、p50/p95/p99；
- 每个 Station transaction duration；
- input pipeline 因 core `Commit` 而重算的次数和 CPU；
- final backpressure 持续时间；
- CDC poll-to-ACK latency；
- 峰值 Change bytes 和 Arrow allocation。

结构验收有两个硬条件：

1. N 个被 inline 的逻辑步骤不能创建 N 个 output logs/subscriptions；
2. 一个 Station turn 的中间 Change 不能进入 IPC codec 或 RocksDB。

## 风险与控制

| 风险 | 控制 |
| --- | --- |
| Station 事务变长 | inline 只允许一次完成的纯变换；记录 transaction duration；用户可显式切 Station |
| 最终 backpressure 更早传到 source | 保持当前 turn 原子回滚；测量 CDC ACK latency；需要隔离时显式 durable boundary |
| input pipeline 在 core Commit 后重复计算 | 首版接受确定性重算；记录次数；只在证据充分时加可丢弃 cache |
| 丢失中间 backlog 可见性 | physical explain 显示全部 inline stages；status 只报告真实 durable backlog |
| standalone 与 inline 语义漂移 | 共享同一 kernel，并对所有 fixture 做差分等价 |
| optimizer 版本改变恢复布局 | build 持久化物理 grouping；open 永不优化 |
| volatile expression 重放不一致 | inline capability 只接受已证明 deterministic 的表达式；时间/随机函数继续拒绝 |
| Definition 变更影响旧库 | 直接更新当前 v1 golden，旧库删除重建，不实现识别、迁移或兼容路径 |

## 明确不引入

- `FusedOperationDefinition` 或递归 CompositeOperation；
- 任意 `Vec<Box<dyn Operation>>`；
- Station 内部 DAG、queue、Claim、offset 或 Subscription；
- 多个 core、多个 runtime resource 或多个 `AfterCommit` 的通用组合器；
- 两个独立 commit Station 之间的 volatile edge；
- open-time 或 runtime fusion；
- code generation、JIT 或新的表达式 AST；
- 跨 Flow state/arrangement 共享；
- exchange、barrier 或全图 epoch checkpoint；
- 依据“没有 Store data”猜测 inline 安全性。

## 完成定义

只有全部满足时，这条主线才完成：

- Station 的公共含义已经从单 Operation 容器变成 one-coordinator pipeline；
- 五个现有纯算子同时支持 standalone 和 inline，并共用一份执行 kernel；
- 每端口 input pipeline、output pipeline、ack-only path 和 stage error attribution 完整；
- canonical build/open/reopen 持久化同一物理分组；
- Flow topology、Claim、Subscription、active input、output capacity 和 group-level coordinator 职责没有漂移；
- `AfterCommit` 的 durable intent、可重试 effect 和 settlement 规则在分页与 crash/reopen 下有明确证据；
- SQL 在完整图上确定性 grouping，并能解释每条剩余 durable boundary；
- correctness、fault injection、catalog layout、system witness 和性能基线全部通过；
- 文档不再把一个逻辑 Operation 等同于一个持久化 Station。

## 研究依据

- Materialize 将逐行 Map/Filter/Project 融入 Read、Join、Aggregate 等物理算子，同时把 arrangement 作为独立的有状态索引能力：
  [plan operators](https://materialize.com/docs/sql/explain-plan-operators/)、
  [arrangements](https://materialize.com/docs/fundamentals/concepts/arrangements/)。
- Apache Beam 明确区分同 worker fusion 与 state、shuffle、checkpoint 引入的序列化/持久化边界：
  [execution model](https://beam.apache.org/documentation/runtime/model/)。
- Apache Flink 将多个逻辑 transformation chain 到一个 task/thread，并允许显式开始新 chain：
  [task chaining](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/)。
- Velox 把线性子树转成 pipeline，将 Filter/Project 合并，而在多输入和 exchange 处拆分 pipeline：
  [plan nodes and operators](https://facebookincubator.github.io/velox/develop/operators.html)。
- DataFusion 把 analyzer、logical optimizer 和 physical optimizer 作为不同规则层，projection/filter pushdown 需要全图 traversal：
  [query optimizer](https://datafusion.apache.org/library-user-guide/query-optimizer.html)。
