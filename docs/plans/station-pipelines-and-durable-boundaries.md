# 线性 Station 与持久化边界

状态：v1 实现契约
日期：2026-09-12

## 目标

一个 Station 保存并按顺序执行一个非空 Operation 列表。列表内部共享一次 Store 写事务，只把最后一个 Operation 的输出写入 `SubscribedLog`。因此短小的变换、有状态关系算子和 Scan 后处理都不再为每一步建立持久日志、Subscription 和调度 turn。

本设计只表达线性组合。分叉、汇合和多个输入仍由 Flow 的 Station DAG 表达。Station 内没有第二张拓扑、内部队列、内部 cursor 或中间持久化结果。

## Operation 身份

每个 sealed `OperationDefinition` 按具体实例显式声明完整身份：

```rust
pub enum OperationKind {
    Scan,
    AtomicTransform(NonZeroU32),
    TurnTransform(NonZeroU32),
    ExclusiveTransform(NonZeroU32),
    Sink(NonZeroU32),
}
```

`AtomicTransform` 表示它能在一次事务中完整处理一个输入 Change。它可以持久化状态、增加输出行或改变 diff；身份不能从空 data、具体 tag 或拓扑位置推断。

`TurnTransform` 使用完整 turn/continuation 协议，只能位于 Station 首项，但可以带单输入 Atomic 尾链。它必须能从未变化的 durable state 安全重放未提交 turn；continuation 和尾项状态随同一次事务提交。

`ExclusiveTransform` 使用完整 turn 协议并独占 Station，表示它需要在下游执行前先形成独立持久化输出边界。表达式算子按 Definition 实例检查确定性：合格实例是 Atomic，不合格实例保持现有单算子绑定和执行行为，但没有融合资格。Aggregate 对全部 group expression 和 aggregate argument 执行同样检查。

装配规则只有以下几条：

- Scan 只能在 ordinal `0`，可以带 Atomic 尾链。
- N 输入 Atomic 可以在 ordinal `0`；ordinal 大于 `0` 时必须是单输入 Atomic。
- N 输入 TurnTransform 只能在 ordinal `0`，可以带单输入 Atomic 尾链。
- Exclusive 与 Sink 必须独占 Station，包括 Discard。
- 首 Operation 的 arity 是 Station 的外部输入数量；末 Operation 的 Schema 是 Station 的最终输出 Schema。

## 唯一装配 API

`FlowFactory::station` 创建带一个 Operation 的 Station，`append` 向它的末尾追加一个单输入 Atomic：

```rust
pub fn append<D: OperationDefinition>(
    &mut self,
    station: StationRef,
    definition: D,
) -> Result<&mut Self, TopologyError>;
```

```rust
let compute = factory.station("compute", scan);
factory.append(compute, filter)?;
factory.append(compute, aggregate)?;
factory.output_capacity_bytes(compute, capacity);

let sink = factory.station("sink", discard);
factory.connect([compute], sink);
```

`StationRef` 始终表示整个 Station 及其最终输出。公共面不提供中间 Operation 引用、per-port pipeline 或另一种 builder。`append` 在修改 Factory 前检查引用归属、尾项身份和目标 Station；canonical build/open 再对完整 Definition 执行相同结构校验。

## 执行契约

事务内完整消费接口为：

```rust
pub trait AtomicOperation: Send + 'static {
    fn apply(
        &mut self,
        input: OperationInput<'_>,
        access: TransactionAccess<'_>,
    ) -> Result<Option<Change>, OperationError>;
}
```

Atomic Operation 不保存当前输入，不产生 `AfterCommit`，不接收事务启动或提交能力，不执行外部 I/O。所有影响重放的状态都经当前 `TransactionAccess` 更新。即使它已经成功，后续 Operation、output admission 或 commit 失败也必须能从未变化的 durable state 重试。

Scan、Sink、TurnTransform 和 ExclusiveTransform 使用 `TurnOperation`，即原有 `turn → PreparedTurn → Action + AfterCommit` 协议。运行实例统一表示为：

```rust
pub enum Operation {
    Atomic(Box<dyn AtomicOperation>),
    Turn(Box<dyn TurnOperation>),
}
```

Atomic 作为首项时由统一适配器生成 `Action::Complete(output)` 与空 `AfterCommit`。运行态 Station 将已验证的列表拆成首 Operation 和 Atomic 尾项，仅用于满足 Rust 的独占借用；Definition、binding 和持久化仍是一条普通列表。

一次 Station turn 的顺序固定为：

```text
head.turn（无写事务）
→ begin
→ head prepared apply
→ tail[0].apply → ... → tail[n].apply
→ 最终 output + 输入 acknowledgement
→ commit
→ head AfterCommit
```

首项接收原始端口，尾项固定接收端口 `0`。某一步返回 `None` 时只停止余下链，首项原来的 `Commit/Complete`、已经写入的状态和 AfterCommit 不变。错误、背压和 commit failure 回滚全部 Operation data、最终 output 与 acknowledgement，并丢弃 AfterCommit。commit 成功后的 AfterCommit 失败继续使用现有 fail-stop/reopen 规则。

Scan 的 checkpoint、尾项状态和最终 output 在同一事务提交，外部 ACK 在提交后执行。Station 不建立中间 log、第二份 input position 或 continuation。

## Binding 与资源

Operation Definition、decoder 和 `OperationBinding` 各只有一套。Binding 内部的 materializer 区分 Atomic 与 Turn，以保证 kind 和运行能力在创建 Store 前一致。表达式只编译一次；不合格 Atomic kernel 通过私有 Turn 适配器独占执行，不复制算子实现。

Schema 从 Station 外部输入开始逐项传播：

```text
ordered input Schemas
→ operation[0].bind
→ operation[1].bind(previous output)
→ ...
→ Station final output Schema
```

所有 Definition binding 和 data 声明成功后才创建 Store。每个 Operation 的 data 按 ordinal 隔离：

```text
station/{station_index:08x}/operation/{operation_index:08x}/{logical_name}
```

单 Operation Station 也使用 ordinal `0`。Runtime resource 仍以 Station ID 注入，只允许首 Operation 消费；Atomic 尾项没有 runtime resource 或外部副作用。

Station 的 `output`、多输入 `active-input`、稠密 subscriber ID 和 schedule 继续由现有 Flow 拓扑唯一派生。output log 的 Schema 来自最后一个 Operation。

## v1 Definition

Station 的 canonical 编码顺序为：

```text
station ID
non-zero operation count
length-prefixed ordinary Operation definitions
input count and ordered producer station IDs
output presence and retained-byte capacity
```

不持久化 atomic flag；decode 后由具体 Definition 重新声明 kind 并接受统一校验。保留当前 Flow magic、version `1` 和 CRC，直接替换 golden。旧布局不识别、不迁移，数据库删除重建。

## SQL 智能装配

SQL lowering 已产生确定性后序 logical arena。编译器先统计每个节点的直接消费边数量，再做一次正向扫描。当前节点只在同时满足以下条件时追加到上游 Station：

1. 当前节点是单输入 Atomic Transform；
2. 它的唯一上游节点只有一条直接消费边；
3. 上游节点仍是所属 Station 的末项；
4. 该 Station 允许追加。

否则当前节点创建新的 Station，并用普通 Flow edge 连接输入。消费数按边计算，同一 producer 同时连接 Union 的两个端口算两条边。

```text
Scan → Filter → Aggregate → Select → Sink

[Scan, Filter, Aggregate, Select] → [Sink]
```

```text
[Scan A] ─┐
          ├→ [UnionAll, Filter, Distinct] → [Sink]
[Scan B] ─┘
```

分叉前的线性尾部可以留在 producer Station；分叉后的每个分支独立成链。TurnTransform 可以吸收后续单输入 Atomic；Exclusive 前后都有持久边界，Sink 独占。v1 使用最大合法线性融合，不设链长阈值或成本模型。Scan ID、稠密 Transform Station ID、Sink ID 和 64 MiB output capacity 规则保持稳定。

`open` 只恢复持久化的 Flow Definition，不重新编译 SQL 或重新分组。

## 验证

公共证据集中验证边界，不按每个算子复制矩阵：

- append 的原子拒绝、canonical 非法列表、Schema 中途失败且无目录副作用；
- 两个真实有状态算子的资源隔离、共同提交、下游错误和背压后的共同回滚、重试与 reopen；
- `None`、多输入 active/Claim、Scan checkpoint 与 AfterCommit 的完整事务语义；
- SQL 的最长线性链、分叉、共享尾部、重复输入边、Union、Exclusive 和独占 Sink；
- v1 golden、operation ordinal data 路径及最终 output subscriber 布局；
- 融合与未融合 fixture 的结果一致性，以及 Station、log、事务和延迟对比。

普通工作区 gate 不依赖 Java 或 PostgreSQL。现有 PostgreSQL gate 用一条 CDC 后接状态 Atomic 的链覆盖外部 ACK、回滚和 reopen 接缝。

## Join 边界

Join 作为多输入 `TurnTransform(2)` 位于 Station 首项。它用 durable continuation 分页处理无界 fan-out，并可在同一 Station 中把每页输出继续交给 Filter、Projection 等单输入 Atomic；尾项错误、背压或 commit failure 会把该页的 Join continuation、状态与尾项状态共同回滚。Join key 表达式归 Join Definition 所有，融合不要求预先创建 `func(A)` 的持久化辅助列。
