# dogpaddle-flow

`dogpaddle-flow` 把算子组织成一条可持久化、可恢复的执行图。第一次读代码时，先记住一句话：

> Operation 做计算，Station 决定事务和持久化边界，Flow 按拓扑顺序推动所有 Station。

这里的 **Operation（算子）** 是一次具体计算，例如 Scan、Filter、Join 或 Sink；**Station（执行站）**
是一组按顺序执行的 Operation；**Flow（流水线）** 是由 Station 和它们之间的连接组成的有向无环图。

## 十分钟理解运行骨架

### 1. 数据沿 Station 之间的持久队列移动

DogPaddle 传递的基本单位是 `Change`：一个 Arrow `RecordBatch` 加上每行的增减权重。Station
之间的连接都经过 producer 在 RocksDB 中的持久输出；同一 Station 内的 Operation 直接传递内存中的
`Change`。一个输出分叉时，下游共享这份日志，但各自保存读取位置。

```text
                    一个 Station                         另一个 Station
             ┌─────────────────────────┐              ┌──────────────┐
无输入 ─────▶│ SequenceScan → Filter   │══持久队列════▶│ SQLiteSink   │
             └─────────────────────────┘              └──────────────┘
                       同一事务                             独占
```

上图中 Scan 和 Filter 已经融合进同一个 Station。两者之间没有序列化、RocksDB 队列或订阅游标；
只有 Filter 的最终结果进入持久队列。Sink 单独占一个 Station，因为它需要自己的提交和恢复协议。

这个边界带来两个性质：

- Operation 的本轮状态修改、最终输出和输入确认一起提交，任一步失败就整体回滚。多输入 Station
  切换端口时，会在这之前用一笔独立的短事务固定新端口；它只保存调度选择，不推进输入位置。
- Station 之间可以独立恢复和承受背压；代价是每跨过一条边都要编码、写盘，之后再读取和解码。

### 2. 一次 `advance` 到底做什么

`Flow::advance()` 是宿主显式调用的一轮调度，不是后台线程。它按确定的拓扑顺序访问每个 Station，
每个 Station 最多执行一个 turn（一次有界尝试）。上游在本轮提交的输出，排在后面的下游可以在同一轮读到。

对一个有输入的 Station，一次 turn 的主路径是：

1. 从某个输入队列查看下一条完整 `Change`，解码并放进内存。这个尚未确认的输入称为
   **Claim**，意思是“本 Station 当前正在处理的那一条输入”。
2. 首 Operation 在没有写事务时准备工作。如果暂时无事可做，直接返回 `Idle`。
3. Flow 开启一笔 RocksDB 写事务，执行首 Operation 已准备好的工作，再依次执行 Station 内的尾部 Operation。
4. 把最后一个 Operation 的输出追加到 Station 的持久队列。
5. 如果首 Operation 已完整处理输入，在同一事务中推进输入队列的订阅位置。
6. 提交成功后，才执行外部 ACK 等 `AfterCommit` 动作。

首 Operation 用三种动作描述事务结果：

| 动作 | 含义 |
| --- | --- |
| `Idle` | 本次没有进展，事务内写入全部回滚 |
| `Commit(output)` | 提交状态和可选输出，但保留当前 Claim，下一轮继续它 |
| `Complete(output)` | 提交状态和可选输出，同时确认整个 Claim 已处理完 |

因此 Join 可以把一个大输入分几轮处理：中间轮使用 `Commit` 保存游标，最后一轮使用 `Complete`。
零输入 Scan 没有 Claim，成功时使用 `Commit`。

如果输出队列达到容量，Operation turn 返回 `Backpressured`：算子状态、输出和输入确认都不提交。
多输入 Station 如果刚切换过端口，之前提交的端口选择仍然保留，因此这一轮 Flow 仍报告发生了进展。
如果 Store 已提交而外部 `AfterCommit` 失败，当前运行实例会停止继续调度；调用方必须丢弃它并 `open`，
让 Operation 从持久状态恢复。

### 3. 多输入为什么需要“固定端口”

Join 和 Union 可能有多个输入。Station 每次只领取一个端口的一条完整 `Change`。如果当前端口没有
数据而另一个端口有，它先用一笔独立的短写事务固定新端口，再领取 Change、运行 Operation。后续
Operation 事务即使空闲、背压或失败，也不会撤销这个选择。当前 Change 完成后才轮到后续端口。
这样即使一个 Change 需要多个 turn，Join 处理左侧输入时，右侧匹配状态也不会在中途变化。

真正的输入进度只有持久队列的 Subscription（某条输入边自己的读取进度）。Claim 只是可丢弃的内存副本；进程重启后，
`open` 会从订阅位置重新读出同一条 Change。零输入和单输入 Station 不需要额外的端口状态。

## 为什么一个 Station 可以有多个 Operation

Station 内部只有一条有序直线，不再引入一套拓扑：

```text
head Operation → atomic tail 0 → atomic tail 1 → ... → durable output
```

这里的 **atomic transform（原子转换）** 指能在当前写事务中完整处理一条输入 Change 的算子。
它不保留“当前输入处理到哪里”的游标，不执行外部 I/O，也不产生提交后的动作，所以可以安全地接在别的
Operation 后面。`FlowFactory::append` 只接受单输入 atomic transform。

每个 Operation Definition（可编码、尚未运行的算子定义）会明确报告自己的结构角色：

| 角色 | 白话含义 | 在 Station 中的位置 |
| --- | --- | --- |
| `Scan` | 不读取上游，主动产生 Change | 可以做首项，也可带 atomic 尾链 |
| `AtomicTransform(n)` | 在一笔事务内完整消费输入 | 任意输入数可做首项；只有单输入实例可追加 |
| `TurnTransform(n)` | 可能用多轮处理一条输入，并持久化进度 | 可以做首项，也可带 atomic 尾链 |
| `ExclusiveTransform(n)` | 需要在它之后形成独立的持久边界 | 必须独占 Station |
| `Sink(n)` | 消费输入但没有 Flow 输出 | 必须独占 Station |

首项决定 Station 有几个输入，末项决定 Station 的输出 Schema。尾项固定接收前一项输出，某一项返回
`None` 时，后续项不再执行。所有已执行的状态修改仍服从首项的 `Commit` 或 `Complete`，并与最终输出
处于同一事务。

这些限制让融合后的失败语义仍然简单：尾项失败、最终输出背压或事务提交失败时，整条线都能从未改变的
持久状态重试。分叉和多输入关系继续由 Station 之间的 Flow 拓扑表达。

## 最小公共 API

下面的 Flow 只有两个 Station。`numbers` 内含 SequenceScan 和 Filter，`sink` 独占：

```rust,no_run
use std::num::NonZeroU64;

use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::{
    col, lit,
    operation::{
        scan::SequenceScanDefinition,
        sink::DiscardDefinition,
        transform::FilterDefinition,
    },
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let state = root.path().join("flow");

    let mut factory = FlowFactory::new(&state);
    let numbers = factory.station("numbers", SequenceScanDefinition::new(0));
    factory.append(
        numbers,
        FilterDefinition::try_new(col("value").eq(lit(7_u64)))?,
    )?;
    factory.output_capacity_bytes(
        numbers,
        NonZeroU64::new(64 * 1024 * 1024).unwrap(),
    );

    let sink = factory.station("sink", DiscardDefinition::new());
    factory.connect([numbers], sink);

    let mut flow = factory.build()?;
    flow.advance()?;

    drop(flow);
    let mut reopened = FlowFactory::new(&state).open()?;
    reopened.advance()?;
    Ok(())
}
```

`FlowFactory` 的核心调用只有这些：

| API | 作用 |
| --- | --- |
| `new(path)` | 指定这条 Flow 独占的状态目录 |
| `station(id, definition)` | 创建 Station，并放入首 Operation |
| `append(station, definition)` | 在现有 Station 末尾追加单输入 atomic transform |
| `connect(inputs, station)` | 按顺序连接 Station 的全部输入端口 |
| `output_capacity_bytes(station, bytes)` | 设置该 Station 持久输出的保留字节高水位 |
| `resource(station_id, value)` | 注入密码、连接配置等不持久化的运行资源 |
| `build()` | 校验声明、创建状态并返回运行态 `Flow` |
| `open()` | 从已有状态目录恢复运行态 `Flow` |

`StationRef` 只是声明期句柄，只能在创建它的 Factory 中使用。字符串 ID 才是持久身份。

## `build` 与 `open`

### 第一次构建

`build` 在创建 Store 前完成能纯计算的工作：

1. 校验 Station、输入顺序、容量和整张 DAG。
2. 稳定编码声明，再立即解码；后续只使用这份即将持久化的 canonical（规范化）Definition。
3. 按拓扑传播 Arrow Schema，并按 Station 内顺序绑定每个 Operation。
4. 校验每个 Operation 声明的持久数据和运行资源。
5. 创建 Store、Operation 状态、Station 输出和订阅位置，最后原子发布 Flow Definition。

因此常见的拓扑、Schema 和资源类型错误不会留下目标目录。底层 Store 在创建后的失败仍可能留下一个
不完整目录，`open` 会拒绝把它当成完整 Flow。

构建规则包括：Station ID 非空、唯一且不含 NUL；图必须无环；输入数和顺序必须与首 Operation 一致；
每个起点必须以 Scan 开始，每个终点必须是 Sink；每个有输出的 Station 必须有直接消费者，并且恰好设置
一次非零容量；Sink 不能设置输出容量。一个输出可被多个下游消费，每条边有独立订阅位置。

### 重新打开

`open` 的 Factory 只应包含路径和临时运行资源：

```rust,no_run
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::operation::scan::PostgresCdcScanConfig;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let config = PostgresCdcScanConfig::new_unencrypted(
    "/opt/dogpaddle-debezium",
    "127.0.0.1",
    5432,
    "shop",
    "cdc",
    std::env::var("CDC_PASSWORD")?,
)?;
let mut factory = FlowFactory::new("./shop-flow");
factory.resource("orders", config)?;
let mut flow = factory.open()?;
flow.advance()?;
# Ok(())
# }
```

`open` 从磁盘读取 Station 拓扑、Operation 顺序、定义和容量，重新做相同的 Schema 绑定，打开所有已声明
状态并装配运行对象。它不会接受另一份拓扑声明，也不会重新决定如何融合。凭据、进程内 connector 等
runtime resource 不写入磁盘，必须按 Station ID 再次注入。

当前 v1 不读取旧布局、不迁移旧数据库。修改定义、拓扑或融合结果后，应删除旧状态或使用新目录。

### 磁盘里有什么

一条 Flow 的持久状态由四部分组成：

- canonical Flow Definition：Station ID、有序 Operation Definition、输入 Station ID 和输出容量；
- 每个 Operation 自己声明的数据，例如计数、Join 两侧关系或 CDC checkpoint；
- 每个 Station 最终输出的 `SubscribedLog` 及每条下游边的订阅位置；
- 多输入 Station 当前固定的输入端口。

内存 Claim、调度顺序、运行连接、物理表达式和派生 Schema 不单独持久化。`open` 从 Definition 和输入
Schema 确定性重建它们。Operation 状态的完整资源名包含 Station 与 Operation 的序号，因此同一 Station
中的多个 Operation 不会冲突。

## 调度结果、背压与状态观察

一轮 `advance` 返回：

| 结果 | 含义 |
| --- | --- |
| `Progressed` | 至少一个 Station 提交了进展 |
| `Backpressured` | 没有提交进展，至少一个输出因为容量被拒绝 |
| `Idle` | 没有提交进展，也没有观察到背压 |

结果优先级是 `Progressed > Backpressured > Idle`，背压不会阻止本轮继续访问其他 Station。宿主可在
整轮 `Idle` 或持续背压时等待；Flow 自身没有后台循环。

`Flow::status()` 使用一个只读快照返回每个 Station 的 ID、最近一轮结果、是否需要 reopen、输入订阅的
`position/tail`，以及输出的 `head/tail/retained_bytes/capacity_bytes`。它不解码 Change、不调用 Operation、
不连接外部系统，也不开写事务。

```rust,no_run
# fn inspect(flow: &dogpaddle_flow::Flow) -> Result<(), dogpaddle_flow::FlowError> {
for station in flow.status()? {
    println!("{}: {:?}", station.id, station.last_outcome);
    for input in station.inputs {
        println!("  waiting Changes: {}", input.tail - input.position);
    }
}
# Ok(())
# }
```

backlog 的单位是完整 `Change`，不是行数。输出容量限制的是持久日志中尚未被所有订阅者释放的逻辑字节；
空日志允许接收一个超过容量的单条 Change，避免永久卡死。因此它是背压高水位，不是进程内存硬上限。

## 从哪里开始读源码

按下面顺序读，可以避开编解码和错误枚举的细节：

1. [`src/build/mod.rs`](src/build/mod.rs)：`FlowFactory` 的全部公共声明 API 和 `build`。
2. [`src/flow/advance.rs`](src/flow/advance.rs)：一轮调度只有几十行，是运行入口。
3. [`src/station/runtime.rs`](src/station/runtime.rs)：一个 Station 如何执行首项、尾链、输出和提交。
4. [`src/station/input.rs`](src/station/input.rs)：Claim、输入端口、订阅位置和多输入固定规则。
5. [`src/build/schema.rs`](src/build/schema.rs)：全图 Schema 如何逐项绑定。
6. [`src/assembly.rs`](src/assembly.rs)：已验证 Definition 如何变成运行期 Station。
7. [`src/build/codec.rs`](src/build/codec.rs) 与 [`src/build/open.rs`](src/build/open.rs)：持久格式和恢复路径。

Flow 只实现装配、拓扑、调度和事务边界，不枚举具体算子，也不包含 SQL planner。算子语义见
[`dogpaddle-operation`](../operation/README.md)，底层集合和事务见
[`dogpaddle-store`](../store/README.md)，SQL 自动装配见 [`dogpaddle-sql`](../sql/README.md)。

## 当前边界与验证

- 调度是单进程、顺序、每轮每 Station 最多一次 turn；没有并发调度器。
- 拓扑、Station program 和持久资源在 build 后不可修改。
- 每条 Flow 独占一个 Store 和唯一写事务能力。
- 外部 Scan/Sink 的连接、快照和幂等协议属于具体 Operation；Flow 没有 connector 专用分支。
- 当前只有开发期 v1，破坏性修改要求重建状态，不提供兼容层。

常用验证命令：

```sh
cargo test -p dogpaddle-flow --test correctness
cargo test -p dogpaddle-flow --doc
cargo test -p dogpaddle-flow --benches --locked
```

真实 PostgreSQL、CDC 和崩溃恢复 gate 见仓库根目录的 [`TESTING.md`](../../TESTING.md)。
