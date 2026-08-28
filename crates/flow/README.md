# dogpaddle-flow

`dogpaddle-flow` 用公共 `FlowFactory` 定义、构建和重新打开一条持久化 Flow；成功返回的
`Flow` 只表示运行态，不承担声明、构建或打开职责。Station 当前是 crate 内部的一对一 Operation
容器；它读取所包裹 Definition 显式声明的 `OperationCategory`，向 Flow 提供 source、sink、输入数量
和 output 属性。Flow 不枚举具体算子；未来一个 Station 包裹多个 Operation 时，只需在 Station
内部归纳这些属性，不必改变 Flow 的拓扑接口。

## 构建 Flow

Factory 的声明阶段没有 Store 副作用：`station()` 返回仅属于当前 `FlowFactory` 的临时
`StationRef`，`output_capacity_bytes()` 为每个具有 output 的 Station 声明持久化字节高水位，
`connect()` 记录目标 Station 完整、有序的输入列表。只有 `build()` 才集中校验并创建 Store。

```rust,no_run
use std::num::NonZeroU64;

use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::{
    operation::sink::DiscardDefinition,
    operation::source::SequenceSourceDefinition,
    operation::transform::CountDefinition,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let count = factory.station("count", CountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    let capacity = NonZeroU64::new(64 * 1024 * 1024).unwrap();
    factory.output_capacity_bytes(source, capacity);
    factory.output_capacity_bytes(count, capacity);
    factory.connect([source], count);
    factory.connect([count], sink);

    let flow = factory.build()?;
    assert_eq!(
        flow.station_ids().collect::<Vec<_>>(),
        ["source", "count", "sink"]
    );
    drop(flow);

    let reopened = FlowFactory::open(&path)?;
    assert_eq!(reopened.station_count(), 3);
    Ok(())
}
```

Station ID 必须非空、不能包含 NUL，并且在一条 Flow 内唯一。连接保留 source 顺序，允许
fan-out 和重复 source；连接数量必须与 Station 对外声明的输入数量完全一致，整个拓扑
必须是 DAG。所有入度为零的起点必须是 Source Station，所有出度为零的终点必须是 Sink Station；
Sink 没有 output，不能作为其他 Station 的上游。允许多个 Source、多个 Sink 和多个互不连接的
合法 DAG 分量。每个 `OperationCategory::has_output()` 为 true 的 Station 必须且只能声明一次非零
output capacity，outputless Station 不得声明；重复、遗漏、类别不匹配或 foreign `StationRef` 都在
纯校验阶段失败。拓扑、容量校验或 manifest 编码失败不会创建目标目录。

## 持久化边界

每条 Flow 独占一个 Store。`FlowFactory::build()` 先完成纯校验，再为 Flow 和每个 Station 各声明一个
持久化 state map，按 Operation Definition 的逻辑数据名声明全部状态空间，并为每个具有外部
output 的 Station 创建一个 output log，最后提交 manifest Cell 作为构建完成
标记。Operation Definition 返回稳定的“逻辑名称 → 完整数据类型”声明；`FlowFactory` 的
build/open 通路负责完整资源名，并通过 Store 将每项声明创建或打开为具体实例，再按逻辑名称
交给 Definition 直接装配 Operation。绑定不依赖声明顺序，具体算子不接触 Store、底层句柄或
物理布局。

Flow definition Cell 固定使用共享布局；Flow state map 和 Station state map 显式声明为
`Small`。Flow state map 保留生命周期状态；Station state map 保存运行期 Station 状态，并为每个
input 保存下一条未处理 Change 的 offset；有输入的 Station 还保存循环查找的 active input。
`build()` 在发布 manifest 的同一事务中把 active input 和全部 cursor 显式初始化为 `0`，不存在
“缺失时从当前 log head 开始”的隐式恢复。output 是
`AppendLog<Vec<u8>>`；每个 value 保存一个内嵌 Schema 的完整 Change IPC Stream，不另建 Schema
Cell。每个 output capacity 直接保存在 Flow Definition 中，build/open 都把它与对应 output log
装配成一个不可失配的运行期 capability，不创建另一份 Station state。端口 Schema 一致性属于
Flow/Station 契约，不依赖 Change codec 之外的 Schema resource 或
fingerprint 维持。运行层只能使用已经声明的 map 和日志，不能动态新增数据空间。

Store 随后被转换为唯一的 `Transactions`；build/open 在取得完整所有权时以 consuming `split`
显式获得同环境的 `ReadTransactions`，Flow 长期持有返回的读写两种能力。`ReadTransactions` 不可
克隆但可安全共享，并且只能开启 RO snapshot。Station 不长期保存任何事务启动能力：内部
输入准备只在调用期间借用 `ReadTransactions`，并在需要把选中端口固定为 active input 时临时借用
`&mut Transactions`；`Station::process` 同样只在调用期间借用 writer。后者无法消费 writer 来
split 出 owned reader。拓扑和资源目录都没有运行期修改入口。

资源创建和 Station 装配分成两遍。第一遍按声明顺序创建或打开全部 state、Operation data 和
output；第二遍才按 Definition 中的有序 source ID 找到上游 output，并将 clone 单向衰减为
`ReadOnly<AppendLog<Vec<u8>>>` 注入下游。声明顺序不必是拓扑顺序，fan-out 仍共享同一份日志。
每条 consumer edge 还把下游 state 衰减成只能读取对应 cursor 的内部 capability，注入上游用于
GC。Station 不知道上下游 Station ID，只拥有自己的有序 input logs、唯一 owned input-Change
cache、state、带固定 capacity 的完整可选 output 以及 output consumer cursor capabilities。
装配同时从已验证的 Definition 派生运行期 schedule：先按拓扑层次排列，同一层按 Station 声明
顺序排列，并为每个 target 保留按上游 Station 去重的 GC 触发列表。二者完全由 Definition 推导，
build/open 得到相同结果，不需要单独持久化。输入准备在 Station 唯一 cache 为空时，通过一个 RO
snapshot 从 durable active input 开始循环检查各端口，跳过空日志，并只选择第一个可用 entry。
选中端口若不是当前 active input，Station 在调用 Operation 前用一个独立的短写事务把它固定为
active input，cursor 保持不变；已有的 `active input + cursor` 因而就是 durable input claim，不增加
另一套 current-input key。随后 cache 保存该 entry 的 input index、AppendLog offset 和完整 owned
Change；cache 只是 durable claim 的内存副本，命中时不访问 Store。重开 Flow 时 cache 为空，并
根据 active input 与对应 cursor 重建同一输入。零输入 Source 不经过 input claim，但仍由相同的
Station `process` 调用 `turn(None, ...)`，没有 Source 专用 outcome 或事务路径。没有 output 的 Sink
使用 `None`，不能被其他 Station 作为 source。

`process(&mut Transactions)` 为每次 Operation 调用开始并持有唯一写事务。有输入 Operation 每次只
接收端口、一个完整 `Change` 和不能提交的 `TransactionAccess`，不会看到 `AppendLog` offset、
cursor 或 Station 运行元数据；Source 在同一入口收到 `None`。Station 在调用前要求 cache 的 port
等于 durable active input，且 cache offset 仍等于该端口的 durable cursor。

Operation 返回的 `TurnDecision` 把“是否提交”与“输入是否完成”分开：`Idle` 丢弃本次事务，因而
不发布 output、不保存 Operation 写入，也不改变当前输入；`Commit` 的 `input` 与可选 `output`
正交。零输入调用必须返回 `input: None`，有输入调用必须返回 `Some(Keep)` 或
`Some(Complete)`，形状不匹配是协议错误。`Keep` 在同一事务中提交 Operation continuation 与可选
output，但不推进 cursor、不轮转 active input，也不清 cache；`Complete` 才原子提交 Operation 状态、
可选 output、cursor 推进和 active input 轮转，并在 commit 成功后清 cache。任何 Operation、编码、
append、Station state 或 commit 错误都会回滚本次事务并保留 durable claim 和 cache。

Operation 在调用前不会因 output 已达到水位而被跳过，因为 Station 尚不知道本次是否产生 output
以及编码后大小。没有 output 的 `Commit` 即使日志已经达到水位也可以正常提交；有 output 时
Station 先完成 Change 编码，再调用 `AppendLog` 的 capacity-aware append。非空日志追加后超过
capacity 会正常返回背压而非错误：本次 Operation 写事务整体回滚，Source position、`Keep`
continuation、`Complete` cursor/active 都不前进，cache 与 durable claim 保留，下一 turn 重新执行
Operation。物理空日志按 `head == tail` 判断并允许一条 oversize entry，避免单个合法 Change 永久
无法前进。这是一项 per-output soft high watermark，不是 MDBX 文件大小或进程内存硬配额。

只要当前输入尚未 `Complete`，无论前一 turn 是 `Idle`、`Keep`、错误、output append 失败、commit
失败还是进程重开，下一次调用都必须收到同一 `(port, offset, bytes)` 所标识的完整 Change；这要求
同一日志 entry 的原始字节不变，不要求重新解码后拥有相同内存地址。Change 内部的处理位置属于
Operation continuation，必须存入该 Operation 通过 Definition 声明的 Store 状态；Station 只拥有
durable active input、各 input cursor 和可丢弃重建的 owned cache。

每条边的 cursor 只定位一个完整 Change IPC entry；它是当前持久化分批下的读取位置，而不是稳定
event ID。Station 可以为了吞吐稳定地合并或切分物理批次，但变换前后展平的输入事件序列必须
逐项相同，也不能隐式 consolidation。不同输入边的 offset 彼此不可比较；active input 既是未完成
输入的 durable port，又在没有未完成输入时规定从哪个端口开始循环寻找下一个可用物理 Change，
不声称还原跨上游事件发生时间。只有 `Complete` 才把 cursor 推进到下一个 offset。Operation 的
展平 output 事件序列和最终业务状态必须同时对稳定重批及同一 Change 的 `Keep` turn 切分保持不变；
需要业务级跨端口顺序时，必须另行引入逻辑 ingress、barrier 或窗口语义。

每个 Station turn 只要成功返回，无论结果是 `Idle`、`Backpressured` 还是 `Progressed`，Flow 都会按去重后的直接
上游列表各触发一次 GC。上游 Station 在独立写事务中读取自己全部 consumer edge 的 durable
cursor，校验它们位于 output 的 `[head, tail]`，以最小 cursor 为安全水位，并调用一次
`truncate_before`，单次最多删除 1024 个 entry。重复 source edge 在触发列表中只调用一次，但每条
edge 的 cursor 都参与最小值；GC 失败以实际执行 GC 的上游 Station ID 返回。Operation 与 cursor
以 `Complete` 成功提交后，后续 GC 才允许回收对应 Change；`Keep` 即使已经发布 output，也因
cursor 不动而继续保护当前 entry。consumer cursor 只表示可回收 target；只有 truncate 事务成功
提交并真正推进物理 head，AppendLog 的 retained-byte 账本才减少，该 GC 才算 progress。即使下游
Operation 已经 Idle，补偿 GC 仍会让本轮返回 `Progressed`，保证调用方继续调度直到容量真实释放。

`Flow::advance` 聚合为 `Progressed > Backpressured > Idle`：任一 Operation、durable input pin 或
物理 GC 有提交就返回 `Progressed`；整轮没有提交、但至少一个实际 output 被容量拒绝时返回
`Backpressured`；既无提交也无容量拒绝才返回 `Idle`。背压不会提前终止 schedule，所以下游和其他
DAG 分量仍获得本轮 turn。fan-out 共享一份 output log 和 capacity，最慢 consumer 的 cursor 会有意
阻塞整个 producer；各下游独立持有的 decoded cache 不计入该容量。

Store 目录和 catalog 已有效、但 manifest 尚未提交时，`FlowFactory::open()` 返回
`IncompleteBuild`；
manifest 已发布却缺少所声明资源时返回 `MissingResource`。如果底层 `Store::create()` 本身只
留下无效目录，则打开时保留相应 Store 错误，不把它误报成有效 Flow 的未完成构建。

`FlowFactory::open()` 先读取、解码并重新校验 manifest，再打开全部数据对象和 output，最后按
source ID 重新注入 inputs、装配 Station 并冻结 Store。调用方不需要重新提交 Definition。

当前磁盘格式使用显式 magic、版本号、定长整数、sealed Operation Definition 集合的稳定 tag
和 IEEE `CRC32` 完整性校验，不依赖 Rust enum 布局或通用序列化框架。以下名称是兼容性边界：

- Flow manifest：`flow/definition`
- Flow 状态：`flow/state`
- Station 状态：`station/{index:08x}/state`
- Station active input key：`input/active`
- Station input cursor key：`input/{input_index:08x}/cursor`
- Station 输出：`station/{index:08x}/output`（仅限具有外部 output 的 Station）
- `SequenceSource` 位置：`station/{index:08x}/operation/sequence_source.position`
- Count 状态：`station/{index:08x}/operation/count`

`index` 是 Station 声明顺序，`input_index` 是该 Station 持久化 source 列表中的端口顺序。active
input value 固定为 4 字节 big-endian `u32`，cursor value 固定为 8 字节 big-endian `u64 offset`。
当前仍是开发期 v1；output capacity 直接成为新的 v1 Station Definition 布局，不解码旧布局。
编码、tag 或资源布局可以破坏性调整，但每次调整必须同步更新黄金字节、布局
和 reopen 测试，不保留旧格式兼容层。

## 源码边界

`build/` 统一拥有 `FlowFactory` 的声明与 build/open 路径、`StationRef`、Flow/Station Definition、
无 Store 副作用的图校验、稳定磁盘编码和完整资源名；构建路径创建全部资源，`build/open.rs`
完成两阶段 Definition 读取、资源打开和重新物化。两条路径都按 Definition 声明的逻辑数据名
通用创建或打开类型化实例，再让 Definition 物化具体 Operation；二者都不枚举具体算子。拓扑只是
Flow Definition 中的连接关系，不再拥有独立构建类型。`flow/mod.rs` 只声明模块并导出
`Flow`，`flow/runtime.rs` 保存 build/open 返回的运行态对象、生命周期状态、全部 Station、派生的
schedule 与分离的读写事务启动能力，不创建或打开 Store 资源。`flow/advance.rs` 定义公共
outcome 并实现一次有界轮次：按 schedule 为每个 Station 至多提供一个 turn，先 `intake` 再进入
`process`，成功后再对直接上游各触发一次 GC，通过公共 `Flow::advance` 暴露。`station/mod.rs`
只声明边界；
`station/runtime.rs` 保存 Station 装配与 `process` 边界，`station/input.rs` 独立拥有 active input、
cursor 和唯一 cache 的 `intake`，`station/gc.rs` 保存 consumer cursor capability 与有界 output GC，
`station/protocol.rs` 只拥有 outcome/error。Station 不长期持有事务启动
能力，也不接收 Store，不知道 Station ID、上游 Station、资源名或底层物理布局。私有
`assembly.rs` 只承接 build/open 共用的 source ID 解析和最终 Station 装配，不公开新的领域类型。
公共错误单独位于 `error.rs`；私有单元测试放在对应源码模块目录的 `tests.rs` 中，`tests/`
使用单一 `correctness` target 验证 crate 的公共行为。dogpaddle-flow 是 Operation 与 Store 的
组合根，因此公共测试可以通过二者的公共 API 检查实际资源布局和重新物化；无需再建立一个
重复的 `flow-store` 集成 package。完整目录、fixture 规则与计时边界见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/TESTING.md)。

## 当前边界

本阶段完成定义、持久化 `build/open`、Flow 对分离读写事务启动能力的所有权，以及 Station 的
state、只读 inputs、可选 output、稳定 active input/cursor 和可重建的确定性拓扑 schedule。
输入准备已能通过 RO snapshot 从 active input 循环查找、durable pin 并幂等准备至多一个带来源
身份的 Change；`process` 已支持 `Idle`、`Commit(Keep)` 和 `Commit(Complete)`，在同一写事务中
按 decision 原子协调 Operation continuation、output、active 与 cursor，
成功 turn 后的直接上游 GC 调度位置和有界回收内核也已经固定。每个 output Station 还拥有持久化
retained-byte 高水位，容量拒绝会按强重放协议回滚完整 turn。Flow 已公开有界的
`Flow::advance`，真实 `SequenceSource → Count → Discard` DAG 可以按拓扑逐轮推进并在 reopen
后续跑。端点校验已经排除完全没有 consumer 的 output，缓慢或停滞 consumer 会通过物理日志水位
自然反压上游。尚未实现 `Flow::start`、中断控制、端口 Schema 静态约束或外部副作用协议；内建 Count
当前仍在一个 turn 中完整处理 Change，但协议已经允许其他 Operation 用自己的持久化状态跨 turn
continuation。

## 验证

```bash
cargo test -p dogpaddle-flow
cargo test -p dogpaddle-flow --test correctness
cargo clippy -p dogpaddle-flow --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-flow --no-deps
cargo bench -p dogpaddle-flow --bench flow_lifecycle
```

`flow_lifecycle` 只测当前确实存在的低频 lifecycle：fresh durable `build` 与 warm committed
`open`，按 Station 数量逐轴扩展。它不报告 rows/s，也不声称代表实际 Station processing
或运行时吞吐。正式结果必须在显式 reference 文件系统上保留逐样本 JSONL；配置与输出协议见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/TESTING.md)。
