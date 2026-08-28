# dogpaddle-flow

`dogpaddle-flow` 用公共 `FlowFactory` 定义、构建和重新打开一条持久化 Flow；成功返回的
`Flow` 只表示运行态，不承担声明、构建或打开职责。Station 是 crate 内部的一对一 Operation
容器；它保存一个实现封闭 `Operation` trait 的运行实例，不再重复枚举具体算子，也不维护
额外的算子图。

## 构建 Flow

Factory 的声明阶段没有 Store 副作用：`station()` 返回仅属于当前 `FlowFactory` 的临时
`StationRef`，`connect()` 记录目标 Station 完整、有序的输入列表。只有 `build()` 才集中校验
拓扑并创建 Store。

```rust,no_run
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::{
    operation::source::SequenceSourceDefinition,
    operation::transform::CountDefinition,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let count = factory.station("count", CountDefinition::new());
    factory.connect([source], count);

    let flow = factory.build()?;
    assert_eq!(flow.station_ids().collect::<Vec<_>>(), ["source", "count"]);
    drop(flow);

    let reopened = FlowFactory::open(&path)?;
    assert_eq!(reopened.station_count(), 2);
    Ok(())
}
```

Station ID 必须非空、不能包含 NUL，并且在一条 Flow 内唯一。连接保留 source 顺序，允许
fan-out 和重复 source；输入数量必须与具体 Definition 完全一致，作为 source 的 Operation
必须声明自己产生输出，整个拓扑必须是 DAG。拓扑校验或 manifest 编码失败不会创建目标目录。

## 持久化边界

每条 Flow 独占一个 Store。`FlowFactory::build()` 先完成纯校验，再为 Flow 和每个 Station 各声明一个
持久化 state map，按 Operation Definition 的逻辑数据名声明全部状态空间，并为每个
`produces_output() == true` 的 Station 创建一个 output log，最后提交 manifest Cell 作为构建完成
标记。Operation Definition 返回稳定的“逻辑名称 → 完整数据类型”声明；`FlowFactory` 的
build/open 通路负责完整资源名，并通过 Store 将每项声明创建或打开为具体实例，再按逻辑名称
交给 Definition 直接装配 Operation。绑定不依赖声明顺序，具体算子不接触 Store、底层句柄或
物理布局。

Flow definition Cell 固定使用共享布局；Flow state map 和 Station state map 显式声明为
`Small`。Flow state map 保留生命周期状态；Station state map 保存运行期 Station 状态，并为每个
input 保存当前尚未完全退休的 Change offset；有输入的 Station 还保存循环查找的 active input。
`build()` 在发布 manifest 的同一事务中把 active input 和全部 cursor 显式初始化为 `0`，不存在
“缺失时从当前 log head 开始”的隐式恢复。output 是
`AppendLog<Vec<u8>>`；每个 value 保存一个内嵌 Schema 的完整 Change IPC Stream，不另建 Schema
Cell。端口 Schema 一致性属于 Flow/Station 契约，不依赖 Change codec 之外的 Schema resource 或
fingerprint 维持。运行层只能使用已经声明的 map 和日志，不能动态新增数据空间。

Store 随后被转换为唯一的 `Transactions`；build/open 在取得完整所有权时以 consuming `split`
显式获得同环境的 `ReadTransactions`，Flow 长期持有返回的读写两种能力。`ReadTransactions` 不可
克隆但可安全共享，并且只能开启 RO snapshot。Station 不长期保存任何事务启动能力：内部
`Station::intake` 只在调用期间借用 `ReadTransactions`，`Station::process` 将只在调用期间借用
`&mut Transactions`；后者无法消费 writer 来 split 出 owned reader。拓扑和资源目录都没有运行期
修改入口。

资源创建和 Station 装配分成两遍。第一遍按声明顺序创建或打开全部 state、Operation data 和
output；第二遍才按 Definition 中的有序 source ID 找到上游 output，并将 clone 单向衰减为
`ReadOnly<AppendLog<Vec<u8>>>` 注入下游。声明顺序不必是拓扑顺序，fan-out 仍共享同一份日志。
每条 consumer edge 还把下游 state 衰减成只能读取对应 cursor 的内部 capability，注入上游用于
GC。Station 不知道上下游 Station ID，只拥有自己的有序 input logs、唯一 owned input-Change
cache、state、完整可选 output 以及 output consumer cursor capabilities。
装配同时从已验证的 Definition 派生运行期 schedule：先按拓扑层次排列，同一层按 Station 声明
顺序排列，并为每个 target 保留按上游 Station 去重的 GC 触发列表。二者完全由 Definition 推导，
build/open 得到相同结果，不需要单独持久化。`intake` 在 Station 唯一 cache 为空时，通过一个 RO
snapshot 从 durable active input 开始循环检查各端口，跳过空日志，并只装入第一个可用 entry。
cache 显式保存该 entry 的 input index、AppendLog offset 和完整 owned Change；命中时不访问 Store。
intake 不修改 active input 或 cursor，也不调用 Operation。后续 `process` 可以多次使用同一 cache；
Change 内部消费进度将由具体 Operation 自己的持久化 data 维护，不进入 Station state。重开 Flow
时 cache 为空，并根据 durable active input 与对应 cursor 重新读取。没有 output 的 Sink 使用
`None`，不能被其他 Station 作为 source。

`process(&mut Transactions)` 及其 `Idle / Progressed` 结果类型已经留下明确位置，但方法体仍为
`todo!()`。Station–Operation 的 Change 批处理、partial consumption、输出和原子提交协议尚未
确定，因此当前代码不会伪造调用或提前承诺其返回类型。Operation 以后仍只会接收不能提交的
`TransactionAccess`，不会读取 AppendLog、cursor 或 Station 运行元数据；它自行解释和持久化
Change 内部消费进度。未来处理所选 Change 后若仍未完成，Station 必须在同一写事务中把 active
input 固定为 cache 的 input，并保留对应 cursor；若已经完整退休，则在该事务中推进 cache 所属
input 的 cursor，并把 active input 移到它的下一个端口。该提交状态机仍位于当前 `process` 的
`todo!()` 之后。

每条边的 cursor 只定位一个完整 Change IPC entry；它是当前持久化分批下的读取位置，而不是稳定
event ID。未来 Station 可以为了吞吐稳定地合并或切分物理批次，但变换前后展平的事件序列必须
逐项相同，也不能隐式 consolidation。不同输入边的 offset 彼此不可比较；active input 只规定从
哪个端口开始循环寻找下一个可用物理 Change，不声称还原跨上游事件发生时间。一个 Change 完全
退休后 cursor 才推进到下一个 offset；Change 内部多次消费所需的额外记录属于具体 Operation。
按完整物理 Change 轮转可能改变跨端口交错，因此多输入 Operation 必须对稳定重批保持可观察结果
不变；需要业务级跨端口顺序时，必须另行引入逻辑 ingress、barrier 或窗口语义。

每个 Station turn 只要成功返回，无论结果是 `Idle` 还是 `Progressed`，Flow 都会按去重后的直接
上游列表各触发一次 GC。上游 Station 在独立写事务中读取自己全部 consumer edge 的 durable
cursor，校验它们位于 output 的 `[head, tail]`，以最小 cursor 为安全水位，并调用一次
`truncate_before`，单次最多删除 1024 个 entry。重复 source edge 在触发列表中只调用一次，但每条
edge 的 cursor 都参与最小值；GC 失败以实际执行 GC 的上游 Station ID 返回。当前 `process` 仍为
`todo!()`，因此公共 `advance` 暂时还没有成功 turn 能到达该调度位置。

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
- Station 输出：`station/{index:08x}/output`（仅限声明产生输出的 Operation）
- `SequenceSource` 位置：`station/{index:08x}/operation/sequence_source.position`
- Count 状态：`station/{index:08x}/operation/count`

`index` 是 Station 声明顺序，`input_index` 是该 Station 持久化 source 列表中的端口顺序。active
input value 固定为 4 字节 big-endian `u32`，cursor value 固定为 8 字节 big-endian `u64 offset`。
当前仍是开发期 v1，编码、tag 或资源布局可以破坏性调整，但每次调整必须同步更新黄金字节、布局
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
`process`，成功后再对直接上游各触发一次 GC，通过公共 `Flow::advance` 暴露。当前调用会明确到达
`Station::process` 的 `todo!()`，不伪造尚未确定的 Operation 协议。`station/mod.rs` 同样只声明边界；
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
`intake` 已能通过 RO snapshot 从 active input 循环查找并幂等准备至多一个带来源身份的 Change；
成功 turn 后的直接上游 GC 调度位置和有界回收内核也已经固定；
`process(&mut Transactions)` 及其
`Idle / Progressed` 位置已经固定，但方法体仍为 `todo!()`。Flow 已公开有界的 `Flow::advance`，
当前会按拓扑 schedule 进入 Station 并停在这个明确边界；尚未实现 `Flow::start`、
Station–Operation 批处理与 partial-consumption 提交协议、背压、中断或完整运行恢复。
`SequenceSource` 只是第一个真实零输入 Definition，用于形成可构建的
`SequenceSource → Count` DAG；它仍未接入运行调用协议。

## 验证

```bash
cargo test -p dogpaddle-flow
cargo test -p dogpaddle-flow --test correctness
cargo clippy -p dogpaddle-flow --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-flow --no-deps
cargo bench -p dogpaddle-flow --bench flow_lifecycle
```

`flow_lifecycle` 只测当前确实存在的低频 lifecycle：fresh durable `build` 与 warm committed
`open`，按 Station 数量逐轴扩展。它不报告 rows/s，也不声称代表尚未可执行的 Station processing
或运行时吞吐。正式结果必须在显式 reference 文件系统上保留逐样本 JSONL；配置与输出协议见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/TESTING.md)。
