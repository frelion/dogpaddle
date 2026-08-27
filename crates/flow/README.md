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
input 保存 next-unread `(AppendLog offset, Change row_index)`。`build()` 在发布 manifest 的同一
事务中把全部 input cursor 显式初始化为 `(0, 0)`，不存在“缺失时从当前 log head 开始”的隐式
恢复。output 是 `AppendLog<Vec<u8>>`；每个 value 保存一个内嵌 Schema 的完整 Change IPC Stream，
不另建 Schema Cell。端口 Schema 一致性属于 Flow/Station 契约，不依赖 Change codec 之外的
Schema resource 或 fingerprint 维持。运行层只能使用已经声明的 map 和日志，不能动态新增数据
空间。

Store 随后被转换为唯一的 `Transactions`；build/open 在取得完整所有权时以 consuming `split`
显式获得同环境的 `ReadTransactions`，Flow 长期持有返回的读写两种能力。`ReadTransactions` 不可
克隆但可安全共享，并且只能开启 RO snapshot。Station 不长期保存任何事务启动能力：内部
`Station::intake` 只在调用期间借用 `ReadTransactions`，`Station::process` 将只在调用期间借用
`&mut Transactions`；后者无法消费 writer 来 split 出 owned reader。拓扑和资源目录都没有运行期
修改入口。

资源创建和 Station 装配分成两遍。第一遍按声明顺序创建或打开全部 state、Operation data 和
output；第二遍才按 Definition 中的有序 source ID 找到上游 output，并将 clone 单向衰减为
`ReadOnly<AppendLog<Vec<u8>>>` 注入下游。声明顺序不必是拓扑顺序，fan-out 仍共享同一份日志。
Station 不知道上游 Station，只知道自己的有序 inputs；它拥有自己的 state 与完整可选 output。
`intake` 通过 RO snapshot 按 durable cursor 幂等准备输入，不推进 cursor，也不调用 Operation；
后续 `process` 才在写事务中处理已准备的输入。重开 Flow 时始终根据 durable cursor 恢复。
没有 output 的 Sink 使用 `None`，不能被其他 Station 作为 source。

`process(&mut Transactions)` 及其 `Idle / Progressed` 结果类型已经留下明确位置，但方法体仍为
`todo!()`。Station–Operation 的 Change 批处理、partial consumption、输出和原子提交协议尚未
确定，因此当前代码不会伪造调用或提前承诺其返回类型。Operation 以后仍只会接收不能提交的
`TransactionAccess`，不会读取 AppendLog、cursor 或 Station 运行元数据。

每条边按 `(AppendLog offset, Change row_index)` 遍历事件；它是当前持久化分批下的坐标，而
不是稳定 event ID。未来 Station 可以为了吞吐稳定地合并或切分物理批次，但变换前后展平的事件
序列必须逐项相同，也不能隐式 consolidation。不同输入边的 offset 彼此不可比较；若多输入
Operation 依赖跨端口交错顺序，运行层必须另行定义并持久化 ingress 顺序，不能从 source 声明
顺序推断事件发生顺序。cursor 表示下一条未消费事件；entry 的最后一行消费完成后必须规范化为
`(offset + 1, 0)`，不能持久化等于 Change 行数的 `row_index`。

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
- Station input cursor key：`input/{input_index:08x}/cursor`
- Station 输出：`station/{index:08x}/output`（仅限声明产生输出的 Operation）
- `SequenceSource` 位置：`station/{index:08x}/operation/sequence_source.position`
- Count 状态：`station/{index:08x}/operation/count`

`index` 是 Station 声明顺序，`input_index` 是该 Station 持久化 source 列表中的端口顺序。cursor
value 固定为 16 字节：big-endian `u64 offset` 后接 big-endian `u64 row_index`。当前仍是开发期
v1，编码、tag 或资源布局可以破坏性调整，但每次调整必须同步更新黄金字节、布局和 reopen
测试，不保留旧格式兼容层。

## 源码边界

`build/` 统一拥有 `FlowFactory` 的声明与构建路径、`StationRef`、Flow/Station Definition、无 Store
副作用的图校验、稳定磁盘编码和完整资源名，并在构建时创建全部资源；拓扑只是 Flow Definition
中的连接关系，不再拥有独立构建类型。`open/` 实现 `FlowFactory::open` 的两阶段读取、资源打开
和重新物化。`build/` 与 `open/` 都按 Definition 声明的逻辑数据名通用创建或打开类型化实例，
再让 Definition 物化具体 Operation；二者都不枚举具体算子。`flow/mod.rs` 只声明模块并导出
`Flow`，`flow/runtime.rs` 保存 build/open 返回的运行态对象、生命周期状态、全部 Station 与分离的
读写事务启动能力，不创建或打开 Store 资源。`station/mod.rs` 同样只声明边界；
`station/runtime.rs` 保存 Station 装配与 `process` 边界，`station/input.rs` 独立拥有 cursor 和
`intake`，`station/protocol.rs` 只拥有 outcome/error。Station 不长期持有事务启动
能力，也不接收 Store，不知道 Station ID、上游 Station、资源名或底层物理布局。私有
`assembly.rs` 只承接 build/open 共用的 source ID 解析和最终 Station 装配，不公开新的领域类型。
公共错误单独位于 `error.rs`；私有单元测试放在对应源码模块目录的 `tests.rs` 中，`tests/`
使用单一 `correctness` target 验证 crate 的公共行为。dogpaddle-flow 是 Operation 与 Store 的
组合根，因此公共测试可以通过二者的公共 API 检查实际资源布局和重新物化；无需再建立一个
重复的 `flow-store` 集成 package。完整目录、fixture 规则与计时边界见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/TESTING.md)。

## 当前边界

本阶段完成定义、持久化 `build/open`、Flow 对分离读写事务启动能力的所有权，以及 Station 的
state、只读 inputs、可选 output 和稳定 cursor。`intake` 已能通过 RO snapshot 幂等准备输入；
`process(&mut Transactions)` 及其
`Idle / Progressed` 位置已经固定，但方法体仍为 `todo!()`。尚未实现 `Flow::start`、
`Flow::advance`、Station–Operation 批处理与 partial-consumption 提交协议、背压、中断、GC 或
完整运行恢复。
`SequenceSource` 只是第一个真实零输入 Definition，用于形成可构建的
`SequenceSource → Count` DAG，并不代表调度器已经存在。

## 验证

```bash
cargo test -p dogpaddle-flow
cargo test -p dogpaddle-flow --test correctness
cargo clippy -p dogpaddle-flow --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-flow --no-deps
cargo bench -p dogpaddle-flow --bench flow_lifecycle
```

`flow_lifecycle` 只测当前确实存在的低频 lifecycle：fresh durable `build` 与 warm committed
`open`，按 Station 数量逐轴扩展。它不报告 rows/s，也不声称代表尚未实现的 Station 调度或运行时
吞吐。正式结果必须在显式 reference 文件系统上保留逐样本 JSONL；配置与输出协议见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/TESTING.md)。
