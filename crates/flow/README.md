# dogpaddle-flow

`dogpaddle-flow` 负责定义、构建和重新打开一条持久化 Flow。Stage 是 crate 内部的一对一
Operation 容器；它保存一个实现封闭 `Operation` trait 的运行实例，不再重复枚举具体算子，
也不维护额外的算子图。

## 构建 Flow

Builder 阶段是纯声明：`stage()` 返回仅属于当前 Builder 的临时 `StageRef`，`connect()`
记录目标 Stage 完整、有序的输入列表。只有 `build()` 才集中校验拓扑并创建 Store。

```rust,no_run
use dogpaddle_flow::Flow;
use dogpaddle_operation::{
    operation::source::SequenceSourceDefinition,
    operation::transform::CountDefinition,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("flow");
    let mut builder = Flow::builder(&path);
    let source = builder.stage("source", SequenceSourceDefinition::new(0));
    let count = builder.stage("count", CountDefinition::new());
    builder.connect([source], count);

    let flow = builder.build()?;
    assert_eq!(flow.stage_ids().collect::<Vec<_>>(), ["source", "count"]);
    drop(flow);

    let reopened = Flow::open(&path)?;
    assert_eq!(reopened.stage_count(), 2);
    Ok(())
}
```

Stage ID 必须非空、不能包含 NUL，并且在一条 Flow 内唯一。连接保留 source 顺序，允许
fan-out 和重复 source；输入数量必须与具体 Definition 完全一致，作为 source 的 Operation
必须声明自己产生输出，整个拓扑必须是 DAG。拓扑校验或 manifest 编码失败不会创建目标目录。

## 持久化边界

每条 Flow 独占一个 Store。`build()` 先完成纯校验，再为 Flow 和每个 Stage 各声明一个
持久化 state map，按 Operation Definition 的逻辑数据名声明全部状态空间，并为每个
`produces_output() == true` 的 Stage 创建一个 output log，最后提交 manifest Cell 作为构建完成
标记。Operation Definition 返回稳定的“逻辑名称 → 完整数据类型”声明；Flow 负责完整资源名，
并通过 Store 将每项声明创建或打开为具体实例，再按逻辑名称交给 Definition 直接装配
Operation。绑定不依赖声明顺序，具体算子不接触 Store、底层句柄或物理布局。

Flow definition Cell 固定使用共享布局；Flow state map 和 Stage state map 显式声明为
`Small`。Flow state map 保留生命周期状态，Stage state map 保存运行期 Stage 状态，并将在运行
协议中保存各 input 自己的 next-unread offset。output 是 `AppendLog<Vec<u8>>`；每个 value 将保存一个内嵌 Schema
的完整 Change IPC Stream，不另建 Schema Cell。端口 Schema 一致性属于 Flow/Stage 契约，不
依赖 Change codec 之外的 Schema resource 或 fingerprint 维持。运行层只能使用已经声明的 map
和日志，不能动态新增数据空间。此后 Store 被转换为可克隆的事务启动能力：Flow 保留一份，每个
Stage 各自获得一份，拓扑和资源目录都没有修改入口。

资源创建和 Stage 装配分成两遍。第一遍按声明顺序创建或打开全部 state、Operation data 和
output；第二遍才按 Definition 中的有序 source ID 找到上游 output，并将 clone 单向衰减为
`ReadOnly<AppendLog<Vec<u8>>>` 注入下游。声明顺序不必是拓扑顺序，fan-out 仍共享同一份日志。
Stage 不知道上游 Stage，只知道自己的有序 inputs；它拥有自己的完整可选 output，因此可以在
同一个事务中读取 input、推进 state、更新 Operation 状态并发布 output。没有 output 的 Sink
使用 `None`，不能被其他 Stage 作为 source。

每条边按 `(AppendLog offset, Change row_index)` 遍历事件；它是当前持久化分批下的坐标，而
不是稳定 event ID。未来 Stage 可以为了吞吐稳定地合并或切分物理批次，但变换前后展平的事件
序列必须逐项相同，也不能隐式 consolidation。不同输入边的 offset 彼此不可比较；若多输入
Operation 依赖跨端口交错顺序，运行层必须另行定义并持久化 ingress 顺序，不能从 source 声明
顺序推断事件发生顺序。

Store 目录和 catalog 已有效、但 manifest 尚未提交时，`Flow::open()` 返回 `IncompleteBuild`；
manifest 已发布却缺少所声明资源时返回 `MissingResource`。如果底层 `Store::create()` 本身只
留下无效目录，则打开时保留相应 Store 错误，不把它误报成有效 Flow 的未完成构建。

`open()` 先读取、解码并重新校验 manifest，再打开全部数据对象和 output，最后按 source ID
重新注入 inputs、装配 Stage 并冻结 Store。调用方不需要重新提交 Definition。

当前磁盘格式使用显式 magic、版本号、定长整数、sealed Operation Definition 集合的稳定 tag
和 IEEE `CRC32` 完整性校验，不依赖 Rust enum 布局或通用序列化框架。以下名称是兼容性边界：

- Flow manifest：`flow/definition`
- Flow 状态：`flow/state`
- Stage 状态：`stage/{index:08x}/state`
- Stage 输出：`stage/{index:08x}/output`（仅限声明产生输出的 Operation）
- `SequenceSource` 位置：`stage/{index:08x}/operation/sequence_source.position`
- Count 状态：`stage/{index:08x}/operation/count`

`index` 是 Stage 声明顺序。当前仍是开发期 v1，编码、tag 或资源布局可以破坏性调整，但每次
调整必须同步更新黄金字节、布局和 reopen 测试，不保留旧格式兼容层。

## 源码边界

`build/` 统一拥有 `FlowBuilder`、`StageRef`、Flow/Stage Definition、无 Store 副作用的图校验、
稳定磁盘编码和完整资源名，并在构建时创建全部资源；拓扑只是 Flow Definition 中的连接关系，
不再拥有独立 Builder。`build/` 与 `flow/` 分别按 Definition 声明的逻辑数据名通用创建或打开
类型化实例，再让 Definition 物化具体 Operation；二者都不枚举具体算子。`stage/` 只保存
事务启动能力、state、装箱后的 `Operation`、只读 inputs 和自己的可选 output；它不接收 Store，
也不知道 Stage ID、上游 Stage、资源名或底层物理布局。
公共错误单独位于 `error.rs`；私有单元测试放在对应源码模块目录的 `tests.rs` 中，`tests/`
使用单一 `correctness` target 验证 crate 的公共行为。Flow 是 Operation 与 Store 的组合根，
因此公共测试可以通过二者的公共 API 检查实际资源布局和重新物化；无需再建立一个重复的
`flow-store` 集成 package。完整目录、fixture 规则与计时边界见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/TESTING.md)。

## 当前边界

本阶段完成定义、持久化 `build/open`，以及 Stage 的事务、只读 inputs 和可选 output 装配。
尚未实现 `run`、Stage 调度、Stage state map 的 offset 键协议、Change 解码与 Operation 批量
接口、背压、中断或运行恢复。
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
`open`，按 Stage 数量逐轴扩展。它不报告 rows/s，也不声称代表尚未实现的 Stage 调度或运行时
吞吐。正式结果必须在显式 reference 文件系统上保留逐样本 JSONL；配置与输出协议见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/TESTING.md)。
