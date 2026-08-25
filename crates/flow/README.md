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
fan-out 和重复 source；输入数量必须与具体 Definition 完全一致，整个拓扑必须是 DAG。
拓扑校验或 manifest 编码失败不会创建目标目录。

## 持久化边界

每条 Flow 独占一个 Store。`build()` 先完成纯校验，再为 Flow 和每个 Stage 各声明一个
持久化 state map，并按 Operation Definition 的有序逻辑数据名声明全部状态空间，最后提交
manifest Cell 作为构建完成标记。Flow 负责完整资源名、`DataPlacement` 和 Store create/open；
Definition 只用已解析的 `DataHandle` 物化具体 Operation。Flow state map 保留生命周期状态；
Stage state map 是未来队列、进度和输出协议的唯一持久化容器。后续运行层只能在这些 map 的
键域内写状态，不能新增数据空间。此后 Store 已转换为事务能力，拓扑和资源目录都没有修改入口。

Store 目录和 catalog 已有效、但 manifest 尚未提交时，`Flow::open()` 返回 `IncompleteBuild`；
manifest 已发布却缺少所声明资源时返回 `MissingResource`。如果底层 `Store::create()` 本身只
留下无效目录，则打开时保留相应 Store 错误，不把它误报成有效 Flow 的未完成构建。

`open()` 分两遍完成：第一遍只读取、解码并重新校验 manifest；第二遍按持久化定义打开全部
资源、物化 Stage，再冻结 Store。调用方不需要重新提交 Definition。

当前磁盘格式使用显式 magic、版本号、定长整数、sealed Operation Definition 集合的稳定 tag
和 IEEE `CRC32` 完整性校验，不依赖 Rust enum 布局或通用序列化框架。以下名称是兼容性边界：

- Flow manifest：`flow/definition`
- Flow 状态：`flow/state`
- Stage 状态：`stage/{index:08x}/state`
- `SequenceSource` 位置：`stage/{index:08x}/operation/sequence_source.position`
- Count 状态：`stage/{index:08x}/operation/count`

`index` 是 Stage 声明顺序。修改编码、tag 或资源名必须作为磁盘格式变更处理并补充迁移设计。

## 源码边界

`build/` 统一拥有 `FlowBuilder`、`StageRef`、Flow/Stage Definition、无 Store 副作用的图校验、
稳定磁盘编码和完整资源名，并在构建时创建全部资源；拓扑只是 Flow Definition 中的连接关系，
不再拥有独立 Builder。`build/` 与 `flow/` 分别按 Definition 声明的逻辑数据名通用创建或打开
句柄，再让 Definition 物化具体 Operation；二者都不枚举具体算子。`stage/` 只保存公共 state
map 和装箱后的 `Operation` trait object，不接收 Store，也不知道资源名或 `DataPlacement`。
公共错误单独位于 `error.rs`；私有单元测试放在对应源码模块目录的 `tests.rs` 中，`tests/`
顶层文件只验证 crate 的公共行为。

## 当前边界

本阶段只完成定义、持久化 `build` 和无 Definition 参数的 `open`。尚未实现 `run`、Stage
调度、Stage state map 的键协议、输入进度、输出发布、背压、中断或运行恢复；
`SequenceSource` 只是第一个真实零输入 Definition，用于形成可构建的
`SequenceSource → Count` DAG，并不代表调度器已经存在。

## 验证

```bash
cargo test -p dogpaddle-flow
cargo clippy -p dogpaddle-flow --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-flow --no-deps
```
