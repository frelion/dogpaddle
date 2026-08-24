# DogPaddle

DogPaddle 是一个用 Rust 构建的嵌入式、持久化流计算引擎。它面向需要长期运行和可靠
恢复的数据流，将计算定义、操作状态和执行进度保存在本地事务存储中。

## DogPaddle 解决什么问题

需要长期运行的数据流不仅要完成计算，还要处理进程崩溃、重复执行、上下游速度不一致和
多份状态的一致性。DogPaddle 将一次 Stage 转换作为一个原子事务：操作状态、检查点、
输出和消费进度要么一起提交，要么一起回滚。业务操作因此可以专注于领域逻辑，而不必
分别协调状态存储、恢复位置和调度进度。

## 设计目标

- **持久化静态 DAG**：构建成功时保存完整定义，此后冻结拓扑和数据空间。
- **原子状态转换**：一次计算推进在同一 Store 事务中提交相关运行状态。
- **崩溃恢复**：重新打开后从最后一次已提交的位置继续运行。
- **持续执行**：暂时没有输入不是完成状态，Flow 会等待后续数据。
- **职责分层**：Flow、Stage、Operation 与 Store 各自维护明确的抽象边界。

## 内部 crate 架构

| crate | 职责 |
| --- | --- |
| [`dogpaddle-flow`](crates/flow/README.md) | Flow 拓扑与生命周期，以及内部 Stage 运行时；当前已实现输入数感知的私有拓扑内核。 |
| [`dogpaddle-operation`](crates/operation/README.md) | 具体 Operation 的强类型定义、状态和执行语义；当前已确定 Definition 与物化边界。 |
| [`dogpaddle-store`](crates/store/README.md) | 提供 MDBX 支持的事务存储、命名数据空间、编解码器和类型化集合。 |

这些 crate 是引擎内核的实现模块，不是最终面向用户的产品入口。

## 适用场景

DogPaddle 面向需要本地持久化和确定恢复边界的数据处理任务，例如嵌入式数据管道、
可恢复的多阶段任务，以及需要在同一事务中更新多份操作状态的处理流程。

## 当前能力边界

- 当前仓库包含 Store 实现、Flow 的私有拓扑内核和 Operation 层边界，尚无可运行的
  流引擎或最终用户二进制。
- Stage 是 Flow 内部的一对一 Operation 运行容器，不是独立的公共模块。
- 持久化构建、重新打开和持续运行尚未实现；当前设计目标不代表已有公共 API。
- SQL、连接器、高层 API 和分布式调度不属于当前内核。

## 深入阅读

- [Flow 内核设计与用法](crates/flow/README.md)
- [Operation 层职责](crates/operation/README.md)
- [Store 存储语义、测试与性能](crates/store/README.md)
- [仓库贡献指南](AGENTS.md)
