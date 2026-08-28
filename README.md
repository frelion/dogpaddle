# DogPaddle

[![CI](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml)

DogPaddle 是一个用 Rust 构建的嵌入式、持久化流计算引擎。它面向需要长期运行、可靠恢复且
不希望部署独立分布式系统的数据流，把静态计算拓扑和算子状态放在同一个本地事务存储中。

## 产品定位

流计算不仅要描述“数据如何转换”，还要明确拓扑何时冻结、状态如何保存，以及进程重启后
如何重新获得同一组计算资源。DogPaddle 用 `FlowFactory` 声明、构建或重新打开完整 DAG 和
所有持久化数据空间；成功返回的 `Flow` 只表示拓扑与资源布局已经冻结的运行态，并分别持有运行
所需的写事务与只读 snapshot 启动能力。

## 已有能力

- **强类型静态 DAG**：sealed `OperationDefinition` trait 声明输入数量、是否产生输出和状态
  形状，`FlowFactory` 校验有序连接、唯一 Station ID、自环、多节点环，以及 source 确实拥有输出。
- **持久化构建**：一条 Flow 对应一个 Store；所有资源声明完成后，manifest 作为构建完成
  标记最后提交。
- **直接重新打开**：`FlowFactory::open(path)` 从持久化 Definition 重建拓扑和 Operation 实例，
  调用方不需要再次组装。
- **稳定格式边界**：Flow 与 Operation Definition 使用显式版本、tag、完整性校验和确定性
  资源命名，不依赖 Rust 内存布局。
- **有序 Arrow 批量差分**：`dogpaddle-change` 提供 `Change = RecordBatch + Int64Array`，逐行绑定
  记录与非 null、非零 diff，并以行位置表达事件顺序；每个持久化 Change 都是内嵌物理 Schema、
  恰好一个 RecordBatch 的完整自描述 Arrow IPC Stream。Schema 绑定的顶层投影允许同一份
  完整编码按消费者需求只物化所需列，内存投影则直接共享原 Arrow buffer。
- **真实定义与状态物化**：当前包含零输入 SequenceSource 和一元事件 Count；build/open 会为
  二者创建并重新绑定持久化 Cell，同时为 Flow 和每个 Station 预先声明通用 state map。
- **Station 数据通道装配**：每个会产生输出的 Station 拥有自己的 `AppendLog<Vec<u8>>`；每个
  下游 input 只拿到对应上游日志的 `ReadOnly` capability，fan-out 不复制日志。Station 不长期持有
  事务启动能力；每个有输入的 Station 持久化一个 active input 和每条边的 Change offset，
  `intake` 经真正的只读 snapshot 从 active input 开始循环寻找第一个可用 entry，并只缓存其
  `input + offset + Change`。cache 命中不再访问 Store；`process` 临时接收
  `&mut Transactions`，在调用 Operation 前校验 cache offset 仍等于 durable cursor，再把输入端口、
  完整 `Change` 和不能提交的 `TransactionAccess` 交给 Operation。
- **类型化事务状态**：Store 提供 `Cell<T>` 与显式 `Small`/`Large` 布局的
  `OrderedMap<K, V, SIZE>`；collection handle 与事务能力分别控制长期写权限和本次访问权限，
  真正的只读事务在类型层没有写入或提交入口。有界 map scan 可完整解码，也可只投影编码中的
  所需字段。
- **差分流存储基础**：Store 提供固定独立布局的 `AppendLog<T>`，具有单调 offset、按需
  投影解码、同事务原样转发和有界前缀回收；Flow 已完成 Station output 与只读 input 的资源
  装配及只读 intake，并从 Definition 派生确定性的分层拓扑 schedule。内部有界轮次已经按该
  schedule 为每个 Station 保留一次 turn，并在每个成功 turn 后为其直接上游各触发一次有界前缀
  GC；安全水位取 output 全部 consumer edge cursor 的最小值。该轮次通过 `Flow::advance` 暴露。
  Operation 的业务状态、本 turn output、Station active/cursor 在同一写事务提交；Operation 成功
  即表示完整 Change 已处理，cursor/active 随 commit 生效，commit 成功后 Station 才清空 cache。

## 内部架构

| crate | 职责 |
| --- | --- |
| [`dogpaddle-flow`](crates/flow/README.md) | `FlowFactory`、拓扑校验、持久化 `build/open`，以及运行态 `Flow` 与内部 Station。 |
| [`dogpaddle-change`](crates/change/README.md) | 共享 `Change`、Schema 校验与自描述 Arrow IPC Stream 编解码。 |
| [`dogpaddle-operation`](crates/operation/README.md) | 具体 Operation 的纯 Definition、稳定编码、持久化 Data 与实例。 |
| [`dogpaddle-store`](crates/store/README.md) | MDBX 事务存储、命名数据空间、编解码器和类型化集合。 |

Station 是 Flow 内部的一对一 Operation 执行单元，不是独立 crate。上述 crate 是引擎内核的
实现模块，不是最终用户二进制入口。正常产品依赖的装配契约归组合根 crate 所有，例如
dogpaddle-flow 负责验证 Operation + Store 的 build/open；只有没有产品组合根的 sibling 接缝
才进入外部测试 package。`integration-tests/change-store` 因而作为不可发布的下游包，只通过
公共 API 验证完整 Change Stream 与 `AppendLog<Vec<u8>>` 的装配边界。

## 适用场景

DogPaddle 适合嵌入式数据管道、可恢复的本地事件处理，以及需要把多份计算状态放进明确
事务边界的长期任务。静态 DAG 和单 Store 所有权尤其适合“先完整定义，再持续执行”的工作负载。

## 当前边界

当前仓库完成了 `FlowFactory` 的持久化定义、构建和重新打开，以及运行态 Flow 的 Station
output/input capability 装配、Arrow `Change`、自描述 Stream 编码和 `AppendLog<Vec<u8>>`
持久化验证。Station 通过只读 `intake` 幂等准备一个完整 input entry，再通过写事务让 Operation
完整处理该 Change，并原子提交 Operation 状态、output、active 和 cursor。`Flow::advance` 已能按
稳定拓扑 schedule 执行真实的
`SequenceSource → Count` 轮次，并在成功 turn 后进行安全的上游有界 GC。尚未实现持续运行的
`Flow::start`、跨 turn 的单 Change continuation、背压、中断控制、端口 Schema 静态约束或外部
Sink 的幂等协议。
仓库也没有最终用户二进制、SQL、连接器或
分布式调度。一个 Store
路径同一时刻只能由一个活动 Flow 打开；外部副作用的幂等协议将在运行层设计时确定。

## 深入阅读

- [Flow 构建、磁盘布局与当前边界](crates/flow/README.md)
- [Arrow Change 与自描述 IPC Stream](crates/change/README.md)
- [Operation Definition 与实例约束](crates/operation/README.md)
- [Store 存储语义、测试与性能](crates/store/README.md)
- [Cell、Small/Large OrderedMap 与 AppendLog 实测报告](crates/store/PERFORMANCE.md)
- [Change + AppendLog 外部集成与长稳协议](integration-tests/change-store/README.md)
- [正确性、集成与性能测试体系](TESTING.md)
- [仓库贡献指南](AGENTS.md)
