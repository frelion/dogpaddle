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

- **强类型静态 DAG**：sealed `OperationDefinition` trait 用一个 `OperationKind` 手动声明 Source、
  Transform 或 Sink、非零输入数量和状态形状；Station 读取所包裹算子的 kind，`FlowFactory` 校验有序连接、
  唯一 Station ID、自环、多节点环，以及所有起点都是 Source、所有终点都是 Sink。
- **完整端口 Schema 绑定**：build/open 按拓扑把每条上游 output 的精确 logical Arrow Schema
  传给下游 Definition；Definition 纯绑定有序 inputs，并为 Source/Transform 固定唯一 output Schema。
  全图绑定成功后才创建或打开运行资源；运行时同一 `Output` capability 再同时约束生产和消费，
  防止算子实现或持久化日志绕过静态结果。derived Schema 不另存 Cell、fingerprint 或 registry。
- **持久化构建**：一条 Flow 对应一个 Store；所有资源声明完成后，manifest 作为构建完成
  标记最后提交。
- **直接重新打开**：`FlowFactory::open(path)` 从持久化 Definition 重建拓扑和 Operation 实例，
  调用方不需要再次组装。
- **稳定格式边界**：Flow 与 Operation Definition 使用显式版本、tag、完整性校验和确定性
  资源命名，不依赖 Rust 内存布局。
- **有序 Arrow 批量差分**：`dogpaddle-change` 提供 `Change = RecordBatch + Int64Array`，逐行绑定
  记录与非 null、非零 diff，并以行位置表达事件顺序；每个持久化 Change 都是内嵌物理 Schema、
  恰好一个 RecordBatch 的完整自描述 Arrow IPC Stream。Schema 绑定的顶层投影允许同一份
  完整编码按消费者需求只物化所需列，内存投影则直接共享原 Arrow buffer。稳定类型集合包含
  Date32、四种时间单位且 timezone 为可选非空字符串的 Timestamp，以及 Decimal128；LargeUtf8、
  LargeBinary、FixedSizeBinary 等尚未纳入 v1。`Change::try_new`、全量解码及被选择字段的投影解码会
  递归验证每个 Decimal128 non-null slot 满足 `|unscaled| < 10^precision`；祖先 List/Struct 为 null
  不豁免物理存在的 non-null child，未选择字段则不读取或验证 value。Operation/Flow 进一步验证了 Date32、无 timezone 的
  Millisecond Timestamp 和 `Decimal128(10, 2)` 经结构算子、显式 cast、同类型组合比较及两次 reopen
  的受限纵向路径；这不泛化为其他时间/Decimal 运算。
- **真实定义与状态物化**：当前包含零输入 SequenceSource、一元事件 RunningEventCount、零拷贝顶层 Project、
  接收 DataFusion `Expr` 的 Filter、Extend、多列表达式 Select 与显式 SchemaAlign、精确 Schema 的
  UnionAll，以及无输出 Discard Sink。RunningEventCount 每观察一行事件加一，不按 diff 维护关系
  cardinality；它不是 Aggregate。Filter/Extend/Select/SchemaAlign 直接用 `datafusion-proto` 持久化表达式；build/open 通过 DataFusion
  `create_physical_expr` 完成类型、nullability、cast 与向量化执行；当前不运行 logical coercion，混合类型需显式 cast。DogPaddle 不维护第二套
  表达式语言，也不因此引入 SQL 层。build/open 会先从 canonical Definition
  产生一次性的 Schema binding，再为有状态算子创建
  或打开持久化 Cell 并消费 binding 装配运行实例，同时为 Flow 和每个 Station 预先声明通用 state map。
- **Station 数据通道装配**：每个会产生输出的 Station 拥有自己的 `AppendLog<Vec<u8>>`；每个
  下游 input 只拿到对应上游日志的 `ReadOnly` capability，fan-out 不复制日志；producer 和全部
  consumers 共享该 output 已绑定的同一个精确 logical Schema。Station 不长期持有
  事务启动能力；每个有输入的 Station 持久化一个 active input 和每条边的 Change offset，
  输入准备经真正的只读 snapshot 从 active input 开始循环寻找第一个可用 entry，并在调用
  Operation 前 durable-pin 选中端口；已有的 `active + cursor` 就是 input claim，不新增另一套 key。
  owned `Claim` 保存 `port + offset + Change`，命中不再访问 Store；`process` 临时接收
  `&mut Transactions`，把输入端口、完整 `Change` 和不能提交的 `TransactionAccess` 交给
  Operation，只有破坏性的 `Complete` 会在提交前再次校验 Claim 与 durable active/cursor。
- **类型化事务状态**：Store 提供 `Cell<T>` 与显式 `Small`/`Large` 布局的
  `OrderedMap<K, V, SIZE>`；collection handle 与事务能力分别控制长期写权限和本次访问权限，
  真正的只读事务在类型层没有写入或提交入口。有界 map scan 可完整解码，也可只投影编码中的
  所需字段。
- **差分流存储基础**：Store 提供固定独立布局的 `AppendLog<T>`，具有单调 offset、按需
  投影解码、同事务原样转发、有界前缀回收和按物理 head 维护的 retained-byte 账本；Flow 已完成 Station output 与只读 input 的资源
  装配及 input 准备，并从 Definition 派生确定性的分层拓扑 schedule。内部有界轮次已经按该
  schedule 为每个 Station 保留一次 turn。每个 output 在提交边界保持
  `head == min(all consumer edge cursors)`；输入 `Complete` 在业务事务内推进一条 cursor，并在
  新最小值前进时原子回收至多一条 entry，不再存在独立 GC phase 或回收 debt。
  每个有 output 的 Station 在 Definition 中显式保存独立字节高水位，空日志允许一条 oversize
  entry；容量拒绝会回滚整个 Operation turn，并通过 `Flow::advance` 的 `Backpressured` 暴露。
  Source 与其他 Operation 使用同一个 `turn` 协议，只以 `None` 区分无输入。`Action::Idle`
  回滚 turn；`Action::Commit` 提交 Operation continuation/output 并保留当前输入，零输入 Source
  也用它表示成功；`Action::Complete` 才推进 cursor、轮转 active、必要时回收上游 entry，并且
  只在 commit 成功后清除 Claim。

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
持久化验证。Station 幂等 claim 并准备一个完整 input entry，再通过写事务执行 Operation 的
`Idle`/`Commit`/`Complete` action。只要尚未 `Complete`，下一 turn（包括 reopen
之后）必须收到相同 `(port, offset, bytes)` 的完整 Change；片段内 continuation 由 Operation 存进
自己声明的状态。`Flow::advance` 已能按稳定拓扑 schedule 执行真实的
表达式链路及 `SequenceSource → Select → UnionAll → RunningEventCount → Discard` 多输入轮次，并在 Complete 事务内安全释放上游前缀。拓扑已经拒绝
没有任何 consumer 的 output，per-output retained-byte 高水位会让缓慢 consumer 自然向上游传播
背压。每个 output 在 build/open 时已经绑定一个精确 logical Schema；算子输出在编码前、持久化
entry 在首次 intake 解码后还会再次校验，Schema 违例均不会提交本 turn 的状态或 cursor。
尚未实现持续运行的 `Flow::start`、中断控制或外部 Sink 的幂等协议。
该高水位不计算 MDBX page、Operation state 或 decoded cache，也允许空日志容纳一条 oversize，
因此不是磁盘或内存硬配额。单输入 Operation 的展平 output 事件序列与最终业务状态必须同时不受
稳定重批和 input-retaining `Commit` turn 切分影响；`UnionAll` 保持各输入内部顺序与最终关系，
但与 SQL 一样不承诺跨输入顺序。比较的每种物理分批都必须能用声明的 Arrow input/output 类型表示。
DataFusion 当前提供表达式 API、protobuf 和向量化执行；仓库仍没有最终用户二进制、SQL、连接器或
分布式调度。一个 Store
路径同一时刻只能由一个活动 Flow 打开；外部副作用的幂等协议将在运行层设计时确定。

算子路线的阶段 0/1 已落成当前基线：九个内建算子共用 Definition/bind/materialize/turn/reopen
conformance，事件计数公共名明确为 RunningEventCount，SchemaAlign 显式表达选择、改名、重排、
cast/nullability 与 metadata，Change 增加 Date32/Timestamp/Decimal128 传输证据，Operation/Flow
增加上述精确 operator/type 纵向证据，并把表达式分成
“已承诺、当前 DataFusion 可规划但未承诺、明确拒绝”三类。`RunningEventCount` 当前 tag 为 `2`，
并使用新的公共 API、data 名与资源路径。不提供旧名称 alias、资源 fallback 或迁移；旧版本数据库
直接删除并按当前基线重建，不承诺或测试旧 manifest 的兼容行为。阶段 2 及以后才处理真实 Ingress/Result/Materialize、
关系状态、Aggregate、Join、时间语义和运行产品化；SQL 只是未来可选的上层适配器之一。

## 深入阅读

- [算子与执行内核分阶段路线、语义边界与退出标准](OPERATOR_ROADMAP.md)
- [Flow 构建、磁盘布局与当前边界](crates/flow/README.md)
- [Arrow Change 与自描述 IPC Stream](crates/change/README.md)
- [Operation Definition 与实例约束](crates/operation/README.md)
- [Store 存储语义、测试与性能](crates/store/README.md)
- [Cell、Small/Large OrderedMap 与 AppendLog 实测报告](crates/store/PERFORMANCE.md)
- [Change + AppendLog 外部集成与长稳协议](integration-tests/change-store/README.md)
- [正确性、集成与性能测试体系](TESTING.md)
- [仓库贡献指南](AGENTS.md)
