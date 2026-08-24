# dogpaddle-operation

`dogpaddle-operation` 是 `DogPaddle` 的强类型 `Operation Definition` 层。

每类 `Operation` 将拥有自己的强类型 `Definition`，并按其算法需要拥有持久化 `Data` 和
运行实例。不同 Definition 最终通过闭合、类型安全的联合交给 `Flow`，不使用
`kind: String`、不透明配置字节或运行时 Registry 分发。

## Definition 边界

Definition 是纯、可形式化的数据，只保存重建 Operation 语义所需的配置，不包含 `Store`、
`DataHandle`、事务、闭包或运行时客户端。每个具体 Definition 决定自己接受的精确、有序
输入数量；`Flow` 在冻结拓扑前验证连接数量。

当前不公开通用 Definition trait。未来持久化异构 Flow 时，闭合的 Definition 联合将提供
拓扑所需的输入数量，并穷尽处理稳定编码与物化。实现一个任意 Rust trait 不会被误解为可以
自动加入可持久化 Flow。当前也不定义通用 Signature、端口 schema 或持久化 codec。

## 物化边界

Operation 实例由纯 Definition 和 `build/open` 阶段解析出的具体、Store-bound 依赖构成。
`Flow` 负责编排 Store 生命周期、Stage 资源作用域和冻结时机；具体 Operation 模块决定自己的
状态布局并构造运行实例。

无状态 Operation 不需要虚构持久化 Data；有状态 Operation 使用与自身算法匹配的具体
依赖。构造函数名称、Definition 的传递方式以及是否需要专用 Data 结构，由第一个真实
Operation 的所有权与状态需求决定。运行实例不得借用嵌套在 Flow 持久化定义中的字段，避免
让 Flow 形成自引用结构。

当前不提供通用 `DataBundle`、`OperationInstance`、factory 或依赖注入容器，也没有临时
No-op、Source、Filter 或 Join 实现。该 crate 目前不依赖 `dogpaddle-store`；具体 Data
布局、事务适配和执行结果协议会随真实 Operation 的状态算法一起确定。
