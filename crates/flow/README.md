# dogpaddle-flow

`dogpaddle-flow` 是 `DogPaddle` 的 `Flow` 定义与运行容器。`Stage` 是该 crate 内部的通用
执行单元：一个 `Stage` 只承载一个 `Operation`，`Stage` 内部不再维护额外的算子图。

## 当前实现

当前只实现私有、无副作用的拓扑内核，尚未公开 `Flow` API。内核负责：

- 使用稳定 `Stage` ID 描述节点身份；
- 将 `Builder` 中的临时 `Stage` 引用解析为有序的上游 `Stage` ID；
- 保留多元 `Operation` 的输入顺序；
- 根据具体 Definition 声明的输入数量执行精确校验；
- 校验重复 ID、外来引用、重复连接、自环和多节点环；
- 允许扇出、重复上游和互不相连的 DAG 分量。

拓扑 `Builder` 不接收路径，不访问 `Store`，也不会创建任何文件。没有上游的 `Stage` 只有
在其 Definition 声明零输入时才合法；一元、二元和 N 元 Definition 必须获得恰好对应数量
的有序上游。

## 后续边界

持久化 `build` 和 `open` 只会在第一个真实 `Operation Definition`、完整数据资源布局和稳定
磁盘编码确定后一起实现。物化时，`Flow` 负责编排同一 `Store` 的生命周期和 Stage 资源
作用域；具体 Operation 模块决定自己的 Data 布局并构造实例。该过程不使用通用 Data bundle
或运行时 Registry。全部资源声明完成后，`Flow` 才会实例化内部 `Stage` 并消费 `Store` 进入
只能运行事务的冻结状态。本 crate 当前没有 `run`、调度、检查点、背压、恢复状态或最终用户
入口。

## 验证

```bash
cargo test -p dogpaddle-flow
cargo clippy -p dogpaddle-flow --all-targets -- -D warnings
cargo doc -p dogpaddle-flow --no-deps
```
