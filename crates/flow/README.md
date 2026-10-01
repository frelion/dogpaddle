# dogpaddle-flow

Flow 把关系算子组成可恢复的数据流。阅读运行时只需抓住一件事：**做出一页，调用下游，下游完成后回来做下一页。调用栈保存在 Store 中。**

Operation 负责计算和具体源、目标适配，Store 负责显式事务，Flow 负责图连接和调用顺序。
详细持久格式、事务和恢复约束见 [运行契约](docs/runtime.md)。

## 从一个输入到目标

```text
源捕获并持久保存输入 → 借用队首输入 → Run 当前帧
                                      ↓
                               head + Atomic 尾链
                                      ↓
                              Send 一页给每个消费者
                                ↙             ↘
                         压入计算 child      写入 Sink outbox
                                ↓             ↓
                            完成后弹栈      独立交付并结算
```

一次只计算一个 root。父页按声明顺序逐个调用消费者，child 完成后才轮到下一分支；全部消费者完成后，
父帧才继续下一页。root 直接借用 Source 的已发布队首，不另存输入或开启激活事务；完整调用结束才消费队首。
一页计算后直接按顺序交给 Sink 或建立 child；只有尚未完成的调用才保存帧与 pending 页。首次计算错误回滚后只保存初始 Run，重开仍重试同一输入。Join 的另一侧不会在分页中途被另一 root 改变，因而无需固定输入端口或维护边订阅进度。
同一生产者连接同一个 Join 的两个端口，也按两个有序调用处理，交叉项只计一次。

图中的最大合法 Atomic 直线链在同一页事务中运行。融合只是一组内存索引：没有额外的持久身份、队列、
Definition 或生命周期。持久 Definition 始终保存完整逻辑 DAG，每个 Operation 保留自己的 ID 和状态前缀。
Flow 直接持久化这份类型化 JSON 计划，只有一个版本和 checksum 外壳；Operation 不再有独立持久 codec。
装配消费纯计划；运行期每个节点只拥有 ID、输入 ordinal、已构造的 Operation、output codec 与待 ACK Delivery，不保留完整 Definition 或平行数组。表达式的逻辑 AST 与 protobuf 不因 Flow 运行而常驻；已编译表达式和算子所需配置仍由具体 Operation 持有。

长 Join 分成很多笔小事务，不把整个输入放进一个大事务。失败会回滚当前 head 与全部融合尾项的本页工作，
已经提交的早页保留。普通 Atomic 首链也分页，因此整个 Change 或 CDC Delivery 不再是计算原子边界。

## 源与目标仍有自己的恢复边界

Source 拥有单一输入 Queue、phase 和 checkpoint，快照封口前隐藏、封口后直接可消费。完整 Delivery 先持久捕获，
真实 CDC Delivery 完成 WAL barrier 后才消费原始 ACK 凭证；内部序列和维护动作共用本轮最终 barrier。未封口快照不会被计算提前看见。

Sink 拥有有界 outbox；事件位置同时表示消费进度和正事件身份，Prepared 只持久化边界与删除 IDs。准备在 Store 事务外进行；Prepared
先提交并持久化，再交付目标，最后短事务结算。目标成功而本地未结算时，重开后重投相同 Prepared。
Sink 不占调用帧；outbox 满时父帧停在同一个消费者，目标 drain 仍能继续。

捕获、计算和交付由同一个 `advance()` 显式驱动，没有后台执行器。一次调用轮转服务一个 Source、
最多 32 个栈动作和一个 Sink。多个源和目标各自按声明顺序跨调用轮转；一个长 root 会阻塞后续 root 的计算，
但不阻止捕获和目标交付。`Idle` 表示本轮没有进展，不代表所有外部源永久结束。

## 最小公共 API

```rust,no_run
use dogpaddle_flow::{FlowFactory, AdvanceOutcome};
use dogpaddle_operation::{col, lit, operation::{
    scan::SequenceScanDefinition,
    transform::FilterDefinition,
    sink::DiscardDefinition,
}};

fn run(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut factory = FlowFactory::new(path);
    let numbers = factory.operation("numbers", SequenceScanDefinition::new(0), []);
    let selected = factory.operation(
        "selected", FilterDefinition::try_new(col("value").gt(lit(10_u64)))?, [numbers],
    );
    factory.operation("sink", DiscardDefinition::new(), [selected]);
    let mut flow = factory.build()?;
    assert_eq!(flow.operation_count(), 3);
    assert_eq!(flow.advance()?, AdvanceOutcome::Progressed);
    drop(flow);
    let mut reopened = FlowFactory::new(path).open()?;
    reopened.advance()?;
    Ok(())
}
```

`resource(operation_id, value)` 注入临时凭据或连接配置，`owner_identity` 设置 build/open 必须精确匹配的身份。
每个输入必须引用同一 factory 中较早声明的 Operation；声明顺序就是唯一的构造与轮询顺序，不另排拓扑序。
`build()` 在创建路径前只解析一次图并检查资源和 Schema，将原计划编码后原子发布完整 catalog 和 Definition，不先把自己编码的计划重新解码；
`open()` 只读已存在的资源，验证帧与图的对应关系后恢复运行。失败不删除、修复或重建状态。

`operation_ids()` 返回全部逻辑 ID；`status()` 返回栈深度、栈顶 Operation、是否正在发送，以及是否必须重开。
提交、barrier 或外部效果不确定时，整个 Flow 进入 fail-stop。`FlowRunError::requires_reopen()` 给出这个区别。

## 固定执行界限

- 图最多 1024 个 Operation，每个 Operation 最多 1024 个输入端口，计算调用深度最多 64；Sink 不占深度。
- 捕获输入最多 8 MiB；后代页最多 1 MiB、256 行、16,384 个顶层标量槽。
- 每次计算 attempt 最多 4 MiB 逻辑访问/写入；head 工作量从 256 逐次减半，最多尝试九次。
- 一次栈动作不会跨调度轮遗忘缩页结果。最小工作项连同完整尾链仍超限时明确失败。
- root 借用 Source 队首，child 借用不变的父 pending 页；栈至多保留 `64 × 1 MiB = 64 MiB` pending 编码。root 输入计入 Source 队列容量，控制和目标 outbox 另计。

这些界限约束编码保留与逻辑事务工作，不是整个进程 RSS 或调用时延保证。关系历史、RocksDB、Arrow、
表达式、JVM 和数据库 driver 的开销另算。完整口径见运行契约和各 Operation owner 文档。

## 阅读与验证

先读 `build/validate.rs` 的融合与深度推导，再读 `flow/frame.rs` 的唯一持久位置、`flow/advance.rs` 的原子转移，
最后读 `flow/runtime.rs` 的重开检查。无需再追踪 Station、SubscribedLog、Claim 或提交回调协议。

公共行为由 `correctness` target 验证，包括逻辑 Definition golden、Schema 重绑、损坏拒绝、SQL 算子组合、
目标结果和晚页错误。私有调用栈测试逐事务重开并检查分叉、回滚和有限缩页。性能入口为 `flow_lifecycle`
与 `flow_runtime`；本轮结果见 [性能记录](PERFORMANCE.md)，统一口径见工作区 [TESTING.md](../../TESTING.md)。
