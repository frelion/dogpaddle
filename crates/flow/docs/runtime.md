# Flow 的事务与持久化契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## 能力与输入领取

Station 不能保存任何事务启动能力：输入准备只在调用期间借用 `ReadTransactions` 与当前调度轮的 `&mut DurabilityBatch`。
`Inbox` 独占可选 active Cell、全部 input ports 和 owned `Claim`；零输入和单输入 Station 不创建 active Cell，多输入 Station 才持久化当前 active port。
没有 Claim 时，先在真正的 RO transaction 中从 active port 开始循环调用各 edge `Subscription::peek`；若多输入 Station 选中不同 port，必须在调用首 Operation 前用独立短写事务把 active durable-pin 到该 port，而 Subscription position 不变。该事务可以加入当前调度轮的 durability batch，但必须服从同一最终 barrier。
每条 edge 的唯一 durable input identity 是 `SubscribedLog` 自己维护的 Subscription position；Station 不得再持久化另一套 input-position 或 current-input 状态。
owned `Claim` 只保存 port、offset 和解码后的完整 Change，是该 durable identity 的可丢弃内存副本。
已有 Claim 时必须在开始事务前直接返回，输入准备不 acknowledge、不拆分或处理 Change、不调用任何 Operation，也不在每个 turn 前重复读取 Store 校验 Claim。
`process` 只在调用期间接收 `&mut DurabilityBatch`，由它开始并提交写事务；batch 只借用 Flow 唯一拥有的 `Transactions`，因此 `process` 无法导出并留存事务启动能力。

## Station 执行协议

运行 Operation 分为两个受限执行接口。
`TurnOperation::turn` 通过独占 `&mut self` 接收 `Option<OperationInput>`，并保持现有 `Turn::Idle` 或一次性 `PreparedTurn`、`Action`、`AfterCommit` 协议；Scan、Sink 和任何跨 turn Transform 使用它。
`AtomicOperation::apply` 接收一个完整 `OperationInput` 与不能提交的 `TransactionAccess`，一次性返回 `Option<Change>`；它不保存当前输入、不产生 AfterCommit、不执行外部 I/O，所有影响重放的状态都必须经同一事务更新。
Atomic 自己成功后仍必须能承受后续 Operation、output 或 commit 失败造成的整体回滚。
统一运行值 `Operation` 只区分 Atomic 与 Turn；Atomic 位于首项时由唯一适配器产生 `Action::Complete` 和空 AfterCommit。
Station 在没有写事务时调用首项 `turn`，随后在一个写事务内执行首项 prepared body 和全部 Atomic 尾项。
首项接收原始 port，尾项固定接收 port `0`；某项输出 `None` 只停止余链，不能改变首项的 Commit/Complete、已写状态或 AfterCommit。
事务内协议仍只有 `Action::Idle`、`Action::Commit(Option<Change>)` 和 `Action::Complete(Option<Change>)`：Idle 回滚全部写入，Commit 提交状态与可选最终输出但保留输入，Complete 才同时确认当前完整 Change。
只有全部 Operation state、最终 output 与适用的 Subscription acknowledgement 提交后才能消费首项 AfterCommit；其中 `durable` completion 还要求当前 durability batch 先完成 barrier，`local` completion 只可发布能从 Store 恢复的进程内状态。错误、背压或 commit failure 必须回滚整个链并丢弃 completion。
AfterCommit 失败不回滚已提交事务，当前 Station 必须 fail-stop 到 reopen。

## 调度轮持久化 barrier

普通 Store 事务保持逐笔同步提交。Flow 在一次 `advance` 内显式建立 `DurabilityBatch`：每个 Station 事务仍独立原子提交、保留 WAL 并立即对同轮后续 Station 的新 snapshot 可见，WAL record 的写出和磁盘同步则延后到共同 barrier。遇到 `durable` AfterCommit 时必须先写出并同步此前所有 pending commit，再运行外部效果；`local` 与空 completion 不切断 batch。一轮成功返回或因普通 intake/Operation/commit 错误提前返回前，也必须完成最终 barrier。

barrier 失败后不能重试或继续本运行实例。自上一个成功 barrier 起可能提交过状态的全部 Station 都必须置为 needs-reopen；错误归属当前触发 barrier 的 Station，轮末 barrier 则归属这组 Station 中拓扑最早者。此前已经成功 barrier 的 Station 不受后续 barrier failure 影响。进程在 barrier 前退出时允许只恢复该轮已经落盘的前缀，因为任何依赖 pending commit 的外部效果都尚未运行，reopen 会按现有 durable input identity 重放其余工作。
跨 turn continuation 只能存入首 Operation 自己声明的 Store state；尾项不能拥有 continuation 或 runtime resource。

## 持久布局

Flow Definition 的 magic、版本、校验算法、可选 32 字节 opaque owner identity、每个 Station 的非空有序 Operation Definition 列表、ordered input Station IDs 与 output capacity，`flow/definition`、`station/{index:08x}/output`、`station/{index:08x}/active-input` 与 `station/{station_index:08x}/operation/{operation_index:08x}/{logical_name}` 数据资源名，output `SubscribedLog` 的固定 subscriber 数和持久布局，以及 output entry 的 schema-bound Change marker、Schema fingerprint 推导、单 batch framing 和 EOS 都是开发期 v1 的持久化边界；修改时必须同步更新当前 v1 黄金字节或布局测试，但不提供旧格式迁移或兼容层，旧数据库直接删除重建。
单 Operation Station 也必须使用 ordinal `0`。
每个具有 output 的 Station 必须在 build 时显式获得一个持久化的非零 retained-byte capacity，Sink 不得配置；build 按 consumer Station 声明顺序、再按 input port 顺序为每个 producer 派生从 `0` 开始的稠密 subscriber ID，并在发布 Definition 的同一事务中初始化 output log 的 metadata 和全部 Subscription positions。
只有首 Operation 为多输入的 Station 创建显式 `Cell<u32>` active state，并在同一事务中初始化为 `0`；零输入和单输入 Station 不创建 Station state。
Station 内中间 Operation 不创建 output log、Subscription 或 Station state。

## 构建与恢复

`FlowFactory::build` 必须先稳定编码再解码 canonical Definition，并只从解码结果解析拓扑、全图前置检查 runtime resource 类型，再创建内存 `StoreSetup` draft；随后按 schedule 传播 producer 最终 Schema，并按列表顺序通过统一 `construct` 直接生成每个 Station 的最终 Operation。
只有全部构造成功后才能在 `StoreSetup::commit(path, initialize)` 中创建路径并原子发布 catalog、Definition 和初始状态。
`open` 先读取持久化 Definition、精确比较 Factory 期望与磁盘中的 owner identity，再全图前置检查 runtime resource，并在同一个 Store ownership 生命周期中通过只查已有资源的 `DataScope` 直接构造全部 Operation、验证 durable Station state，最后消费 Store 取得事务能力；Some/None 也必须匹配，identity mismatch 不得进入 Schema 编译或运行构造。
derived edge Schema 不单独持久化为 Cell 或 registry；Operation tag、payload、有序 input Schema 到最终构造语义及任何 Schema 相关状态 codec 都属于 reopen ABI。每条 output entry 携带的固定 fingerprint 只用于拒绝错绑或损坏 entry，不能替代从 Definition 确定性重建 exact Schema。
Flow 负责 Store draft/open 生命周期、Station 与 Operation ordinal 组成的稳定资源名前缀、全图 resource preflight 和最终 Station 装配，但不能枚举具体算子。
Flow 在调用 construct 前通过 `DataScope::scoped` 限定每个 Operation 的资源作用域；Operation 只接收这个子 scope 并声明本地逻辑名，不接收或拼接完整前缀。build/open 使用相同前缀，持久资源名不变。
只有首 Operation 可以消费按 Station ID 注入的 runtime resource，Atomic 尾项不能接收 runtime resource。
固定的逻辑资源名和 codec 由具体算子代码与独立布局/golden/reopen 测试共同守护；公开 Store catalog 只验证 collection kind，不宣称运行时审计整个 catalog 或同 kind 的 codec 身份。
runtime Operation 只保存执行参数、已取得的 collection handle 与可由 durable state 重建的临时运行资源，不得保存 Definition、DataScope 或 Store。
运行态 `Flow` 不接收 Store、不保留完整 Flow Definition，也不负责定义、创建或打开算子资源。

## 装配所有权

拓扑校验在 consumer Station 声明顺序和 input port 顺序中派生每条 edge 的确定性 subscriber ID；assembly 从 producer 的完整 `SubscribedLog` handle 派生对应 `Subscription`，再把每个 producer 的 writer、capacity 与已绑定 Station 最终 exact logical Schema 的 Change codec 唯一 move 进一个 `Arc<Output>`；producer Station 和所有 `InputPort` 只共享该 `Arc<Output>`，每个 `InputPort` 另持有自己唯一的 `Subscription`，不得另存完整 log handle 或第二份 Schema 判断。
`construct_stations` 逐项构造和校验 Operation，并在创建 `StationParts` 时拆成首 Operation 与 Atomic 尾链；`assemble_flow` 只连接这些已构造的 Station 构件，不重新判断融合资格或改变列表顺序。
Store 的 collection kind、稳定资源名和 codec 共同构成持久化 schema；Store catalog 只验证 kind，每项 Operation tag 对应的代码 schema 负责具体 key/value codec 一致性。
Flow 的每个起点必须是首项为 Scan 的 Station，每个终点必须是独占 Sink Station；允许多个起点、多个终点和多个合法 DAG 分量，Sink 没有 output，任何 Scan 或 Transform output 都必须至少有一个直接 consumer。

## 调度与观察

Flow 的只读 `status` 从一次短 RO snapshot 读取全部 Station active、每条 input Subscription 的 position/tail、output head/tail/retained bytes/capacity，不解码 Change、不调用 Operation、不访问外部系统，也不另持久化统计状态。
`InputStatus` 对外字段是 `position` 和 `tail`；Scan 的 active 是 None，单输入固定为 0，多输入读取 active Cell。
Station 只额外缓存最近一次 advance 的处理 outcome；每轮预检前全部清空，未执行或失败为 None。
最近处理的 Backpressured 不被同轮 progress 或 durable pin 掩盖；needs-reopen 状态下仍能查询。

公开的 `Flow::advance` 必须真实按派生的确定性拓扑 schedule 为每个 Station 至多提供一个 turn。
input entry 必须由 producer 共享 `Output` 的 Schema-bound codec 完整解码后才能安装 Claim 或 durable-pin；format、fingerprint、batch 或值校验失败不能产生持久化写入，不再另做一遍运行时 Schema equality 判断。
Station 首 Operation 在写事务外执行 `turn`；prepared body 产生可选 Change 后，全部 Atomic 尾项在同一事务内按列表顺序执行，每项都必须自行拒绝不符合 exact binding 的输入，尾项 port 固定为 `0`。
某项返回 `None` 只停止余链，不能改写首项 Action 或 AfterCommit。
最终非空 Change 必须由 Station output 的 Schema-bound codec 编码后再做 capacity 判定；codec 统一拒绝 Schema 不匹配和编码错误，任何此类错误或 Operation 错误都回滚整个 turn。
`Turn::Idle` 不开始写事务；prepared `Action::Idle` 丢弃本次写事务并保留 Claim；`Commit` 在同一写事务中提交首项 continuation、全部尾项状态和可选最终 output，但不得 acknowledge Subscription、轮转 active input或清空 Claim；`Complete` 才在同一事务中提交全部 Operation state、可选最终 output、Claim 所属 Subscription 的精确 offset acknowledgement 与多输入 active 轮转，并且只有 commit 成功后才清空 Claim。
`Inbox` 独占可选 active Cell 和 Claim；intake 固定 Claim 后，当前唯一 writer 在 Complete 前没有第二条修改 active 的路径，因此 Complete 直接由 Claim 的 port/offset 推导 Subscription acknowledgement 与下一 active，无需重新读取或验证 active。
`Subscription::acknowledge` 仍必须验证 Claim offset 正是当前 position。
`SubscribedLog` 自己维持 `head == min(all subscription positions)`；单个 acknowledgement 只推进一个 position 一步，并在新的最小 position 越过旧 head 时同事务回收至多一个 entry。
Flow 不得复制 subscriber positions、retained-byte 计费或回收逻辑，也不得保留独立回收 phase、回收 debt 或每个 turn 前的 Claim 重校验。
首 Operation 即使 output 已达容量也仍可运行；无 output 的 Commit 可以提交，实际最终 output 则由 `SubscribedLogWriter::try_append` 判定。
容量拒绝不是错误，必须回滚整个 Station 事务并保留 durable identity 和 owned Claim，Scan state、Atomic 状态、Commit continuation、Complete acknowledgement/active 都不得提前前进。
只要尚未 Complete，下一 turn（包括 reopen 后）必须提供同一 `(port, offset, bytes)` 标识的完整 Change 并重跑整个确定性链。
只有事务成功后才能运行首项 `AfterCommit`；`durable` completion 还必须先完成 durability barrier，`local` completion 与空 completion 不强制中途 barrier。运行前必须先置 fail-stop latch，completion 成功且 Claim 清理后才解除，error 或 panic 都要求 reopen。
`Flow::advance` 每轮执行 schedule 前必须预检全部 Station，已有 fail-stop 时不能让任何更早 Station 继续提交。
`Flow::advance` 在任何正常返回路径前必须完成最终 durability barrier；barrier failure 覆盖的全部 pending Station 都必须 fail-stop 到 reopen。
公共结果按 `Progressed > Backpressured > Idle` 聚合，其中 durable pin 和 Complete acknowledgement 属于 Progressed；背压不得跳过后续 schedule。
当前仍不公开 `Flow::start`。

## 源码职责与能力边界

`build/` 拥有声明、定义、编码、校验与 build/open；`build/validate.rs` 同时派生调度顺序、subscriber ID 和各 output 的 subscriber 数。

`assembly.rs` 按调度顺序传播 Schema 并构造 Operation，按声明顺序保存 Station 构件；build 初始化状态、open 验证状态后，两条路径都在这里组装最终 Flow。
Operation 的 Schema 绑定失败直接映射到 `FlowError::Schema { station_id, operation, source }`，不增加单变体的错误包装层；Store 资源缺失和其他 setup 错误继续保持各自的失败分类。

`flow/runtime.rs` 保存轻量 Station ID、运行对象、确定性 schedule 与读写事务能力；`flow/advance.rs` 保存调度 outcome 和 advance。

`station/program.rs` 保存首项和 Atomic 尾链，`station/runtime.rs` 只保存运行态及 process；构建期 `StationParts`、初始化与恢复验证归 `assembly.rs`。

`station/input.rs` 拥有 Output/Inbox/Claim，`station/protocol.rs` 拥有 outcome/error；`flow/mod.rs` 和 `station/mod.rs` 只声明模块。

`build/validate.rs` 的唯一分层拓扑算法校验持久 Station 图并生成 schedule；assembly 只消费校验结果，不引入公共装配类型。

Flow 长期唯一持有不可克隆的 `Transactions`，并在取得所有权时 consuming `split` 获得同环境、不可克隆但可共享的 `ReadTransactions`。

运行态 Flow 不接收 Store、不保留完整 Definition、不创建或打开资源；Station 不知道物理 key、稳定资源名，也不能长期持有任何事务启动能力。

合法 DAG 可以有多个 Scan 起点、Sink 终点和独立分量。
逻辑声明和容量 API 以 [README](../README.md#最小公共-api) 为准；open 不重新规划。

## 分页 Join 的失败边界

EquiJoin 与 ASOF 都按已提交 turn 推进，不预演整个 Claim。后页错误只回滚当前 Station turn，之前的输出及下游 Sink 交付保留；active pin 与输入 position 仍表示同一未完成 Claim。reopen 从 continuation 继续，不能重发已提交页、提前 ACK、跳过确定性错误或自动修复状态。成功关系等价不意味着 turn 数变化后的全图调度轨迹等价。
