# dogpaddle-flow

`dogpaddle-flow` 用公共 `FlowFactory` 定义、构建和重新打开一条持久化 Flow；成功返回的
`Flow` 只表示运行态，不承担声明、构建或打开职责。Station 当前是 crate 内部的一对一 Operation
容器；它读取所包裹 Definition 显式声明的 `OperationKind`，向 Flow 提供 source、sink、输入数量
和 output 属性，并在 build/open 时沿拓扑传递精确 logical Arrow Schema。Flow 不枚举具体算子；未来一个 Station 包裹多个 Operation 时，只需在 Station
内部归纳这些属性，不必改变 Flow 的拓扑接口。

## 构建 Flow

Factory 的声明阶段没有 Store 副作用：`station()` 返回仅属于当前 `FlowFactory` 的临时
`StationRef`，`output_capacity_bytes()` 为每个具有 output 的 Station 声明持久化字节高水位，
`connect()` 记录目标 Station 完整、有序的输入列表。只有 `build()` 才集中校验并创建 Store。

```rust,no_run
use std::num::NonZeroU64;

use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::{
    col, lit,
    operation::sink::DiscardDefinition,
    operation::source::SequenceSourceDefinition,
    operation::transform::{CountDefinition, ExtendDefinition, FilterDefinition},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let extend = factory.station(
        "extend",
        ExtendDefinition::try_new("is_seven", col("value").eq(lit(7_u64)))?,
    );
    let filter = factory.station(
        "filter",
        FilterDefinition::try_new(col("is_seven"))?,
    );
    let count = factory.station("count", CountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    let capacity = NonZeroU64::new(64 * 1024 * 1024).unwrap();
    factory.output_capacity_bytes(source, capacity);
    factory.output_capacity_bytes(extend, capacity);
    factory.output_capacity_bytes(filter, capacity);
    factory.output_capacity_bytes(count, capacity);
    factory.connect([source], extend);
    factory.connect([extend], filter);
    factory.connect([filter], count);
    factory.connect([count], sink);

    let flow = factory.build()?;
    assert_eq!(
        flow.station_ids().collect::<Vec<_>>(),
        ["source", "extend", "filter", "count", "sink"]
    );
    drop(flow);

    let reopened = FlowFactory::open(&path)?;
    assert_eq!(reopened.station_count(), 5);
    Ok(())
}
```

Station ID 必须非空、不能包含 NUL，并且在一条 Flow 内唯一。连接保留 source 顺序，允许
fan-out 和重复 source；连接数量必须与 Station 对外声明的输入数量完全一致，整个拓扑
必须是 DAG。所有入度为零的起点必须是 Source Station，所有出度为零的终点必须是 Sink Station；
Sink 没有 output，不能作为其他 Station 的上游。允许多个 Source、多个 Sink 和多个互不连接的
合法 DAG 分量。每个 `OperationKind::has_output()` 为 true 的 Station 必须且只能声明一次非零
output capacity，outputless Station 不得声明；重复、遗漏、类别不匹配或 foreign `StationRef` 都在
纯校验阶段失败。拓扑解析后，Flow 还按确定性拓扑顺序把每个 producer 的精确 output Schema
传到 consumer 的对应有序 input port，并调用 Definition 的纯 bind；任一 Schema 拒绝都会带
Station ID 返回 `FlowError::Schema`。拓扑、容量、Schema、Operation data 声明校验或 manifest
编码失败都不会创建目标目录。

Filter/Extend 的 Definition 在 `station()` 之前已通过 fallible `try_new` 将 `DataFusion` `Expr` 编码为
`DataFusion` protobuf；无法编码的表达式直接作为构造错误返回。表达式的字段解析、type、nullability、
cast 和 `PhysicalExpr` 创建留在上述全图 bind 阶段，由 `DataFusion` 完成。`create_physical_expr` 假定
logical coercion 已完成，而 Flow 不运行 logical/SQL planner，因此混合类型需要调用方显式 cast；失败同样不会创建
目标目录。Project 仍使用严格递增的顶层字段索引。

## 持久化边界

每条 Flow 独占一个 Store。`FlowFactory::build()` 先完成声明并稳定编码，再立即解码这份 canonical
manifest；拓扑解析、Schema 绑定和后续资源布局都只使用将要持久化的 Definition，而不依赖调用方
原始 Rust 对象的额外状态。全部纯校验成功后，build 才为 Flow 和每个 Station 各声明一个
持久化 state map，按 Operation Definition 的逻辑数据名声明全部状态空间，并为每个具有外部
output 的 Station 创建一个 output log，最后提交 manifest Cell 作为构建完成
标记。Operation Definition 返回稳定的“逻辑名称 → 完整数据类型”声明；`FlowFactory` 的
build/open 通路负责完整资源名，并通过 Store 将每项声明创建或打开为具体实例，再按逻辑名称
交给先前 Schema bind 产生的一次性 `OperationBinding` 装配 Operation。数据实例绑定不依赖声明
顺序，具体算子不接触 Store、底层句柄或物理布局。

Flow definition Cell 固定使用共享布局；Flow state map 和 Station state map 显式声明为
`Small`。Flow state map 保留生命周期状态；Station state map 保存运行期 Station 状态，并为每个
input 保存下一条未处理 Change 的 offset；有输入的 Station 还保存循环查找的 active input。
`build()` 在发布 manifest 的同一事务中把 active input 和全部 cursor 显式初始化为 `0`，不存在
“缺失时从当前 log head 开始”的隐式恢复。output 是
`AppendLog<Vec<u8>>`；每个 value 保存一个内嵌 Schema 的完整 Change IPC Stream，不另建 Schema
Cell。每个 output capacity 直接保存在 Flow Definition 中；build/open 都把它、对应 output log、
完整 consumer frontier 和绑定得到的精确 logical Schema 装配成同一个不可失配的运行期
`Output` capability，不创建另一份 Station state。端口 Schema 一致性不依赖 Change codec 之外的
Schema resource、fingerprint 或 registry：持久化 Definition 加上有序上游 Schema 在 reopen 时
确定性重建同一 binding。运行层只能使用已经声明的 map 和日志，不能动态新增数据空间。
Filter/Extend 的 manifest payload 直接包含 `DataFusion` `Expr` protobuf，但不持久化 `PhysicalExpr`。
build/open 都从 protobuf 还原 `Expr`，并针对 exact input Schema 重新调用 `create_physical_expr`。
`DataFusion` 依赖与 Arrow 精确 pin；跨 `DataFusion` 版本不保证读取兼容，不兼容升级必须 bump 外层
Operation Definition tag/version 并重建 Flow。

Store 随后被转换为唯一的 `Transactions`；build/open 在取得完整所有权时以 consuming `split`
显式获得同环境的 `ReadTransactions`，Flow 长期持有返回的读写两种能力。`ReadTransactions` 不可
克隆但可安全共享，并且只能开启 RO snapshot。Station 不长期保存任何事务启动能力：内部
输入准备只在调用期间借用 `ReadTransactions`，并在需要把选中端口固定为 active input 时临时借用
`&mut Transactions`；`Station::process` 同样只在调用期间借用 writer。后者无法消费 writer 来
split 出 owned reader。拓扑和资源目录都没有运行期修改入口。

资源创建和 Station 装配分成两遍。Schema binding 在两遍之前已对全图完成。第一遍按声明顺序创建或打开全部 state、Operation data 和
output；第二遍先从每个 target state 派生各 consumer edge 的只读 cursor capability，再把每个
producer 的 output log、capacity、精确 Schema 和完整 consumer frontier 绑定成唯一、不可错配的 output capability。
producer append、capacity 判定与所有下游 intake、frontier 校验和物理回收必须引用同一 capability，
不得分别持有可独立替换的日志或 retention handle；每条 input edge 只补充自己的 consumer slot。
声明顺序不必是拓扑顺序，fan-out 仍共享同一个物理日志与保留边界。Station 不知道上下游 Station ID，
只拥有 Operation、带固定 capacity 的可选 output capability，以及统一拥有 state、有序 ports 与至多
一个 owned `Claim` 的 `Inbox`。
装配还从已验证 Definition 派生唯一运行期 schedule：先按拓扑层次排列，同一层按 Station 声明
顺序排列。build/open 得到相同结果，不需要持久化 schedule，也没有第二套回收 schedule。

`Inbox` 没有 Claim 时，输入准备通过一个 RO snapshot 从 durable active input 开始循环检查各端口，
跳过空日志，并只选择第一个可用 entry。选中端口若不是当前 active input，Station 在调用 Operation
前用一个独立的短写事务把它固定为 active input，cursor 保持不变；已有的
`active input + cursor` 因而就是唯一 durable input identity，不增加另一套 current-input key。随后
Claim 保存该 entry 的 port、AppendLog offset 和完整 owned Change；它只是 durable identity 的可丢弃
内存副本。IPC 解码完成后，`intake` 必须先把 Change 的完整 logical Schema 与该 input 共享的
`Output` Schema 精确比较，匹配后才能安装 Claim；Schema 不匹配不会 pin、推进 cursor 或产生其他
持久化写入。Claim 存在时 `intake` 不访问 Store。重开 Flow 时 Claim 为空，并根据 active input 与对应 cursor
重建同一输入。零输入 Source 没有 Claim，但仍由相同的 Station 路径调用 `turn(None, ...)`；没有
Source 专用 outcome 或事务路径。没有 output 的 Sink 不能被其他 Station 作为 source。

`process(&mut Transactions)` 为每次 Operation 调用开始并持有唯一写事务。有输入 Operation 每次只
接收端口、一个完整 `Change` 和不能提交的 `TransactionAccess`，不会看到 `AppendLog` offset、
cursor 或 Station 运行元数据；Source 在同一入口收到 `None`。已有 Claim 会直接提供给 Operation，
Station 不在每个 turn 前另开读事务重校验 active input 和 cursor；需要消费该 Claim 时，验证与状态
迁移一起进入原子 Complete 事务。

Operation 只返回三个 `Action`：`Idle` 丢弃本次事务，因而不发布 output、不保存 Operation 写入，
也不改变当前 Claim；`Commit(output)` 提交 Operation 状态与可选 output，但保留已经提供的 Claim；
`Complete(output)` 只用于有输入的 turn，并声明该完整 Change 已处理。Source 的成功 turn 使用
`Commit`，input-free Operation 返回 `Complete` 是协议错误。Complete 会在同一事务中验证 Claim 的
port/offset 与 durable active input/cursor 相同，然后提交 Operation 状态、可选 output、cursor 推进、
active input 轮转和必要的上游物理回收；只有外层 commit 成功后才清除 Claim。任何 Operation、编码、
append、Station state、retention 或 commit 错误都会回滚本次事务并保留 durable identity 与 Claim。
Operation 返回 output 时，Station 在 IPC 编码和 capacity 判定之前先按同样的精确规则校验其
logical Schema；不匹配是协议错误，整个 turn 的 Operation 状态、output 与输入进展全部回滚。

Operation 在调用前不会因 output 已达到水位而被跳过，因为 Station 尚不知道本次是否产生 output
以及编码后大小。没有 output 的 `Commit` 即使日志已经达到水位也可以正常提交；有 output 时
Station 先完成 Change 编码，再调用 `AppendLog` 的 capacity-aware append。非空日志追加后超过
capacity 会正常返回背压而非错误：本次 Operation 写事务整体回滚，Source position、`Commit`
continuation、`Complete` cursor/active 都不前进，Claim 与 durable identity 保留，下一 turn 重新执行
Operation。物理空日志按 `head == tail` 判断并允许一条 oversize entry，避免单个合法 Change 永久
无法前进。这是一项 per-output soft high watermark，不是 MDBX 文件大小或进程内存硬配额。

只要当前输入尚未 `Complete`，无论前一 turn 是 `Idle`、`Commit`、错误、output append 失败、commit
失败还是进程重开，下一次调用都必须收到同一 `(port, offset, bytes)` 所标识的完整 Change；这要求
同一日志 entry 的原始字节不变，不要求重新解码后拥有相同内存地址。Change 内部的处理位置属于
Operation continuation，必须存入该 Operation 通过 Definition 声明的 Store 状态；`Inbox` 只拥有
durable active input、各 input cursor 和可丢弃重建的 owned Claim。

每条边的 cursor 只定位一个完整 Change IPC entry；它是当前持久化分批下的读取位置，而不是稳定
event ID。Station 可以为了吞吐稳定地合并或切分物理批次，但变换前后展平的输入事件序列必须
逐项相同，也不能隐式 consolidation。不同输入边的 offset 彼此不可比较；active input 既是未完成
输入的 durable port，又在没有未完成输入时规定从哪个端口开始循环寻找下一个可用物理 Change，
不声称还原跨上游事件发生时间。只有 `Complete` 才把 cursor 推进到下一个 offset。Operation 的
展平 output 事件序列和最终业务状态必须同时对稳定重批及同一 Change 的重复 `Commit` turn 切分保持不变；
需要业务级跨端口顺序时，必须另行引入逻辑 ingress、barrier 或窗口语义。

每个 producer output 在已提交的事务边界都维持 `head == min(all consumer edge cursors)`；build 时
所有值都是 `0`，open 会拒绝不满足该等式或 cursor 落在 `[head, tail]` 之外的运行状态。一次 Complete
只把当前 edge cursor 从 `offset` 推进到 `offset + 1`。在旧等式成立时，所有 cursor 都不小于 head，
所以新的最小值只能仍是 head 或成为 `head + 1`：前者不回收，后者在同一 Complete 事务中调用一次
`truncate_before` 并精确删除 head entry。重复 source edge 各有独立 cursor 和 consumer slot，全部
参与最小值；最慢 consumer 因而继续保护共享 entry。cursor advance、active rotation、物理 head 和
`AppendLog` retained-byte 账本要么一起提交，要么一起回滚，不存在独立回收 phase、补偿轮次或回收 debt。
open 只把这些 retention 不变量违例报告为 `InvalidRuntimeState`；底层 Store 访问失败继续保留为结构化
`FlowError::Store`，不会被字符串化或误分类。

`Flow::advance` 聚合为 `Progressed > Backpressured > Idle`：任一 Operation、durable input pin 或
Complete 内联回收有提交就返回 `Progressed`；整轮没有提交、但至少一个实际 output 被容量拒绝时返回
`Backpressured`；既无提交也无容量拒绝才返回 `Idle`。背压不会提前终止 schedule，所以下游和其他
DAG 分量仍获得本轮 turn。fan-out 共享一份 output log 和 capacity，最慢 consumer 的 cursor 会有意
阻塞整个 producer；各下游独立持有的 decoded Claim 不计入该容量。SequenceSource 提交
`u64::MAX` 后稳定返回 `Idle`，因此即使进程在最终 source commit 后退出，重开后的 schedule 仍会
继续排空下游。

Store 目录和 catalog 已有效、但 manifest 尚未提交时，`FlowFactory::open()` 返回
`IncompleteBuild`；
manifest 已发布却缺少所声明资源时返回 `MissingResource`。如果底层 `Store::create()` 本身只
留下无效目录，则打开时保留相应 Store 错误，不把它误报成有效 Flow 的未完成构建。

`FlowFactory::open()` 先读取、解码并重新校验 manifest，再解析拓扑并纯重建全部 Schema bindings；
只有成功后才重新打开全部数据对象和 output，最后按
source ID 重新注入 inputs、装配 Station；第二次 Definition 读取和所有 output frontier 校验共享
同一个 RO snapshot，不启动或提交写事务。open 不扫描全部 backlog；合法 IPC 中与绑定不一致的
Schema 会在对应 entry 首次 intake 时被拒绝且不推进 cursor。调用方不需要重新提交 Definition。

当前磁盘格式使用显式 magic、版本号、定长整数、sealed Operation Definition 集合的稳定 tag
和 IEEE `CRC32` 完整性校验，不依赖 Rust enum 布局或通用序列化框架。以下名称是兼容性边界：

- Flow manifest：`flow/definition`
- Flow 状态：`flow/state`
- Station 状态：`station/{index:08x}/state`
- Station active input key：`input/active`
- Station input cursor key：`input/{input_index:08x}/cursor`
- Station 输出：`station/{index:08x}/output`（仅限具有外部 output 的 Station）
- `SequenceSource` 位置：`station/{index:08x}/operation/sequence_source.position`
- Count 状态：`station/{index:08x}/operation/count`
- Project、Filter 和 Extend 不声明 Operation data，只使用通用 Station state 和 output

`index` 是 Station 声明顺序，`input_index` 是该 Station 持久化 source 列表中的端口顺序。active
input value 固定为 4 字节 big-endian `u32`，cursor value 固定为 8 字节 big-endian `u64 offset`。
当前仍是开发期 v1；output capacity 直接成为新的 v1 Station Definition 布局，不解码旧布局。
derived edge Schema 不单独持久化，但相同 Operation tag/payload 与有序 input Schemas 的绑定语义
属于 reopen ABI。编码、tag、绑定语义或资源布局可以破坏性调整，但每次调整必须同步更新黄金字节、布局
和 reopen 测试，不保留旧格式兼容层。`DataFusion` `Expr` protobuf 的不兼容升级必须明确 bump 外层
Operation Definition tag/version，并要求重建 Flow。

## 源码边界

`FlowFactory` 负责声明、纯拓扑校验、稳定编码以及 build/open 时的资源装配；运行态 `Flow` 只持有
已装配 Station、确定性 schedule 和分离的事务启动能力。私有 `build/schema.rs` 只负责拓扑序
Schema 传播和 Definition bind，不创建资源或进入运行调度；Station 是 crate 私有的单轮执行壳，拥有
输入 claim 与 output retention，但不接收 Store、不知道物理 placement 或稳定资源名。Flow 作为
Operation 与 Store 的组合根，通过单一公共 `correctness` target 验证资源布局、重新物化和运行
协议，不再建立重复的集成 package。具体目录 ownership 与私有实现约束见仓库 `AGENTS.md`；fixture、
证据分层和计时边界见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。

## 当前边界

本阶段完成定义、持久化 `build/open`、Flow 对分离读写事务启动能力的所有权，以及 Inbox 独占的
state/inputs、Station 可选的统一 Output、稳定 active input/cursor 和可重建的确定性拓扑 schedule。
输入准备已能通过 RO snapshot 从 active input 循环查找、durable pin 并幂等准备至多一个带来源
身份的 Claim；`process` 已支持 `Action::{Idle, Commit, Complete}`，Complete 在同一写事务中
原子协调 Operation continuation、output、active、cursor 与至多一个 head entry 的物理回收，
运行期持续维护 `head == min(consumer cursors)`。每个 output Station 还拥有持久化
retained-byte 高水位，容量拒绝会按强重放协议回滚完整 turn。Flow 已公开有界的
`Flow::advance`，真实 `SequenceSource → Extend → Filter → Project → Count → Discard` DAG 可以按拓扑逐轮推进并在 reopen
后续跑。端点校验已经排除完全没有 consumer 的 output，缓慢或停滞 consumer 会通过物理日志水位
自然反压上游。端口已经在 build/open 时绑定完整精确 Schema，运行期 producer append 与 consumer
intake 还会在事务提交前后两侧兜底校验。尚未实现 `Flow::start`、中断控制或外部副作用协议；内建 Count
当前仍在一个 turn 中完整处理 Change，但协议已经允许其他 Operation 用自己的持久化状态跨 turn
continuation。
`DataFusion` 集成目前止于 Filter/Extend 的 `Expr` protobuf、physical expression planning 与向量化执行，
不提供 SQL planner、catalog 或任意 `DataFusion` logical/physical plan 的 Flow 映射。

## 验证

```bash
cargo test -p dogpaddle-flow
cargo test -p dogpaddle-flow --test correctness
cargo clippy -p dogpaddle-flow --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-flow --no-deps
cargo bench -p dogpaddle-flow --bench flow_lifecycle
cargo bench -p dogpaddle-flow --bench flow_runtime
```

`flow_lifecycle` 只测当前确实存在的低频 lifecycle：fresh durable `build` 与 warm committed
`open`，按 Station 数量逐轴扩展。它不报告 rows/s，也不声称代表实际 Station processing
或运行时吞吐。`flow_runtime` 则测预先构建的 source/sink、Count chain、fan-out 和 capacity-pressure
Flow 的连续 `advance` 轮次，fixture、预热和结果校验都在计时外。`run` plan 保存静态 work counts，
machine sample 只保留总 `elapsed_ns` 和 raw `round_latencies_ns`；统一 reporter 从这些 raw
值重建 round p50/p95/p99，以及 advances/s、committed Station turns/s 和 input completions/s。
相邻 `.plan.json` 由 `cargo xtask bench-plan-check` 在不构建 Flow fixture 的情况下验证 smoke/reference
Plan。正式结果必须在显式 reference 文件系统上保留逐样本 JSONL；配置与输出协议见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。
