# dogpaddle-flow

`dogpaddle-flow` 用公共 `FlowFactory` 定义、构建和重新打开一条持久化 Flow；成功返回的
`Flow` 只表示运行态，不承担声明、构建或打开职责。Station 当前是 crate 内部的一对一 Operation
容器；它读取所包裹 Definition 显式声明的 `OperationKind`，向 Flow 提供 Scan、Sink、输入数量
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
    operation::scan::SequenceScanDefinition,
    operation::transform::{
        ExtendDefinition, FilterDefinition, RunningEventCountDefinition,
        SchemaAlignDefinition, SchemaAlignField,
    },
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let scan = factory.station("scan", SequenceScanDefinition::new(0));
    let extend = factory.station(
        "extend",
        ExtendDefinition::try_new("is_seven", col("value").eq(lit(7_u64)))?,
    );
    let filter = factory.station(
        "filter",
        FilterDefinition::try_new(col("is_seven"))?,
    );
    let align = factory.station(
        "align",
        SchemaAlignDefinition::try_new([
            SchemaAlignField::try_new("event", col("value"), true)?,
        ])?,
    );
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    let capacity = NonZeroU64::new(64 * 1024 * 1024).unwrap();
    factory.output_capacity_bytes(scan, capacity);
    factory.output_capacity_bytes(extend, capacity);
    factory.output_capacity_bytes(filter, capacity);
    factory.output_capacity_bytes(align, capacity);
    factory.output_capacity_bytes(count, capacity);
    factory.connect([scan], extend);
    factory.connect([extend], filter);
    factory.connect([filter], align);
    factory.connect([align], count);
    factory.connect([count], sink);

    let flow = factory.build()?;
    assert_eq!(
        flow.station_ids().collect::<Vec<_>>(),
        ["scan", "extend", "filter", "align", "count", "sink"]
    );
    drop(flow);

    let reopened = FlowFactory::new(&path).open()?;
    assert_eq!(reopened.station_count(), 6);
    Ok(())
}
```

Station ID 必须非空、不能包含 NUL，并且在一条 Flow 内唯一。连接保留 input 顺序，允许
fan-out 和重复 input；连接数量必须与 Station 对外声明的输入数量完全一致，整个拓扑
必须是 DAG。所有入度为零的起点必须是 Scan Station，所有出度为零的终点必须是 Sink Station；
Sink 没有 output，不能作为其他 Station 的 input。允许多个 Scan、多个 Sink 和多个互不连接的
合法 DAG 分量。每个 `OperationKind::has_output()` 为 true 的 Station 必须且只能声明一次非零
output capacity，outputless Station 不得声明；重复、遗漏、类别不匹配或 foreign `StationRef` 都在
纯校验阶段失败。拓扑解析后，Flow 还按确定性拓扑顺序把每个 producer 的精确 output Schema
传到 consumer 的对应有序 input port，并调用 Definition 的纯 bind；任一 Schema 拒绝都会带
Station ID 返回 `FlowError::Schema`。拓扑、容量、Schema、Operation data 声明校验或 manifest
编码失败都不会创建目标目录。

### 外部算子的运行配置

`FlowFactory::resource(station_id, value)` 为指定 Station 装配一个拥有型、非持久的运行资源。
Flow 只按 ID 路由，不知道具体 connector 类型；Operation binding 在 Store 创建前检查精确资源类型。
缺失、错误类型、重复或多余资源都会报错，错误不会打印资源内容。普通算子不接收资源。

build/open 统一从 `FlowFactory::new(path)` 进入。open 使用磁盘中的 Definition，不接受重新声明
Station/连接/容量；只需重新传入临时配置。没有保留旧的 static open 兼容入口。

```no_run
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::operation::scan::PostgresCdcScanConfig;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let config = PostgresCdcScanConfig::new_unencrypted(
    "/opt/dogpaddle-debezium", "127.0.0.1", 5432, "shop", "cdc",
    std::env::var("CDC_PASSWORD")?,
)?;
let mut factory = FlowFactory::new("./shop-flow");
factory.resource("orders", config)?;
let mut flow = factory.open()?;
flow.advance()?;
# Ok(())
# }
```

`PostgresSinkConfig` 也通过同一个 `resource(station_id, value)` 入口装配。增加 `PostgresSink` 没有
改变 `FlowFactory`、`Flow::advance` 或 Station 协议，也没有引入通用 Sink trait、ORM 或 backend registry。
它的 `discover_target` 由调用方在 build 前显式执行，并会进行只读 `PostgreSQL` catalog I/O。

`PostgresCdcScan`/`MySqlCdcScan` 的 discovery 由调用方在 build 之前显式执行。build/open 不解析 secret、
不连接源库、不启动 JVM；快照、poll、转换、ACK 全在各自 concrete Operation 内完成。两者均持久
`phase + checkpoint + bootstrap_spool`，按 `Fresh → Capturing → Publishing → Streaming` 先私有捕获完整初始快照，
再逐条发布到普通 Station output。`bootstrap_spool` 是私有 `Queue<Vec<u8>>`；发布期 dequeue 与
output append 同事务，捕获中断后逐条清空并重做完整快照。
必填 `NonZeroU64 bootstrap_spool_bytes` 以 `queued bytes + 8-byte private sequence + IPC` 硬限制
私有 spool；超限 delivery 不 ACK，需以更大容量重建。
poll 不等待数据；宿主在整轮 Idle 或持续 Backpressured 时自行安排等待，避免忙轮询。
它只输出完整 Change，Flow 没有 `ingest`、PG 专用分支或另一套调度状态。
完整真实 PG→SQLite 与进程恢复 host 见 `system-tests/postgres/hosts/src/bin/postgres_cdc.rs`，显式验收命令见根目录 TESTING.md。

Filter/Extend/Select/`SchemaAlign` 的 Definition 在 `station()` 之前已通过 fallible 构造入口将
`DataFusion` `Expr` 编码为
`DataFusion` protobuf；无法编码的表达式直接作为构造错误返回。表达式的字段解析、type、nullability、
cast 和 `PhysicalExpr` 创建留在上述全图 bind 阶段，由 `DataFusion` 完成。`create_physical_expr` 假定
logical coercion 已完成，而 Flow 不运行 logical/SQL planner，因此混合类型需要调用方显式 cast；失败同样不会创建
目标目录。`SchemaAlign` 的每个目标字段显式声明名称、Expr、目标 nullability 与 Field metadata，并
显式声明 Schema metadata；metadata 输入顺序不影响 canonical 编码，重复 key 在构造期拒绝。字段类型
只从 Expr 推导，nullable → non-null 收窄在纯 bind 阶段拒绝。
Project 仍使用严格递增的顶层字段索引；UnionAll 只要求其所有输入具有完全相同的 Schema，不执行
隐式对齐。

Change v1 现可稳定承载 Date32、四种单位且 timezone 为可选非空字符串的 Timestamp，以及 precision
`1..=38` 的 Decimal128。Change 构造、全量解码和被选择字段的投影解码递归验证每个 Decimal128
non-null slot 的 `|unscaled| < 10^precision`；祖先 List/Struct null 不豁免物理 non-null child，
未选择字段不读取或验证 value。Operation/Flow 已有一个受限纵向证据：Date32、无 timezone 的 Millisecond
Timestamp 和 `Decimal128(10, 2)` 经显式 `SchemaAlign` cast、Project、Select、Extend、Filter 同类型
组合比较与 `RunningEventCount` 到 Discard；真实 Flow 在 build 后运行，并跨两次 reopen 继续得到
最终 count `3`。这不承诺其他 Timestamp unit/timezone、时间/Decimal 运算、舍入或任意跨类型 cast。
已承诺/当前版本可规划但未承诺/明确拒绝的表达式矩阵以
[`dogpaddle-operation`](../operation/README.md#表达式能力状态) 为准。

`SqliteSink` 为 Flow 提供首个可查询终点。build/open 只完成 Schema、SQL 与行编码绑定，不打开
`SQLite` 文件；首次收到输入后才初始化新的 `STRICT` 目标表。Sink 用自身 Store continuation 幂等
覆盖 `SQLite` commit 与 Flow commit 之间的窗口。面向产品的可运行入口是
[`dogpaddle-sql` quickstart](https://github.com/frelion/dogpaddle/blob/main/crates/sql/examples/quickstart.rs)，Flow 级端到端恢复证据位于
[`tests/correctness/sqlite_sink.rs`](https://github.com/frelion/dogpaddle/blob/main/crates/flow/tests/correctness/sqlite_sink.rs)。

`PostgresSink` 是单输入、无 output 的 `PostgreSQL` exact relation 终点。Definition 只保存非敏感
target spec，numeric IP、端口与凭据随 `PostgresSinkConfig` 在 build/open 时注入；build/open 对 PG 目标
不做 I/O，只完成 Schema/resource 装配与 Store 生命周期。它与 `SQLite` 共用一个关系 Sink 内核：
先把至多 1024 个具体 mutation 作为 Prepared 批次提交到 `relation_sink.state`，随后才在
`AfterCommit` 中用一个目标事务先批量 insert-ignore、再按固定 ID delete；下一 turn 返回
`Commit` continuation 或 `Complete`。目标已提交但本地未结算时，reopen 重投同一组 ID，
不需要回执表。只保证批次提交后的关系，不保证目标 WAL 事件顺序。目标对象由 Sink 独占且
Schema 固定；同一 target spec 不得由其他 Flow 接管或共享。远端 marker 只是 ownership/layout-version
标记，精确 logical Schema 由 Flow binding 与运行时 guard 保证。连接和每个数据库工作单元有 5 秒
client deadline；DNS endpoint、TLS、在线演进与外部修改不在当前协议内。真实验收见根目录 `TESTING.md` 的
`system-tests/postgres/check_sink.py`。面向产品的同库实时订单履约示例从 `sales.orders` 捕获 CDC，经过
多段 SQL 转换后写入 `ops.fulfillment_queue`，完整程序见
[`fulfillment.sql`](https://github.com/frelion/dogpaddle/blob/main/crates/sql/examples/fulfillment.sql)。

## 运行状态

`Flow::status()` 按声明顺序返回 `Vec<StationStatus>`：每个 Station 的 ID、当前 input（Scan 为 `None`、
单输入为 `0`、多输入来自 active Cell）、各 Subscription 的 position/tail、output
head/tail/retained bytes/capacity，以及最近一次调度结果和 fail-stop 标记。全部持久计数来自同一个
短 RO snapshot，不解码 Change、不调用 Operation、不连接外部系统，也不启动写事务；即使 Flow
需要 reopen 也能查询。

```no_run
# fn inspect(flow: &dogpaddle_flow::Flow) -> Result<(), dogpaddle_flow::FlowError> {
for station in flow.status()? {
    println!("{}: {:?}, reopen={}", station.id, station.last_outcome, station.needs_reopen);
    for (port, input) in station.inputs.iter().enumerate() {
        println!("  input {port}: {} Changes waiting", input.tail - input.position);
    }
}
# Ok(())
# }
```

`last_outcome` 保留实际处理的 Backpressured，不受整轮 `Progressed` 或该 Station 的 durable pin
覆盖；它只描述最近那一轮，不预测下一轮。未执行或失败的 Station 为 None，reopen 后全部为 None。
backlog 单位是完整 Change 而非行数；capacity 仍是允许空日志单条 oversize 的软水位，不是硬内存上限。
这不是指标历史库或后台监控线程。

## 持久化边界

每条 Flow 独占一个 Store。`FlowFactory::build()` 先完成声明并稳定编码，再立即解码这份 canonical
manifest；拓扑解析、Schema 绑定和后续资源布局都只使用将要持久化的 Definition，而不依赖调用方
原始 Rust 对象的额外状态。全部纯校验成功后，build 才按 Operation Definition 的逻辑数据名创建
状态，并为每个具有 output 的 Station 创建一个 `SubscribedLog<Vec<u8>>`，最后提交 manifest Cell
作为构建完成标记。Operation Definition 返回稳定的
“逻辑名称 → 完整数据类型”声明；`FlowFactory` 的
build/open 通路负责完整资源名，并通过 Store 将每项声明创建或打开为具体实例，再按逻辑名称
交给先前 Schema bind 产生的一次性 `OperationBinding` 装配 Operation。数据实例绑定不依赖声明
顺序，具体算子不接触 Store、底层句柄或物理布局。

零输入和单输入 Station 不创建 Station 自有状态。只有多输入 Station 创建一个 `Cell<u32>`，记录下一次
轮询从哪个端口开始；build 在发布 manifest 的同一事务中将它初始化为 `0`。每个 output log 同时
按拓扑固定其直接 consumer 数量，并初始化一组稠密 Subscription identity；subscriber ID 按 consumer
Station 声明顺序、再按其 input port 顺序从 `0` 递增。重复 input edge 获得彼此独立的 Subscription。

每个 output value 是一个内嵌 Schema 的完整 Change IPC Stream，不另建 Schema Cell。output capacity
直接保存在 Flow Definition 中；build/open 将它、`SubscribedLogWriter` 和绑定得到的精确 logical
Schema 装配成一个运行期 `Output`。每个 `InputPort` 持有对应的 `Subscription` 并共享 producer 的
`Output` Schema。端口 Schema 一致性不依赖 Change codec 之外的
Schema resource、fingerprint 或 registry：持久化 Definition 加上有序 input Schema 在 reopen 时
确定性重建同一 binding。运行层只能使用已经声明的数据对象，不能动态新增数据空间。
Filter/Extend/Select/`SchemaAlign` 的 manifest payload 直接包含 `DataFusion` `Expr` protobuf，但不持久化 `PhysicalExpr`。
build/open 都从 protobuf 还原 `Expr`，并针对 exact input Schema 重新调用 `create_physical_expr`。
`DataFusion` 依赖与 Arrow 精确 pin；跨 `DataFusion` 版本不保证读取兼容。不兼容升级只更新当前
Operation Definition 基线并重建 Flow；旧数据库直接删除，不承诺兼容或迁移旧 payload。

Store 随后被转换为唯一的 `Transactions`；build/open 在取得完整所有权时以 consuming `split`
显式获得同环境的 `ReadTransactions`，Flow 长期持有返回的读写两种能力。`ReadTransactions` 不可
克隆但可安全共享，并且只能开启 RO snapshot。Station 不长期保存任何事务启动能力：内部
输入准备只在调用期间借用 `ReadTransactions`，并在多输入 Station 需要固定选中端口时临时借用
`&mut Transactions`；`Station::process` 同样只在调用期间借用 writer。后者无法消费 writer 来
split 出 owned reader。拓扑和资源目录都没有运行期修改入口。

Schema binding 先对全图完成，随后按声明顺序创建或打开必要的多输入 active Cell、Operation data 和 output。
拓扑为每个 input edge 确定 producer 与 subscriber ID；装配时从 producer log 派生对应
`Subscription`，再把 writer、capacity 与精确 Schema 唯一移入共享 `Output`。声明顺序不必是
拓扑顺序，fan-out 仍共享同一个物理 log。Station 不知道相连的 Station ID，只拥有 Operation、
可选 `Output`，以及带可选 active Cell、有序 ports 与至多一个 owned `Claim` 的 `Inbox`。
装配还从已验证 Definition 派生唯一运行期 schedule：先按拓扑层次排列，同一层按 Station 声明
顺序排列。build/open 得到相同结果，不需要持久化 schedule。

`Inbox` 没有 Claim 时，输入准备在一个 RO snapshot 中从当前端口开始循环调用各 Subscription 的
`peek`，跳过空端口并只选择第一个可用 entry。零输入没有当前端口，单输入恒为端口 `0`；多输入才
读取 active Cell。多输入选中其他端口时，Station 在调用 Operation 前用独立短写事务将该端口写入
active Cell。随后 Claim 保存 port、Subscription offset 和完整 owned Change，作为可由持久状态重建
的内存副本。

IPC 解码完成后，`intake` 先把 Change 的完整 logical Schema 与该 input 共享的 `Output` Schema
精确比较，匹配后才能安装 Claim；Schema 不匹配不会 pin、acknowledge 或产生持久化写入。Claim
存在时 `intake` 不访问 Store。reopen 后 Claim 为空，由 active Cell（若有）和 Subscription position
重建同一输入。零输入 Scan 没有 Claim，但仍由相同 Station 路径调用 `turn(None)`；没有 Scan 专用
outcome 或事务路径。没有 output 的 Sink 不能被其他 Station 作为 input。

`process(&mut Transactions)` 先在没有活动写事务时调用 `Operation::turn`。有输入 Operation 每次只
接收端口和一个完整 borrowed `Change`，不会看到 Subscription offset 或 Station 运行元数据；
Scan 在同一入口收到 `None`。`Turn::Idle` 直接结束，不开启写事务；`Turn::Ready` 携带一个只能
消费一次的 `PreparedTurn`，Station 此时才开始唯一写事务，并只把不能提交的
`TransactionAccess` 交给其 `apply`。已有 Claim 会直接提供给 Operation，不在每个 turn 前重新读取
Store；需要消费该 Claim 时，验证与状态迁移一起进入原子 Complete 事务。

prepared turn 只返回三个 `Action`：`Idle` 丢弃本次事务，因而不发布 output、不保存 Operation 写入，
也不改变当前 Claim；`Commit(output)` 提交 Operation 状态与可选 output，但保留已经提供的 Claim；
`Complete(output)` 只用于有输入的 turn，并声明该完整 Change 已处理。Scan 的成功 turn 使用
`Commit`，input-free Operation 返回 `Complete` 是协议错误。Complete 会在同一事务中验证 Claim 的
port，并以 Claim 携带的 expected offset 调用 `Subscription::acknowledge`，同时提交 Operation 状态、
可选 output 和多输入 active 轮转；Subscription 会验证该 offset 正是当前位置。只有外层 commit
成功后才清除 Claim。日志位置和共享保留完全由 `SubscribedLog` 维护。
任何 Operation、编码、append、active、Subscription 或 commit 错误都会回滚本次事务并保留 Claim。
Operation 返回 output 时，Station 在 IPC 编码和 capacity 判定之前先按同样的精确规则校验其
logical Schema；不匹配是协议错误，整个 turn 的 Operation 状态、output 与输入进展全部回滚。

`PreparedTurn::apply` 还可返回一个 `AfterCommit`。Station 只在上述事务成功提交后消费它；
`Action::Idle`、协议错误、Schema 错误、背压、Store 错误或 commit 失败都只丢弃 completion，绝不
调用。外部 ACK 等不能回滚的确认动作只允许放在这个阶段。若 completion 失败，本地提交仍然有效，
已完成输入的内存 Claim 仍会清除；错误通过 `FlowRunError::requires_reopen()` 明确标记，当前运行态
Station 随即 fail-stop，必须 reopen Flow 后从 durable state 恢复。
普通提交前 `OperationError` 不设置该标记；Operation 必须保持同一运行实例可从未改变的 durable
state 重试，或在下一 turn 自行重建失败的临时资源。

Operation 在调用前不会因 output 已达到水位而被跳过，因为 Station 尚不知道本次是否产生 output
以及编码后大小。没有 output 的 `Commit` 即使日志已经达到水位也可以正常提交；有 output 时
Station 先完成 Change 编码，再调用 `SubscribedLogWriter::try_append`。非空日志追加后超过
capacity 会正常返回背压而非错误：本次 Operation 写事务整体回滚，Scan position、`Commit`
continuation、`Complete` acknowledgement/active 都不前进，Claim 保留，下一 turn 重新执行 Operation。
物理空日志允许一条 oversize entry，避免单个合法 Change 永久无法前进。这是一项 per-output soft
high watermark，不是 Store 文件大小或进程内存硬配额。

只要当前输入尚未 `Complete`，无论前一 turn 是 `Idle`、`Commit`、错误、output append 失败、commit
失败还是进程重开，下一次调用都必须收到同一 `(port, offset, bytes)` 所标识的完整 Change；这要求
同一日志 entry 的原始字节不变，不要求重新解码后拥有相同内存地址。Change 内部的处理位置属于
Operation continuation，必须存入该 Operation 通过 Definition 声明的 Store 状态；`Inbox` 只拥有
可选 active Cell、各 input Subscription 和可丢弃重建的 owned Claim。

每条 Subscription 的 offset 只定位一个完整 Change IPC entry，是当前持久化分批下的读取位置，
不是业务 event ID。Station 可以为了吞吐稳定地合并或切分物理批次，但变换前后展平的输入事件序列必须
逐项相同，也不能隐式 consolidation。不同输入边的 offset 彼此不可比较。对多输入 Station，active
Cell 既记录未完成输入的 durable port，也在没有未完成输入时规定轮询起点；单输入恒用端口 `0`，
无需该状态。端口选择不声称还原跨 input 的事件发生时间。只有 `Complete` 才 acknowledge 当前
offset。Operation 的
展平 output 事件序列和最终业务状态必须同时对稳定重批及同一 Change 的重复 `Commit` turn 切分保持不变。
显式跨端口无序的 `UnionAll` 只保持各端口内部事件顺序和最终关系状态；跨端口交织由上述 durable
Station schedule 决定，可随各 input 的分批和可用性变化。需要业务级跨端口总序时，必须另行引入逻辑
ingress、barrier、窗口或排序语义。

`SubscribedLog` 在 Store 内维护每个 Subscription 的 position、共享保留范围和 retained-byte 账本。
重复 input edge 各有独立 Subscription；最慢 consumer 继续保护共享 entry。acknowledgement、active
轮转、Operation 状态和 output 位于同一 Station 写事务中。open 只要求 Store 验证固定 subscriber
数量与日志状态；Store 错误继续保留为结构化 `FlowError::Store`。

`Flow::advance` 聚合为 `Progressed > Backpressured > Idle`：任一 Operation、durable input pin 或
input completion 有提交就返回 `Progressed`；整轮没有提交、但至少一个实际 output 被容量拒绝时返回
`Backpressured`；既无提交也无容量拒绝才返回 `Idle`。背压不会提前终止 schedule，所以 consumers 和其他
DAG 分量仍获得本轮 turn。fan-out 共享一份 output log 和 capacity，最慢 Subscription 会有意
阻塞整个 producer；各 consumer 独立持有的 decoded Claim 不计入该容量。SequenceScan 提交
`u64::MAX` 后稳定返回 `Action::Idle`，因此即使进程在最终 scan commit 后退出，重开后的 schedule 仍会
继续通过 consumers 排空已提交 output。

Store 目录和 catalog 已有效、但 manifest 尚未提交时，`FlowFactory::new(path).open()` 返回
`IncompleteBuild`；
manifest 已发布却缺少所声明资源时返回 `MissingResource`。如果底层 `Store::create()` 本身只
留下无效目录，则打开时保留相应 Store 错误，不把它误报成有效 Flow 的未完成构建。

`FlowFactory::new(path).open()` 在一次 Store setup 生命周期中读取、解码并重新校验 manifest，再解析拓扑并
纯重建全部 Schema bindings；只有成功后才用同一个 Store 打开其余数据对象和 output，最后按
input ID 重新注入 inputs、装配 Station。Store 始终独占同一路径，Definition 只读取一次；已有 active Cell
与 output log 在 setup 的同一个 RO snapshot 中校验，之后才消费 Store 获得运行期事务能力，不启动或提交写事务。open 不扫描或解码全部 backlog；合法 IPC 中与绑定不一致的
Schema 会在对应 entry 首次 intake 时被拒绝且不 acknowledge。调用方不需要重新提交 Definition。

当前磁盘格式的外层使用显式 magic、版本号、定长整数、sealed Operation Definition 集合的稳定 tag
和 IEEE `CRC32` 完整性校验，不依赖 Rust enum 布局。具体 Operation payload 有各自的固定编码：
例如表达式使用 pinned protobuf，tag11 `PostgresCdcScan` 与 tag12 `PostgresSink` 使用各自的 canonical
JSON。以下名称是兼容性边界：

- Flow manifest：`flow/definition`
- 多输入 Station active input：`station/{index:08x}/active-input`
- Station 输出：`station/{index:08x}/output`（仅限具有外部 output 的 Station）
- `SequenceScan` 位置：`station/{index:08x}/operation/sequence_scan.position`
- `RunningEventCount` 状态：`station/{index:08x}/operation/running_event_count.count`
- `PostgresCdcScan` phase：`station/{index:08x}/operation/postgres_cdc_scan.phase`
- `PostgresCdcScan` checkpoint：`station/{index:08x}/operation/postgres_cdc_scan.checkpoint`
- `PostgresCdcScan` spool：`station/{index:08x}/operation/postgres_cdc_scan.bootstrap_spool`
- `MySqlCdcScan` phase：`station/{index:08x}/operation/mysql_cdc_scan.phase`
- `MySqlCdcScan` checkpoint：`station/{index:08x}/operation/mysql_cdc_scan.checkpoint`
- `MySqlCdcScan` spool：`station/{index:08x}/operation/mysql_cdc_scan.bootstrap_spool`
- `SqliteSink` / `PostgresSink` 状态：`station/{index:08x}/operation/relation_sink.state`
- Project、Filter、Extend、Select、`SchemaAlign` 和 `UnionAll` 不声明 Operation data；只有输入数大于
  `1` 的 Station 额外拥有 active Cell

`index` 是 Station 声明顺序。active input value 是 `Cell<u32>`；Subscription position 与 output
保留元数据由 `SubscribedLog` 私有持久化。
当前仍是开发期 v1；output capacity 直接属于当前 Station Definition 布局。
derived edge Schema 不单独持久化，但相同 Operation tag/payload 与有序 input Schemas 的绑定语义
属于 reopen ABI。`RunningEventCount` 当前 tag 为 `2`，并使用新的 API、逻辑 data 名与资源路径。
不提供旧名称 alias、资源路径 fallback 或迁移；旧数据库直接删除并按当前 Definition
重建。编码、tag、绑定语义或资源布局可以在开发期直接调整，但每次调整必须同步更新当前黄金字节、
布局和 reopen 测试。`DataFusion` `Expr` protobuf 的不兼容升级同样只更新当前基线并重建数据库。

## 源码边界

`FlowFactory` 负责声明、纯拓扑校验、稳定编码以及 build/open 时的资源装配；运行态 `Flow` 只持有
Station ID、已装配 Station、确定性 schedule 和分离的事务启动能力，不保留完整 Definition。私有
`build/schema.rs` 只负责拓扑序 Schema 传播和 Definition bind，不创建资源或进入运行调度；Station
是 crate 私有的单轮执行壳，拥有
输入 claim 与 output capability，但不接收 Store、不知道稳定资源名。Flow 作为
Operation 与 Store 的组合根，通过单一公共 `correctness` target 验证资源布局、重新物化和运行
协议，不再建立重复的集成 package。具体目录 ownership 与私有实现约束见仓库 `AGENTS.md`；fixture、
证据分层和计时边界见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。

## 当前边界

本阶段完成定义、持久化 `build/open`、Flow 对分离读写事务启动能力的所有权，以及 Inbox 独占的
可选多输入 active Cell 与 input Subscriptions、Station 可选的统一 Output 和可重建的确定性拓扑
schedule。输入准备通过 RO snapshot 从当前端口循环查找、在多输入时 durable pin，并幂等准备至多一个带来源
身份的 Claim；`process` 已支持事务外 `Operation::turn`、线性 `PreparedTurn`、事务内
`Action::{Idle, Commit, Complete}` 与仅在提交成功后运行的 `AfterCommit`，Complete 在同一写事务中
原子协调 Operation continuation、output、active 与 Subscription acknowledgement。每个 output Station
拥有持久化 retained-byte 高水位，容量拒绝会按强重放协议回滚完整 turn。Flow 已公开有界的
`Flow::advance`，真实表达式链路与 `SequenceScan → Select → UnionAll → RunningEventCount → Discard`
多输入 DAG 可以按拓扑逐轮推进并在 reopen
后续跑。端点校验已经排除完全没有 consumer 的 output，缓慢或停滞 consumer 会通过物理日志水位
自然反压 producer。端口已经在 build/open 时绑定完整精确 Schema，运行期 producer append 与 consumer
intake 还会在事务提交前后两侧兜底校验。运行资源注入、具体 `PostgresCdcScan`、`MySqlCdcScan` 与 `PostgresSink` 已沿
通用 Operation 调度协议承接事务外初始化/poll、durable state 写入与提交后确认；`SqliteSink` 与
`PostgresSink` 分别实现自己的目标专用幂等提交边界。尚未实现 `Flow::start` 或中断控制。内建 `RunningEventCount`
当前仍在一个 turn 中完整处理 Change，但协议已经允许其他 Operation 用自己的持久化状态跨 turn
continuation。
`DataFusion` 集成目前止于 Filter/Extend/Select/`SchemaAlign` 的 `Expr` protobuf、physical expression planning 与向量化执行，
不提供 SQL planner、catalog 或任意 `DataFusion` logical/physical plan 的 Flow 映射。

## 验证

```bash
cargo test -p dogpaddle-flow
cargo test -p dogpaddle-flow --test correctness
cargo clippy -p dogpaddle-flow --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-flow --no-deps
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-flow --bench flow_lifecycle
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-flow --bench flow_runtime

# 需要独立临时 PostgreSQL；脚本会构建系统验收 host
python3 system-tests/postgres/check_sink.py --postgres-bin /absolute/path/to/postgresql/bin
```

`flow_lifecycle` 使用 Criterion，只测当前确实存在的低频 lifecycle：fresh durable `build` 与 warm
committed `open`，按 Station 数量扩展。它不报告 rows/s，也不声称代表 Station processing 或运行时
吞吐。Criterion 原生 artifacts 与环境上下文都写入同一个 `RunRoot`。

`flow_runtime` 是 Flow 自有 JSONL runner，测预先构建的 scan/sink、`RunningEventCount` chain、fan-out
和 capacity-pressure Flow。预热只推进并断言 outcome，不计时也不输出；随后保留每一次采样
`advance` 的原始 latency、outcome、静态 work counts 和场景末 durable oracle。每条记录随产生立即写出，
stderr 只报告人类进度。fixture、输出和结果校验不进入 `Flow::advance` 的计时区间。不存在中央 plan、
fingerprint、validator 或跨 target schema。
正式结果必须在显式 `DOGPADDLE_PERF_ROOT` reference 文件系统上保留；配置与输出规则见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。
