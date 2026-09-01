# dogpaddle-operation

`dogpaddle-operation` 提供具体、强类型的 Operation Definition、持久化 Data 和运行实例。
Definition 是无副作用、可持久化的数据；它声明所需数据对象的稳定逻辑名、collection 类型
及该 collection 真正需要的布局参数。Flow 在 `build/open` 阶段创建或打开这些类型化对象，再交给
Definition 直接装配运行实例。具体算子不接触 `Store`、`DataHandle` 或物理放置策略。

## 数据边界

共享的 Arrow Schema、批量差分模型和“每个 Change 一个自描述 IPC Stream”的编码属于独立的
`dogpaddle-change` crate。Operation 的运行接口以内存中的 `Change` 为输入输出；Operation 只
负责数据变换和自己声明的持久化状态，不读写 IPC、不读取边日志，也不决定物理
batch 的合并与 flush。Change 的行位置是事件顺序；Operation 必须依次观察输入事件，并按
其声明的语义产生有序输出，不能把未 consolidation 的输入当作可交换集合。除非将来接收到
独立定义的窗口、barrier 或 flush 信号，Operation 的展平输出事件序列和最终业务状态必须在稳定
合并或切分 Change 后保持不变，也不能因同一个 Change 被分成多少个 `Commit` turn 而改变；物理
Change 边界和 turn 边界都不能被算子当成业务事件。

运行时 [`operation::Operation`] trait 提供统一、object-safe 的 `turn`。零输入 Source 与其他
Operation 走同一个调用协议，只是收到 `None`；Transform 与 Sink 每次收到一个完整 borrowed
Change，以及它在 Definition 有序输入中的 `usize` 端口序号。Operation 不接收 `AppendLog`
offset。

turn 返回 [`operation::Action`]：`Idle` 表示没有可提交进展，调用方必须回滚该 turn 的全部写入；
`Commit` 提交 Operation 状态和可选 output，但不完成当前输入，下一 turn 仍收到同一端口、同一日志
offset 和逐字节相同的完整 Change。零输入 Source 也用 `Commit` 表示一次成功 turn。`Complete` 才在
同一事务中提交 Operation 状态、可选 output 和当前输入完成。两种提交动作都至多产生一个 owned
output Change；filter 或 Sink 可以使用 `None`。
跨 turn continuation 必须放在 Operation 自己通过 Definition 声明的持久化 Store 状态中，不能
隐藏在 Station。具体错误统一擦除为标准 boxed [`operation::OperationError`]：算子语义错误保留
具体算子错误类型，Store、Arrow 和 Change 等基础错误保留原始类型，均可按具体类型 downcast。
由于 `Idle`、错误、Station output 容量拒绝或外层 commit 失败都会导致 turn 重放，Operation 不得在 `turn` 内直接执行无法随
Store transaction 回滚的可观察副作用；外部 Sink 需要独立的幂等提交协议，当前尚未定义。

| action | 本 turn 写入与 output | 当前输入 |
| --- | --- | --- |
| `Idle` | 全部回滚 | 有输入时保持不变 |
| `Commit(output)` | 提交 | 有输入时保留，下一 turn 完整重放 |
| `Complete(output)` | 提交 | 完成，调用方才可推进 |

下文所说的 data class 指一个完整的 Rust 持久化数据类型，包括 collection、值类型，以及该
collection 存在选择时的 `SIZE`，例如 `Cell<u64>` 或 `OrderedMap<u64, String, Large>`。

运行实例及具体算子统一组织在 `operation` 模块中，其下按语义分为三个公共模块：`source`
保存无上游输入的源算子，`transform` 保存消费并产生记录的转换算子，`sink` 保存只消费记录
的终点算子。当前 `source` 包含 `SequenceSource`，`transform` 包含 Count，`sink` 包含
Discard。目录分类不作为运行时类型系统；每个 Definition 必须通过
[`OperationDefinition::kind`] 显式声明包含输入数量的结构类型。

## Definition 与持久化

具体 Definition 统一实现 sealed [`OperationDefinition`] trait。trait 要求每个具体算子手动返回
[`OperationKind`]，并以 `{ 逻辑名: data class }` 的形式向 Flow 声明完整数据 schema。
`OperationKind::Source` 固定为零输入，Transform 与 Sink variant 携带非零 `u32` 输入数量，因此类别、
input arity 和 output 属性不会形成非法组合。kind 不是从拓扑位置推断：Source、Transform 和 Sink
分别声明自己在数据流中的结构语义；Station 读取所包裹 Definition 的 kind，再向 Flow 提供自己的
source/sink 与 output 属性。Flow 负责生成完整
资源名，并调用声明携带的类型化 create/open 能力；得到的实例按逻辑名组成集合，再交给
Definition 的 `materialize`。具体 Definition 只按名称取得已经创建或打开的
`Cell<T>` 或 `OrderedMap<K, V, SIZE>`，声明顺序不参与绑定，也不接收 Store。

collection 只暴露真实存在的布局选择：`Cell<T>` 永远使用共享空间；`OrderedMap` 的 `Small`
形式共享底层物理空间，`Large` 形式拥有独立物理空间。Size 属于支持选择的具名数据对象的
静态 schema，而不是由 Flow 根据数据量猜测。Flow 只解释声明，不枚举具体算子或 collection
类型。

```rust
use dogpaddle_operation::{
    OperationDefinition, OperationKind, decode_definition, encode_definition,
    operation::source::SequenceSourceDefinition,
};

let source = SequenceSourceDefinition::new(10);
assert_eq!(source.kind(), OperationKind::Source);
assert_eq!(source.kind().input_count(), 0);

let encoded = encode_definition(&source);
let decoded = decode_definition(&encoded).unwrap();
assert_eq!(encode_definition(decoded.as_ref()), encoded);
```

Definition 集合在本 crate 内保持封闭，但不再使用公共 enum。稳定 decoder 由一张私有
`tag → decode function` 静态表选择；每个具体算子模块拥有自己的 tag、payload codec、
数据声明和物化逻辑。Flow 不枚举具体算子，也不解析 Operation payload。

为穿过 object-safe 的 [`OperationDefinition`] 边界，数据实例仅在一次性的 build/open 装配
过程中进行私有类型擦除；具名声明在 `materialize` 中将其安全恢复为精确 data class。
类型不匹配会返回错误而不是 panic。类型擦除不会进入运行实例、事务访问路径或持久化格式。

物化后的具体实例统一实现 [`operation::Operation`] trait。Flow 将异构实例保存为
`Box<dyn operation::Operation>`，并通过同一个 `turn` 分派运行。Definition 单向物化 Operation；运行
trait 不再反向暴露 Definition，Flow 在 build/open 时已经从持久 Definition 获得 kind 与资源声明。
Operation 本身可以在外部实现，但 Flow 只从 sealed Definition 物化运行实例；开放可注入 Flow 的
第三方算子仍需另行设计 tag 分配、decoder 注册和运行错误边界。

## `operation::source::SequenceSource`

[`operation::source::SequenceSourceDefinition`] 是零输入源，记录首个 `u64` 值。物化后的
[`operation::source::SequenceSourceOperation`] 直接持有 `Cell<u64>`，保存最后一次
已提交的值；首次产生 `start`，随后逐一递增。每个 turn 产生一行，输出固定为一个
non-null `UInt64` `value` 字段，所有 diff 都是 `+1`。包含 `u64::MAX` 的最后一批可以成功提交，
后续 turn 返回 `Idle`，不再写 position 或产生 output，使 Flow 仍能调度下游并排空已经提交的
Change。每次产生值的 turn 返回 `Action::Commit(Some(_))`；Station 不为
Source 建立另一套 outcome 或事务路径。它声明自己产生输出。

它声明一个逻辑数据名 `sequence_source.position`，由 Flow 解析为稳定 Station 资源名。

## `operation::transform::Count`

[`operation::transform::CountDefinition`] 要求一个输入。每成功推进一次，
[`operation::transform::CountOperation`] 按输入行序计算事件数量：每一行恰好令直接持有的
`Cell<u64>` 加一，输入 diff 的符号和数值不改变“一个有序事件”的计数。每个已处理输入行输出
一个 non-null `UInt64` `count`，diff 固定为 `+1`；因此它是插入式的运行计数事件流，不是维护
单例关系的 cardinality aggregate。未写入的 count Cell 解释为 `0`，溢出返回
[`operation::transform::CountError::Overflow`]。Count 显式声明为携带一个输入的
[`OperationKind::Transform`]；拓扑位置不会把它隐式变成 Sink，因此完整 Flow 必须把它连接到
一个下游 Sink。

Count 只声明 `count: Cell<u64>`。当前实现每个 turn 一次处理完整 Change，并返回
`Action::Complete(Some(_))`；它在写状态前预检整批行数，若最终值无法用 `u64` 表示，则返回 overflow，
整个 turn 不产生部分进展。协议允许其他 Operation 用声明的持久化状态在多个 `Commit` turn 中处理
同一 Change，这不是 Count 必须采用的实现策略。

## `operation::sink::Discard`

[`operation::sink::DiscardDefinition`] 显式声明为携带一个输入的 [`OperationKind::Sink`]，
不声明 Operation data，也没有 output。物化后的 [`operation::sink::DiscardOperation`] 接受端口
零上的完整 Change，并返回 `Action::Complete(None)`。输入完成仍由 Station 在同一事务中
持久化 cursor；失败或回滚不会丢失输入。Discard 只提供一个无外部副作用的显式 Flow 终点，外部
Sink 的幂等提交协议仍需单独设计。

```rust,no_run
use dogpaddle_operation::operation::{
    Action, Operation,
    source::{SequenceSourceDefinition, SequenceSourceOperation},
};
use dogpaddle_store::{Cell, Store};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::create(root.path().join("store"))?;
    let position = store.create_data::<Cell<u64>>("position")?;
    let operation = SequenceSourceOperation::new(SequenceSourceDefinition::new(42), position);
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin()?;
    let action = operation.turn(None, transaction.access())?;
    assert!(matches!(
        action,
        Action::Commit(Some(_))
    ));
    transaction.commit()?;
    Ok(())
}
```

Operation 业务逻辑不接收、开始、提交或保存 Transaction。Flow 长期持有事务启动能力；
Station 的输入准备先用只读事务选择上游日志 entry，必要时再用独立短写事务 durable-pin active
input；它不推进 cursor，也不调用 Operation。进入 process 阶段后，Station 开始并持有读写
Transaction，只把不能提交的
`TransactionAccess` 交给 Operation。Operation 可以用自己持有的 `Cell` 或
`OrderedMap` 直接读写持久化状态。输入中的 `port` 只表达 Definition 中的端口位置，不是 Change
identity。Station 在同一写事务中协调 Operation 状态、output 和输入进展：`Commit` 提交前两者但不
推进 offset，`Complete` 才同时推进 offset，`Idle` 或错误则回滚。只要输入尚未 `Complete`，后续
turn 必须重放相同的完整 Change；Operation 把片段内 continuation 保存在自己声明的状态中。因此
Flow 不需要知道算子的业务数据结构，Operation 也不能控制事务边界。

## 扩展约束

新增内建 Operation 时，在 `operation/source`、`operation/transform` 或 `operation/sink`
模块中加入 Definition 和运行实例，实现 sealed `OperationDefinition` 和运行态
`Operation`，手动声明包含精确输入数量的 [`OperationKind`]，并声明唯一稳定 tag、逻辑资源名、
类型化 collection class、payload codec 与物化逻辑；公共 decoder 表只增加一条
`tag → decode function` 记录。运行实例直接保存所需 collection，不再为每个算子增加只包裹
字段的 `OperationData` 类型。Flow 的 build/open 不应出现具体算子分支。

分类模块只负责容纳多个具体算子并重导出它们的公共类型，不拥有或重导出分类级的单一 tag
或 decoder。tag 与 decoder 始终属于具体算子模块，decoder 表按具体模块路径注册，因此同一
分类内增加任意数量的算子都不会产生注册名称冲突。

一个 Operation 的 tag、payload、显式 kind 及有序 port 语义、
逻辑数据名称、类型化 collection、codec 和适用时的 `SIZE` 共同决定持久化 schema。
Flow 根据声明创建实例，materialize 再按逻辑名
取出；实例集合拒绝重复、缺失、错误 class 或未消费的资源。当前仍是开发期 v1，允许在不保留
旧格式兼容层的前提下破坏性调整已有 tag 对应的 schema，但必须同步更新 decoder、黄金字节、
资源布局和 reopen 测试。格式稳定后，这类变化才需要新 tag、新版本或明确迁移。编码 tag 与
decoder 表必须复用具体模块中的同一个 tag 常量。

声明使用普通静态 Rust 值表达，不引入 Slot、Assembler、Factory registry 或位置 ABI。只有在
出现稳定且机械的声明样板后，才考虑用很薄的 `macro_rules!` 生成声明常量；宏不得生成算子
主体、materialize、codec 或运行逻辑。

## 测试与性能

私有 decoder registry 和类型擦除不变量由源码白盒测试拥有；全部公开行为合并在单一
`correctness` target，按 codec、Definition、Source、Transform 与 Sink 分区。Definition v1 使用版本化
黄金字节约束，具体 Operation 覆盖完整 turn、commit、rollback、reopen、极值错误不改状态、固定
output Schema/diff 和 Store 错误透明传播。完整目录所有权、测试矩阵和 fixture 规则见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/operation/TESTING.md)。

`operation_core` 是本 crate 唯一的 release benchmark，分别测 Definition encode/decode、活动事务
内的一行 `turn` body，以及包含 begin、turn 和 durable commit 的完整事务。固定大小 Cell 的长稳
归 Store 所有，因此当前不设置 Operation endurance。
benchmark 使用工作区的 `dogpaddle-bench-protocol` 严格解析配置、采集主机指纹、计算持续时间
统计并输出 typed JSONL。Operation 本地 support 仍拥有 workload 字段、计时/oracle 和
`BenchRoot`/`SampleStore`；`SampleStore` 在所属场景或 durable 样本校验后立即释放，
不积累到 run root 最终 drop。
smoke 默认使用临时目录，正式回归必须选择 `reference` profile 并显式指定固定文件系统目录，具体
环境变量和 typed JSONL 输出协议见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/crates/operation/TESTING.md)；
全工作区统一规则以根目录
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md) 为准。

## 验证命令

```bash
cargo test -p dogpaddle-operation
cargo test -p dogpaddle-operation --test correctness
cargo clippy -p dogpaddle-operation --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-operation --no-deps
cargo bench -p dogpaddle-operation --bench operation_core

# PR benchmark protocol smoke
cargo xtask bench-smoke
```
