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
独立定义的窗口、barrier 或 flush 信号，Operation 的可观察结果必须在稳定合并或切分 Change
后保持不变；物理 Change 边界不能被算子当成业务事件。

封闭的 [`operation::Operation`] trait 提供统一、object-safe 的 `turn`。零输入 Source 收到
`None`；Transform 与 Sink 每次只收到一个完整 borrowed Change，以及它在 Definition 有序输入中的
`usize` 端口序号。Operation 不接收 `AppendLog` offset。成功返回表示本次提供的工作已经完整完成，
并至多返回一个 owned Change；filter 或 Sink 可以返回 `None`。具体错误统一擦除为标准 boxed
[`operation::OperationError`]，仍可按原始具体错误类型 downcast。

下文所说的 data class 指一个完整的 Rust 持久化数据类型，包括 collection、值类型，以及该
collection 存在选择时的 `SIZE`，例如 `Cell<u64>` 或 `OrderedMap<u64, String, Large>`。

运行实例及具体算子统一组织在 `operation` 模块中，其下按语义分为三个公共模块：`source`
保存无上游输入的源算子，`transform` 保存消费并产生记录的转换算子，`sink` 保存只消费记录
的终点算子。当前 `source` 包含 `SequenceSource`，`transform` 包含 Count，`sink` 尚无具体
实现。目录分类目前不引入额外 marker trait 或运行时类型系统。

## Definition 与持久化

具体 Definition 统一实现 sealed [`OperationDefinition`] trait。trait 提供精确输入数量和
`produces_output()`，并以 `{ 逻辑名: data class }` 的形式向 Flow 声明完整数据 schema。
`produces_output()` 表示 Operation 是否拥有语义输出端口，不表示某次处理一定产生记录，也不
取决于当前拓扑是否存在下游；Source 与 Transform 可以是 `true`，Sink 是 `false`。Flow 负责生成完整
资源名，并调用声明携带的类型化 create/open 能力；得到的实例按逻辑名组成集合，再交给
Definition 的 `materialize`。具体 Definition 只按名称取得已经创建或打开的
`Cell<T>` 或 `OrderedMap<K, V, SIZE>`，声明顺序不参与绑定，也不接收 Store。

collection 只暴露真实存在的布局选择：`Cell<T>` 永远使用共享空间；`OrderedMap` 的 `Small`
形式共享底层物理空间，`Large` 形式拥有独立物理空间。Size 属于支持选择的具名数据对象的
静态 schema，而不是由 Flow 根据数据量猜测。Flow 只解释声明，不枚举具体算子或 collection
类型。

```rust
use dogpaddle_operation::{
    OperationDefinition, decode_definition, encode_definition,
    operation::source::SequenceSourceDefinition,
};

let source = SequenceSourceDefinition::new(10);
assert_eq!(source.input_count(), 0);
assert!(source.produces_output());

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

物化后的具体实例统一实现 sealed [`operation::Operation`] trait。Flow 将异构实例保存为
`Box<dyn operation::Operation>`，并通过 [`operation::Operation::definition`] 取得实例实际
持有的 Definition，再通过同一个 `turn` 分派运行。两个 trait 都不是外部 crate 的扩展点；开放
第三方算子需要另行设计 tag 分配、decoder 注册和运行错误边界。

## `operation::source::SequenceSource`

[`operation::source::SequenceSourceDefinition`] 是零输入源，记录首个 `u64` 值。物化后的
[`operation::source::SequenceSourceOperation`] 直接持有 `Cell<u64>`，保存最后一次
已提交的值；首次产生 `start`，随后逐一递增。每个 turn 产生一行，输出固定为一个
non-null `UInt64` `value` 字段，所有 diff 都是 `+1`。包含 `u64::MAX` 的最后一批可以成功提交，
下一次 turn 返回 [`operation::source::SequenceSourceError::Exhausted`]。它声明自己产生输出。

它声明一个逻辑数据名 `sequence_source.position`，由 Flow 解析为稳定 Station 资源名。

## `operation::transform::Count`

[`operation::transform::CountDefinition`] 要求一个输入。每成功推进一次，
[`operation::transform::CountOperation`] 按输入行序计算事件数量：每一行恰好令直接持有的
`Cell<u64>` 加一，输入 diff 的符号和数值不改变“一个有序事件”的计数。每个已处理输入行输出
一个 non-null `UInt64` `count`，diff 固定为 `+1`；因此它是插入式的运行计数事件流，不是维护
单例关系的 cardinality aggregate。未写入的 count Cell 解释为 `0`，溢出返回
[`operation::transform::CountError::Overflow`]。即使 Count 当前是终端 Station，它仍声明自己产生
输出；拓扑位置不会把 Transform 隐式变成 Sink。

Count 只声明 `count: Cell<u64>`。每个 turn 一次处理完整 Change；它在写状态前预检整批行数，若
最终值无法用 `u64` 表示，则返回 overflow，整个 turn 不产生部分进展。

```rust,no_run
use dogpaddle_operation::operation::{
    Operation,
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
    let output = operation.turn(None, transaction.access())?;
    assert!(output.is_some());
    transaction.commit()?;
    Ok(())
}
```

Operation 业务逻辑不接收、开始、提交或保存 Transaction。Flow 长期持有事务启动能力；
Station 的 intake 阶段只用只读事务读取上游日志，不推进 cursor，也不调用 Operation。进入
process 阶段后，Station 开始并持有读写 Transaction，只把不能提交的
`TransactionAccess` 交给 Operation。Operation 可以用自己持有的 `Cell` 或
`OrderedMap` 直接读写持久化状态。输入中的 `port` 只表达 Definition 中的端口位置，不是 Change
identity。Station 在同一写事务中协调 Operation 状态、output 和完整 Change 的 input offset；
Operation 成功后才推进 offset，Operation 返回错误时调用方必须回滚。因此 Flow 不需要知道算子的
数据结构，Operation 也不能控制事务边界。

## 扩展约束

新增内建 Operation 时，在 `operation/source`、`operation/transform` 或 `operation/sink`
模块中加入 Definition 和运行实例，实现两个 sealed trait，并声明唯一稳定 tag、逻辑资源名、
类型化 collection class、payload codec 与物化逻辑；公共 decoder 表只增加一条
`tag → decode function` 记录。运行实例直接保存所需 collection，不再为每个算子增加只包裹
字段的 `OperationData` 类型。Flow 的 build/open 不应出现具体算子分支。

分类模块只负责容纳多个具体算子并重导出它们的公共类型，不拥有或重导出分类级的单一 tag
或 decoder。tag 与 decoder 始终属于具体算子模块，decoder 表按具体模块路径注册，因此同一
分类内增加任意数量的算子都不会产生注册名称冲突。

一个 Operation 的 tag、payload、`produces_output()`、逻辑数据名称、类型化 collection、codec
和适用时的 `SIZE` 共同决定持久化资源 schema。Flow 根据声明创建实例，materialize 再按逻辑名
取出；实例集合拒绝重复、缺失、错误 class 或未消费的资源。当前仍是开发期 v1，允许在不保留
旧格式兼容层的前提下破坏性调整已有 tag 对应的 schema，但必须同步更新 decoder、黄金字节、
资源布局和 reopen 测试。格式稳定后，这类变化才需要新 tag、新版本或明确迁移。编码 tag 与
decoder 表必须复用具体模块中的同一个 tag 常量。

声明使用普通静态 Rust 值表达，不引入 Slot、Assembler、Factory registry 或位置 ABI。只有在
出现稳定且机械的声明样板后，才考虑用很薄的 `macro_rules!` 生成声明常量；宏不得生成算子
主体、materialize、codec 或运行逻辑。

## 测试与性能

私有 decoder registry 和类型擦除不变量由源码白盒测试拥有；全部公开行为合并在单一
`correctness` target，按 codec、Definition、Source 与 Transform 分区。Definition v1 使用版本化
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
