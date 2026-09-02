# dogpaddle-operation

`dogpaddle-operation` 提供具体、强类型的 Operation Definition、持久化 Data 和运行实例。
Definition 是无副作用、可持久化的数据；它声明所需数据对象的稳定逻辑名、collection 类型
及该 collection 真正需要的布局参数，也把有序、精确的输入 logical Arrow Schema 纯绑定为一个
一次性的编译结果。Flow 在 `build/open` 阶段先绑定完整拓扑，再创建或打开类型化对象，并消费
binding 装配运行实例。方向严格单向：`Definition → OperationBinding → runtime Operation`；运行实例
只保存执行参数和具体持久化 collection，不保留 Definition 或 binding。具体算子不接触 `Store`、
`DataHandle` 或物理放置策略。

## 数据边界

共享的 Arrow Schema、批量差分模型和“每个 Change 一个自描述 IPC Stream”的编码属于独立的
`dogpaddle-change` crate。Operation 的运行接口以内存中的 `Change` 为输入输出；Operation 只
负责数据变换和自己声明的持久化状态，不读写 IPC、不读取边日志，也不决定物理
batch 的合并与 flush。Change 的行位置是事件顺序；Operation 必须依次观察输入事件，并按
其声明的语义产生有序输出，不能把未 consolidation 的输入当作可交换集合。除非将来接收到
独立定义的窗口、barrier 或 flush 信号，Operation 的展平输出事件序列和最终业务状态必须在稳定
合并或切分 Change 后保持不变，也不能因同一个 Change 被分成多少个 `Commit` turn 而改变；物理
Change 边界和 turn 边界都不能被算子当成业务事件。这个比较域要求每种分批的输入和对应输出都能
由其声明的 Arrow 类型物理表示；例如不能要求 `Utf8` offset 已溢出的单个 `RecordBatch` 成功构造。

## Schema 绑定

这里的 Schema 是一个端口承载记录的完整、精确 logical Arrow Schema，不包含物理 IPC 中固定的
`$dogpaddle.diff` 字段。字段名称、顺序、类型、nullability、嵌套结构以及 Schema/Field metadata
都属于匹配内容；它不是“需要哪些列”的局部约束，也不是每个 Change 可以变化的动态类型。

[`OperationDefinition`] 的统一 `bind` 入口接收按端口顺序排列的 `SchemaRef`：Source 收到空 slice，Transform
和 Sink 收到恰好由 [`OperationKind`] 声明的数量。绑定先验证每个输入都是合法 `DogPaddle` logical
Schema，再由具体 Definition 接受或拒绝，并为 Source/Transform 返回唯一、完整的 output Schema；
Sink 必须没有 output。一个 Definition 可以在不同 Flow 中绑定不同输入，但同一次 Flow build/open
完成后，每条 output 只对应一个精确 Schema。

绑定必须是纯且确定的：相同持久化 tag、payload 和有序 input Schemas 必须得到相同语义。结果是
短生命周期、只能消费一次的 `OperationBinding`，可携带 Schema 相关的已编译执行信息及最终
materialize closure；它不写 Store，也不进入持久化格式或运行态对象。目前 `SequenceSource` 固定
输出 `{ value: UInt64 non-null }`；Count 接受任意合法的单一输入并固定输出
`{ count: UInt64 non-null }`；Project 按稳定顶层字段索引绑定输入，拒绝越界、重复或重排，
并以选中字段的完整 Schema 作为 output；Filter 用绑定后的 Boolean 表达式保持 input Schema；
Extend 由绑定表达式唯一推导一个新增字段的类型和 nullability；Discard 接受任意合法的单一输入且没有 output。无需额外的
`Any/Exact` 约束 DSL、Schema registry 或 fingerprint。

Filter 与 Extend 的公共入口直接接收 `DataFusion` [`Expr`]；`dogpaddle_operation` 在 crate 根级重导出
[`Expr`]、[`col`]、[`ident`]、[`lit`]、[`cast`]、[`try_cast`] 和 [`ScalarValue`]，调用方不再学习另一套表达式
builder。需要按 Arrow 字段名逐字引用时使用 [`ident`]；[`col`] 保留 `DataFusion` 自身的大小写正规化和
multipart identifier 解析规则。Definition 的 `try_new` 立即使用 `datafusion-proto` 编码 `Expr`，无法编码时返回构造错误；
公开 getter 从同一表达式定义返回 `Expr`，不引入 `DogPaddle` 自有 AST。

Definition payload 直接保存 `DataFusion` Expr protobuf，不保存 `PhysicalExpr`。Schema bind 将完整 input
Schema 交给 `DataFusion` `create_physical_expr`；表达式的字段解析、type、nullability、cast 与
运行期 `evaluate` 全部由 `DataFusion` 定义。该 API 假定 logical coercion 已完成，而本 crate 不运行
logical/SQL planner，因此不会额外插入隐式 cast；混合类型表达式需要调用方显式 [`cast`]。binding 只保存 exact input Schema、physical expression 和
派生 output 属性；open 从 protobuf 还原 `Expr` 后重新完成同一过程。DogPaddle 继续负责完整 Schema
guard、Filter/Extend 的 output Schema 约束，以及 records/diffs 的 Change 语义。

这份 protobuf 是版本绑定的持久格式，不承诺跨 `DataFusion` 版本兼容。工作区精确 pin 相互匹配的
`DataFusion`、`datafusion-proto` 与 Arrow；升级必须审查 proto roundtrip、physical planning 和执行语义。
若新版本不能兼容旧 payload，必须 bump 外层 Operation Definition tag/version 并重建 Flow，不在同一
版本内猜测或迁移旧表达式。DataFusion 的采用不等于引入 SQL 层。

```rust
use arrow_schema::DataType;
use dogpaddle_operation::{ScalarValue, cast, col, ident, lit, try_cast};
use dogpaddle_operation::operation::transform::{ExtendDefinition, FilterDefinition};

let is_seven = col("value").eq(lit(7_u64));
let extend = ExtendDefinition::try_new("is_seven", is_seven.clone()).unwrap();
let filter = FilterDefinition::try_new(is_seven).unwrap();
assert_eq!(extend.field_name(), "is_seven");
assert_eq!(filter.predicate(), extend.expression());

let typed_null = lit(ScalarValue::Utf8(None));
let strict_text = cast(col("value"), DataType::Utf8);
let nullable_text = try_cast(col("value"), DataType::Utf8);
let exact_arrow_name = ident("Case.Sensitive");
assert!(ExtendDefinition::try_new("missing", typed_null).is_ok());
assert!(ExtendDefinition::try_new("strict_text", strict_text).is_ok());
assert!(ExtendDefinition::try_new("nullable_text", nullable_text).is_ok());
assert!(ExtendDefinition::try_new("copy", exact_arrow_name).is_ok());
```

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
具体算子错误类型；具体错误可以透明包装 `DataFusion` expression、Store、Arrow 或 Change source，调用方可以
downcast 顶层算子错误并沿标准 error source chain 检查基础原因。
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
的终点算子。当前 `source` 包含 `SequenceSource`，`transform` 包含 Count、Project、Filter 和 Extend，`sink` 包含
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
此前 Schema bind 产生的 `OperationBinding::materialize`。binding 只按名称取得已经创建或打开的
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
过程中进行私有类型擦除；具名声明在 binding 的 `materialize` 中将其安全恢复为精确 data class。
类型不匹配会返回错误而不是 panic。类型擦除不会进入运行实例、事务访问路径或持久化格式。

物化后的具体实例统一实现 [`operation::Operation`] trait。Flow 将异构实例保存为
`Box<dyn operation::Operation>`，并通过同一个 `turn` 分派运行。Schema binding 只在 build/open
期间连接 Definition 与实例；运行 trait 和具体运行类型都不反向保存或暴露 Definition，Flow
已经从持久 Definition 获得 kind、资源声明和端口 Schema。
Operation 本身可以在外部实现，但 Flow 只从 sealed Definition 物化运行实例；开放可注入 Flow 的
第三方算子仍需另行设计 tag 分配、decoder 注册和运行错误边界。

## `operation::source::SequenceSource`

[`operation::source::SequenceSourceDefinition`] 是零输入源，记录首个 `u64` 值。物化后的
[`operation::source::SequenceSourceOperation`] 只持有复制出的 `start: u64` 和直接的
`Cell<u64>` position；首次产生 `start`，随后根据最后一次已提交的值逐一递增。每个 turn 产生一行，输出固定为一个
non-null `UInt64` `value` 字段，所有 diff 都是 `+1`。包含 `u64::MAX` 的最后一批可以成功提交，
后续 turn 返回 `Idle`，不再写 position 或产生 output，使 Flow 仍能调度下游并排空已经提交的
Change。每次产生值的 turn 返回 `Action::Commit(Some(_))`；Station 不为
Source 建立另一套 outcome 或事务路径。它声明自己产生输出。

Schema bind 不接收输入，并固定完整 output Schema 为一个 non-null `UInt64` `value` 字段。

它声明一个逻辑数据名 `sequence_source.position`，由 Flow 解析为稳定 Station 资源名。

## `operation::transform::Count`

[`operation::transform::CountDefinition`] 要求一个输入。每成功推进一次，
[`operation::transform::CountOperation`] 按输入行序计算事件数量：每一行恰好令直接持有的
唯一字段 `Cell<u64>` 加一，输入 diff 的符号和数值不改变“一个有序事件”的计数。每个已处理输入行输出
一个 non-null `UInt64` `count`，diff 固定为 `+1`；因此它是插入式的运行计数事件流，不是维护
单例关系的 cardinality aggregate。未写入的 count Cell 解释为 `0`，溢出返回
[`operation::transform::CountError::Overflow`]。Count 显式声明为携带一个输入的
[`OperationKind::Transform`]；拓扑位置不会把它隐式变成 Sink，因此完整 Flow 必须把它连接到
一个下游 Sink。

Count 只声明 `count: Cell<u64>`。当前实现每个 turn 一次处理完整 Change，并返回
`Action::Complete(Some(_))`；它在写状态前预检整批行数，若最终值无法用 `u64` 表示，则返回 overflow，
整个 turn 不产生部分进展。协议允许其他 Operation 用声明的持久化状态在多个 `Commit` turn 中处理
同一 Change，这不是 Count 必须采用的实现策略。

Schema bind 接受任意合法的精确单一输入，并固定完整 output Schema 为一个 non-null `UInt64`
`count` 字段。

## `operation::transform::Project`

[`operation::transform::ProjectDefinition`] 要求一个输入，并用稳定的 zero-based `u32` 顶层字段
索引描述投影。Schema bind 把这些索引编译为绑定精确 input Schema 的 `ChangeProjection`；索引必须
严格递增且都存在，空投影合法。output Schema 完整保留所选字段及 Schema/Field metadata，嵌套字段
只按完整子树选择，隐式 diff 始终保留。越界、重复或重排在 Flow 创建任何 Store 资源前以结构化
[`operation::transform::ProjectSchemaError`] 拒绝。

[`operation::transform::ProjectOperation`] 不声明 Store data，只保留 binding 编译出的
`ChangeProjection`。每个 turn 对完整 Change 做保持行序和 diff 的顶层投影，并返回
`Action::Complete(Some(_))`；所选 Arrow Array buffer 与输入共享，不复制列数据。Definition 的字段
索引数量和每个索引使用稳定 big-endian `u32` 编码，tag/payload 与 input Schema 一起决定 reopen 后
重建的精确 output Schema。

## `operation::transform::Filter`

[`operation::transform::FilterDefinition`] 通过 fallible `try_new` 接收 `DataFusion` [`Expr`]，要求一个输入，
并持久化 `DataFusion` Expr protobuf。Schema bind 通过 `DataFusion` 把表达式编译到精确 input Schema；
最终类型不是 Boolean 或 `DataFusion` 无法规划
都会在 Flow 创建 Store 前以结构化 [`operation::transform::FilterSchemaError`] 拒绝。output Schema
与 input 完全相同。

[`operation::transform::FilterOperation`] 不声明 Store data。每个 turn 只保留 predicate 为 non-null
`true` 的行；`false` 和 null 都删除，同一个 Arrow filter predicate 同时筛选 records 与 diff，因而
相对事件顺序和每个保留事件的 diff 不变。没有行被选中时返回 `Action::Complete(None)`，因为空
Change 不可表示；全部选中时直接 clone Change 并共享全部 buffer。部分筛选前只把可能的第三方 Arrow
Array wrapper 通过 `to_data/make_array` 规范为标准 Array class（底层 buffer 仍共享），避免 Arrow
kernel 对自定义 concrete type panic，随后才进行一次向量筛选。

## `operation::transform::Extend`

[`operation::transform::ExtendDefinition`] 通过 fallible `try_new` 接收 `field_name + Expr`，要求一个输入，
并只持久化 field name 与 `DataFusion` Expr protobuf。
Schema bind 从表达式唯一推导新增字段的 `DataType` 和 nullability；调用者不重复声明 Field/type，避免两套
真相。output 依次保留所有 input `FieldRef` 与 Schema metadata，再追加一个 metadata 为空的新 Field；
重复名称、`$dogpaddle.` 保留名称和非法派生 Schema 仍由统一 output Schema 校验拒绝。一次只追加一列，
多列通过串联多个 Extend 明确表达，不引入同一算子内部的列依赖顺序。

[`operation::transform::ExtendOperation`] 不声明 Store data，只保存 exact-Schema-bound private plan 和
最终 output Schema。每个 turn 共享全部 input `ArrayRef` 与 diff buffer，只为真正计算出的列分配数据；
若表达式只是 Column，新列本身也与源列共享同一 ArrayRef。结果保持行序并返回
`Action::Complete(Some(_))`。

## `operation::sink::Discard`

[`operation::sink::DiscardDefinition`] 显式声明为携带一个输入的 [`OperationKind::Sink`]，
不声明 Operation data，也没有 output。物化后的 [`operation::sink::DiscardOperation`] 是零状态
unit struct；它接受端口零上的完整 Change，并返回 `Action::Complete(None)`。输入完成仍由 Station 在同一事务中
持久化 cursor；失败或回滚不会丢失输入。Discard 只提供一个无外部副作用的显式 Flow 终点，外部
Sink 的幂等提交协议仍需单独设计。

Schema bind 接受任意合法的精确单一输入，并返回无 output 的 binding。

```rust,no_run
use dogpaddle_operation::operation::{
    Action, Operation,
    source::SequenceSourceOperation,
};
use dogpaddle_store::{Cell, Store};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::create(root.path().join("store"))?;
    let position = store.create_data::<Cell<u64>>("position")?;
    let operation = SequenceSourceOperation::new(42, position);
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
类型化 collection class、payload codec、纯 Schema bind 与一次性物化逻辑；公共 decoder 表只增加一条
`tag → decode function` 记录。运行实例只直接保存执行所需的标量参数与 collection，不能保存
Definition 或提供回到 Definition 的 getter，也不再为每个算子增加只包裹字段的 `OperationData`
类型。Flow 的 build/open 不应出现具体算子分支。

分类模块只负责容纳多个具体算子并重导出它们的公共类型，不拥有或重导出分类级的单一 tag
或 decoder。tag 与 decoder 始终属于具体算子模块，decoder 表按具体模块路径注册，因此同一
分类内增加任意数量的算子都不会产生注册名称冲突。

一个 Operation 的 tag、payload、显式 kind、有序 port 语义以及“有序 input Schemas → binding”规则，
逻辑数据名称、类型化 collection、codec 和适用时的 `SIZE` 共同决定持久化 schema。
derived input/output Schemas 不单独持久化；因此改变同一 tag/payload 对同一输入的绑定结果，或改变
Schema 相关状态的 codec，仍是持久化 ABI 变化。Flow 根据声明创建实例，binding materialize 再按逻辑名
取出；实例集合拒绝重复、缺失、错误 class 或未消费的资源。当前仍是开发期 v1，允许在不保留
旧格式兼容层的前提下破坏性调整已有 tag 对应的 schema，但必须同步更新 decoder、黄金字节、
资源布局和 reopen 测试；DataFusion Expr protobuf 的不兼容升级仍必须按上文 bump 外层 tag/version。
格式稳定后，其他这类变化也需要新 tag、新版本或明确迁移。编码 tag 与
decoder 表必须复用具体模块中的同一个 tag 常量。

声明使用普通静态 Rust 值表达，不引入 Slot、Assembler、Factory registry 或位置 ABI。只有在
出现稳定且机械的声明样板后，才考虑用很薄的 `macro_rules!` 生成声明常量；宏不得生成算子
主体、Schema bind、materialize、codec 或运行逻辑。

## 测试与性能

私有 decoder registry 和类型擦除不变量由源码白盒测试拥有；全部公开行为合并在单一
`correctness` target，按 codec、definition 与 runtime 分区。Definition v1 使用版本化黄金字节约束，
Schema 测试覆盖六个 built-in 的精确传播、decoded golden 再绑定、错误 arity、非法 logical Schema，
以及 Project、Filter、Extend 对合法但不兼容 Schema 的结构化拒绝；表达式测试覆盖 `DataFusion` protobuf
编码失败、roundtrip 与精确版本 golden。runtime trace 统一覆盖完整 turn、commit、rollback、
reopen、固定 output Schema/diff、Project/Extend 零拷贝、Filter 的空/全量选择及覆盖
Null/bitmap/fixed/variable/List/Struct 全部 v1 layout family 的部分选择、DataFusion
`create_physical_expr` 的 type/nullability、scalar/array evaluate 与 null 传播、
携带混合 diff 的稳定重批和 Store 错误。Expression golden
会经过 `decode → bind → materialize → turn` 检查 protobuf 到执行语义，而不仅是重编码。完整目录所有权、
测试矩阵和 fixture 规则见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。

`operation_core` 是本 crate 唯一的 release benchmark，分别测 Definition encode/decode、活动事务
内的一行 `turn` body，以及包含 begin、turn 和 durable commit 的完整事务。固定大小 Cell 的长稳
归 Store 所有，因此当前不设置 Operation endurance。
benchmark 使用工作区的 `dogpaddle-bench-protocol` 严格解析配置、采集主机指纹，在 run plan 中声明
稳定 series，并让 raw sample 只引用紧凑 case ID；统一 artifact 派生 `operations/s` 等人类统计，machine
consumer 可由 raw `elapsed_ns / operations` 无损派生 per-operation 耗时，不重复保存。Operation 本地 support 仍拥有 workload 字段、计时/oracle 和
`SampleStore`；`SampleStore` 在所属场景或 durable 样本校验后立即释放，
不积累到 run root 最终 drop。
相邻 `operation_core.plan.json` 由 `cargo xtask bench-plan-check` 在不创建 Store fixture 的情况下
验证 smoke/reference 两档冻结 Plan。
smoke 默认使用临时目录，正式回归必须选择 `reference` profile 并显式指定固定文件系统目录；
环境变量和 typed JSONL 输出协议以根目录
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
