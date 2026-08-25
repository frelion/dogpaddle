# dogpaddle-operation

`dogpaddle-operation` 提供具体、强类型的 Operation Definition、持久化 Data 和运行实例。
Definition 是无副作用、可形式化的数据；实例由 Definition 与 Flow 在 `build/open` 阶段
注入的 `DataHandle` 构成。

运行实例及具体算子统一组织在 `operation` 模块中，其下按语义分为三个公共模块：`source`
保存无上游输入的源算子，`transform` 保存消费并产生记录的转换算子，`sink` 保存只消费记录
的终点算子。当前 `source` 包含 `SequenceSource`，`transform` 包含 Count，`sink` 尚无具体
实现。目录分类目前不引入额外 marker trait 或运行时类型系统。

## Definition 与持久化

具体 Definition 统一实现 sealed [`OperationDefinition`] trait。trait 提供精确输入数量，并
向 Flow 声明稳定逻辑数据名；Flow 负责生成完整资源名、创建或打开 Store 数据空间，再把得到
的句柄按逻辑名放入具名绑定集合。具体算子按名称取出资源，同时提供 `Cell<T>` 或
`OrderedMap<K, V>` 等明确的类型化 collection 构造函数；声明顺序不参与绑定。Definition
不接收 Store，不决定 `DataPlacement`，也不开始或提交 Transaction。

```rust
use dogpaddle_operation::{
    OperationDefinition, decode_definition, encode_definition,
    operation::source::SequenceSourceDefinition,
};

let source = SequenceSourceDefinition::new(10);
assert_eq!(source.input_count(), 0);

let encoded = encode_definition(&source);
let decoded = decode_definition(&encoded).unwrap();
assert_eq!(encode_definition(decoded.as_ref()), encoded);
```

Definition 集合在本 crate 内保持封闭，但不再使用公共 enum。稳定 decoder 由一张私有
`tag → decode function` 静态表选择；每个具体算子模块拥有自己的 tag、payload codec、
数据声明和物化逻辑。Flow 不枚举具体算子，也不解析 Operation payload。

物化后的具体实例统一实现 sealed [`operation::Operation`] trait。Flow 将异构实例保存为
`Box<dyn operation::Operation>`，并通过 [`operation::Operation::definition`] 取得实例实际
持有的 Definition。两个 trait 都不是外部 crate 的扩展点；开放第三方算子需要另行设计 tag
分配与 decoder 注册。

## `operation::source::SequenceSource`

[`operation::source::SequenceSourceDefinition`] 是零输入源，记录首个 `u64` 值。物化后的
[`operation::source::SequenceSourceOperation`] 使用 [`operation::source::SequenceSourceData`]
中的 `Cell<u64>` 保存最后一次已提交的值；首次产生 `start`，随后逐一递增。`u64::MAX` 可以
产生一次，再次推进返回 [`operation::source::SequenceSourceError::Exhausted`]。

它声明一个逻辑数据名 `sequence_source.position`，由 Flow 解析为稳定 Stage 资源名。

## `operation::transform::Count`

[`operation::transform::CountDefinition`] 要求一个输入。每成功应用一条记录，
[`operation::transform::CountOperation`] 将 [`operation::transform::CountData`] 中的
`Cell<u64>` 加一并返回新计数；未写入的 Cell 解释为 `0`，溢出返回
[`operation::transform::CountError::Overflow`]。

```rust,no_run
use dogpaddle_operation::operation::transform::{CountData, CountDefinition, CountOperation};
use dogpaddle_store::{Cell, DataPlacement, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::create(root.path().join("store"))?;
    let count = Cell::new(store.create_data("count", DataPlacement::Shared)?);
    let operation = CountOperation::new(CountDefinition::new(), CountData::new(count));
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin()?;
    let mut count = operation.data().count().access(&transaction)?;
    assert_eq!(operation.apply(&mut count)?, 1);
    transaction.commit()?;
    Ok(())
}
```

Operation 业务逻辑不接收、开始、提交或保存 Transaction。未来 Flow 内部的 Stage 负责事务
边界，从 Operation 持有的 Cell 取得具体 `CellAccess`，再注入 `apply()`。这使状态、输入
进度和输出以后能够由 Stage 原子提交。

## 扩展约束

新增内建 Operation 时，在 `operation/source`、`operation/transform` 或 `operation/sink`
模块中加入 Definition、Data 和实例，实现两个 sealed trait，并声明唯一稳定 tag、逻辑资源
名、payload codec 与物化逻辑；公共 decoder 表只增加一条 `tag → decode function` 记录。
Flow 的 build/open 不应出现具体算子分支。

分类模块只负责容纳多个具体算子并重导出它们的公共类型，不拥有或重导出分类级的单一 tag
或 decoder。tag 与 decoder 始终属于具体算子模块，decoder 表按具体模块路径注册，因此同一
分类内增加任意数量的算子都不会产生注册名称冲突。

一个 Operation 的 tag、payload、逻辑数据名称、类型化 collection、值 codec 和构建 placement
共同构成持久化 schema。materialize 按逻辑名取出绑定，并在同一调用点提供该资源的类型化
collection 构造函数；绑定集合拒绝重复、缺失或未消费的资源。修改已有 schema 必须分配新
tag、提升格式版本或提供迁移；不能让同一 tag 在不同版本中要求不同资源。编码 tag 与 decoder
表必须复用具体模块中的同一个 tag 常量，并通过 tag 唯一性、黄金字节、资源布局和 reopen
测试约束。

当前所有 Operation 自有资源由 Flow 以 `DataPlacement::Shared` 创建。真正出现 Dedicated
需求后再扩展资源声明，不提前增加 placement descriptor。

## 验证

```bash
cargo test -p dogpaddle-operation
cargo clippy -p dogpaddle-operation --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-operation --no-deps
```
