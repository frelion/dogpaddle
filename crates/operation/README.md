# dogpaddle-operation

`dogpaddle-operation` 提供具体、强类型的 Operation Definition、持久化 Data 和运行实例。
Definition 是无副作用、可形式化的数据；实例由 Definition 与 Flow 在 `build/open` 阶段
注入的 Store-bound Data 构成。

不同 Definition 通过闭合的 [`OperationDefinition`] 联合进入 Flow，不使用 `kind: String`、
不透明配置字节或运行时 Registry。联合提供精确输入数量，并以显式版本和稳定 tag 编解码；
用户只构造具体 Definition：

```rust
use dogpaddle_operation::{OperationDefinition, SequenceSourceDefinition};

let source = OperationDefinition::from(SequenceSourceDefinition::new(10));
assert_eq!(source.input_count(), 0);
assert_eq!(OperationDefinition::decode(&source.encode()).unwrap(), source);
```

## `SequenceSource`

[`SequenceSourceDefinition`] 是零输入源，记录首个 `u64` 值。物化后的
[`SequenceSourceOperation`] 使用 [`SequenceSourceData`] 中的 `Cell<u64>` 保存最后一次已
提交的值；首次产生 `start`，随后逐一递增。`u64::MAX` 可以产生一次，再次推进返回
[`SequenceSourceError::Exhausted`]。

## Count

[`CountDefinition`] 要求一个输入。每成功应用一条记录，[`CountOperation`] 将
[`CountData`] 中的 `Cell<u64>` 加一并返回新计数；未写入的 Cell 解释为 `0`，溢出返回
[`CountError::Overflow`]。

```rust,no_run
use dogpaddle_operation::{CountData, CountDefinition, CountOperation};
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
边界，从 Operation 持有的 Cell 取得具体 `CellAccess`，再注入 `apply()`。这使 Operation
保持可测试，同时让状态、输入进度和输出以后能够由 Stage 原子提交。

## 扩展约束

新增 Operation 时应新增自己的 Definition、Data 和实例类型，再显式加入
`OperationDefinition`、稳定 tag、输入数量分发以及 Flow 的穷尽物化分支。Data 类型表达该
算子的具体状态形状；Flow 分支负责按这一形状创建或打开全部命名资源并注入构造函数。不要
增加通用 Data bundle、字符串 Registry、factory 或让 Definition 持有 `DataHandle`。

`OperationDefinition` 刻意保持可穷尽，使新增变体时编译器强制 `dogpaddle-flow` 同步补齐
资源布局和物化。两个 crate 因此必须锁步升级；新增变体属于协调的 API 兼容性变更。已发布
的格式版本和 tag 必须继续可读，不能通过简单覆盖 V1 decoder 来升级。修改已有 tag、字段
编码或状态资源布局都需要迁移设计。

## 验证

```bash
cargo test -p dogpaddle-operation
cargo clippy -p dogpaddle-operation --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-operation --no-deps
```
