# dogpaddle-store

`dogpaddle-store` 是 DogPaddle 内部的事务状态层。它基于 MDBX 提供命名键值空间、
原子事务、编解码器和类型化集合，为 Flow 的运行进度与 Operation 状态提供统一的持久化
边界。该 crate 是引擎实现模块，不是最终面向用户的存储产品。

## 能力分层

`dogpaddle-store` 将初始化配置与运行时访问分离：

- `Store` 创建和打开命名数据；进入运行期前，其所有权会被消费；
- `Transactions` 是运行期内启动事务的唯一能力；
- `DataHandle` 表示一个命名的、经过编码的键值命名空间；
- `Transaction` 持有一个原子提交边界，并在被丢弃时回滚；
- 与事务绑定的访问值负责执行实际的数据操作。

类型化数据结构位于 `collections/`，并组合通用句柄。它们自行管理编解码器和语义；
事务本身并不知道 `Cell` 或 `OrderedMap`。

## 数据放置

持久化目录将名称绑定到经过编码的键值命名空间及其物理放置方式。它不记录或验证集合
类型与编解码器。重新打开数据句柄时，必须使用与写入内容时相同的集合和编解码器。

`DataPlacement::Shared` 适合数量较多、规模较小或频繁提交的命名空间，它们共享主
B+Tree。`DataPlacement::Dedicated` 为热点或面向批处理的命名空间分配独立的 MDBX
命名表。放置方式在初始化时选定一次并持久化到目录中，对集合 API 不可见。单个 Store
最多支持 `Store::DEDICATED_CAPACITY` 个独立命名空间；具体工作负载应通过基准测试选择。

## 完整示例

```rust,no_run
use dogpaddle_store::DataPlacement::{Dedicated, Shared};
use dogpaddle_store::{Cell, OrderedMap, ScanDirection, ScanLimit, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::create("./dogpaddle-store-data")?;
    let counter_data = store.create_data("counter", Shared)?;
    let users_data = store.create_data("users", Dedicated)?;
    let counter = Cell::<u64>::new(counter_data);
    let users = OrderedMap::<u64, String>::new(users_data);

    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin()?;
        let mut counter = counter.access(&transaction)?;
        let mut users = users.access(&transaction)?;

        counter.set(&1)?;
        users.put(&42, &"Shiba".to_owned())?;
        transaction.commit()?;
    }

    let transaction = transactions.begin()?;
    let counter = counter.access(&transaction)?;
    let users = users.access(&transaction)?;
    assert_eq!(counter.get()?, Some(1));
    assert_eq!(users.get(&42)?.as_deref(), Some("Shiba"));

    let batch = users.scan(
        ..,
        ScanDirection::Descending,
        None,
        ScanLimit::new(100, 1024 * 1024)?,
    )?;
    assert_eq!(batch.items, vec![(42, "Shiba".to_owned())]);
    Ok(())
}
```

## 事务与扫描语义

丢弃事务会触发回滚。`Transaction` 没有显式中止方法，也不包含集合专用操作。通过所有
访问值进行的读写共享同一事务快照，因此任意数量的数据对象都可以原子提交。MDBX 适配器
和物理布局均为私有实现。访问值只在一次尝试中有效：每个新事务都必须重新绑定，不能跨
Stage 步骤缓存。

`DataHandle::access` 提供点读取、写入、删除和双向有界有序扫描。扫描返回拥有所有权的
数据项和一个可选的排他续传键；下一批扫描应复用相同的范围与方向，并传入该续传键。
内置集合通过同一个事务中毒边界执行编解码。自定义集合应使用
`DataAccess::poison_on_error` 处理严重的编解码或不变量错误；原始存储错误会被自动分类。

## 测试

架构行为与集合行为使用独立的集成测试目标：

```bash
cargo test -p dogpaddle-store --test architecture
cargo test -p dogpaddle-store --test collections
```

也可以直接过滤并运行单个测试区域：

```bash
cargo test -p dogpaddle-store --test architecture transaction::
cargo test -p dogpaddle-store --test collections scan::
```

## 性能

使用以下命令运行 release 模式的存储基准测试：

```bash
cargo bench -p dogpaddle-store --bench store
```

基准测试会为 `Shared` 和 `Dedicated` 生成成对样本，覆盖原始与类型化批量写入、点读取、
升序与降序扫描、持久化覆盖写入、类似 Stage 的多集合事务，以及存在多个共享后台命名
空间时的预热读取。结果包含最小值、中位数和最大值；它们描述当前机器与临时文件系统上的
表现，不代表冷缓存或断电场景。

可通过 `DOGPADDLE_BENCH_ENTRIES`、`DOGPADDLE_BENCH_COMMITS`、
`DOGPADDLE_BENCH_SAMPLES` 和 `DOGPADDLE_BENCH_BACKGROUND_NAMESPACES` 调整工作负载
规模；`DOGPADDLE_BENCH_SCAN_ITEMS` 控制每批扫描允许返回的条目数量。
