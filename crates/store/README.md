# dogpaddle-store

`dogpaddle-store` 是 `DogPaddle` 内部的事务状态层。它基于 MDBX 提供具名的类型化数据对象、
原子事务和稳定编解码器，为 Flow 运行进度与 Operation 状态提供统一的持久化边界。该 crate
是引擎实现模块，不是最终面向用户的通用存储产品。

## 能力分层

Store 将资源装配与运行时访问分离：

- `Store` 创建或打开全部具名数据对象；进入运行期前，其所有权会被消费；
- `Cell<T, SIZE>` 保存一个可选的类型化值；
- `OrderedMap<K, V, SIZE>` 保存按稳定 key codec 排序的类型化映射；
- `Transactions` 是运行期内启动事务的唯一能力；
- `Transaction` 持有一个原子提交边界，并在被丢弃时回滚；
- `TransactionAccess` 从活动 Transaction 临时借用，只允许已有类型化数据对象绑定访问；
- `CellAccess` 与 `OrderedMapAccess` 绑定一次具体事务并执行实际读写。

底层数据句柄、物理放置和 MDBX 访问均为 crate 私有实现。集合不能脱离 `Store` 构造，调用方
也不能绕过类型上的 `Size` 自行组合物理资源。

## Size 是持久化 schema

每个持久化数据对象都必须显式选择 `Small` 或 `Large`，没有默认值：

```rust
use dogpaddle_store::{Cell, Large, OrderedMap, Small};

type Counter = Cell<u64, Small>;
type Cache = OrderedMap<u64, String, Small>;
type Records = OrderedMap<u64, Vec<u8>, Large>;
```

`Size` 描述这个具名对象的静态存储类别，不是运行时容量上限，也不会根据当前数据量自动改变：

- `Small` 与其他小对象共享主 B+Tree，适合数量较多、规模较小或频繁提交的状态；
- `Large` 使用独立的 MDBX named table，适合可能很大、热点或面向批处理的数据；
- 单个 Store 最多包含 `Store::LARGE_DATA_CAPACITY` 个 Large 对象。

`Size` 在创建时写入 catalog，之后属于该逻辑资源的持久化 schema。`Store::open_data::<D>`
会校验请求类型的 `Size`；以 `Large` 打开实际为 `Small` 的资源，或反过来，都会返回
`StoreError::DataSizeMismatch`。

Store catalog 不记录或验证 `Cell`/`OrderedMap`、`K`、`V` 或 codec 类型。同一个 `Size` 下，
调用方必须按照创建者定义的稳定 schema 重新打开数据。不要把 Rust `TypeId`、类型名或内存
布局当作磁盘格式。

## 完整示例

```rust,no_run
use dogpaddle_store::{Cell, Large, OrderedMap, ScanDirection, ScanLimit, Small, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::create("./dogpaddle-store-data")?;
    let counter = store.create_data::<Cell<u64, Small>>("counter")?;
    let users = store.create_data::<OrderedMap<u64, String, Large>>("users")?;

    let mut transactions = store.into_transactions();

    {
        let transaction = transactions.begin()?;
        let access = transaction.access();
        let mut counter = counter.access(access)?;
        let mut users = users.access(access)?;

        counter.set(&1)?;
        users.put(&42, &"Shiba".to_owned())?;
        transaction.commit()?;
    }

    let transaction = transactions.begin()?;
    let access = transaction.access();
    let counter = counter.access(access)?;
    let users = users.access(access)?;
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

重新打开时必须声明同一个完整数据类型：

```rust,no_run
use dogpaddle_store::{Cell, Small, Store};

# fn open() -> Result<(), Box<dyn std::error::Error>> {
let store = Store::open("./dogpaddle-store-data")?;
let counter = store.open_data::<Cell<u64, Small>>("counter")?;
let mut transactions = store.into_transactions();
let transaction = transactions.begin()?;
let access = transaction.access();
assert_eq!(counter.access(access)?.get()?, Some(1));
# Ok(())
# }
```

`StoreData` 是 Store 泛型 create/open 使用的 sealed marker trait。它只由内建集合的
`Small`/`Large` 形式实现；一般业务代码不需要直接引用它。

## 事务与扫描语义

丢弃事务会触发回滚。`Transaction` 没有显式中止方法，也不包含集合专用操作。调用
`Transaction::access()` 会得到可复制但不能提交的 `TransactionAccess`；它只能让调用方已经
持有的 `Cell` 或 `OrderedMap` 创建事务级 Access。Flow/Stage 因此可以保留 Transaction
所有权并把受限能力交给 Operation，Operation 无法开始或结束原子边界，也无法访问 Store
catalog。通过同一能力创建的所有访问值共享事务快照，因此任意数量、任意 `Size` 的数据对象
都可以原子提交。

`TransactionAccess`、`CellAccess` 和 `OrderedMapAccess` 都不能脱离所属事务；每个新事务都
必须重新绑定，不能跨 Stage 步骤缓存。能力及访问值仍然是线程绑定的，不能跨线程移动正在
执行的事务。

内置集合在同一个事务中毒边界内执行编解码。严重的编解码或存储失败会毒化事务，之后的操作
以及提交返回 `TransactionPoisoned`；无法容纳单个扫描项的 `ItemTooLarge` 是可调整 scan
limit 后重试的软错误，不会毒化事务。

`OrderedMapAccess::scan` 支持有界范围、升序或降序、条目数与逻辑编码字节数双重限制，并
返回拥有所有权的结果与可选排他续传 key。下一批扫描应复用相同范围和方向，并传入上一批的
续传 key。

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

架构测试会对 `Small` 与 `Large` 运行相同的数据、事务与扫描语义，并通过 MDBX 白盒适配器
锁定 catalog binding、共享前缀和独立 named table 的物理布局。`Size` 不匹配的 reopen、
崩溃恢复、事务中毒和 codec 失败也有独立覆盖。

## 性能

使用以下命令运行 release 模式的存储基准测试：

```bash
cargo bench -p dogpaddle-store --bench store
```

基准测试为 `Small` 与 `Large` 生成成对样本，覆盖 byte map 与业务类型 map 的批量写入、热点
读取、升序与降序扫描、持久化覆盖写入、类似 Stage 的多集合事务，以及存在多个 `Small` 后台
命名空间时的预热读取。两种 map 使用各自独立的具名数据对象；byte map 的类型是
`OrderedMap<Vec<u8>, Vec<u8>, SIZE>`，不依赖私有裸句柄。结果包含最小值、中位数和最大值；
它们描述当前机器与临时文件系统上的表现，不代表冷缓存或断电场景。

可通过 `DOGPADDLE_BENCH_ENTRIES`、`DOGPADDLE_BENCH_COMMITS`、
`DOGPADDLE_BENCH_SAMPLES` 和 `DOGPADDLE_BENCH_BACKGROUND_NAMESPACES` 调整工作负载
规模；`DOGPADDLE_BENCH_SCAN_ITEMS` 控制每批扫描允许返回的条目数量。
