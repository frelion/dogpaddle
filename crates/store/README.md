# dogpaddle-store

`dogpaddle-store` 是 `DogPaddle` 的事务状态层。它在 `RocksDB` 上提供具名、类型化的数据结构，
让 Flow 和 Operation 只表达自己的持久状态与原子更新，不接触存储引擎、列族、物理 key 或
压缩配置。

这个 crate 的公共边界刻意很小：setup 阶段用 `Store` 声明或重新打开资源，运行阶段只保留
类型化 handle、唯一写事务启动能力和只读 snapshot 启动能力。当前 Flow 仍顺序执行；切换到
`RocksDB` 为更大的单机状态和未来并发留下空间，但这里没有引入并行 writer、后台调度或历史版本
查询 API。

## 生命周期与事务

`Store::create` 只接受尚不存在的目录；`Store::open` 校验已有数据库的 marker 与资源 catalog。
资源只能在 `Store` 阶段通过 `create_data` 或 `open_data` 获得。`StoreData` 是 sealed trait，外部
crate 不能绕过六种内建结构自造物理资源。

进入运行期时，`Store::into_transactions` 消费 setup owner，产生不可克隆的 `Transactions`。
`Transactions::begin(&mut self)` 开启一个写事务，因此同一个 owner 在类型层面一次只能持有一个
活动写事务。`Transaction::commit` 使用 WAL 与同步写入原子提交；直接丢弃 transaction 会回滚。
这条唯一写能力是当前顺序执行模型的明确边界，不是 `RocksDB` 并发能力的上限。

`Transactions::split` 消费 owner，返回原来的唯一写能力和一个不可克隆、但 `Send + Sync` 的
`ReadTransactions`。共享的 `&ReadTransactions` 可以各自调用 `begin()`，在本线程开启独立的稳定
snapshot。snapshot 可以与 writer 同时存活：它持续看到开始时的已提交视图，之后开始的 snapshot
才会看到新的 commit。活动的 `Transaction`、`ReadTransaction` 及其 access 都不能跨线程。

setup 阶段也可以直接调用 `Store::read_transaction()` 借用一个短期只读 snapshot；它结束后仍可
继续打开资源。三个事务启动方法都是 infallible，存储错误在实际访问或提交时返回。

`TransactionAccess` 与 `ReadTransactionAccess` 是临时借用的装配能力。前者允许类型化结构产生
读写 access，后者只能产生只读 access；两者都不能创建资源、开始事务或提交。一个 write
transaction 内通过同一 `TransactionAccess` 修改的任意多个结构共享同一个原子边界。

## 六种数据结构

| 结构 | 适用状态 | 核心语义 |
| --- | --- | --- |
| `Cell<T>` | checkpoint、phase、计数器、小型控制状态 | 一个可缺省值；`get`、`set`、`clear` |
| `OrderedMap<K, V>` | 按 key 定位或有序分页的状态 | `get`、`put`、`remove`，以及范围、方向、条目数和字节数都有界的 scan |
| `OrderedMultiset<K>` | Distinct、精确 admission、带撤回的计数 | 缺失即 multiplicity `0`；`adjust` 做 checked signed 更新，归零即删除 |
| `PartitionedMultiset<P, K>` | 每组独立的有序索引，例如 grouped MIN/MAX | 先选择 partition，再做 multiplicity、`adjust`、`first`、`last` 或有界 scan |
| `Queue<T>` | 单一 owner 的持久 FIFO continuation 或私有 spool | 事务内 `try_push` 与 `pop_front`；硬字节容量；空队列没有持久 metadata |
| `SubscribedLog<T>` | 一个 producer、固定多个 consumer 的 Flow output | setup 时固定 subscriber；writer 追加，subscription 只 peek/ack 自己的下一项，最慢订阅者决定逻辑保留范围 |

`Cell`、`OrderedMap`、`OrderedMultiset` 和 `PartitionedMultiset` 同时提供写事务 access 与只读
transaction access。`Queue` 的读取就是消费，因此只绑定写事务。`SubscribedLog` 在 setup 时由
完整 handle 初始化或校验，随后派生职责更窄的 `SubscribedLogWriter` 和 `Subscription`；writer
不能确认消费，subscription 不能追加、跳过、倒退或截断。

`Queue::try_push` 的容量是硬上限，空队列也不会接纳超限项。每项按完整编码 value 加私有八字节
sequence key 计费；metadata 与 `RocksDB` 自身开销不计入。队列变空时删除 metadata 并重置私有
sequence，这个编号从不暴露为业务身份。

`SubscribedLog::initialize` 必须在创建资源后恰好调用一次，并与拥有它的 durable definition 在
同一事务发布。subscriber 是固定的稠密整数 `0..subscriber_count`。重新打开时用 `validate`
核对定义派生出的数量，再从同一 setup handle 派生 writer 与 subscriptions。log offset 永不重置；
每个 subscription 的 durable `position` 表示下一条待读项。`peek` 只读该项，`acknowledge` 只能确认
这个精确 offset 并前进一步；position 与 retention accounting 在同一事务中更新。物理 key 的清理
时机属于 Store 内部实现，不是 subscription 契约。

Subscribed log 的 capacity 是防止 producer 因 backlog 无限增长而淹没磁盘的软上限：非空 backlog
超限时 `try_append` 返回 `false` 且不写入；空 backlog 始终允许一条大项通过，避免单条合法消息
永久阻塞。`retained_bytes` 同样按每项完整编码 value 加八字节 offset 计费。

## 完整示例

下面的例子只使用公共 API，并把六种结构的更新放进真实事务边界：

```rust,no_run
use std::{num::{NonZeroU64, NonZeroUsize}, path::Path};

use dogpaddle_store::{
    Cell, OrderedMap, OrderedMultiset, PartitionedMultiset, Queue, ScanDirection,
    ScanLimit, Store, StoreError, SubscribedLog,
};

fn run(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::create(path)?;
    let checkpoint = store.create_data::<Cell<u64>>("checkpoint")?;
    let users = store.create_data::<OrderedMap<u64, String>>("users")?;
    let distinct = store.create_data::<OrderedMultiset<Vec<u8>>>("distinct")?;
    let extrema = store.create_data::<PartitionedMultiset<u64, i64>>("extrema")?;
    let spool = store.create_data::<Queue<Vec<u8>>>("spool")?;
    let output = store.create_data::<SubscribedLog<Vec<u8>>>("output")?;

    let writer = output.writer();
    let subscriber = output.subscription(0);
    let (mut writes, reads) = store.into_transactions().split();

    // SubscribedLog 的固定订阅集合属于持久定义的一部分。
    let transaction = writes.begin();
    output.initialize(NonZeroU64::MIN, transaction.access())?;
    transaction.commit()?;

    let transaction = writes.begin();
    let access = transaction.access();
    checkpoint.access(access)?.set(&1)?;
    users.access(access)?.put(&42, &"Shiba".to_owned())?;
    distinct.access(access)?.adjust(&b"row".to_vec(), 1)?;
    extrema.access(access)?.partition(&7)?.adjust(&-3, 1)?;
    assert!(spool
        .access(access)?
        .try_push(&b"private".to_vec(), NonZeroU64::new(1024).unwrap())?);
    assert!(writer.try_append(
        &b"public".to_vec(),
        NonZeroU64::new(1024).unwrap(),
        access,
    )?);
    transaction.commit()?;

    let snapshot = reads.begin();
    let read = snapshot.access();
    assert_eq!(checkpoint.read(read)?.get()?, Some(1));
    assert_eq!(users.read(read)?.get(&42)?.as_deref(), Some("Shiba"));
    assert_eq!(distinct.read(read)?.multiplicity(&b"row".to_vec())?, 1);
    assert_eq!(
        extrema
            .read(read)?
            .partition(&7)?
            .first()?
            .map(|entry| entry.key),
        Some(-3),
    );

    let mut page = Vec::new();
    let continuation = users.read(read)?.scan(
        ..,
        ScanDirection::Ascending,
        None,
        ScanLimit::new(100, 1024 * 1024)?,
        |entry| -> Result<(), StoreError> {
            page.push(entry.decode_owned()?);
            Ok(())
        },
    )?;
    assert_eq!(page, vec![(42, "Shiba".to_owned())]);
    assert_eq!(continuation, None);

    let next = subscriber.peek(read)?.expect("one committed output");
    assert_eq!(next, (0, b"public".to_vec()));
    assert_eq!(writer.status(read)?.retained_bytes, 8 + 6);
    drop(snapshot);

    let transaction = writes.begin();
    assert_eq!(
        spool.access(transaction.access())?.pop_front()?,
        Some(b"private".to_vec()),
    );
    subscriber.acknowledge(next.0, transaction.access())?;
    transaction.commit()?;

    let snapshot = reads.begin();
    assert!(subscriber.peek(snapshot.access())?.is_none());
    let partition = extrema
        .read(snapshot.access())?
        .partition(&7)?
        .scan(ScanDirection::Descending, NonZeroUsize::MIN)?;
    assert_eq!(partition[0].multiplicity, 1);
    Ok(())
}
```

重新打开必须使用相同的资源名和完整 collection kind。Subscribed log 还要在 setup snapshot 中
验证固定订阅数：

```rust,no_run
use std::{num::NonZeroU64, path::Path};

use dogpaddle_store::{Cell, OrderedMap, Store, SubscribedLog};

fn reopen(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::open(path)?;
    let checkpoint = store.open_data::<Cell<u64>>("checkpoint")?;
    let _users = store.open_data::<OrderedMap<u64, String>>("users")?;
    let output = store.open_data::<SubscribedLog<Vec<u8>>>("output")?;

    {
        let snapshot = store.read_transaction();
        output.validate(NonZeroU64::MIN, snapshot.access())?;
        assert_eq!(checkpoint.read(snapshot.access())?.get()?, Some(1));
    }

    let _writer = output.writer();
    let _subscriber = output.subscription(0);
    let _transactions = store.into_transactions();
    Ok(())
}
```

## 编码与持久布局责任

Store 的持久契约包括数据库 marker、资源 catalog、每个资源的稳定 namespace、collection kind，
以及各结构自己的 key framing、metadata 和计数编码。所有结构共享一个 `RocksDB` database；物理前缀、
WAL、同步提交、LZ4 压缩和引擎句柄都不进入公共 API。调用方不选择物理存储类别或 column family，
也不能取得裸 namespace。

Catalog 只记录 collection kind，不记录 `K`、`V`、`T` 的 Rust 类型或 codec 版本。因此资源 owner
必须把“稳定资源名 + 完整 collection 类型 + `StoreKey`/`StoreValue` 编码”当作自己的持久 schema。
以另一种 value codec 打开同一个 `OrderedMap` 不会在 catalog 阶段被识别，第一次解码才会失败。
`StoreKey` 编码必须 canonical、可逆、injective，并逐字节保持 Rust `Ord`；`StoreValue` 编码必须能
跨进程重启稳定还原。内建 codec 覆盖 `Vec<u8>`、`String`、`u32`、`u64`、`i64`、`bool` 和 `()`。

当前开发期格式不提供旧布局识别、迁移或兼容层。修改资源名、collection kind、codec、内部 framing
或 metadata 就是在修改持久 ABI；应同步更新 owner 的 golden/raw-layout 与 reopen 证据，并要求旧
Flow 删除后重建。

## 有序扫描

`OrderedMapAccess::scan` 和 `OrderedMapReadAccess::scan` 接受 `RangeBounds<K>`、升降序、排他的
`resume_after` 与 `ScanLimit`。limit 同时约束一页的条目数和 encoded key + value 逻辑字节数。
返回的 `Option<K>` 只在达到限制且范围内仍有下一项时出现；下一页复用相同 range/direction，并把
它原样传回 `resume_after`。

Store 在调用第一个 visitor 前先准入整页并计算 continuation，不让业务 callback 与 `RocksDB` iterator
交错。callback 可以修改同一事务中的其他数据；已准入的当前页保持不变，后续页看到下一次 scan
时的事务状态。`OrderedMapEntry::decode_owned` 解码完整 `(K, V)`；`project` 让宽 value 场景只解析
所需字节，projection 的返回值不能借用 entry 编码。

第一条匹配项单独超过 byte limit 时返回 `StoreError::ItemTooLarge`。这是唯一可调整 limit 后在同一
事务重试的 Store 错误，不会使事务中毒。`PartitionedMultiset` 的 scan 更窄：它只接受方向和非零
最大条目数，并直接返回拥有型 `Vec<MultisetEntry<K>>`；`first` 与 `last` 是 Top-K/极值维护的常用
单项路径。

## 错误与事务中毒

编码失败、解码失败、损坏的持久 metadata、wrong-store handle、`RocksDB` 访问失败、multiset
underflow/overflow、非法 subscription acknowledgement 等硬错误都会使所属读或写 transaction
中毒。之后的访问返回 `StoreError::TransactionPoisoned`，写 transaction 也不能提交；其全部 Store
写入最终回滚。visitor 自己返回错误同样会毒化 scan 所属 transaction，因此 callback 不应执行
无法随 Store 回滚的外部副作用。

容量不足不是错误。`Queue::try_push` 或 `SubscribedLogWriter::try_append` 返回 `false` 时不写入、
不中毒，调用方可以在同一 transaction 内选择背压、更新其他状态或正常提交。

`Cell`、map、multiset 和 `Queue` handle 可以 clone，但每次访问仍会校验它属于开启 transaction 的
同一个 `Store`。`SubscribedLog` 通过 setup handle 派生所需的 writer 和 subscriptions。Access 借用
活动 transaction，不能缓存到下一个 turn；transaction 的 owner 始终保留唯一的 commit 权力。

## 验证与性能

Store 的公共 correctness target 覆盖事务、snapshot、六种结构、reopen、raw layout、损坏拒绝、
中毒回滚和 SIGKILL crash consistency：

```bash
cargo test -p dogpaddle-store --test correctness --locked -- --test-threads=1
```

需要私有故障注入的 collection 单元测试随 library test 运行：

```bash
cargo test -p dogpaddle-store --lib --locked -- --test-threads=1
```

完整工作区 gate、证据所有权和系统验收入口见 [`TESTING.md`](../../TESTING.md)。Store 只保留两个
owner benchmark：`cell` 测 hot read 与 durable read-modify-write，`ordered_map` 测批量写、点读、
有界正反向 scan、projection、Station 形状的原子更新和 durable hot overwrite。workload、fixture、
结果字段与可比性规则见 [`PERFORMANCE.md`](PERFORMANCE.md)。快速 smoke：

```bash
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench cell
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench ordered_map
```
