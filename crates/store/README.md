# dogpaddle-store

`dogpaddle-store` 是 `DogPaddle` 内部的事务状态层。它基于 MDBX 提供具名的类型化数据对象、
原子事务和稳定编解码器，为 Flow 运行进度与 Operation 状态提供统一的持久化边界。该 crate
是引擎实现模块，不是最终面向用户的通用存储产品。

## 能力分层

Store 将资源装配与运行时访问分离：

- `Store` 创建或打开全部具名数据对象；进入运行期前，其所有权会被消费；
- `Cell<T>`、`OrderedMap<K, V, SIZE>` 与 `AppendLog<T>` 是 collection 的完整能力，可以产生
  完整的事务级读写 Access；
- `ReadOnly<C>` 由完整 collection 显式单向衰减得到，长期移除该 handle 的写权限；
- `Transactions` 是唯一、不可克隆的运行期写事务启动能力；它可以在空闲时移动到其他线程，
  也可以由完整 owner 消费式 `split`，但不暴露 catalog；
- `ReadTransactions` 由 `Transactions::split` 显式产生，不可 clone、可以在线程间共享，并为
  同一个 MDBX environment 开启彼此独立的只读 snapshot；
- `Transaction` 持有一个原子提交边界，并在被丢弃时回滚；
- `ReadTransaction` 持有一个没有提交能力的只读 snapshot，并在被丢弃时释放；
- `TransactionAccess` 从活动 Transaction 临时借用，只允许已有类型化数据对象绑定访问；
- `ReadTransactionAccess` 只能绑定 `CellReadAccess`、`OrderedMapReadAccess` 或
  `AppendLogReadAccess`，类型层面没有写方法；
- 完整的 `CellAccess`、`OrderedMapAccess` 与 `AppendLogAccess` 执行实际读写；
  `ReadOnly<C>::access` 与 `ReadOnly<C>::read` 都统一返回对应的 `*ReadAccess`。

底层数据句柄、物理放置和 MDBX 访问均为 crate 私有实现。集合不能脱离 `Store` 构造，调用方
也不能绕过数据类型自行组合物理资源。

## 数据结构决定物理布局

只有确实存在两种合理规模的 collection 才暴露 `Size`。`Cell` 的基数永远至多为一，因此
固定使用共享物理空间；`AppendLog` 为持续增长并分批回收的流数据而设计，因此固定使用独立
物理表；只有 `OrderedMap` 可能很小，也可能随业务 key 增长，所以必须显式选择 `Small` 或
`Large`：

```rust
use dogpaddle_store::{AppendLog, Cell, Large, OrderedMap, Small};

type Counter = Cell<u64>;
type Cache = OrderedMap<u64, String, Small>;
type Records = OrderedMap<u64, Vec<u8>, Large>;
type Changes = AppendLog<Vec<u8>>;
```

`Size` 描述支持规模选择的具名对象的静态存储类别，不是运行时容量上限，也不会根据当前
数据量自动改变：

- `Cell` 与 `Small` collection 共享主 B+Tree，适合数量较多、规模较小或频繁提交的状态；
- `Large` map 与 `AppendLog` 使用独立的 MDBX named table，适合可能很大、热点或面向批处理的数据；
- 单个 Store 最多包含 `Store::LARGE_DATA_CAPACITY` 个独立物理表对象。

物理布局在创建时写入 catalog，之后属于该逻辑资源的持久化 schema。
`Store::open_data::<D>` 会校验数据类型要求的布局；以 `Large` 打开实际为 `Small` 的资源，
或用固定为共享布局的 `Cell` 打开 dedicated 资源，都会返回 `StoreError::DataSizeMismatch`。

Store catalog 不记录或验证 `Cell`/`OrderedMap`/`AppendLog`、`K`、`V`、`T` 或 codec 类型。
同一个物理布局下，调用方必须按照创建者定义的稳定 schema 重新打开数据。不要把 Rust
`TypeId`、类型名或内存布局当作磁盘格式。

## 完整示例

```rust,no_run
use dogpaddle_store::{
    AppendLog, Cell, Large, OrderedMap, ScanDirection, ScanLimit, Store, StoreError,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::create("./dogpaddle-store-data")?;
    let counter = store.create_data::<Cell<u64>>("counter")?;
    let users = store.create_data::<OrderedMap<u64, String, Large>>("users")?;
    let changes = store.create_data::<AppendLog<Vec<u8>>>("changes")?;

    let (mut transactions, read_transactions) = store.into_transactions().split();

    {
        let transaction = transactions.begin()?;
        let access = transaction.access();
        let mut counter = counter.access(access)?;
        let mut users = users.access(access)?;
        let mut changes = changes.access(access)?;

        counter.set(&1)?;
        users.put(&42, &"Shiba".to_owned())?;
        changes.append(&b"insert user 42".to_vec())?;
        transaction.commit()?;
    }

    let transaction = read_transactions.begin()?;
    let access = transaction.access();
    let counter = counter.read(access)?;
    let users = users.read(access)?;
    let changes = changes.read(access)?;
    assert_eq!(counter.get()?, Some(1));
    assert_eq!(users.get(&42)?.as_deref(), Some("Shiba"));

    let mut users_page = Vec::new();
    let continuation = users.scan(
        ..,
        ScanDirection::Descending,
        None,
        ScanLimit::new(100, 1024 * 1024)?,
        |entry| -> Result<(), StoreError> {
            users_page.push(entry.decode_owned()?);
            Ok(())
        },
    )?;
    assert_eq!(users_page, vec![(42, "Shiba".to_owned())]);
    assert_eq!(continuation, None);
    let mut observed = Vec::new();
    let scan = changes.scan(
        0,
        ScanLimit::new(100, 1024 * 1024)?,
        |entry| -> Result<(), StoreError> {
            observed.push((entry.offset(), entry.decode_owned()?));
            Ok(())
        },
    )?;
    assert_eq!(observed, vec![(0, b"insert user 42".to_vec())]);
    assert!(scan.caught_up);
    Ok(())
}
```

重新打开时必须声明同一个完整数据类型：

```rust,no_run
use dogpaddle_store::{Cell, Store};

# fn open() -> Result<(), Box<dyn std::error::Error>> {
let store = Store::open("./dogpaddle-store-data")?;
let counter = store.open_data::<Cell<u64>>("counter")?;
let (_, read_transactions) = store.into_transactions().split();
let transaction = read_transactions.begin()?;
let access = transaction.access();
assert_eq!(counter.read(access)?.get()?, Some(1));
# Ok(())
# }
```

`StoreData` 是 Store 泛型 create/open 使用的 sealed marker trait。它只由布局完整的内建
collection 实现；一般业务代码不需要直接引用它。权限不属于持久化 schema：`ReadOnly<C>`
不实现 `StoreData`，Store catalog 也不记录 capability。重新打开时总是先按完整 collection
类型打开，再由运行期装配者重新衰减。

## 只读能力衰减

`ReadOnly::new` 消费一个完整 collection handle，并且不提供 `Deref`、`AsRef`、`Borrow`、
`into_inner` 或任何能重新取得内部 handle 的 callback。需要同时保留完整能力时，装配者必须
显式 clone，再衰减其中一个 clone：

```rust,no_run
use dogpaddle_store::{AppendLog, ReadOnly, ScanLimit, Store, StoreError};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut store = Store::create("./dogpaddle-read-only-example")?;
let output = store.create_data::<AppendLog<Vec<u8>>>("changes")?;
let input = ReadOnly::new(output.clone());
let mut transactions = store.into_transactions();

let transaction = transactions.begin()?;
let access = transaction.access();
output.access(access)?.append(&b"change".to_vec())?;

let mut observed = Vec::new();
input.access(access)?.scan(
    0,
    ScanLimit::new(100, 1024 * 1024)?,
    |entry| -> Result<(), StoreError> {
        observed.push(entry.decode_owned()?);
        Ok(())
    },
)?;
assert_eq!(observed, vec![b"change".to_vec()]);
transaction.commit()?;
# Ok(())
# }
```

衰减不会撤销装配者仍然持有的完整 alias；它保证的是只收到 `ReadOnly<C>` 的组件无法在 safe
Rust 中升级能力。`ReadOnly<C>` 的 `Clone` 仍只产生 `ReadOnly<C>`，因此多个下游可以安全地
共享同一个输入 collection。当前白名单是：`Cell` 的 `get`，`OrderedMap` 的 `get/scan`，以及
`AppendLog` 的 `bounds/scan`。它与只读事务是两个正交约束：`ReadOnly<C>::access` 可在一个写
事务中提供受限读取，`ReadOnly<C>::read` 则绑定真正的只读 snapshot；两条路径统一返回没有
写方法的 `CellReadAccess`、`OrderedMapReadAccess` 或 `AppendLogReadAccess`。完整 collection 的
`read` 也返回同一组类型。

## 事务与扫描语义

`Store::into_transactions()` 只发生一次，并产生唯一、不可克隆的写能力 `Transactions`。运行
协调者集中持有它；`begin(&mut self)` 返回的 Transaction guard 在存活期间独占借用该能力，
因此无法在前一 guard 被提交或丢弃前再次开始写事务。显式泄漏 guard 会同时泄漏底层写事务，
不属于正常 RAII 生命周期。写能力本身可以在线程之间移动，但活动 Transaction 及其访问值仍然
绑定在创建它们的线程。

`Transactions::split(self)` 消费完整 owner，并把同一个唯一写能力与一个只读启动能力一起返回，
不会创建第二个 MDBX environment。完整 owner 取回写能力后可以有意地再次授权；只拿到
`&mut Transactions` 的 Station 无法消费它，因而不能 split 或导出 owned reader。返回的
`ReadTransactions` 不可 clone、但为 `Send + Sync`；共享引用可以通过 `begin(&self)` 开启彼此
独立的 `ReadTransaction`，借用方无法把事务启动能力留到借用期之外。只读 snapshot 可以和唯一
写能力同时存活：它持续观察开始时的稳定视图，writer 的后续 commit 只对之后开始的 snapshot
可见。活动 `ReadTransaction` 与 `ReadTransactionAccess` 仍然是线程绑定的；并发读取应在线程间
共享启动能力的引用，再在线程内开始、读取并结束 snapshot，而不是把活动事务跨线程传递。

丢弃事务会触发回滚。`Transaction` 没有显式中止方法，也不包含集合专用操作。调用
`Transaction::access()` 会得到可复制但不能提交的 `TransactionAccess`；它只能让调用方已经
持有的完整或 `ReadOnly` collection handle 创建事务级 Access。事务协调者因此可以保留当前
Transaction 的所有权并把受限能力交给 Operation；Operation 无法开始或结束原子边界，也无法
访问 Store catalog。通过同一能力创建的所有访问值共享事务快照，因此任意数量、任意固定或显式
选择布局的数据对象都可以原子提交。

`TransactionAccess` 属于写事务，但它自身没有 commit 权限；一次原子工作可以用它同时读取 input
并写入 state/output。静态 collection 权限仍由 handle 决定：完整 handle 的 `access` 绑定完整
Access，`ReadOnly<C>::access` 则绑定对应的 `*ReadAccess`。`ReadTransactionAccess` 是更强的
事务级衰减：无论调用方持有完整还是 `ReadOnly` collection，都只能通过 `read` 得到同一组
`CellReadAccess`、`OrderedMapReadAccess` 或 `AppendLogReadAccess`。这些类型根本没有
`set/put/remove/append/truncate` 方法，`ReadTransaction` 也没有 `commit`。

两种 `TransactionAccess` 和所有 collection Access 都不能脱离所属事务；每个新事务都必须重新
绑定，不能跨 Station 步骤缓存。能力及访问值仍然是线程绑定的，不能跨线程移动正在执行的事务。

内置集合在同一个事务中毒边界内执行编解码。严重的编解码或存储失败会毒化事务，之后的操作
以及提交返回 `TransactionPoisoned`；无法容纳单个扫描项的 `ItemTooLarge` 是可调整 scan
limit 后重试的软错误，不会毒化事务。

`StoreKey::decode_key` 与 `StoreValue::decode_value` 都接收 `Cow<'_, [u8]>`。只读 decoder 应从
`as_ref()` 解析，确实需要拥有完整编码的类型应使用 `into_owned()`；两种 variant 表示相同的
持久字节，codec 不得依赖某个 variant 必然出现。MDBX clean page 可以直接借用，dirty page
则会安全地物化为 owned buffer。具体 variant 是性能行为，不是正确性契约。

`OrderedMapAccess::scan` 支持有界范围、升序或降序、条目数与逻辑编码字节数双重限制。它先在
Store 内完成当前页的范围判断、限额和续传 key 解码，释放 MDBX cursor，随后才把短生命周期的
`OrderedMapEntry` 逐条交给 callback。因此 callback 可以修改同一事务中的 map，而当前已经准入
的页保持不变；后续页则读取事务当时的状态。`entry.project` 可以只读取 key、diff 或少数列，
`entry.decode_owned` 仅在确实需要完整业务对象时物化结果。返回的 `Option<K>` 是排他的续传 key：
只有当前页达到限制且仍存在另一条匹配记录时才为 `Some`。下一页必须复用相同范围和方向，并把
它作为 `resume_after` 传回；scan 不会为了判断续传而预读下一条 value。

## `AppendLog` 语义

`AppendLog` 的保留区间始终是 `[head, tail)`：`head` 是最早保留 offset，`tail` 是下一次
append 使用的 offset。读取 cursor 同样表示“下一条尚未读取的 offset”，必须位于
`head..=tail`；offset 永不重编号，也不会在前缀删除后复用。

`append_batch(&[T])` 为一批值一次性分配连续 offset，并返回对应的半开区间。批量追加只读取和
验证一次边界、复用一个 MDBX cursor，并在全部记录成功后只推进一次 metadata；空 slice 是
不物化 metadata 的 no-op。任意编码、碰撞或存储失败都会毒化事务，已经写入的批内前缀不能
单独提交。单条 `append` 与批量追加共享同一套持久化不变量。

`retained_bytes()` 返回物理 `[head, tail)` 中所有 entry 的逻辑编码字节和：每项包含八字节
offset key 与完整 encoded value，不包含 `AppendLog` metadata、MDBX page/index、MVCC 旧页或文件
系统分配开销。`try_append(value, capacity)` 使用同一账本做准入：非空日志只有在追加后不超过
capacity 时才写入，否则返回不毒化事务的 `Ok(None)`；空日志始终允许写入一项，因此单个超大
entry 不会造成永久等待。该 capacity 是 backlog 高水位，不是 MDBX 文件大小硬配额。

`AppendLogAccess::scan` 使用与 map 相同的 `ScanLimit`，每项按八字节 offset key 加完整编码
value 计费。它不会预先构造完整的 `T`，而是把短生命周期的 `AppendLogEntry` 逐条交给
callback：`project` 可以只解码 diff 或少数列，`decode_owned` 只在确实需要完整值时执行，
`append_entry` 则能把相同 `T` 的编码原样写入同一事务中的另一个 log。Entry 不公开 MDBX
借用或裸字节，也不能逃出 callback 或跨线程。

作为输入时，`ReadOnly<AppendLog<T>>` 只公开 `bounds/retained_bytes/scan`，不能 append 或
truncate。它既能
通过 `access(TransactionAccess)` 参与包含其他写入的原子事务，也能通过
`read(ReadTransactionAccess)` 绑定独立只读 snapshot。多个消费者可以 clone 同一份只读 handle
并读取同一个物理日志。写事务中的只读 view 扫出的 entry 仍可原样转发给同一事务内自有的 output；
真正只读 snapshot 的 entry 不携带这种写事务关联。各自的 next-unread offset 由上层分别持久化，
不属于 `AppendLog` 或只读 capability。

一个 scan 在调用 callback 前先验证选中 offset 连续。callback 的任意错误都会毒化事务，
避免已经写入部分输出后仍被提交；第一项无法装入 byte limit 的 `ItemTooLarge` 仍是可增大
limit 后重试的软错误。`AppendLogScan::next_offset` 可直接持久化为下游 next-unread cursor，
`caught_up` 表示本批已经追到 scan 开始时捕获的 tail。

`truncate_before(target, max_items)` 只删除 `target` 以下且当前仍保留的连续前缀，并限制单次
删除条数。删除过程通过 MDBX cursor 读取 value length，不复制或解码 value；entry 删除、head
推进和 retained-byte 扣账处于同一个 MDBX 事务。调用方应以所有下游 cursor 的最小值作为
target，并分批提交 GC。

持久布局固定为一个独立表：空 key 保存 24 字节 big-endian
`head || tail || retained_bytes`，八字节 big-endian offset key 保存 `T` 的稳定编码。全新且
从未使用的空表可以没有 metadata；一旦使用，即使回收到空区间也保留单调的 head/tail，并把
retained bytes 记为零。旧的 8 字节或 16 字节 metadata 不做迁移，按损坏拒绝。该布局和 `T` 的
codec 都属于调用方需要稳定维护的持久化 schema。

## 测试

全部公共行为与持久化契约使用一个按语义分模块的外部正确性 target：

```bash
cargo test -p dogpaddle-store --test correctness
cargo test -p dogpaddle-store --doc
```

也可以直接过滤并运行单个测试区域：

```bash
cargo test -p dogpaddle-store --test correctness transaction::
cargo test -p dogpaddle-store --test correctness scan::
```

该 target 会对 `OrderedMap` 的 `Small` 与 `Large` 形式运行相同的数据、事务与扫描语义，并
通过 MDBX 持久化适配器锁定 `Cell` 的共享布局、`Small` map 的共享前缀、`Large` map 与
`AppendLog` 的
独立 named table。布局不匹配的 reopen、崩溃恢复、事务中毒和 codec 失败也有独立覆盖。目录
中的 `capability` 模块验证衰减后仍读取同一物理对象、fan-out 与 reopen 后重新衰减；transaction
模块验证旧只读 snapshot 与 writer 同时存在时保持稳定，并验证共享的读能力引用可在线程中独立
开始 snapshot。写方法不可用、事务启动能力不可 clone、活动事务不能跨线程、不能恢复完整 handle
且不能作为 `StoreData` 打开的静态边界由 Rustdoc compile-fail 测试锁定。测试目录所有权、模块
职责和完整验证协议见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。

## 性能

PR 中统一通过工作区 smoke runner 实际执行缩小后的 benchmark 协议：

```bash
cargo xtask bench-smoke
```

Store 仍有四个按数据对象与运行目的隔离的 release 入口，供单场景诊断和固定 reference 测量：

```bash
cargo bench -p dogpaddle-store --bench cell
cargo bench -p dogpaddle-store --bench ordered_map
cargo bench -p dogpaddle-store --bench append_log
cargo bench -p dogpaddle-store --bench append_log_endurance
```

按 `Cell`、`Small OrderedMap`、`Large OrderedMap` 和 `AppendLog` 分别整理的本机基线、读法与
设计结论见
[`PERFORMANCE.md`](https://github.com/frelion/dogpaddle/blob/main/crates/store/PERFORMANCE.md)。
普通 target 覆盖各 collection 的独立与组合事务；`append_log_endurance` 单独观察长期前缀回收、
页复用、尾延迟和实际文件占用。所有规模由 `smoke` 或 `reference` profile 固定，fixture、预热和
结果 oracle 位于计时外。正式 reference 必须同时设置绝对路径 `DOGPADDLE_BENCH_ROOT`；机器记录、
工作负载、统计口径与目录所有权见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。
