# dogpaddle-store

`dogpaddle-store` 是 `DogPaddle` 的本地事务状态层。它把 `RocksDB` 包装成少量具名、类型化的数据结构，
上层只需要表达“保存一个计数”“更新一张有序表”“发布一条消息”，不需要接触 column family、物理 key
或 `RocksDB` 句柄。

第一次阅读时先记住一句话：**先声明所有持久资源，再用同一笔事务更新任意多个资源。**

## 一条状态更新如何发生

以一个同时更新计数和结果表的算子为例：

```text
创建或打开 Store
    ↓
取得 Cell<u64> 和 OrderedMap<u64, String> handle
    ↓
开始一笔写事务
    ↓
修改两个结构
    ↓
commit：两个修改一起可见；丢弃事务：两个修改一起回滚
```

对应的最小公共 API 是：

```rust,no_run
use std::path::Path;

use dogpaddle_store::{Cell, OrderedMap, Store};

fn initialize(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::create(path)?;
    let checkpoint = store.create_data::<Cell<u64>>("checkpoint")?;
    let users = store.create_data::<OrderedMap<u64, String>>("users")?;
    let mut transactions = store.into_transactions();

    let transaction = transactions.begin();
    let access = transaction.access();
    checkpoint.access(access)?.set(&1)?;
    users.access(access)?.put(&42, &"Shiba".to_owned())?;
    transaction.commit()?;
    Ok(())
}

fn inspect(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::open(path)?;
    let checkpoint = store.open_data::<Cell<u64>>("checkpoint")?;
    let users = store.open_data::<OrderedMap<u64, String>>("users")?;

    let snapshot = store.read_transaction();
    let read = snapshot.access();
    assert_eq!(checkpoint.read(read)?.get()?, Some(1));
    assert_eq!(users.read(read)?.get(&42)?.as_deref(), Some("Shiba"));
    Ok(())
}
```

`Cell` 和 `OrderedMap` 是可以长期保存的 handle；`access` 只是当前事务的临时借用凭证。handle 不能开始或
提交事务，因此 collection 代码无法偷偷改变事务边界。

## 两个生命周期

Store 刻意把生命周期分成两段：

| 阶段 | 做什么 | 主要类型 |
| --- | --- | --- |
| setup | 创建或打开具名资源，固定资源类型和名字 | `Store`、`StoreSetup` |
| runtime | 读取和更新已经声明的资源 | `Transactions`、`ReadTransactions` |

普通使用可以调用 `Store::create` / `Store::open`，再逐个 `create_data` / `open_data`。需要一次发布完整资源集合时，
先用 `StoreSetup::new()` 建立纯内存 draft；资源声明只分配最终 Store token、catalog namespace 和 typed handle，
不会创建路径。最后由 `StoreSetup::commit(path, initialize)` 创建数据库，并把 marker、完整 catalog、初始状态和
owner Definition 放进同一笔同步事务。

`StoreSetup` 不是普通 `Store`：它不能读取或打开资源，也不能直接进入 runtime。`commit` 无论成功、初始化失败，
还是遇到结果不确定的底层提交错误，都会消费这个 setup owner；不能在失败后继续追加资源。丢弃未 commit 的 draft
没有文件系统副作用；commit 开始后失败则可能留下没有 marker 的 incomplete path，`Store::open` 必须拒绝它。

短期装配代码可以通过 `StoreSetup::data_scope()` 或 `Store::data_scope()` 取得 `DataScope`。前者固定为 declare，
`data::<D>(name)` 只创建新 binding 并拒绝重复名；后者固定为 existing，只查找已有 binding 并拒绝缺失或 kind
不匹配。`DataScope` 不暴露事务、读取、裸 ID 或模式切换。

`data.scoped("owner")` 借用一个只处理该前缀下名称的子 scope，子 scope 的 `data("count")`
声明或查找完整名称 `owner/count`。嵌套 scope 继续追加前缀，不能重置父 scope；释放子 scope 后
父 scope 保持原样。名称是平面 catalog 字符串：只按 `/` 字面拼接，不清理重复斜线或解释 `..`。
根 scope 不加前缀，而显式 `scoped("")` 会保留空段，`data("count")` 得到 `/count`。
前缀本身不提前校验，只有请求数据时才校验完整名称；声明/查找错误均报告完整名称，包含
`DataIdExhausted { name }`。scope 只用于短期装配，返回的 handle 不保留它或前缀。

`Store::into_transactions` 结束 setup，返回唯一的写事务启动能力。调用 `split` 后得到两种能力：

```text
Transactions       → begin() → Transaction       → TransactionAccess
ReadTransactions   → begin() → ReadTransaction   → ReadTransactionAccess
```

- `Transactions` 不可克隆，并且 `begin(&mut self)` 需要独占借用；当前运行模型因此一次只有一个 writer。
- `ReadTransactions` 不可克隆但可以共享；每次 `begin()` 得到一个稳定的只读 snapshot。
- snapshot 只看见它开始时已经提交的数据，后续提交由新的 snapshot 看见。
- transaction 和 access 借用各自的启动能力，并且都不是 `Send` / `Sync`。Flow 进一步约定只在当前 turn 内使用它们。

读写权限同时由 collection handle 与 transaction access 两层约束；只读 handle 不能借写事务获得写入权限。

这套类型不是为了模拟 `RocksDB` 的全部能力，而是让上层代码很难绕过 `DogPaddle` 的事务边界。

## 六种持久数据结构

| 结构 | 用一句话理解 | `DogPaddle` 中的典型用途 |
| --- | --- | --- |
| `Cell<T>` | 一个可缺省的值 | checkpoint、phase、计数器 |
| `OrderedMap<K, V>` | 可点查、增删和有序分页的 map | 分组状态、业务索引 |
| `OrderedMultiset<K>` | `K → 正 u64 份数`，归零即删除 | Distinct、关系行权重 |
| `PartitionedMultiset<P, K>` | 每个 `P` 下有一棵独立的 multiset | Aggregate 极值、Join 的 key 分区 |
| `Queue<T>` | 没有独立读取游标、pop 即删除的持久 FIFO | 私有 continuation、CDC 快照 spool |
| `SubscribedLog<T>` | 一个 producer、固定多个独立 consumer 的日志 | Station 之间的持久输出 |

`StoreData` 是 sealed trait；产品代码只能使用这些结构，不能绕过 catalog 自造新的物理布局。

`Cell<Vec<u8>>` 另提供 `get_bounded(max_bytes)`。它在同一个 snapshot 中先检查编码长度，再决定是否
复制 owned value；超限返回 `ItemTooLarge`，不会毒化事务，调用方可以在同一事务提高 limit 后重试。

### Queue 与 `SubscribedLog` 的区别

两者都保存有序数据，但用途不同：

- `Queue` 的读取就是删除，没有 consumer cursor。它可以 clone，因此调用方必须自己保证只有一个协调者消费；
  容量是硬上限，空队列也拒绝超大项。
- `SubscribedLog` 允许多个 consumer 分别读取。每个 subscription 保存自己的下一条位置，最慢的 consumer
  决定数据何时可以回收。

`Queue` 每项按完整编码 value 加 8-byte 私有 sequence 计费；队列变空时删除 metadata 并重置该私有编号。`Queue<Vec<u8>>` 消费但不需要读取值时，`discard_front` 只读 front 的编码长度以精确扣减计费，不复制或解码完整 value；和 `pop_front` 共用删除、最后一项清理与损坏检查，仍由调用方事务统一提交或回滚。
`SubscribedLog` 每项按完整编码 value 加 8-byte offset 计费。两者的容量都不包含 `RocksDB` 自身开销。

`SubscribedLogWriter::try_append` 的容量是 backlog 高水位：非空 backlog 超限时返回 `false`，但空日志会
接受一个超大 entry，避免单条合法消息永久卡住。容量不足不是 Store 错误，也不会使事务中毒。

两种容量都由 owner 在每次 `try_push` / `try_append` 时传入，不保存进 collection metadata；同一资源的 owner
应稳定使用同一策略值。Queue 变空后私有 sequence 可以重置，SubscribedLog 的公开 offset 则单调递增且不复用。

Flow 正是用 `SubscribedLog<Vec<u8>>` 连接 Station：producer 追加一个完整 Change，各 consumer 用自己的
`Subscription::peek` 读取，并在处理结果提交的同一笔事务里 `acknowledge` 精确 offset。

完整 log handle 只在 setup 使用。新日志必须先以非零 subscriber 数初始化；reopen 时先验证同一个数量，再派生
职责更窄的 writer 和 subscriptions：

```rust,no_run
use std::{num::NonZeroU64, path::Path};

use dogpaddle_store::{Store, SubscribedLog};

fn build(path: &Path) -> Result<(), dogpaddle_store::StoreError> {
    let mut setup = dogpaddle_store::StoreSetup::new();
    let log = setup.create_data::<SubscribedLog<Vec<u8>>>("output")?;
    let _transactions = setup.commit(path, |access| {
        log.initialize(NonZeroU64::MIN, access)
    })?;
    let _writer = log.writer();
    let _consumer = log.subscription(0);
    Ok(())
}

fn reopen(path: &Path) -> Result<(), dogpaddle_store::StoreError> {
    let store = Store::open(path)?;
    let log = store.open_data::<SubscribedLog<Vec<u8>>>("output")?;
    let snapshot = store.read_transaction();
    log.validate(NonZeroU64::MIN, snapshot.access())?;
    drop(snapshot);
    let _writer = log.writer();
    let _consumer = log.subscription(0);
    Ok(())
}
```

`peek` 返回下一条精确 offset 和 owned、已解码的值，不推进位置。`acknowledge` 只接受该 subscription 当前的
精确 offset；通常应与消费结果和业务状态放在同一笔写事务。

## 有序分页

`OrderedMap`、`OrderedMultiset` 和 `PartitionedMultiset` 的 scan 同时限制条目数和编码后的逻辑字节数。
返回页拥有已经解码的 entries，以及可选的排他 `continuation`；页面不借用事务，可以在事务结束后继续遍历。

```rust,no_run
# use dogpaddle_store::{OrderedMap, ScanDirection, ScanLimit, Store};
# fn read(store: &Store, users: &OrderedMap<u64, String>) -> Result<(), dogpaddle_store::StoreError> {
let snapshot = store.read_transaction();
let page = users.read(snapshot.access())?.scan(
    ..,
    ScanDirection::Ascending,
    None,
    ScanLimit::new(100, 1024 * 1024)?,
)?;
for (id, name) in page.entries {
    println!("{id}: {name}");
}
let next = page.continuation;
# let _ = next;
# Ok(())
# }
```

Store 在返回前完成准入、复制和完整解码；错误不会交付半页。第一项单独超过 byte limit 时返回
`StoreError::ItemTooLarge`，调用方可以在同一事务中提高 limit 后重试。其他 codec 或存储错误会使事务中毒。

分区扫描只省略 range，仍保留 direction、排他 `resume_after` 和同一 `ScanLimit`。页面不提供 visitor 或 encoded-entry projection；业务层自行遍历并处置业务错误。私有扫描先检查范围和准入再复制 payload；分区项按完整 framing + 行 key + multiplicity 计入字节预算，但准入后只复制行 key 后缀供 owned 解码。存在性与长度检查不得构造完整 owned value。

## 提交、错误与恢复

底层使用 `RocksDB` `OptimisticTransactionDB`。写事务使用 WAL 并同步提交。`Transaction::commit` 成功后，整笔修改一起持久化；未 commit 的事务被丢弃时
全部回滚。

以下错误会使当前事务中毒：编码或解码失败、损坏的 metadata、使用另一个 Store 的 handle、RocksDB 访问失败、
multiset underflow/overflow，以及非法 subscription acknowledgement。之后的访问返回
`StoreError::TransactionPoisoned`，写事务不能提交。

`StoreSetup::commit` 会消费 draft。初始化闭包确定失败时会留下没有有效 marker 的不完整目录，不能作为 Store 打开；
底层 `RocksDB` commit 返回存储错误时结果可能不确定，只能通过 reopen 判断。Flow 运行期遇到不确定提交则进入
fail-stop，并要求重新打开 Flow。Store 不用额外日志去猜测一次不确定提交的结果。

`Cell`、Map、Multiset 和 `Queue` handle 可以 clone，但每次访问都会检查它属于当前事务所在的 Store。
完整 `SubscribedLog` setup handle、writer 和 subscription 都不可 clone；应在 setup 时各派生一次并 move 给唯一 owner。
writer 不能确认消费，subscription 不能追加，两者也不能取得 commit 权力。

## 持久格式由谁负责

Store catalog 记录资源名、collection kind 和独立 namespace，但不知道 `K`、`V`、`T` 的 Rust 类型。
因此资源 owner 必须把下面三项一起视为持久 schema：

```text
稳定资源名 + collection 类型 + StoreKey / StoreValue codec
```

`StoreKey` 编码必须 canonical、可逆、无碰撞，并按字节保持 Rust `Ord`；`StoreValue` 编码必须能在重启后稳定还原。
所有结构共享一个启用 LZ4 与 whole-key bloom filter 的默认 column family，物理前缀、namespace、压缩与过滤器设置和 `RocksDB` 句柄都不对外暴露。

当前是开发期 v1。修改资源名、collection kind、codec、key framing 或 metadata 就是修改持久 ABI；同步更新布局和
reopen 测试，然后删除旧 Flow 重建，不增加旧格式迁移或兼容分支。

`SubscribedLog` 的稳定 metadata 保存 big-endian `subscriber_count`、`tail` 和 `retained_bytes`，每个稠密 subscriber 另有一个 big-endian `position`；`head` 只由全部 positions 的最小值派生，不单独持久化。offset 永不重置，`retained_bytes` 只计算 `[head, tail)` 中每条 entry 的 8-byte offset 与完整 encoded value，不包含 metadata、subscriber positions、RocksDB block/WAL/MVCC 或文件开销；`try_append` 与 `acknowledge` 在自身事务内精确维护，Flow 不得复制这套计费与 retention 逻辑。非空 backlog 只有在追加后不超过 capacity 时接受 entry；空 backlog 允许一条 oversize entry，因而该机制是 per-output soft high watermark，不是磁盘或内存硬配额。`Queue` 使用相同的每项逻辑计费，但其 capacity 是硬上限，空队列也拒绝 oversize entry，并在弹出最后一项时删除 metadata、重置不对外暴露的 sequence。

## 读代码的顺序

1. [`src/store/mod.rs`](src/store/mod.rs)：`Store`、事务和 access 类型。
2. [`src/store/transaction.rs`](src/store/transaction.rs)：snapshot、commit 与中毒规则。
3. [`src/collections/`](src/collections/)：六种结构的公共语义。
4. [`src/store/data.rs`](src/store/data.rs)：catalog、namespace 和底层读写入口。
5. [`src/codec.rs`](src/codec.rs)：稳定 key/value 编码契约。

## 验证与性能

公共 correctness target 覆盖事务、snapshot、六种结构、reopen、raw layout、损坏拒绝、中毒回滚和 SIGKILL
crash consistency：

```bash
cargo test -p dogpaddle-store --test correctness --locked -- --test-threads=1
cargo test -p dogpaddle-store --lib --locked -- --test-threads=1
```

完整工作区 gate 见 [`TESTING.md`](../../TESTING.md)。benchmark 的 workload 和解释见
[`PERFORMANCE.md`](PERFORMANCE.md)：

```bash
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench cell
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench ordered_map
```
