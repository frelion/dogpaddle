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
                   → durability_batch() → DurabilityBatch → BatchedTransaction
ReadTransactions   → begin() → ReadTransaction   → ReadTransactionAccess
```

- `Transactions` 不可克隆，并且 `begin(&mut self)` 需要独占借用；当前运行模型因此一次只有一个 writer。
- `DurabilityBatch` 仍保持一个 writer 和逐事务原子性，只把多笔 WAL write 与 fsync 合并到显式 barrier。
- `ReadTransactions` 不可克隆但可以共享；每次 `begin()` 得到一个稳定的只读 snapshot。
- snapshot 只看见它开始时已经提交的数据，后续提交由新的 snapshot 看见。
- transaction 和 access 借用各自的启动能力，并且都不是 `Send` / `Sync`。Flow 只在当前页的事务内使用它们。

读写权限同时由 collection handle 与 transaction access 两层约束；只读 handle 不能借写事务获得写入权限。

这套类型不是为了模拟 `RocksDB` 的全部能力，而是让上层代码很难绕过 `DogPaddle` 的事务边界。

## 三种持久数据结构

| 结构 | 用一句话理解 | `DogPaddle` 中的典型用途 |
| --- | --- | --- |
| `Cell<T>` | 一个可缺省的值 | checkpoint、frame control、计数器 |
| `OrderedMap<K, V>` | 可点查、增删和有序分页的 map | 分组状态、行权重、分区索引 |
| `Queue<T>` | pop 即删除的持久 FIFO | source ingress、CDC 快照 spool |

`StoreData` 是 sealed trait；只有这三种 catalog kind 和 collection 生命周期。OrderedMap 的当前 kind tag 仍是 2；退役 tag 4、5、7 不被打开，旧数据库直接重建，不提供迁移或 fallback。

`OrderedMap::remove` 确认 key 存在性并返回结果；已有同事务存在性证据时可用 `erase` 直接暂存 tombstone。

`OrderedMap<K, NonZeroU64>` 以标准库正整数表示权重，value 是严格八字节 big-endian，零和其他长度是 codec 错误。其 `multiplicity` 把缺失读为零；`adjust` 逐事件 checked signed adjustment，underflow/overflow 毒化事务，结果零删除 key。`set_multiplicity` 直接写此前已逐事件检查的最终权重，零删除，不再点读。公共 `checked_weight` 提供同一纯算术检查；单独调用它不毒化事务。

`OrderedMap<PartitionKey<P, K>, V>` 使用普通 Map handle 与 catalog。`partition(&P)` 产生当前事务内的泛型 view，只处理 local key `K`，支持点读、写入、erase、有界 scan 和 first/last。分区 framing 将 P 的零字节 escape 为 `00 ff`，并以 `00 00` 终止，K 原样跟随，严格保持 `(P, K)` 字典序，空值、前缀和零字节不会跨分区。分区 view 的正权重方法复用 Map 的 codec 与 checked adjustment，不另设集合、metadata 或状态事实。

Queue 的只读 `front_bounded(max_value_bytes)` 返回当前 snapshot 的队首，写访问的 `pop_front_bounded(max_value_bytes)` 同时删除队首；两者都在复制或解码前检查编码长度，超限保持事务健康，可在同一 snapshot 或事务提高上限重试。Queue 每项按完整 encoded value 加八字节私有 sequence 计费；空队列也拒绝超大项，队列变空删除 metadata 并重置编号。owner 在每次 `try_push` 传入稳定容量策略，不保存进 metadata；容量不包含 `RocksDB` 开销。`Queue<Vec<u8>>::discard_front(max_entries)` 有界读取长度、验证连续性并暂存删除，不复制或解码完整 value，末尾一次更新 metadata。先只读队首、后事务消费时，owner 必须保持唯一协调消费者；只读访问不预留条目。

`Cell<T>::get_bounded(max_bytes)` 通过 pinned lookup 在复制前检查 encoded value 长度；超限返回 `ItemTooLarge`，不毒化事务。

Cell 与 Map 的 `get` 复用 `get_bounded(..., usize::MAX)`；有界与无界读取共享 owned decode、snapshot 和事务中毒语义。

## 有序分页

`OrderedMap` 及其 partition view 的 scan 同时限制条目数和编码后的逻辑字节数。
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

底层使用 `RocksDB` `OptimisticTransactionDB`。普通 `Transactions::begin` 产生的写事务使用 WAL 并同步提交；
没有暂存写入的健康事务在 `commit` 时直接完成，不进入 `RocksDB` 写队列或同步 WAL。
`Transaction::commit` 成功后，整笔修改一起持久化；未 commit 的事务被丢弃时全部回滚。

Store 启用 `RocksDB` 的 manual WAL flush。普通同步事务仍在成功返回前写出并同步自己的 WAL；需要让多笔独立
事务共享 WAL write 与持久化等待的协调者可以显式创建 `DurabilityBatch`。batch 内每笔事务仍原子提交、启用 WAL
并立即对后续 snapshot 可见，但 WAL record 先留在 `RocksDB` 的进程内 buffer，由 `sync` 或最终 `finish` 统一写出并
同步到磁盘。
在 barrier 完成前不得执行依赖这些提交的外部效果，也不得把成功返回给上层。无 pending write 的 barrier 不进入
`RocksDB`。一次有写的 commit 即使返回错误也保留 pending，因为底层结果可能不确定，owner 返回前仍须尝试最终
barrier。barrier 失败表示这一组提交的持久化结果不确定，owner 必须 fail-stop 并从磁盘重新打开，不能继续复用。

以下错误会使当前事务中毒：编码或解码失败、损坏的 metadata、使用另一个 Store 的 handle、RocksDB 访问失败、
checked weight underflow/overflow。之后的访问返回
`StoreError::TransactionPoisoned`，写事务不能提交。

`StoreSetup::commit` 会消费 draft。初始化闭包确定失败时会留下没有有效 marker 的不完整目录，不能作为 Store 打开；
底层 `RocksDB` commit 返回存储错误时结果可能不确定，只能通过 reopen 判断。Flow 运行期遇到不确定提交则进入
fail-stop，并要求重新打开 Flow。Store 不用额外日志去猜测一次不确定提交的结果。

`Cell`、Map 和 `Queue` handle 可以 clone，但每次访问都检查它属于当前事务所在的 Store；持久 queue 的 owner 必须保证唯一协调消费者。

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

`Queue` metadata 保存私有 head/tail 与 retained bytes；最后一项删除后清除 metadata，编号不对外公开。分区和正权重是 Map key/value codec 语义，无独立 metadata 或 collection kind。

## 读代码的顺序

1. [`src/store/mod.rs`](src/store/mod.rs)：`Store`、事务和 access 类型。
2. [`src/store/transaction.rs`](src/store/transaction.rs)：snapshot、commit 与中毒规则。
3. [`src/collections/`](src/collections/)：三种结构的公共语义。
4. [`src/store/data.rs`](src/store/data.rs)：catalog、namespace 和底层读写入口。
5. [`src/codec.rs`](src/codec.rs)：稳定 key/value 编码契约。

## 验证与性能

公共 correctness target 覆盖事务、snapshot、三种结构、reopen、raw layout、损坏拒绝、中毒回滚和 SIGKILL
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

### 有界点读与分区端点

`OrderedMapAccess::get_bounded` 与只读 view 的同名方法按 encoded value 长度准入，不计 caller 已持有的 lookup key；读取 pinned bytes 时先检查长度，再复制和解码。`MapPartition::first_bounded/last_bounded` 与只读 partition 同名方法按完整 partition framing、key、value 的 encoded bytes 准入，等同一项有界 scan。超限返回可重试的 `ItemTooLarge`，不毒化事务；端点零 byte limit 返回 `InvalidScanLimit`。类型化 codec 的 storage/encoding/decoding 错误仍毒化事务。这些是返回 payload 的逻辑界，不限制 `RocksDB` page cache、I/O 时间或自定义 codec 内部任意分配。
