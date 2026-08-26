# dogpaddle-store

`dogpaddle-store` 是 `DogPaddle` 内部的事务状态层。它基于 MDBX 提供具名的类型化数据对象、
原子事务和稳定编解码器，为 Flow 运行进度与 Operation 状态提供统一的持久化边界。该 crate
是引擎实现模块，不是最终面向用户的通用存储产品。

## 能力分层

Store 将资源装配与运行时访问分离：

- `Store` 创建或打开全部具名数据对象；进入运行期前，其所有权会被消费；
- `Cell<T>` 保存一个可选的类型化值，并固定使用共享物理空间；
- `OrderedMap<K, V, SIZE>` 保存按稳定 key codec 排序的类型化映射；
- `AppendLog<T>` 保存具有稳定 offset、支持有界前缀回收的追加序列，并固定使用独立物理表；
- `Transactions` 是运行期内启动事务的唯一能力；
- `Transaction` 持有一个原子提交边界，并在被丢弃时回滚；
- `TransactionAccess` 从活动 Transaction 临时借用，只允许已有类型化数据对象绑定访问；
- `CellAccess`、`OrderedMapAccess` 与 `AppendLogAccess` 绑定一次具体事务并执行实际读写。

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

    let mut transactions = store.into_transactions();

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

    let transaction = transactions.begin()?;
    let access = transaction.access();
    let counter = counter.access(access)?;
    let users = users.access(access)?;
    let changes = changes.access(access)?;
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
let mut transactions = store.into_transactions();
let transaction = transactions.begin()?;
let access = transaction.access();
assert_eq!(counter.access(access)?.get()?, Some(1));
# Ok(())
# }
```

`StoreData` 是 Store 泛型 create/open 使用的 sealed marker trait。它只由布局完整的内建
collection 实现；一般业务代码不需要直接引用它。

## 事务与扫描语义

丢弃事务会触发回滚。`Transaction` 没有显式中止方法，也不包含集合专用操作。调用
`Transaction::access()` 会得到可复制但不能提交的 `TransactionAccess`；它只能让调用方已经
持有的 `Cell`、`OrderedMap` 或 `AppendLog` 创建事务级 Access。Flow/Stage 因此可以保留 Transaction
所有权并把受限能力交给 Operation，Operation 无法开始或结束原子边界，也无法访问 Store
catalog。通过同一能力创建的所有访问值共享事务快照，因此任意数量、任意固定或显式选择布局
的数据对象都可以原子提交。

`TransactionAccess`、`CellAccess` 和 `OrderedMapAccess` 都不能脱离所属事务；每个新事务都
必须重新绑定，不能跨 Stage 步骤缓存。能力及访问值仍然是线程绑定的，不能跨线程移动正在
执行的事务。

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

`AppendLogAccess::scan` 使用与 map 相同的 `ScanLimit`，每项按八字节 offset key 加完整编码
value 计费。它不会预先构造完整的 `T`，而是把短生命周期的 `AppendLogEntry` 逐条交给
callback：`project` 可以只解码 diff 或少数列，`decode_owned` 只在确实需要完整值时执行，
`append_entry` 则能把相同 `T` 的编码原样写入同一事务中的另一个 log。Entry 不公开 MDBX
借用或裸字节，也不能逃出 callback 或跨线程。

一个 scan 在调用 callback 前先验证选中 offset 连续。callback 的任意错误都会毒化事务，
避免已经写入部分输出后仍被提交；第一项无法装入 byte limit 的 `ItemTooLarge` 仍是可增大
limit 后重试的软错误。`AppendLogScan::next_offset` 可直接持久化为下游 next-unread cursor，
`caught_up` 表示本批已经追到 scan 开始时捕获的 tail。

`truncate_before(target, max_items)` 只删除 `target` 以下且当前仍保留的连续前缀，并限制单次
删除条数。删除与 head 推进处于同一个 MDBX 事务；调用方应以所有下游 cursor 的最小值作为
target，并分批提交 GC。

持久布局固定为一个独立表：空 key 保存 16 字节 big-endian `head || tail`，八字节
big-endian offset key 保存 `T` 的稳定编码。全新且从未使用的空表可以没有 metadata；一旦
使用，即使回收到空区间也保留单调的 head/tail。该布局和 `T` 的 codec 都属于调用方需要
稳定维护的持久化 schema。

## 测试

全部公共行为与持久化契约使用一个按语义分模块的外部正确性 target：

```bash
cargo test -p dogpaddle-store --test correctness
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
所有权、模块职责和完整验证协议见 [`TESTING.md`](./TESTING.md)。

## 性能

Store 有四个按数据对象与运行目的隔离的 release 基准入口：

```bash
cargo bench -p dogpaddle-store --bench cell
cargo bench -p dogpaddle-store --bench ordered_map
cargo bench -p dogpaddle-store --bench append_log
cargo bench -p dogpaddle-store --bench append_log_endurance
```

按 `Cell`、`Small OrderedMap`、`Large OrderedMap` 和 `AppendLog` 分别整理的本机基线、读法与
设计结论见 [`PERFORMANCE.md`](./PERFORMANCE.md)。基准同时输出人类可读表格与逐样本 JSON，
并记录实际 rustc、CPU、OS/kernel、git 状态、文件系统和运行档位；输出及计时口径见
[`TESTING.md`](./TESTING.md#性能-target)。

`cell` 独立覆盖同事务 warm get，以及每次读取、更新并 durable commit 的状态事务。
`ordered_map` 为 `Small` 与 `Large` map 生成成对样本，覆盖 byte map 与业务类型 map 的批量写入、
热点读取、升序与降序扫描、持久化覆盖写入、类似 Stage 的多集合事务，以及存在多个 `Small`
后台命名空间时的预热读取。扫描还单独覆盖固定宽度 `u64` 完整解码，以及 8 KiB value 的完整
解码与单字段投影；后者以交错配对样本直接报告 projection 的收益。两种 map 使用各自独立的具名
数据对象；byte map 的类型是 `OrderedMap<Vec<u8>, Vec<u8>, SIZE>`，不依赖私有裸句柄。可通过
`DOGPADDLE_BENCH_ENTRIES`、`DOGPADDLE_BENCH_COMMITS`、`DOGPADDLE_BENCH_SAMPLES`、
`DOGPADDLE_BENCH_BACKGROUND_NAMESPACES`、`DOGPADDLE_BENCH_SCAN_ITEMS`、
`DOGPADDLE_BENCH_SCAN_BYTES` 与 `DOGPADDLE_BENCH_WIDE_SCAN_ENTRIES` 调整它。扫描页同时受
item 和 byte limit 约束；默认 byte budget 为 4 MiB，所以 8 KiB wide workload 的实际页大小
会先被 byte limit 限制。
Cell 的读取次数由 `DOGPADDLE_BENCH_CELL_READS` 控制；commit 数与样本数复用
`DOGPADDLE_BENCH_COMMITS` 和 `DOGPADDLE_BENCH_SAMPLES`。

### `AppendLog` 场景基准

`append_log` 使用 `[diff: i64][key: u64][payload]` 的 CDC 记录，配置的 record bytes 是包含
16 字节头部在内的精确稳定编码大小。它不只测孤立 API，而是覆盖 Store 在差分 Stage 中承担的
实际工作：

- 对 128 B、1 KiB 与 8 KiB 记录分别测试已编码值 append、业务 codec append、只投影 diff
  的扫描和完整解码扫描；
- 对同一批 typed value 成对交错测试逐条 `append` 与 `append_batch`，分别报告只计追加事务体
  的 rollback workload 和包含一次 durable commit 的总耗时；
- Source 按 1、64 与 1024 条一批开启事务、append 并 durable commit，显示提交摊销；
- Count Stage 在一个事务内从自己的 `Small OrderedMap<Vec<u8>, Vec<u8>>` 读取输入 cursor，
  从 log 投影 diff，更新 `Cell<i64>`，推进 cursor 并提交；
- 直通与 50% filter Stage 在同一个事务内扫描输入、使用 `append_entry` 原样转发编码、推进
  cursor 并提交；filter 同时对比 key 投影和完整值解码；
- 一个共享 log 被 1 个或 4 个下游读取时，每个下游持有独立的 Stage state map 与 cursor，并按
  单线程调度顺序分别完成扫描、推进和提交；这里不会为 fan-out 复制多份输入；
- prefix GC 按固定条数删除并提交；steady workload 先保留一个非空窗口，再交替 append 新批次和
  回收等量旧前缀，用于观察非零 offset、长期页复用及分批提交的组合成本。

每个会改变数据的样本都在所选 benchmark base 下使用新 Store，避免前一个样本的 tail、空闲页或
文件尺寸污染后一个样本。资源创建、输入构造、预填充和结果校验均在计时外；需要反映完整事务的
durable/production workload 把 begin、全部相关 Store 访问和 commit 一起计时。明确标为 rollback
body 的配对场景只计追加 body，事务准备和回滚均在计时外。只读扫描复用已填充 Store，明确测
warm-cache；每项先执行一次不计入结果的预热，再报告样本的最小值、中位数和最大值。

`records/s` 表示该 workload 处理的逻辑记录数。`encoded MiB/s` 使用完整输入编码大小计算：多下游
读取按 delivery 次数累计，filter 按检查的输入累计，GC 按回收的逻辑记录累计；它不是 MDBX 的
物理写放大或磁盘带宽。

默认配置可通过以下环境变量覆盖：

- `DOGPADDLE_BENCH_LOG_ENTRIES`：宽度、Stage、fan-out、GC 和 steady 样本的记录数，默认
  10,000；
- `DOGPADDLE_BENCH_COMMITS`：Source workload 最多执行的 commit 数，默认 1,000，避免 batch=1
  遮蔽其余样本；
- `DOGPADDLE_BENCH_SAMPLES`：计入统计的样本数，默认 9；
- `DOGPADDLE_BENCH_LOG_RECORD_BYTES`：逗号分隔的记录宽度矩阵，默认 `128,1024,8192`；
- `DOGPADDLE_BENCH_LOG_SOURCE_BATCH_ITEMS`：逗号分隔的 Source 批量矩阵，默认
  `1,64,1024`；
- `DOGPADDLE_BENCH_LOG_STAGE_RECORD_BYTES`：事务场景使用的记录宽度，默认 1024；
- `DOGPADDLE_BENCH_LOG_STAGE_BATCH_ITEMS`：Stage 与独立扫描的批量上限，默认 1024；
- `DOGPADDLE_BENCH_LOG_GC_ITEMS`：每次 GC 最多删除的条目数，默认 1024；
- `DOGPADDLE_BENCH_LOG_READERS`：逗号分隔的下游数量，默认 `1,4`。

### `AppendLog` 长稳与空间复用基准

`append_log_endurance` 是刻意与日常场景基准分开的长稳入口。对每种记录宽度，它先在计时外填满
固定保留窗口，然后持续执行两次独立的 durable transaction：`append_batch` 一批，随后
`truncate_before` 等量旧前缀。默认 `smoke` 档每种宽度累计 8 MiB、保留约 2 MiB；`full` 档维持
原协议的每种宽度 1 GiB/64 MiB，并强制使用显式 reference 文件系统。因此它观察的是持续运行中
的页复用和尾延迟，而不是无限增长日志的顺序写峰值。

输出分别报告 append transaction 与 GC transaction 的 p50、p95、p99 和最大延迟、只累计这两类
事务的协议吞吐、实际 wall time，以及 MDBX data file 的逻辑大小、文件系统已分配字节、峰值、相对
保留 payload 的空间放大和后半程空间波动。逻辑大小可能包含稀疏区间，因此在 Unix 上空间判断应以
已分配字节为主；非 Unix 平台无法读取 block allocation 时退化为逻辑文件大小。

每个宽度结束后，基准会关闭事务环境、重新打开 Store、校验持久化的 `[head, tail)`，再逐条扫描整个
保留窗口并验证 offset、diff、key、记录长度和 payload。校验不计入协议延迟。这个入口不设置依赖
机器和文件系统的性能阈值；它提供可复现的测量与正确性断言，回归判定应在相同环境中对比。

长稳 workload 可通过以下环境变量调整：

- `DOGPADDLE_STORE_ENDURANCE_RECORD_BYTES`：逗号分隔的完整记录宽度，默认
  `128,1024,8192`；
- `DOGPADDLE_STORE_ENDURANCE_LOGICAL_MIB`、`_WINDOW_MIB`、`_BATCH_MIB` 与
  `_CHECKPOINT_EPOCHS`：覆盖所选 workload 档位；
- `DOGPADDLE_STORE_ENDURANCE_MAX_WORKING_SET_BYTES` 与 `_MAX_TOTAL_WRITTEN_BYTES`：创建
  Store 前执行的硬预算。

四套基准都使用 MDBX durable sync 和单线程执行。默认 `smoke` 可使用临时 base；正式回归必须设置
`DOGPADDLE_STORE_BENCH_PROFILE=reference` 与绝对路径
`DOGPADDLE_STORE_BENCH_STORE_DIR`。结果描述当前机器、文件系统与 warm-cache 条件下的 Store
协议成本，不代表冷缓存、断电恢复、网络 CDC、Operation 动态分发或完整调度器的端到端吞吐。
这些场景只用于测量 Store 数据结构，不规定 Flow 将来如何选择 batch、事务或调度策略。
