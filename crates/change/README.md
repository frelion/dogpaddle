# dogpaddle-change

这个 crate 定义 DogPaddle 中流动的数据。第一次读代码时，只需要先记住一句话：

> 一个 `Change` 是一批**有顺序的增删行**。

它用 Arrow `RecordBatch` 保存数据列，再用一列 `Int64` 保存每行的变化量。正数表示增加，
负数表示撤回。

| 行位置 | `id` | `name` | diff | 含义 |
| ---: | ---: | --- | ---: | --- |
| 0 | 7 | Alice | `+1` | 增加一份这条记录 |
| 1 | 7 | Alice | `+2` | 再增加两份 |
| 2 | 7 | Alice | `-1` | 撤回一份 |

如果此前权重为零，依次应用这三个事件后，记录的权重是 `2`。diff 可以大于一，因为
DogPaddle 维护的是带整数权重的关系，而不只是普通的插入和删除消息。

## 最小用法

```rust
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;

let schema = Arc::new(Schema::new(vec![Field::new(
    "name",
    DataType::Utf8,
    false,
)]));
let records = RecordBatch::try_new(
    schema,
    vec![Arc::new(StringArray::from(vec!["Alice", "Alice"]))],
)?;

let change = Change::try_new(records, Int64Array::from(vec![1, -1]))?;

assert_eq!(change.num_rows(), 2);
assert_eq!(change.diffs().value(0), 1);
# Ok::<(), Box<dyn std::error::Error>>(())
```

构造成功后有四个保证：

- 至少有一行；Operation 没有输出时返回 `None`，不构造空 `Change`。
- 记录和 diff 行数相同。
- diff 不为 null，也不为零。
- 记录使用 DogPaddle v1 支持的精确 Schema。

`Change` 不检查撤回是否合法。例如第一条事件就是 `-1`，仍然可以构造 `Change`。它不知道
应用前的关系状态；`Distinct`、`Aggregate`、Join 和 Sink 等真正维护关系的组件会检查权重不能
降到零以下，并在失败时回滚整个事务。

## 顺序为什么属于语义

下面两批数据的最终净变化都是零，但它们不是同一个事件序列：

```text
A +1, A -1
A -1, A +1
```

第二个序列可能在第一步就因非法撤回而失败。因此 `Change` 不排序、不去重，也不把相同记录的
diff 提前相加。Operation 必须从第零行开始依次观察。

如果只比较一条合法事件流展平后的关系结果，大批次可以拆成多个 `Change`，多个小批次也可以合并，
前提是行与 diff 的顺序完全相同。但 `Change` 同时是一次事务、确认和重试的输入单位；重批会改变哪些
前缀可以先提交，遇到非法撤回等错误时也会改变失败边界，因此运行层不能静默重批后声称事务行为不变。
日志 offset 加行号只是当前分批方式下的位置，不能当作长期稳定的事件 ID。

`Change` 也没有事件时间、watermark、来源 offset 或已物化关系。它只回答：这批记录按什么顺序，
各自增加或撤回多少。

## Schema 是完整契约

`Change::schema()` 返回记录列的 logical Arrow Schema。物理 diff 列不在这个 Schema 中。字段的
顺序、名称、类型、nullability、嵌套结构以及 Schema/Field metadata 都参与 identity；这些内容
有任何不同，就是不同的 Schema。

当前 v1 支持：

| 类别 | 类型 |
| --- | --- |
| 标量 | Null、Boolean、8/16/32/64 位有符号与无符号整数、Float32、Float64 |
| 字节与文本 | Utf8、Binary |
| 时间与数值 | Date32、四种精度的 Timestamp、Decimal128 |
| 嵌套 | List、Struct，最多嵌套 60 层 |

同一个 Schema 或 Struct 作用域内不能有重名字段。以 `$dogpaddle.` 开头的字段名和以
`dogpaddle.` 开头的 metadata key 留给物理协议使用。

Timestamp 保留可选 timezone 字符串，但拒绝空字符串；`None` 表示无时区。Decimal128 precision
必须在 `1..=38`，正 scale 不能超过 precision。构造和完整解码还会检查每个 non-null 物理值确实
落在声明的 precision 内。这一层只验证表示是否合法，不定义时区换算、舍入或算术规则。

需要只验证 Schema 时，调用 [`validate_schema`]。

## 两种轻量操作

### 切片

`try_slice(offset, length)` 产生一个保持顺序的非空子段，并共享原来的 Arrow buffer。它适合把
一批工作切成更小的内存视图；长度为零或越界会返回错误。

### 顶层投影

[`ChangeProjection`] 绑定到一个精确输入 Schema，只能按原顺序保留一部分顶层字段。索引必须
严格递增，所以它不能重排或复制列。空投影合法：结果仍保留原行数和 diff，只是没有记录列。

```rust
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{
    Change, ChangeProjection, decode_change_projected, encode_change,
};

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::UInt64, false),
    Field::new("payload", DataType::UInt64, false),
    Field::new("tail", DataType::UInt64, false),
]));
let records = RecordBatch::try_new(
    Arc::clone(&schema),
    vec![
        Arc::new(UInt64Array::from(vec![7])),
        Arc::new(UInt64Array::from(vec![8])),
        Arc::new(UInt64Array::from(vec![9])),
    ],
)?;
let change = Change::try_new(records, Int64Array::from(vec![1]))?;
let projection = ChangeProjection::try_new(schema, [0, 2])?;

let in_memory = change.try_project(&projection)?;
let encoded = encode_change(&change)?;
let from_ipc = decode_change_projected(&encoded, &projection)?;
assert_eq!(in_memory.records(), from_ipc.records());
# Ok::<(), Box<dyn std::error::Error>>(())
```

内存投影只重组 Schema 和 `ArrayRef`，不会复制选中的 Arrow 数据。选择性 IPC 解码会跳过未选字段
的值区，但仍验证整个消息结构以及字段和 buffer 描述。它减少解码和分配，不会改变已经写入的
日志大小，也不承诺 RocksDB 或设备层面的字段级 I/O。未选字段的 UTF-8、List offset、Decimal
value 等值级约束不会被读取和验证；需要审计全部内容时使用 [`decode_change`]。

## 在系统中的位置

```text
Operation ──产生/消费──> Change
Flow      ──编码并路由──> 每条 Station output log
Store     ──只保存──────> Vec<u8>
```

这个 crate 不依赖 Operation、Flow 或 Store。反过来，Operation 用内存中的 `Change` 表达输入输出；
Flow 把它编码后放入持久日志；Store 看到的只是字节。这个依赖方向让 Arrow 数据契约不需要知道
事务、拓扑、subscriber 或 RocksDB key。

## 持久化格式

[`encode_change`] 把每个 `Change` 写成一条完整、自描述的标准 Arrow IPC Stream：

```text
Schema message
  └─ $dogpaddle.diff: non-null Int64
  └─ 所有 logical fields
RecordBatch message/body（恰好一个非空 batch）
canonical EOS
```

没有额外的 DogPaddle envelope，也不依赖日志外部的 Schema。标准 Arrow reader 可以读取这条
Stream；[`decode_change`] 只凭一条 entry 的字节恢复完整记录、diff 和顺序。调用方已经拥有编码
字节时，[`decode_change_owned`] 可以继续共享满足对齐要求的 Arrow body 分配。

物理 Schema 的第零字段固定为 non-null Int64 `$dogpaddle.diff`，随后是完整 logical fields；
Schema metadata 固定包含 `dogpaddle.kind = change` 和 `dogpaddle.change.version = 1`。

写入端固定使用 Metadata V5、8 字节对齐、非 legacy framing 和无压缩。decoder 会拒绝错误 marker、
大端、压缩、多个 batch、非 canonical EOS、尾随字节以及不合法的 DogPaddle Schema。writer options、
物理 diff 布局、允许的 Arrow 类型和行序都是 v1 持久化边界。

canonical 约束 framing、EOS、writer options，以及有序且唯一的 metadata key；decoder 不要求把
输入重新编码后逐字节相等。`encode_change` 的确定性输出和 golden bytes 是写入端基准。

这是开发期 v1。修改物理 diff 布局、Schema marker、writer options、允许类型或解码规则时，应同步更新
golden 和 reopen 证据并重建旧 Flow，不增加旧格式迁移或兼容分支。

运行时每个日志 entry 恰好保存一个完整 Change Stream。多个订阅者可以对同一 entry 使用不同投影，
最慢订阅者决定 entry 何时回收；这些属于 Flow 和 Store 的职责。

## 从哪里继续读

建议按这个顺序：

1. [`src/change.rs`](src/change.rs)：核心不变量、切片和投影入口。
2. [`src/schema.rs`](src/schema.rs)：v1 Schema 边界。
3. [`src/projection.rs`](src/projection.rs)：精确 Schema 绑定的顶层投影。
4. [`src/codec/`](src/codec/)：一条 Change 如何变成 Arrow IPC Stream。
5. [`tests/correctness/`](tests/correctness/)：构造、Schema、投影和损坏输入的公共证据。

## 验证与性能

```bash
cargo test -p dogpaddle-change
cargo clippy -p dogpaddle-change --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-change --no-deps
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-change --bench change_core
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-change --bench change_codec
```

工作区测试分层见 [`TESTING.md`](../../TESTING.md)，Change 单体 benchmark 的 workload 与结果解释见
[`PERFORMANCE.md`](PERFORMANCE.md)。真实的 `Change + SubscribedLog<Vec<u8>>` 接缝由
[`integration-tests/change-store/`](../../integration-tests/change-store/) 从公共 API 验证。
