# dogpaddle-change

这个 crate 定义 `DogPaddle` 中流动的数据。第一次读代码时，只需要先记住一句话：

> 一个 `Change` 是一批**有顺序的增删行**。

它用 Arrow `RecordBatch` 保存数据列，再用一列 `Int64` 保存每行的变化量。正数表示增加，
负数表示撤回。

| 行位置 | `id` | `name` | diff | 含义 |
| ---: | ---: | --- | ---: | --- |
| 0 | 7 | Alice | `+1` | 增加一份这条记录 |
| 1 | 7 | Alice | `+2` | 再增加两份 |
| 2 | 7 | Alice | `-1` | 撤回一份 |

如果此前权重为零，依次应用这三个事件后，记录的权重是 `2`。diff 可以大于一，因为
`DogPaddle` 维护的是带整数权重的关系，而不只是普通的插入和删除消息。

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
- 记录使用 `DogPaddle` v1 支持的精确 Schema。

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
前提是行与 diff 的顺序完全相同。运行层可以按明确的页界分批提交；这会改变哪些前缀可以先提交，以及非法撤回等错误的回滚边界。
`Change` 本身不承诺整批事务或外部确认，具体边界由调用它的执行层定义。
日志 offset 加行号只是当前分批方式下的位置，不能当作长期稳定的事件 ID。

`Change` 也没有事件时间、watermark、来源 offset 或已物化关系。它只回答：这批记录按什么顺序，
各自增加或撤回多少。

有效事件流要求每条记录的「应用前权重 + 已处理前缀累计 diff」非负，维护关系的组件负责报错与回滚。
Aggregate 不保留完整输入行，只按分组与调用参数检查被跟踪权重，具体例外由 [关系算子契约](../operation/docs/relations.md#aggregate) 定义。

每个端口稳定重批必须逐项保持展平事件序列；Operation 的展平输出和最终业务状态必须同时对稳定重批及同一 Change 的分页切分不变。
只有显式窗口、barrier 或 flush 才能引入额外语义边界。跨端口明确无序的算子只要求每端口子序列和最终关系不变，当前 `UnionAll` 采用此契约。
比较域内每种分批都必须能由声明 Arrow 类型表示。分页位置由 Operation 返回给调用方；Flow 将位置保存于调用帧，输入仍由 Source 队首或父调用的输出页拥有。

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

v1 还固定限制为最多 16,384 个顶层加嵌套字段、49,152 个 Schema/Field metadata entry，以及
8 MiB 的字段名、Timestamp timezone、metadata key/value 全局 UTF-8 字节总量。codec 构造时验证资源所有者提供的
Schema；entry 不携带另一份 Schema。batch metadata 仍以 64 MiB apparent-size ceiling 限制 `FlatBuffer` 的展开大小。

Timestamp 保留可选 timezone 字符串，但拒绝空字符串；`None` 表示无时区。Decimal128 precision
必须在 `1..=38`，正 scale 不能超过 precision。构造和完整解码还会检查每个 non-null 物理值确实
满足 `|unscaled| < 10^precision`。检查递归进入 List/Struct，祖先 null 不豁免物理 non-null child；List slice 只检查当前 offsets 可达的 child 区间。这一层只验证表示是否合法，不定义时区换算、舍入或算术规则。

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
use dogpaddle_change::{Change, ChangeProjection, SchemaBoundChangeCodec};

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
let codec = SchemaBoundChangeCodec::try_new(change.schema())?;
let encoded = codec.encode(&change)?;
let from_ipc = codec.decode(&encoded)?.try_project(&projection)?;
assert_eq!(in_memory.records(), from_ipc.records());
# Ok::<(), Box<dyn std::error::Error>>(())
```

内存投影只重组 Schema 和 `ArrayRef`，不会复制选中的 Arrow 数据。List/Struct 按完整子树选择。
IPC 解码始终验证并恢复全部字段，再按需进行内存投影；没有跳过未选字段坏值的选择性解码入口。

## 在系统中的位置

```text
Operation ──产生/消费──> Change
Flow      ──编码并路由──> 挂起调用的输出页
Store     ──只保存──────> Vec<u8>
```

这个 crate 不依赖 Operation、Flow 或 Store。反过来，Operation 用内存中的 `Change` 表达输入输出；
Flow 在需要跨事务保留数据时编码；Store 看到的只是字节。这个依赖方向让 Arrow 数据契约不需要知道
事务、拓扑或 `RocksDB` key。

## 持久化格式

[`SchemaBoundChangeCodec`] 在编解码前绑定资源所有者的精确 Schema。
每条 entry 保存固定身份和恰好一个非空 batch：

```text
DPCHB001                                      8-byte format/version marker
BLAKE3(canonical physical Schema message)   32-byte Schema fingerprint
RecordBatch message/body                     exactly one non-empty batch
canonical EOS
```

canonical physical Schema 的 non-null Int64 `$dogpaddle.diff` 在第零列，
其后是 logical fields，并包含固定 kind/version metadata。fingerprint 因此覆盖字段顺序、名称、类型、
nullability、嵌套结构以及全部 Schema/Field metadata。codec 在编码时要求 `Change` 的 logical Schema
逐项相等；解码时先比较 fingerprint，避免把物理 buffer 布局恰好相同但语义不同的 entry 错绑到当前
Schema。

绑定格式仍固定为 Metadata V5、8 字节对齐、非 legacy framing、无字典和无压缩，并复用完整 decoder
的 batch layout、值、diff 与 canonical EOS 检查。它不是自描述的标准 Arrow Stream；资源必须先可靠地
恢复 exact Schema，再构造 codec。[`SchemaBoundChangeCodec::decode_owned`] 对满足对齐要求的 IPC body
继续共享传入的 `Vec<u8>` 分配，[`SchemaBoundChangeCodec::encode_bounded`] 同时限制未压缩 body 与完整
entry。

绑定格式也是开发期 v1 持久边界。marker、fingerprint 输入、writer options、物理布局或 framing 变化
必须同步更新 bound golden/layout/reopen 证据并重建受影响资源，不增加旧格式兼容分支。

Source 队列、Flow 挂起页和 Sink outbox 都使用此格式。Flow 为 Source 的原始输出和融合段末端输出绑定 codec，
恢复输入时完整解码；子调用直接读取父调用保存的页。页的保留、容量和回收由其 owner 负责。

公开自描述 Arrow Stream 导入/导出、从 entry 发现 Schema，以及选择性 IPC 解码能力已经退役。
不提供 `StreamReader` fallback。现有 schema-bound marker、fingerprint 和 entry bytes 不变。
`encode_bounded` 写入前先计算逻辑 slice 的未压缩 body 大小，再由限长 writer 约束完整输出；超限返回容量错误。
完整零列 entry 有 literal golden；切片和时间/Decimal 的独立 golden 覆盖 `RecordBatch` 与 EOS 后缀。

## 从哪里继续读

建议按这个顺序：

1. [`src/change.rs`](src/change.rs)：核心不变量、切片和投影入口。
2. [`src/schema.rs`](src/schema.rs)：v1 Schema 边界。
3. [`src/projection.rs`](src/projection.rs)：精确 Schema 绑定的顶层投影。
4. [`src/codec/`](src/codec/)：一条 Change 如何变成绑定 Schema 的 IPC entry。
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
[`PERFORMANCE.md`](PERFORMANCE.md)。持久页的组合与恢复由 Flow 和 Operation 的 owner 测试验证。
