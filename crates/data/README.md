# dogpaddle-data

`dogpaddle-data` 定义 `DogPaddle` 各执行层共享的 Arrow 数据契约。它的库代码与正常依赖
不依赖 Flow、Operation 或 Store：Operation 消费和产生批量变化，Flow/Stage 负责路由与
持久化调用，Store 仍只保存不透明字节。

## Change

`Change` 将一个 Arrow `RecordBatch` 与一列 `Int64` 差分按行配对。第 `i` 个 diff 是第 `i` 行
记录的权重变化；正数增加权重，负数撤回权重。行位置是事件顺序：消费者必须从第零行开始
依次应用，不能交换两行。构造时拒绝空 Change、行数不一致、null diff、零 diff 和不支持的
Schema，但不排序、不 consolidation，也不以其他方式规范化事件。

`DogPaddle` 第一版把 Change 正式定义为：无事件时间、有稳定事件顺序、允许重复、未
consolidation 的 Arrow 批量差分。一个 Change 是有序变化流的一个非空连续物理片段；相同
记录可以出现多次，Data 不自动合并或抵消 diff；没有输出时产生零个 Change，而不是一个空
Change。

顺序约束的是整条边上的事件序列。一个 Change 内按 `row_index` 遍历；持久化到
`AppendLog<Vec<u8>>` 后，Change 之间按单调 `offset` 遍历，因此 `(offset, row_index)` 是当前
持久化分批下的单边遍历坐标，不是跨重批处理保持不变的 event ID。批次和日志 entry 边界仍是
物理传输、持久化与调度边界；未来 Stage 可以稳定地合并或切分批次，但变换前后展平的事件序列
必须逐项相同，不能重排、丢弃或隐式 consolidation。

例如，从空状态开始的 `+A, -A` 是合法顺序，而 `-A, +A` 不能被应用，因为第一个事件试图
撤回尚不存在的记录。更一般地，对任意记录和任意已处理前缀，应用前权重加该前缀的累计 diff
都不得小于零。`Change` 本身不知道应用前的关系状态，所以 Data 只负责保留该顺序，不能独立
判断一个负 diff 是否删得到。事件生产者必须遵守有效流契约；维护或物化相应关系的组件在其
验证边界按记录等价关系检查撤回，并在负权重前缀处报错、回滚，不能把撤回静默处理为 no-op。
其他 Operation 可以依赖上游已经产生有效流，不必各自复制整份关系状态。单个 Change 以负
diff 开头仍然可表示且可能合法，因为对应记录可能已由更早的日志前缀或已恢复状态加入。

```rust
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_data::Change;

let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
let records = RecordBatch::try_new(
    schema,
    vec![Arc::new(StringArray::from(vec!["A", "A"]))],
)?;
let change = Change::try_new(records, Int64Array::from(vec![1, -1]))?;

assert_eq!(change.num_rows(), 2);
# Ok::<(), Box<dyn std::error::Error>>(())
```

第一版递归支持 Null、Boolean、整数、Float32/Float64、Utf8、Binary、List 和 Struct；
Dictionary、Union、Map、View、RunEndEncoded 及其他尚未定义稳定语义的类型会被拒绝。
字段顺序、名称、类型、nullability 和 metadata 都属于 Schema identity。同一 Schema 或
Struct 作用域内不允许重名字段，List/Struct 最多嵌套 60 层；该上限由完整 IPC Stream
roundtrip 测试约束，而不只是内存 Schema 校验。

`$dogpaddle.` 字段名和 `dogpaddle.` Schema/Field metadata key 是物理协议的保留命名空间，
不能用于逻辑记录。

## 投影

`ChangeProjection` 表示绑定到一个精确逻辑 Schema 的顶层列删除计划。索引必须严格递增，因而
投影只能删列，不能重排列；空投影合法，表示记录侧不保留任何列，但仍保留原行数、事件顺序和
全部 diff。物理 `$dogpaddle.diff` 不属于逻辑索引，始终由 Data 隐式读取。选择 List 或 Struct
会选择它的完整子树，第一版不提供嵌套字段裁剪。

同一份投影计划用于两条路径：

```rust
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_data::{
    Change, ChangeProjection, decode_change_projected, encode_change,
};

let logical_schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::UInt64, false),
    Field::new("payload", DataType::UInt64, false),
    Field::new("tail", DataType::UInt64, false),
]));
let records = RecordBatch::try_new(
    Arc::clone(&logical_schema),
    vec![
        Arc::new(UInt64Array::from(vec![7])),
        Arc::new(UInt64Array::from(vec![8])),
        Arc::new(UInt64Array::from(vec![9])),
    ],
)?;
let change = Change::try_new(records, Int64Array::from(vec![1]))?;
let encoded = encode_change(&change)?;

let projection = ChangeProjection::try_new(logical_schema, [0, 2])?;

// 已在内存中的 Change：只重组 Schema 和 ArrayRef，底层 Arrow buffer 不复制。
let narrow = change.try_project(&projection)?;

// AppendLog entry 中的完整 IPC：只物化 diff 和所选逻辑字段。
let narrow = decode_change_projected(&encoded, &projection)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

内存路径返回普通的 owned `Change`，其 Arrow 引用可以独立于源值存活。IPC 路径先读取 entry
自带的 Schema，要求它与投影绑定的 Schema 完全相同；随后验证完整 Stream framing、全部 field
node 和 buffer descriptor 的数量、顺序与边界，将 diff 和所选字段的 buffer 复制到紧凑的新
Arrow body，再交给 Arrow 官方 decoder。未选字段的 payload 不会被 Data decoder 访问、复制或
物化，因此不同消费者可以对同一个完整 entry 使用各自的读取计划，而完整 Change 仍只需写一次。

少读也确定了验证边界：未选字段的 descriptor 仍必须合法，但其 UTF-8 内容、List offsets 等
值级约束不会被读取和验证；所选字段和 diff 仍执行完整 Arrow 解码与 `Change` invariant 校验。
需要审计全部字段内容时使用 `decode_change`。投影减少的是算子对 Arrow body 的访问、解码和
owned allocation；它不减少日志写入内容、entry 大小或 `ScanLimit` 的字节计费，也不承诺 MDBX
或操作系统提供字段级物理 I/O。投影只增加读取能力，不改变下面的持久化格式。

## 持久化编码

`encode_change` 把每个 Change 编码成一个标准、完整、自描述的 Arrow IPC Stream，不再增加
`DogPaddle` 二进制 envelope。Stream 的物理 Schema 把固定的非 null Int64
`$dogpaddle.diff` 放在第零列，后续列原样保存逻辑记录 Schema；Schema metadata 中保存
`dogpaddle.kind = change` 和 `dogpaddle.change.version = 1`。随后恰好写入一个非空
`RecordBatch`，按原始行序保存记录及其 diff，并以 canonical EOS 结束：

```text
Arrow Schema message（diff + logical fields + DogPaddle metadata）
Arrow RecordBatch message/body（恰好一个）
Arrow EOS
```

因此 `decode_change` 只凭一个 entry 的字节就能按相同行序恢复完整 Change；不需要独立 Schema
resource、外部 Schema、fingerprint 或有状态 codec。输出也是普通 Arrow IPC Stream，标准
Arrow reader 可以直接读取其物理 Schema 和 batch。版本 marker 为未来迁移保留分派位置；
Schema 自描述使日志 entry 可以被独立复制、检查、转发和重新打开。

IPC writer 固定使用 Metadata V5、8 字节对齐、非 legacy framing 和无压缩；物理 diff 布局、
Schema marker、writer options、允许的 Arrow 类型和完整 Stream 字节都是持久化兼容性边界。
decoder 在调用 Arrow reader 前先用原字节检查两条 message 的长度均落在 entry 内，再验证 V5、
小端、无压缩、无 Schema feature、非 legacy framing、恰好一个 batch、canonical EOS 且无尾随
字节，避免损坏长度触发超出 entry 大小的预分配。Schema 在每个 Change 中重复是本设计有意
接受的固定成本，由 Stage 的批量策略摊薄；第一版不在其外再增加 segment 层。

data crate 不实现 Store collection，只有持久化集成测试将 `dogpaddle-store` 用作开发依赖。
运行层以 `AppendLog<Vec<u8>>` 保存结果，每个 value 恰好是一条完整 Stream，Store 仍只处理
不透明字节。

本格式破坏性替换旧的逐行 `operation::data::Change` 和未接入 Flow 的中间 envelope 设计，
不保留兼容 decoder。现有 Flow 磁盘布局不受影响，因为它尚未创建边日志或接入运行时数据
通道。

## 验证

```bash
cargo test -p dogpaddle-data
cargo clippy -p dogpaddle-data --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-data --no-deps
```
