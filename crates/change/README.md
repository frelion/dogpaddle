# dogpaddle-change

`dogpaddle-change` 定义 `DogPaddle` 各执行层共享的有序 Arrow 变化契约。它不依赖 Flow、
Operation 或 Store：Operation 消费和产生 `Change`，Flow 负责路由与持久化，Store 只保存
不透明字节。

## Change

`Change` 把一个 Arrow `RecordBatch` 与一列 `Int64` diff 按行配对。第 `i` 个 diff 是第 `i`
行记录的权重变化；正数增加权重，负数撤回权重。构造时拒绝空 Change、行数不一致、null
diff、零 diff 和不支持的 Schema。

`Change` 没有事件时间，有稳定事件顺序，允许重复且不做 consolidation。行位置属于语义；
`Change` 本身、其 codec 和运行层都不能排序或隐式抵消事件。一个 `Change` 是有序变化流的
非空连续物理片段；没有输出时产生零个 `Change`。物理批次可以合并或切分，但重批前后展平的
事件序列必须逐项相同。

持久化后，`(AppendLog offset, Change row_index)` 只是当前分批下的单边遍历坐标，不是稳定
event ID。`Change` 不携带应用前的关系状态，因此允许以负 diff 开头，也不判断一次撤回
能否应用。事件生产者必须产生有效流；维护或物化关系的组件负责验证任意记录的应用前权重加
已处理前缀累计 diff 不得为负，并在失败时回滚。

```rust
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;

let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
let records = RecordBatch::try_new(
    schema,
    vec![Arc::new(StringArray::from(vec!["A", "A"]))],
)?;
let change = Change::try_new(records, Int64Array::from(vec![1, -1]))?;

assert_eq!(change.num_rows(), 2);
# Ok::<(), Box<dyn std::error::Error>>(())
```

当前递归支持 Null、Boolean、整数、Float32/Float64、Utf8、Binary、List 和 Struct；其他尚未
定义稳定语义的 Arrow 类型会被拒绝。字段顺序、名称、类型、nullability 和 metadata 都属于
Schema identity；同一 Schema 或 Struct 作用域内不允许重名，List/Struct 最多嵌套 60 层。
`$dogpaddle.` 字段名和 `dogpaddle.` Schema/Field metadata key 是保留命名空间。

## 投影

`ChangeProjection` 是绑定到精确 logical Schema 的顶层删列计划。字段索引必须严格递增，不能
重排或复制列；空投影与全量投影都合法。物理 `$dogpaddle.diff` 不属于逻辑索引且始终保留，
选择 List 或 Struct 时选择其完整子树，当前不做嵌套字段裁剪。

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

内存投影只重组 Schema 和 `ArrayRef`，共享原 Arrow buffer。选择性 IPC 解码先校验内嵌 Schema、
完整 framing，以及所有无需读取 payload 就能判断的 field node 和 buffer descriptor，再只复制
并解码 diff 与所选字段；结果是普通 owned `Change`，不借用日志 entry 或事务。

未选字段的 descriptor 仍必须合法，但其 UTF-8 内容、List offsets 等值级约束不会被读取或
验证；需要完整审计时使用 `decode_change`。投影减少 `Change` codec 对 Arrow body 的访问、
解码和 owned allocation，不改变写入内容、entry 大小或 `ScanLimit` 计费，也不保证 MDBX、
操作系统或存储设备产生字段级物理 I/O。

## 持久化编码

每个 Change 编码为一个标准、完整、自描述的 Arrow IPC Stream，不增加 `DogPaddle` envelope、
外部 Schema、fingerprint 或 segment：

```text
Arrow Schema message（$dogpaddle.diff + logical fields + format metadata）
Arrow RecordBatch message/body（恰好一个非空 batch）
Arrow canonical EOS
```

物理 Schema 的第零字段固定为非 null Int64 `$dogpaddle.diff`，后续字段原样保存 logical
record Schema；Schema metadata 包含 `dogpaddle.kind = change` 和
`dogpaddle.change.version = 1`。标准 Arrow reader 可以直接读取这条 Stream，`decode_change`
也只凭单个 entry 的字节恢复完整 Change 和事件顺序。

writer 固定使用 Metadata V5、8 字节对齐、非 legacy framing 和无压缩。decoder 在交给 Arrow
前预检 message 长度和 entry 边界，并要求 V5、小端、无压缩、无 Schema feature、恰好一个
batch、canonical EOS 且无尾随字节。Arrow IPC version、writer options、Schema marker、物理
diff 布局、允许的 Arrow 类型和行序都是持久化兼容性边界。

这里的 canonical 限定的是消息 framing、EOS 和 writer options，并不表示 decoder 会把输入重新
编码后逐字节比较，也不要求每个逻辑 `Change` 只有一种可接受的字节表示。符合 Arrow framing
且不改变语义的 `FlatBuffer` 布局、默认值或 body padding 内容变体可能被接受；
`encode_change` 的确定性输出及其黄金字节才是 `DogPaddle` 写入端的持久化基准。

运行层可以用 `AppendLog<Vec<u8>>` 保存完整 Stream，每个日志 entry 恰好对应一个 Change；
同一 entry 可供不同消费者独立投影。`dogpaddle-change` 不实现 Store collection，Store 也不
依赖 Arrow。

## 验证

```bash
cargo test -p dogpaddle-change
cargo clippy -p dogpaddle-change --all-targets --no-deps -- -D warnings
cargo doc -p dogpaddle-change --no-deps
cargo bench -p dogpaddle-change --bench change_core
cargo bench -p dogpaddle-change --bench change_codec
```

正确性分层、fixture 所有权和负向测试口径见工作区
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)，Change 单体 benchmark
的 workload 与结果解释见
[`PERFORMANCE.md`](https://github.com/frelion/dogpaddle/blob/main/crates/change/PERFORMANCE.md)。真实
`Change + AppendLog<Vec<u8>>` 正确性和性能属于工作区下游
`integration-tests/change-store/`，不由本 crate 的测试依赖 Store。
