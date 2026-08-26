# dogpaddle-change 测试说明

本 crate 只拥有 `Change` 单体测试。任何同时依赖 Store 的测试都属于下游
`dogpaddle-change-store-integration` package；`dogpaddle-change` 的正常依赖和 dev-dependency
均不应引入 Store。

manifest 关闭自动 test/bench 发现，只显式声明一个公共 `correctness` target 和两个 benchmark，
避免新增文件静默改变工作区执行矩阵。

## 目录职责

- `src/codec/tests.rs`：需要访问私有 stream parser、batch layout 或 FlatBuffer builder 的白盒
  测试，包括结构化畸形输入和 no-panic probe。
- `tests/correctness.rs` 与 `tests/correctness/`：只通过公共 API 验证 Change、Schema、Projection、
  IPC round-trip、标准 Arrow reader、黄金字节和变形性质；`properties.rs` 保存带固定 seed 且失败
  时打印 seed 的轻量性质矩阵。
- `tests/fixtures/v1/`：写入端持久化黄金字节。fixture 只由确定性 encoder 生成，评审时按完整
  文件比较；畸形输入用可读 builder 构造，不堆积不透明二进制。
- `benches/change_core.rs`：构造、projection 创建、slice 和内存 projection。
- `benches/change_codec.rs`：encode、完整 decode，以及 diff-only、narrow、identity 选择性 decode。

公共测试不能依赖私有实现来计算预期值。编码互操作以标准 Arrow reader 为独立 oracle；选择性
解码以内存 projection 为 oracle；顺序和稳定重批以简单展平事件向量为 oracle。

benchmark target 与 `benches/support/` 只保留 Change 特有的 Arrow fixture、结果 oracle、尺寸预检、
场景顺序和人类可读表格。严格配置解析、主机指纹、持续时间统计和 typed JSONL record/writer 由
工作区内部的 `dogpaddle-bench-protocol` 提供；它不拥有 Change workload、计时边界或结果校验。

## 数据边界

正确性覆盖全部 v1 类型的真实值和 null 值，并特别覆盖：数值极值、浮点特殊值、7/8/9 与
63/64/65 行 bitmap 边界、空/多字节 Utf8、任意 Binary、null/empty List、nullable Struct、
非零 Arrow slice offset、零 logical column，以及 60/61 层嵌套边界。

decoder 负向测试按 Schema、framing、RecordBatch metadata、buffer descriptor 和 value body
分组。稳定的持久化边界匹配 `CodecError` 类别；“任何输入不 panic”使用独立 probe，不能让
`catch_unwind` 返回的错误冒充正常拒绝路径。

## 命令

```bash
cargo test -p dogpaddle-change
cargo test -p dogpaddle-change --test correctness
cargo test -p dogpaddle-change --release
cargo clippy -p dogpaddle-change --all-targets -- -D warnings
cargo doc -p dogpaddle-change --no-deps

cargo bench -p dogpaddle-change --bench change_core
cargo bench -p dogpaddle-change --bench change_codec
```

完整的跨 crate 所有权、性能口径和 reference 规则见[根目录统一测试协议](../../TESTING.md)。
