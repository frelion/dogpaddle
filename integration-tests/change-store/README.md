# Change + Store 外部集成测试

这个 `publish = false` 的工作区 package 只拥有 `dogpaddle-change` 与
`dogpaddle-store` 的装配接缝。它模拟生产形态 `AppendLog<Vec<u8>>`：每个 value 恰好是一个
完整、独立、自描述的 Arrow IPC Change Stream。

它不验证 Change codec 私有实现，也不重复 MDBX、事务或通用 AppendLog 的底层不变量；这些
分别属于两个产品 crate。产品 crate 不得反向依赖本 package，`src/` 也只能放测试 fixture、
oracle 和 workload，不能形成新的产品抽象。

manifest 关闭自动 test/bench 发现，只显式声明一个 `correctness` target 和两套性能协议。

## 目录

- `tests/correctness.rs`：单一公共 API 集成 target；子模块按 entry、scan、sequence 和
  transaction 契约拆分。
- `src/fixture.rs`、`oracle.rs`、`store_fixture.rs`、`workload.rs`：正确性与性能共享的确定性输入、
  Store 装配和独立 oracle。
- `benches/change_append_log.rs`：有界正常路径，区分 rows/Change 与 Changes/transaction。
- `benches/change_append_log_endurance.rs`：固定 retained encoded-byte 窗口的 append、前缀回收、
  空间复用和 reopen 长稳协议。
- [`PERFORMANCE.md`](./PERFORMANCE.md)：计时边界、环境变量、原始样本和 reference 运行规则。

## 正确性

```bash
cargo test -p dogpaddle-change-store-integration
cargo test -p dogpaddle-change-store-integration --test correctness
```

当前契约覆盖：

- 一个完整 Stream 对应一个 entry，原始字节边界精确不变，同一日志允许异构 Schema；
- `append_entry` 原样转发，不重新编码；
- 选择性解码结果在事务结束、原 Store 释放、prefix truncate 和 reopen 后仍拥有全部 Arrow
  buffer；
- 稳定重批只改变 `(offset, row_index)`，展平事件序列逐项不变；
- `ScanLimit` 的 byte admission 精确包含 entry 字节与八字节 offset，并支持同事务重试；
- projection Schema mismatch 与损坏 Change 作为 `StoreError::Codec` 穿过 Store 边界并 poison
  事务；一个 callback 已写入的 output/cursor 也随之后的坏 entry 一起回滚。

## 性能

```bash
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
cargo bench -p dogpaddle-change-store-integration --bench change_append_log_endurance
```

默认 `smoke` 使用隔离临时目录。正式结果必须显式选择 reference runner 的文件系统：

```bash
DOGPADDLE_CHANGE_STORE_BENCH_PROFILE=reference \
DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR=/absolute/path/on/reference/filesystem \
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
```

长稳的 `full` workload 还需要同时设置
`DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE=full`。所有 fixture、预热、严格结果比较和临时目录
清理都在计时外。stdout 中每个以 `{` 开头的行都是可筛选为 JSONL 的 environment、configuration、
sample 或 summary record；正式运行必须保存这些原始记录，不能只复制控制台摘要。

全工作区的测试所有权、目录约束与统一数据规格见根目录 [`TESTING.md`](../../TESTING.md)。
