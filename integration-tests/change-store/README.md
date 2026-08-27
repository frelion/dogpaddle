# Change + Store 外部集成测试

这个 `publish = false` 的工作区 package 只拥有 `dogpaddle-change` 与
`dogpaddle-store` 的装配接缝。它模拟生产形态 `AppendLog<Vec<u8>>`：每个 value 恰好是一个
完整、独立、自描述的 Arrow IPC Change Stream。

它不验证 Change codec 私有实现，也不重复 MDBX、事务或通用 AppendLog 的底层不变量；这些
分别属于两个产品 crate。产品 crate 不得反向依赖本 package，`src/` 也只能放测试 fixture、
oracle 和 workload，不能形成新的产品抽象。

manifest 关闭自动 test/bench 发现，只显式声明一个 `correctness` target 和两套性能协议。

benchmark 作为 dev target 使用零产品依赖的 `dogpaddle-bench-protocol`：共享 crate 只负责
严格 profile/环境变量解析、rustc/CPU/git/文件系统指纹、持续时间统计与 typed JSONL。
本 package 仍拥有 Change fixture/oracle、Store 生命周期、workload 字段、计时边界、严格结果
比较和人类可读摘要；共享协议不吸收 Change 或 Store 语义。

## 目录

- `tests/correctness.rs`：单一公共 API 集成 target；子模块按 entry、persona、scan、sequence 和
  transaction 契约拆分。
- `src/persona/`：稳定 workload descriptor、确定性 Change 生成器与有效 churn relation model；
  `fixture.rs`、`oracle.rs`、`store_fixture.rs`、`workload.rs` 保留较小的接缝 fixture 和 oracle。
- `benches/change_append_log.rs` 与同名子目录：有界 anchor、逐轴 sweep、分页 replay、reopen 和
  output/cursor durable pipeline；入口只负责装配，case、fixture、measure、oracle、report 分开。
- `benches/change_append_log_endurance.rs` 与同名子目录：homogeneous control 和 heterogeneous
  consumer pipeline 的变长 retained encoded-byte 窗口、空间复用与周期 reopen。
- `benches/support/mod.rs`：两个 target 共用的本地 `BenchStoreRoot`/`SampleStore`、Change 解码适配
  与 typed environment/JSONL 输出薄层。
- `benches/support/regular.rs`：只由 `change_append_log` 编译，拥有普通路径的 `SampleWork`、
  checked workload 运算、投影解码适配、typed configuration/sample/summary 与人类摘要。
- [`TESTING.md`](./TESTING.md)：persona、有效 diff model、correctness 矩阵与执行层级。
- [`PERFORMANCE.md`](./PERFORMANCE.md)：计时边界、环境变量、原始样本和 reference 运行规则。

## 正确性

```bash
cargo test -p dogpaddle-change-store-integration
cargo test -p dogpaddle-change-store-integration --test correctness
```

当前契约覆盖：

- 一个完整 Stream 对应一个 entry，原始字节边界精确不变，同一日志允许异构 Schema；
- 八种 concrete persona 在 1、7、8、9、63、64、65 行边界经过 append、commit、reopen、raw、
  full decode 和全部合法 projection；descriptor 与实际 Schema 树相互校验；
- `append_entry` 原样转发，不重新编码；
- 选择性解码结果在事务结束、原 Store 释放、prefix truncate 和 reopen 后仍拥有全部 Arrow
  buffer；
- 稳定重批只改变 `(offset, row_index)`，展平事件序列逐项不变；
- insert-only 性能流和 valid-churn correctness 流都从空关系满足任意记录的非负权重前缀；
- heterogeneous 变长 entry 支持多页追赶、按实际 encoded bytes 选择 retained window、分步 truncate
  和再次 reopen；
- `ScanLimit` 的 byte admission 精确包含 entry 字节与八字节 offset，并支持同事务重试；
- projection Schema mismatch 与损坏 Change 作为 `StoreError::Codec` 穿过 Store 边界并 poison
  事务；一个 callback 已写入的 output/cursor 也随之后的坏 entry 一起回滚。

## 性能

```bash
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
cargo bench -p dogpaddle-change-store-integration --bench change_append_log_endurance
```

默认 `smoke` 使用隔离临时目录。主 anchor `mixed_event_16` 的每行是 16 个业务顶层列，加固定 diff
后是 17 个物理顶层列；其它 persona、嵌套 leaf 数、投影比例、逐轴矩阵和准确计时边界见
[`PERFORMANCE.md`](./PERFORMANCE.md)。正式结果必须通过 reference runner 选择固定文件系统并保存
每个独立进程的原始输出：

```bash
cargo xtask change-store-reference \
  --store-dir /absolute/path/on/reference-filesystem \
  --output-dir /absolute/new/result-directory
```

长稳使用 `--target endurance`，runner 会选择 `full` workload；默认同时执行变长异构 consumer
pipeline 和等长 homogeneous control。所有 fixture、预热、严格结果比较和临时目录清理都在计时外。
stdout 中每个以 `{` 开头的行都是通过共享 typed record 与
validated `Fields` 输出的 JSONL：普通路径使用标准 environment、configuration、sample、summary，
长稳路径另使用 `cycle_sample` 与 `endurance_summary` extension；本地 support 不拼接 JSON
fragment。正式运行必须保存这些原始记录，不能只复制控制台摘要。

PR 不手工复制缩小环境变量；使用根 xtask 中受审查的 release smoke matrix：

```bash
cargo xtask bench-smoke
```

全工作区的测试所有权、typed benchmark 协议、smoke matrix 与 reference 规则
以根目录 [`TESTING.md`](../../TESTING.md) 为准。
