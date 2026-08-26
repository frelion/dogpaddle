# Change + AppendLog 性能协议

本协议测量真实持久化装配 `AppendLog<Vec<u8>>`，不把 Change codec、通用 Store 基准和组合路径
混成一个数字。结果只在相同 git revision、rustc、机器、profile 和文件系统上可比较；当前没有
绝对吞吐 SLA。

## 有界正常路径

`change_append_log` 使用同一组确定性 wide Change，并把 `rows/Change` 与
`Changes/transaction` 作为独立参数。

| scenario | 计时区间 | 目的 |
| --- | --- | --- |
| `preencoded_append_body_rollback` | `append_batch` body；随后 rollback | 隔离预编码 AppendLog 写入 body |
| `preencoded_append_durable_commit` | begin + append + durable commit | 隔离预编码持久化成本 |
| `encode_append_durable_commit` | begin + encode + append + durable commit | 真实端到端写入路径 |
| `raw_scan_append_entry_durable_commit` | begin + raw scan + `append_entry` + commit | 不重编码的事务内转发 |
| `warm_scan_raw` | 已提交日志的 raw scan body | Store/entry 固定成本 |
| `warm_scan_full_decode` | 已提交日志的 full decode scan body | 全量 Change replay |
| `warm_scan_diff_only_decode` | 同一日志、同一 `ScanLimit` | 最小选择性 replay |
| `warm_scan_narrow_decode` | 同一日志、同一 `ScanLimit` | 跳过宽 payload 的 replay |

所有变更型 warmup 和正式 sample 使用新 Store；读取场景明确为 warm committed，不声称是 cold
cache。full、diff-only、narrow 各自先预热，正式样本轮换执行顺序并保留相同 sample index。
timed scan callback 只执行解码和统一 O(1) sink；计时前后另做逐 offset 原字节比较，以及 source
Change 与 full/内存 projection 的完整 Schema、array、diff 比较。

### 配置

| 环境变量 | 默认值 | 含义 |
| --- | ---: | --- |
| `DOGPADDLE_CARGO_PROFILE` | `bench` | 显式 `--profile` 时必须设为同名值 |
| `DOGPADDLE_CHANGE_STORE_BENCH_ROWS_PER_CHANGE` | 1024 | 每个 Change 的行数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_CHANGES_PER_TX` | 32 | 每个事务的 Change 数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_PAYLOAD_BYTES` | 256 | 每行 Binary payload 字节 |
| `DOGPADDLE_CHANGE_STORE_BENCH_SAMPLES` | 7 | 正式样本数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_WARMUPS` | 1 | 每个场景的预热次数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_MAX_WORKING_SET_BYTES` | 512 MiB | fixture 工作集硬预算 |

启动时先用 checked arithmetic 检查事件 ID、工作集与 Arrow Binary 的 i32 offset 上限，再构造
大 fixture。每条 sample JSONL 包含 elapsed ns、transactions、changes、rows、encoded/logical
bytes 及每事务数据量；summary 给出 min/median/max 和基于 median 的吞吐。

## 长稳与空间复用

`change_append_log_endurance` 先在 measured protocol 之前，把日志 seed 为
`floor(target / encoded_entry_bytes)` 个完整 entry；若目标连一个 entry 都容不下则在写入前拒绝
配置。随后每个正式 cycle append N 条、回收 N 条，并恢复到相同有效 byte window。具体顺序是
在计时外生成并编码一批 Change；计时 begin + `append_batch` + durable commit；按 byte 目标更新
轻量 metadata 队列；计时 begin + 分步 `truncate_before` + durable commit。每个正式 cycle 都
必须推进 head，truncate 分位数不会混入启动阶段的 no-op commit，也绝不会切开 Stream。

每个 cycle 在计时外记录 MDBX `mdbx.dat` 的 logical 与 allocated bytes，并同时保留 append 后和
truncate 后的 peak。最终关闭、reopen 后重新生成预期 Change，一项一项验证 offset、完整原始
字节、全量解码和顺序 checksum。长期内存队列只保存 offset、event seed、encoded length 和
checksum，不复制 retained payload，避免 oracle 与 MDBX 争抢数百 MiB page cache。

append 与 truncate 都从 `begin()` 前开始计时并包含 durable commit。逐 cycle JSONL 保留两类
原始 latency、append/truncate 后的文件尺寸及 head/target/tail/removed 进度；机器可读摘要报告
p50/p95/p99/max、initial/seed/final/reopened/peak logical/allocated bytes、验证 checksum 和相对
retained encoded bytes 的 allocated amplification。可比较的 `protocol_ns` 是所有 durable append
与 truncate 样本之和，changes/s、rows/s 与 encoded MiB/s 都以 measured（不含 seed）工作量及
该时长计算；包含 fixture、`stat`、bookkeeping 和输出的
`wall_ns` 只用于观察，不能当作同一吞吐口径。

### 长稳配置

| 环境变量 | `smoke` | `full` | 含义 |
| --- | ---: | ---: | --- |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_ROWS_PER_CHANGE` | 256 | 4096 | 每个 Change 行数 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_CHANGES_PER_CYCLE` | 8 | 32 | 每次 append 数 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_CYCLES` | 16 | 500 | append/GC 周期数 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_PAYLOAD_BYTES` | 128 | 1024 | 每行 payload 字节 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_RETAINED_BYTES` | 4 MiB | 512 MiB | retained encoded-byte 目标 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_TRUNCATE_ITEMS` | 64 | 4096 | 单次前缀回收上限 |

`DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_WORKING_SET_BYTES` 默认 1 GiB，
`DOGPADDLE_CHANGE_STORE_ENDURANCE_MAX_TOTAL_WRITTEN_BYTES` 默认 1 TiB；它们在 representative
fixture 分配前检查 batch 临时副本，在 entry 长度已知后继续合并检查 retained metadata 与所有
latency sample，并约束 seed 与正式 cycle 的实际编码长度和累计写入。这些预算限制错误配置的
内存及 encoded I/O 工作量，不是 MDBX allocated bytes 或文件系统剩余空间的磁盘配额；reference
runner 仍须预留足够空间。`full` 必须搭配：

```bash
DOGPADDLE_CHANGE_STORE_BENCH_PROFILE=reference \
DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR=/absolute/path/on/reference/filesystem \
DOGPADDLE_CHANGE_STORE_ENDURANCE_PROFILE=full \
cargo bench -p dogpaddle-change-store-integration --bench change_append_log_endurance
```

## 环境与解释

`DOGPADDLE_CHANGE_STORE_BENCH_PROFILE=smoke` 默认使用 tempfile；也可给
`DOGPADDLE_CHANGE_STORE_BENCH_STORE_DIR` 把 smoke 放到指定文件系统。
`profile=reference` 则强制要求该绝对基目录。每次运行输出 git revision/dirty state、rustc、
OS/arch/kernel、CPU、并行度、resolved filesystem path、`df` mount 信息、MDBX durable sync mode
和完整 workload 配置。

stdout 同时包含人类摘要和单行 JSON records；reference runner 应保存完整 stdout，再筛选以
`{` 开头的记录形成 JSONL，并以同 sample index 做配对分析。不要比较不同文件系统上的 durable
commit，也不要把 reopen 称为 cold cache。没有稳定历史基线前只报告数据，不在普通
`cargo test` 中加入 wall-clock 断言。
