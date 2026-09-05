# Change + AppendLog 性能口径

`change_append_log` 是 Change 与 Store 唯一的性能接缝，使用 Criterion 0.8.2 和真实
`AppendLog<Vec<u8>>`。每个 entry 都是一个完整 Arrow IPC Change Stream；它不复制 Store 的 churn、
truncate、crash 或 endurance 矩阵。

四个 case：

| Case | 计时边界 |
| --- | --- |
| `append_durable` | 分批 begin、append 预编码 Stream、durable commit |
| `full_replay` | 已打开事务中的分页 scan 与完整 decode |
| `projected_replay` | 已打开事务中的分页 scan 与选择性 decode |
| `consumer_durable` | 每页 begin、decode、forward、cursor set 与 durable commit |

fixture 构造与编码、Store create/seed、预热、严格 reopen oracle 和清理都在计时外。完整与投影读取会
校验 diff、首列 ID 顺序和 checksum；consumer 还验证 output bytes 与 input bytes 完全相同、cursor
精确到 tail。

Criterion 原生 raw samples 与 estimates 写到 `RunRoot/criterion/`；相邻 `criterion-context.json` 记录 profile、
rustc、CPU、git、文件系统和固定 workload。不存在 plan、fingerprint、统一 JSONL 或中央 validator。

```bash
DOGPADDLE_PERF_PROFILE=smoke \
cargo bench --locked -p dogpaddle-change-store-integration --bench change_append_log

DOGPADDLE_PERF_PROFILE=reference \
DOGPADDLE_PERF_ROOT=/absolute/reference-filesystem \
cargo bench --locked -p dogpaddle-change-store-integration --bench change_append_log
```

不同 baseline epoch、git、rustc、CPU、profile 或文件系统的结果不可直接比较。全局准入规则见
[`TESTING.md`](../../TESTING.md)。
