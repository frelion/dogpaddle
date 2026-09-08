# Change + SubscribedLog 性能口径

`change_subscribed_log` 是 Change 与 Store 唯一的性能接缝，使用 Criterion 0.8.2 和真实
`SubscribedLog<Vec<u8>>`。每个 entry 都是一个完整 Arrow IPC Change Stream；benchmark 只测量
Flow 实际拥有的追加与单 subscriber 消费路径，不复制 Store 的底层压力或长稳矩阵。

两个 case：

| Case | 计时边界 |
| --- | --- |
| `append_durable` | 分批 begin、逐 entry `try_append` 与 durable commit |
| `consume_durable` | 每个 entry 的 read snapshot、`peek`、完整 Change decode、精确 offset `acknowledge` 与 durable commit |

fixture 构造与编码、Store create/seed、log 初始化、预热、reopen oracle 和清理都在计时外。
append 会在 reopen 后校验 retained range、精确字节计费和首个 entry；consume 校验完整序列解码后的
diff、首列 ID 顺序、checksum，以及最终 `head == tail`、retained bytes 为零。

Criterion 原生 raw samples 与 estimates 写到 `RunRoot/criterion/`；相邻
`criterion-context.json` 记录 profile、rustc、CPU、git、文件系统、RocksDB durable write 模式、
固定 subscriber 数和 workload。不存在 plan、fingerprint、统一 JSONL 或中央 validator。

```bash
DOGPADDLE_PERF_PROFILE=smoke \
cargo bench --locked -p dogpaddle-change-store-integration --bench change_subscribed_log

DOGPADDLE_PERF_PROFILE=reference \
DOGPADDLE_PERF_ROOT=/absolute/reference-filesystem \
cargo bench --locked -p dogpaddle-change-store-integration --bench change_subscribed_log
```

不同 baseline epoch、git、rustc、CPU、profile 或文件系统的结果不可直接比较。全局准入规则见
[`TESTING.md`](../../TESTING.md)。
