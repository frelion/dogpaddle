# Change + AppendLog 性能协议

`change_append_log` 测量真实的 `AppendLog<Vec<u8>>`，每个 entry 是完整 Arrow IPC Change Stream。
当前没有绝对吞吐 SLA；结果只有在 git、rustc、CPU、profile 和文件系统一致时可比较。

## 统一配置

benchmark 只读取：

- `DOGPADDLE_BENCH_PROFILE=smoke|reference`：唯一规模选择；未设置时为 smoke；
- `DOGPADDLE_BENCH_ROOT=/absolute/path`：Store 父目录；smoke 可省略，reference 必填。

所有 workload 数值都由 profile 固定，不能用环境变量生成任意笛卡尔组合。

| profile | rows/Change | Changes/transaction | transactions/sample | payload | samples / warmups | max fixture |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| smoke | 8 | 2 | 2 | 16 B | 1 / 1 | 64 MiB |
| reference | 1024 | 32 | 8 | 256 B | 15 / 3 | 512 MiB |

`change_append_log` 只有四个场景：

| scenario | fixture | 计时边界 |
| --- | --- | --- |
| `append_durable` | heterogeneous pages | 每批 begin + append 预编码 Stream + durable commit |
| `full_replay` | heterogeneous pages | 已打开事务内的分页 scan + full decode |
| `projected_replay` | projectable | 已打开事务内的分页 scan + projected decode |
| `consumer_durable` | heterogeneous pages | 每页 begin + full decode + `append_entry` + cursor set + durable commit |

fixture 构造与编码、Store create/seed、warmup、严格 reopen oracle 和清理不计时。full/projected
结果消费 diff 与首列 ID 的顺序 checksum；计时后仍逐 entry 比较持久化原字节。consumer 还验证
output 与 input bytes 完全相同、cursor 精确到 tail。

物理窗口 churn、文件增长、truncate 和 crash/reopen 长稳性质只取决于 Store 对 opaque bytes 的
处理，由 Store 的 `append_log_endurance` 唯一拥有；本组合包不维护第二套 endurance 协议。

## 运行

```bash
cargo bench -p dogpaddle-change-store-integration --bench change_append_log

DOGPADDLE_BENCH_PROFILE=reference \
DOGPADDLE_BENCH_ROOT=/absolute/reference-filesystem \
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
```

PR 使用 `cargo xtask bench-smoke` 执行 release target 并验证 JSONL metadata 与完整样本。
每个 target 只有完成全部验证后才发出最后一个 completion。reference 必须保存完整 stdout；不要
只复制人类摘要，也不要把 reopen 称作 cold cache。
