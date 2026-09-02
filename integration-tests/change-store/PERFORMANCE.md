# Change + AppendLog 性能协议

两个 target 都测量真实的 `AppendLog<Vec<u8>>`，每个 entry 是完整 Arrow IPC Change Stream。
当前没有绝对吞吐 SLA；结果只有在 git、rustc、CPU、profile 和文件系统一致时可比较。

## 统一配置

benchmark 只读取：

- `DOGPADDLE_BENCH_PROFILE=smoke|reference`：唯一规模选择；未设置时为 smoke；
- `DOGPADDLE_BENCH_ROOT=/absolute/path`：Store 父目录；smoke 可省略，reference 必填。

所有 workload 数值都由 profile 固定，不能用环境变量生成任意笛卡尔组合。

### 常规 target

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

### Endurance target

| profile | rows/Change | Changes/cycle | cycles | payload | retained target | truncate limit | validation page | checksum interval | budgets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| smoke | 8 | 2 | 3 | 16 B | 64 KiB | 8 items | 2 items / 1 MiB | 1 cycle | 64 MiB / 64 MiB |
| reference | 4096 | 32 | 500 | 1 KiB | 512 MiB | 4096 items | 16 items / 128 MiB | 25 cycles | 1 GiB / 1 TiB |

`change_append_log_endurance` 只运行 `fixed_wide_window`。编码在计时外；producer 计时
begin + append_batch + durable commit，truncate 计时 begin + bounded `truncate_before` loop +
durable commit。retained charge 精确使用每个 entry 的完整 encoded bytes 加八字节 offset key。
窗口至少保留一个完整 Stream，因此小于单 entry 的 target 仍允许一个 oversize entry。

Store 每周期关闭，下一周期重新打开。按 checksum interval 以及结束时重新分页读取，逐 entry
full decode，并比较 bounds、exact bytes、order checksum。队列维护、reopen、验证与 JSON 输出均
在计时外。cycle_sample 原样报告 producer/truncate duration；endurance_summary 的
p50/p95/p99/max 必须能从这些样本重算。

## 运行

```bash
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
cargo bench -p dogpaddle-change-store-integration --bench change_append_log_endurance

DOGPADDLE_BENCH_PROFILE=reference \
DOGPADDLE_BENCH_ROOT=/absolute/reference-filesystem \
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
```

PR 使用 `cargo xtask bench-smoke` 执行 release target 并验证 JSONL metadata、样本与 summary。
每个 target 只有完成全部验证后才发出最后一个 completion。reference 必须保存完整 stdout；不要
只复制人类摘要，也不要把 reopen 称作 cold cache。
