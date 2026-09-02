# Change + Store 外部接缝

这个不可发布 package 只验证 `dogpaddle-change` 与 `dogpaddle-store` 无法由任一产品 crate
单独证明的公共组合契约：一个 `AppendLog<Vec<u8>>` entry 恰好保存一个完整、自描述的 Arrow
IPC Change Stream。产品 crate 不得反向依赖它，`src/` 只提供测试数据，不形成产品抽象。

manifest 关闭自动 target 发现，只声明：

- `correctness`：唯一公共正确性 target；
- `change_append_log`：四个代表性成本边界；
- `change_append_log_endurance`：独立进程中的固定宽度 byte-window 长稳协议。

## 三个 fixture

所有测试和 benchmark 只复用三个有明确责任的复合 fixture：

- `ordered_diff`：重复记录、正负 diff，以及同一事件序列的稳定重批；
- `projectable`：nullable `Utf8`、`Binary`、`List<Int64>`、非相邻投影与非零 slice；
- `heterogeneous_pages`：交错的窄/宽 Schema 和不同 entry 大小，用于事务、分页、复制、
  truncate 与 reopen。

`wide_change` 只是 endurance 构造固定 Schema 数据的底层函数，不是第四套 workload 模型。
没有额外的命名 workload 层或 benchmark 专用 fixture 层。

## 验证

```bash
cargo test -p dogpaddle-change-store-integration --test correctness
cargo clippy -p dogpaddle-change-store-integration --all-targets -- -D warnings
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
cargo bench -p dogpaddle-change-store-integration --bench change_append_log_endurance
```

正确性覆盖稳定重批后的逐事件相等、full/projected owned Change、异构 entry 的 item/byte
分页、rollback、`append_entry` 原字节复制、有界 truncate、reopen 后精确 bytes/order，以及坏
Change poison 同事务中已经发生的 output/cursor 写入。

benchmark 只读取两个统一环境变量：`DOGPADDLE_BENCH_PROFILE=smoke|reference` 和
`DOGPADDLE_BENCH_ROOT=/absolute/path`。smoke 未设置 root 时使用临时目录；reference 必须指定
固定绝对目录。规模由 profile 唯一决定，不存在 target-specific 参数矩阵。

详细覆盖、计时边界和固定 profile 见根目录 [`TESTING.md`](../../TESTING.md)；历史性能说明见
[`PERFORMANCE.md`](./PERFORMANCE.md)。
