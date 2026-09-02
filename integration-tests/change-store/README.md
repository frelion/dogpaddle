# Change + Store 外部接缝

这个不可发布 package 只验证 `dogpaddle-change` 与 `dogpaddle-store` 无法由任一产品 crate
单独证明的公共组合契约：一个 `AppendLog<Vec<u8>>` entry 恰好保存一个完整、自描述的 Arrow
IPC Change Stream。产品 crate 不得反向依赖它，`src/` 只提供测试数据，不形成产品抽象。

manifest 关闭自动 target 发现，只声明：

- `correctness`：唯一公共正确性 target；
- `change_append_log`：四个代表性成本边界。

## 两种测试数据

测试和 benchmark 只复用两种有明确责任的数据：

- `projectable`：nullable `Utf8`、`Binary`、`List<Int64>`、非相邻投影与非零 slice；
- `heterogeneous_pages`：交错的窄/宽 Schema 和不同 entry 大小，只用于常规 benchmark。

没有 persona、命名 workload 层或可配置 fixture 框架。

## 验证

```bash
cargo test -p dogpaddle-change-store-integration --test correctness
cargo clippy -p dogpaddle-change-store-integration --all-targets -- -D warnings
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
```

正确性只保留两个不能由产品 crate 单独推出的接缝 witness：完整和投影 Change 在 entry
transaction 结束后仍然 owned；坏 Change poison 同一事务并回滚已经发生的 forwarding/cursor
写入。稳定重批归 Change/Operation，分页、计费、truncate、reopen 和物理长稳归 Store。

benchmark 只读取两个统一环境变量：`DOGPADDLE_BENCH_PROFILE=smoke|reference` 和
`DOGPADDLE_BENCH_ROOT=/absolute/path`。smoke 未设置 root 时使用临时目录；reference 必须指定
固定绝对目录。规模由 profile 唯一决定，不存在 target-specific 参数矩阵。

详细覆盖、计时边界和固定 profile 见根目录 [`TESTING.md`](../../TESTING.md)；历史性能说明见
[`PERFORMANCE.md`](./PERFORMANCE.md)。
