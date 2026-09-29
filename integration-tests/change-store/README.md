# Change + Store 外部接缝

这个不可发布 package 只验证 `dogpaddle-change` 与 `dogpaddle-store` 无法由任一产品 crate
单独证明的公共组合契约：一个资源先绑定唯一 exact Schema，同一 `SubscribedLog<Vec<u8>>` 的每个
entry 再保存一个 schema-bound Change。产品 crate 不得反向依赖它，`src/` 只提供测试数据，不形成
产品抽象。

manifest 关闭自动 target 发现，只声明：

- `correctness`：唯一公共正确性 target；
- `change_subscribed_log`：durable append 与单 subscriber durable consume 两个成本边界。

## 两种测试数据

测试和 benchmark 只复用两种有明确责任的数据：

- `nested_change`：nullable `Utf8`、`Binary`、`List<Int64>` 和非零 slice；
- `fixed_schema_changes`：同一 exact Schema、不同 entry payload 宽度，只用于常规 benchmark。

没有 persona、命名 workload 层或可配置 fixture 框架。

## 验证

```bash
cargo test -p dogpaddle-change-store-integration --test correctness
cargo clippy -p dogpaddle-change-store-integration --all-targets -- -D warnings
DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-change-store-integration --bench change_subscribed_log
```

正确性只保留两个不能由产品 crate 单独推出的接缝 witness：schema-bound Change 在 read snapshot
结束后仍然 owned；坏 Change 不会推进 subscription。稳定重批归 Change/Operation，
容量、订阅位置、回收、reopen 和物理长稳归 Store。

benchmark 只读取两个统一环境变量：`DOGPADDLE_PERF_PROFILE=smoke|reference` 和
`DOGPADDLE_PERF_ROOT=/absolute/path`。profile 必填；smoke 未设置 root 时使用临时目录，reference 必须指定
固定绝对目录。规模由 profile 唯一决定，不存在 target-specific 参数矩阵。

详细覆盖、计时边界和固定 profile 见根目录 [`TESTING.md`](../../TESTING.md)；owner 性能说明见
[`PERFORMANCE.md`](./PERFORMANCE.md)。
