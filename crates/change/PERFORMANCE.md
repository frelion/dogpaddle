# dogpaddle-change 性能口径

Change 自己拥有 Arrow fixture、工作量、预热、正确性 oracle 和输出字段。共享的
`dogpaddle-perf-context` 只提供 profile、运行目录、主机信息和 release-build 检查。

## `change_core` — Criterion

六种 fixture（diff-only、narrow fixed、wide projectable、mixed nullable、nested、sliced）分别测量：

- `Change::try_new`；
- `ChangeProjection::try_new`；
- `Change::try_slice`；
- `Change::try_project`。

构造输入、projection、slice 参数和独立结果验证都在计时外。Criterion 原生 raw samples 与 estimates
位于 `RunRoot/criterion/`，相邻 `criterion-context.json` 记录 profile、host、固定 workload 和 encoded
bytes。该 target 设置 `test = true`，Cargo test mode 自动使用最小 smoke fixture。

## `change_codec` — 五路旋转 runner

同一 fixture 的五个 case 在每个 sample 内执行一次，并逐 sample 轮换首个 case：

1. encode；
2. full decode；
3. diff-only projected decode；
4. narrow projected decode；
5. identity projected decode。

这样保留同一 sample 下的配对关系，同时分散固定顺序和热度偏差。所有 projection、预编码字节、独立
oracle 和 warm-up 在计时外。stdout 逐行输出 owner-specific JSONL：context、fixture、包含五个有序
measurement 的 paired sample、completion；stderr 只输出进度。失败前已 flush 的样本仍然可用。

`smoke` 使用 4 rows/Change 和 16-byte 宽 payload；`reference` 使用 1、64、1024、16384 rows/Change、
1 KiB 宽 payload及 9 次旋转 sample。类型全集属于 correctness，这里只选不同成本形状。

## 运行

```bash
cargo test --locked -p dogpaddle-change --bench change_core
DOGPADDLE_PERF_PROFILE=smoke \
cargo bench --locked -p dogpaddle-change --bench change_codec
```

正式 reference：

```bash
DOGPADDLE_PERF_PROFILE=reference \
DOGPADDLE_PERF_ROOT=/absolute/reference-root \
cargo bench --locked -p dogpaddle-change --bench change_codec
```

Change 不要求持久化 fixture，但仍通过统一的绝对 reference root 保存运行上下文。真实
Change + `SubscribedLog` 成本由 `integration-tests/change-store` 单独测量。不存在 plan、fingerprint、中央
validator 或跨 target 结果 schema；不同 baseline epoch 的结果不可直接比较。全局规则见
[`TESTING.md`](../../TESTING.md)。
