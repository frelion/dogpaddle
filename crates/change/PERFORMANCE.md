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

## `change_codec` — 两路旋转 runner

同一 fixture 的两个 case 在每个 sample 内执行一次，并逐 sample 轮换首个 case：

1. schema-bound encode；
2. schema-bound full decode。

这样保留同一 sample 下的配对关系，同时分散固定顺序和热度偏差。Schema 绑定、预编码字节、独立
records/diffs oracle 和 warm-up 在计时外。stdout 逐行输出 owner-specific JSONL：context、fixture、包含两个有序
measurement 的 paired sample、completion；fixture record 记录每 Change 的 schema-bound 字节数。
stderr 只输出进度。失败前已 flush 的样本仍然可用。旧七路 runner 的时序不用于本轮配对比较。
产品原本已经使用 schema-bound 路径，删除自描述能力不等于产品获得该格式的既有空间收益。

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

Change 不要求持久化 fixture，但仍通过统一的绝对 reference root 保存运行上下文。
持久页的读写成本由 Flow 与 Operation 的 owner benchmark 测量。不同 baseline epoch 的结果不可直接比较。全局规则见
[`TESTING.md`](../../TESTING.md)。

## 2026-10-01：收敛持久 codec 的配对观测

基线 Change 源码与 `5be727a` 相同；基线 worktree 为 `4d82c1c`，只临时移植与候选相同的两路 runner。
本机 Apple M5 / macOS 26.6.2 / Rust 1.96，release；每 case 每轮 9 个 warm sample。
先按基线→候选，再按候选→基线测量。四轮的 fixture、encoded bytes、工作量、checksum 和轮转顺序逐项相同；
两侧均实际重新编译，采样期间没有其他 Cargo、JVM 或数据库 gate。桌面进程负载未隔离。
原始日志及逐项汇总保存在本机 `/tmp/dogpaddle-bound-change-performance/`。

下表为每轮每 Change 耗时中位数的候选相对基线变化；正值表示候选耗时增加。没有统计置信区间。

| Workload | Rows | Encode：首轮 / 反序 | Decode：首轮 / 反序 |
| --- | ---: | ---: | ---: |
| diff_only | 1 | -6.9% / +5.8% | -20.7% / -2.7% |
| diff_only | 64 | +0.0% / -1.9% | -4.6% / -6.0% |
| diff_only | 1024 | +4.3% / -0.2% | -2.0% / +0.1% |
| diff_only | 16384 | -6.1% / +8.8% | -1.4% / -2.2% |
| mixed_nullable | 1 | +0.4% / -0.6% | -2.1% / -11.9% |
| mixed_nullable | 64 | +0.9% / +1.3% | -5.5% / -5.9% |
| mixed_nullable | 1024 | +0.3% / -1.5% | -1.5% / -1.1% |
| mixed_nullable | 16384 | +13.5% / -2.6% | +2.4% / -1.5% |
| narrow_fixed | 1 | -10.8% / +1.4% | -13.4% / -1.8% |
| narrow_fixed | 64 | +3.9% / +0.9% | -3.8% / -6.3% |
| narrow_fixed | 1024 | +13.5% / +10.7% | +1.7% / +0.7% |
| narrow_fixed | 16384 | -10.1% / +2.7% | +2.3% / -4.0% |
| nested | 1 | -0.4% / -4.7% | -8.5% / -8.5% |
| nested | 64 | +2.1% / -0.7% | -4.7% / -5.4% |
| nested | 1024 | +1.6% / -7.3% | -0.9% / -1.2% |
| nested | 16384 | +16.5% / +3.8% | +2.7% / +1.8% |
| sliced | 1 | +1.2% / -3.9% | -6.2% / -9.4% |
| sliced | 64 | +4.0% / -1.7% | +5.7% / -7.8% |
| sliced | 1024 | +20.7% / -10.0% | +10.1% / -0.5% |
| sliced | 16384 | +12.1% / -0.3% | +9.1% / +1.5% |
| wide_projectable | 1 | -2.9% / -4.9% | -11.4% / -9.9% |
| wide_projectable | 64 | +1.6% / +0.5% | -0.6% / +14.0% |
| wide_projectable | 1024 | +21.9% / -1.8% | +4.2% / +0.5% |
| wide_projectable | 16384 | +14.9% / +5.2% | +13.3% / +3.9% |

结果混合且部分受测量顺序影响。1024 行窄列编码两轮均约增加 11–14%，宽列 64 行解码反序轮约增加 14%；
这些仍偏高的 case 没有被丢弃。编码实现未改不能代替实测结论；这组数据不支持全面提速或全面无回归的断言。
本轮删除的是公开自描述/选择性 IPC 能力及其暂存结构，产品此前已经使用绑定 Schema 的格式。
不将退休 Schema 解析器计为产品热路径收益，也不推断 RSS、JVM、持久 I/O 或整体引擎吞吐。
首个仍保留额外 oracle 解码结果的测量已作废，`discarded-retained-oracle-*` 仅作诊断记录。
