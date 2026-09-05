# dogpaddle-store 性能口径

Store 自己拥有 workload、fixture、seed、预热、正确性断言和结果字段。工作区共享的
`dogpaddle-perf-context` 只解析 profile、管理结果目录、采集主机环境并拒绝非 release 实测；这里没有
中央 case registry、plan、fingerprint、结果 schema 或 validator。

## Targets

### `cell` — Criterion

- `hot_get_one_tx`：在一个事务中重复读取已预热的 `Cell<u64>`；
- `read_update_commit`：每次 read-modify-write 都提交一个 durable transaction。

Criterion 原生 raw samples 与 estimates 位于该次 `RunRoot` 的 `criterion/`，配置与环境写入相邻
`context.json`。Cargo test mode 使用最小 smoke 配置，不执行正式测量。

### `ordered_map` — owner paired runner

覆盖 Small/Large、primitive/typed/byte map、point get、bulk put、扫描解码、分页、Station-shaped
事务与 durable overwrite。需要比较的两个 variant 在同一 fixture 条件下成对采样，样本顺序按
AB/BA 轮换，不能用两次独立运行的 median 代替。

### `append_log` — owner counterbalanced paired runner

覆盖不同记录宽度的 append、batch append、投影/完整 decode、durable append、Station-shaped
count/filter、fan-out readers、steady window 与 prefix GC。配对 case 按 AB/BA/BA/AB 循环，抵消固定
顺序和热度偏差。AppendLog 自己验证 head/tail、记录内容、consumer cursor 与 GC 结果。

### `append_log_endurance` — owner streaming runner

在固定窗口中持续 append 和 bounded truncate，记录每个事务的原始 latency、定期 head/tail 与物理文件
checkpoint，以及终止后的 reopen checksum。JSONL 随样本产生立即写入 stdout；进程失败时之前的样本
仍可保留。它观察长期页复用与尾延迟，不设置 wall-clock gate。

## 输出

三个 owner runner 的 stdout 是各自定义的 JSONL；stderr 只用于人类进度和摘要。每次运行首先记录：

- benchmark 与 `smoke|reference` profile；
- 实际 workload 配置；
- rustc、OS/kernel、CPU、git revision 和 dirty state；
- 结果文件系统与 MDBX durable 模式。

随后逐条输出原始 duration/checkpoint，最后输出完成记录。字段只属于当前 target，不形成跨 target ABI。
fixture 创建、数据填充、预热和 oracle 都在计时区间外。

## 运行

快速 smoke：

```bash
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench cell
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench ordered_map
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench append_log
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench append_log_endurance
```

正式 reference 必须指定固定的绝对目录：

```bash
DOGPADDLE_PERF_PROFILE=reference \
DOGPADDLE_PERF_ROOT=/absolute/path/on/reference-filesystem \
cargo bench --locked -p dogpaddle-store --bench append_log
```

其余 target 同理。不同 baseline epoch、提交、rustc、机器、profile、文件系统或 workload 的结果不可
直接比较。工作区分类和准入规则见根目录 [`TESTING.md`](../../TESTING.md)。
