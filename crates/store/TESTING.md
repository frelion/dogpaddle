# dogpaddle-store 测试协议

本文件描述 Store 自身的正确性、性能与长稳验证。跨 crate 的真实 Change Stream 持久化不属于
这里，统一由 `integration-tests/change-store/` 负责。

## 正确性所有权

Store 只有一个外部正确性 target：

```text
tests/
├── correctness.rs
└── correctness/
    ├── support.rs
    ├── codec.rs
    ├── store.rs
    ├── placement.rs
    ├── transaction.rs
    ├── cell.rs
    ├── ordered_map.rs
    ├── scan.rs
    ├── append_log.rs
    ├── append_log_layout.rs
    └── crash.rs
```

这些模块作为下游 crate 编译，只通过 `dogpaddle_store` 的公共 Rust API 观察行为。`store`、
`placement` 和 `append_log_layout` 使用 `libmdbx` 适配器构造损坏数据库或读取稳定磁盘布局；适配器
只是 fixture 准备与持久化审计工具，最终错误、回滚和 reopen 结果仍通过 Store 公共 API 断言。
它们不能为了测试方便给产品库增加公开后门。

内置 codec 的稳定字节、顺序保持、borrowed/owned 输入和拒绝路径也属于公共持久化契约，因此放在
`tests/correctness/codec.rs`，而不是源码内联 unit test。只有确实必须访问私有实现的测试才允许进入
对应源码目录唯一的 `tests.rs`；当前 Store 不需要这种白盒 target。Rustdoc 中的运行示例与
`compile_fail` 生命周期/线程边界继续由 doctest 验证。

常用命令：

```bash
cargo test -p dogpaddle-store
cargo test -p dogpaddle-store --test correctness
cargo test -p dogpaddle-store --test correctness transaction::
cargo test -p dogpaddle-store --test correctness append_log_layout::
```

普通正确性测试不使用耗时阈值。`crash` 的子进程 watchdog 只防止失控进程永久挂起，不是性能
断言；其 worker 必须保持顶层路径 `crash::crash_worker`。

## 性能 target

```text
benches/
├── support/mod.rs
├── cell.rs
├── ordered_map.rs
├── append_log.rs
└── append_log_endurance.rs
```

四个 `harness = false` target 共用 `support`：

- 拒绝 debug profile；
- 打印实际 rustc、CPU、OS/kernel、git revision/state、文件系统、运行档位和 MDBX durable sync；
- 每个有效样本输出一条 `record=sample` JSON，另有 configuration、summary、pair_summary、
  checkpoint 或 endurance_summary；人类可读表格与 JSON 会同时写到 stdout；
- fixture、seed、预热和结果 oracle 位于计时外；需要模拟生产事务的 workload 才把 begin、访问和
  durable commit 一起计时；
- 成对比较交错执行并保存逐样本耗时，不能只比较两列独立最小值；
- 修改型普通样本使用新的 Store，warm read 明确复用已提交 fixture。

先筛出以 `{` 开头的 JSON 行，再用 `jq` 提取记录，例如：

```bash
rg '^\{' benchmark.log | jq -c 'select(.record == "sample")'
```

### 文件系统档位

`DOGPADDLE_STORE_BENCH_PROFILE` 控制运行档位：

- `smoke`：默认值。未设置目录时在系统临时文件系统创建统一 benchmark base；适合 PR 协议验收，
  不能作为正式磁盘回归基线；
- `reference`：要求 `DOGPADDLE_STORE_BENCH_STORE_DIR` 是显式绝对路径，所有样本都在该固定文件
  系统下创建隔离 Store。正式对比必须固定机器、rustc、环境变量与该目录。

显式使用 `cargo bench --profile <name>` 时还必须设置
`DOGPADDLE_CARGO_PROFILE=<name>`；标准 `cargo bench` 留空并自动记录为 `bench`。

例如：

```bash
DOGPADDLE_STORE_BENCH_PROFILE=reference \
DOGPADDLE_STORE_BENCH_STORE_DIR=/absolute/path/on/reference-disk \
cargo bench -p dogpaddle-store --bench append_log
```

## 普通 benchmark

- `cell`：同事务 warm get，以及 read-update-durable-commit；
- `ordered_map`：Small/Large 的写入、点读、双向 scan、完整解码/投影、共享 namespace 干扰和
  Cell + map 多集合原子事务；
- `append_log`：固定宽度通用记录的 scalar/batch append、durable commit、warm scan、投影、原样
  转发、fan-out、前缀 GC 和非空固定窗口。

它们保留已有 workload 环境变量：`DOGPADDLE_BENCH_SAMPLES`、`DOGPADDLE_BENCH_COMMITS`、
`DOGPADDLE_BENCH_CELL_READS`、`DOGPADDLE_BENCH_ENTRIES`、`DOGPADDLE_BENCH_SCAN_ITEMS`、
`DOGPADDLE_BENCH_SCAN_BYTES`、`DOGPADDLE_BENCH_WIDE_SCAN_ENTRIES`、
`DOGPADDLE_BENCH_BACKGROUND_NAMESPACES` 以及 `DOGPADDLE_BENCH_LOG_*`。所有计数必须非零。

## AppendLog endurance

`append_log_endurance` 先在计时外形成固定保留窗口，随后每个 cycle 分别计量一次
`append_batch + durable commit` 和一次 `truncate_before + durable commit`。每个 cycle 输出原始
append/truncate 延迟，checkpoint 输出 MDBX logical/allocated bytes，最终报告 p50/p95/p99/max、
峰值空间、后半程空间波动，并关闭后重新打开 Store，全量校验保留区间与 checksum。

`DOGPADDLE_STORE_ENDURANCE_PROFILE` 有两个 workload 档位：

- `smoke`：默认每种宽度累计 8 MiB、保留 2 MiB，适合日常 release 实跑；
- `full`：每种宽度累计 1 GiB、保留 64 MiB，并强制要求 benchmark 文件系统档位为
  `reference`。

可用 `DOGPADDLE_STORE_ENDURANCE_RECORD_BYTES`、`_LOGICAL_MIB`、`_WINDOW_MIB`、`_BATCH_MIB`
和 `_CHECKPOINT_EPOCHS` 覆盖维度。`DOGPADDLE_STORE_ENDURANCE_MAX_WORKING_SET_BYTES` 限制保守
内存工作集估算，`_MAX_TOTAL_WRITTEN_BYTES` 限制逻辑编码写入；runner 在创建 Store 前完成溢出检查
和估算，超预算直接拒绝运行。这两个预算用于防止误配置，并不代表 MDBX 的物理写放大上界。

## 验收命令

```bash
cargo bench -p dogpaddle-store --bench cell
cargo bench -p dogpaddle-store --bench ordered_map
cargo bench -p dogpaddle-store --bench append_log
cargo bench -p dogpaddle-store --bench append_log_endurance

cargo fmt -p dogpaddle-store -- --check
cargo clippy -p dogpaddle-store --all-targets -- -D warnings
```

PR smoke 应缩小普通 benchmark 的条目数、commit 数和 samples，但仍实际执行每个 target；不能只
编译。正式性能结论只比较同一 reference 环境产生的原始配对样本。
