# dogpaddle-store 测试说明

本文件只记录 Store 拥有的测试目录、fixture 和 workload。工作区统一的测试所有权、计时边界、
typed JSONL 协议与验证矩阵见[根目录 `TESTING.md`](../../TESTING.md)。跨 crate 的真实 Change
Stream 持久化由 `integration-tests/change-store/` 负责，不属于 Store target。

## 正确性目录与所有权

Store 只有一个外部正确性 target：

```text
tests/
├── correctness.rs
└── correctness/
    ├── support.rs
    ├── capability.rs
    ├── codec.rs
    ├── store.rs
    ├── placement.rs
    ├── transaction.rs
    ├── cell.rs
    ├── ordered_map.rs
    ├── ordered_map_errors.rs
    ├── ordered_map_scans.rs
    ├── scan.rs
    ├── append_log.rs
    ├── append_log_errors.rs
    ├── append_log_projection.rs
    ├── append_log_layout.rs
    └── crash.rs
```

`ordered_map_errors.rs` 与 `ordered_map_scans.rs` 仍是 `ordered_map::{errors,scans}` 的子模块；
`append_log_errors.rs` 与 `append_log_projection.rs` 同样属于
`append_log::{errors,projection}`。文件拆开是为了让成功路径、错误路径、扫描和投影各自聚焦，
不会增加 Cargo target，也不会改变测试过滤路径。

`capability.rs` 统一拥有 collection 能力衰减、共享底层对象、只读 fan-out 和 reopen 后重新
衰减的公共行为；写方法不可用、不能升级回完整 handle 且不能作为 `StoreData` 打开的静态边界
由 `ReadOnly` 的 Rustdoc compile-fail 测试拥有。

`transaction.rs` 验证唯一事务启动能力可以顺序复用并在线程间移动，同时验证活动 Transaction
及其访问值的回滚、中毒和线程绑定语义；`Transactions` 不可克隆、存活 guard 阻止再次 begin，
以及 `Transaction` 的 `!Send + !Sync` 静态边界由对应 Rustdoc compile-fail 测试拥有。

这些模块作为下游 crate 编译，只通过 `dogpaddle_store` 的公共 Rust API 观察行为。`store`、
`placement` 和 `append_log_layout` 可以使用 `libmdbx` 适配器准备损坏数据库或审计稳定磁盘布局，
但错误、回滚和 reopen 结果仍通过 Store 公共 API 断言，不能为测试给产品库增加公开后门。
`crash` 的子进程 worker 必须保持顶层路径 `crash::crash_worker`。

常用命令：

```bash
cargo test -p dogpaddle-store --test correctness
cargo test -p dogpaddle-store --doc
cargo test -p dogpaddle-store --test correctness transaction::
cargo test -p dogpaddle-store --test correctness ordered_map::scans::
cargo test -p dogpaddle-store --test correctness append_log::projection::
```

## 性能目录

四个显式 `harness = false` Cargo target 保持不变；`ordered_map` 和 `append_log` 只在 target
内部按职责拆分：

```text
benches/
├── support/
│   └── mod.rs
├── cell.rs
├── ordered_map.rs
├── ordered_map/
│   ├── fixture.rs
│   ├── measure.rs
│   ├── oracle.rs
│   └── report.rs
├── append_log.rs
├── append_log/
│   ├── fixture.rs
│   ├── measure.rs
│   ├── oracle.rs
│   └── report.rs
└── append_log_endurance.rs
```

两个场景入口负责读取配置和编排 workload；`fixture` 拥有 Store 与测试数据生命周期，`measure`
固定计时边界，`oracle` 构造期望值，`report` 负责场景字段和人类报告。fixture、oracle、workload
和计时语义都留在 Store，未外移到共享 crate。

`benches/support/mod.rs` 是 Store 薄适配：它只管理 `smoke`/`reference` benchmark root、样本
目录和临时目录生命周期，附加 Store 的 durable-sync 环境字段，并提供人类可读的 duration 格式。
共享 `dogpaddle-bench-protocol` 负责严格的正整数/列表设置解析、Cargo 与运行 profile、
rustc/CPU/OS/git/文件系统环境指纹、typed JSONL、普通样本统计、长稳延迟分位数以及配对顺序。
`ordered_map` 使用交替 AB/BA，`append_log` 使用 counterbalanced ABBA/BAAB；两者都保留按语义
归位的 first/second 原始样本和 paired summary。

机器输出是每行一个 typed JSON record，稳定 discriminator 包括 `environment`、
`configuration`、`sample`、`summary`、`pair_summary`、`checkpoint` 和
`endurance_summary`。场景配置通过类型化字段写入，不拼接裸 JSON fragment；stdout 同时保留薄的
人类可读报告。可这样筛选机器记录：

```bash
rg '^\{' benchmark.log | jq -c 'select(.record == "sample")'
```

## Store 文件系统档位

`DOGPADDLE_STORE_BENCH_PROFILE` 控制 Store fixture 落盘位置：

- `smoke`：默认值。未设置目录时在系统临时文件系统创建统一 benchmark base，适合协议验收，
  不作为正式磁盘回归基线；
- `reference`：要求 `DOGPADDLE_STORE_BENCH_STORE_DIR` 是显式绝对路径，所有样本都在该固定文件
  系统下创建隔离 Store。正式对比必须固定机器、rustc、配置与该目录。

显式使用 `cargo bench --profile <name>` 时还必须设置
`DOGPADDLE_CARGO_PROFILE=<name>`；标准 `cargo bench` 留空并记录为 `bench`。

```bash
DOGPADDLE_STORE_BENCH_PROFILE=reference \
DOGPADDLE_STORE_BENCH_STORE_DIR=/absolute/path/on/reference-disk \
cargo bench -p dogpaddle-store --bench append_log
```

## Store workload

- `cell`：同事务 warm get，以及 read-update-durable-commit；
- `ordered_map`：Small/Large 的写入、点读、双向 scan、完整解码/投影、共享 namespace 干扰和
  Cell + map 多集合原子事务；
- `append_log`：固定宽度记录的 scalar/batch append、durable commit、warm scan、投影、原样
  转发、fan-out、前缀 GC 和非空固定窗口；
- `append_log_endurance`：固定保留窗口内持续执行 `append_batch + durable commit` 与
  `truncate_before + durable commit`，记录延迟和空间，并在 reopen 后校验完整窗口。

普通 target 保留 `DOGPADDLE_BENCH_SAMPLES`、`DOGPADDLE_BENCH_COMMITS`、
`DOGPADDLE_BENCH_CELL_READS`、`DOGPADDLE_BENCH_ENTRIES`、`DOGPADDLE_BENCH_SCAN_ITEMS`、
`DOGPADDLE_BENCH_SCAN_BYTES`、`DOGPADDLE_BENCH_WIDE_SCAN_ENTRIES`、
`DOGPADDLE_BENCH_BACKGROUND_NAMESPACES` 和 `DOGPADDLE_BENCH_LOG_*` 设置；计数必须为正。

`DOGPADDLE_STORE_ENDURANCE_PROFILE` 默认为每种宽度累计 8 MiB、保留 2 MiB 的 `smoke`；`full`
为每种宽度累计 1 GiB、保留 64 MiB，并强制使用 reference 文件系统。记录宽度、逻辑写入量、
窗口、batch、checkpoint 和两项安全预算可通过 `DOGPADDLE_STORE_ENDURANCE_*` 设置覆盖。

## 验收入口

Store correctness 可单独运行；完整 PR 验收使用工作区规范入口：

```bash
cargo test -p dogpaddle-store --test correctness
cargo xtask check
cargo xtask bench-smoke
```

`cargo xtask bench-smoke` 在代码内固定缩小配置，并实际执行工作区全部十个 release benchmark；
其中 Store 的 `cell`、`ordered_map`、`append_log` 和 `append_log_endurance` 都会运行，不是只编译。
单独的 `cargo bench -p dogpaddle-store --bench <target>` 用于本地场景诊断或固定 reference
环境的正式测量。完整矩阵和统一 PR 规则以[根目录 `TESTING.md`](../../TESTING.md)为准。
