# DogPaddle 工作区测试体系

DogPaddle 用同一套规则组织全部产品 crate 和跨 crate 接缝。测试先按目的分为正确性与性能，
再按所有权分为源码白盒、crate 公共契约和外部组合契约。正向行为、负向输入、持久化兼容、
性质测试与崩溃恢复都属于正确性；性能测试只回答有效工作负载的成本与回归，不替代行为断言。

## 工作区矩阵

| 所有者 | 源码白盒 | 公共正确性 | 常规性能 | 长稳性能 |
| --- | --- | --- | --- | --- |
| Change | IPC 私有 framing/layout | Change、Schema、Projection、codec | `change_core`、`change_codec` | 暂无独立状态，不适用 |
| Store | 当前无必须访问私有实现的测试 | Store、事务、布局、Cell、OrderedMap、AppendLog、崩溃恢复 | `cell`、`ordered_map`、`append_log` | `append_log_endurance` |
| Operation | decoder registry、类型擦除和具名实例绑定 | Definition、codec、materialize、SequenceSource、Count | `operation_core` | 当前只有定长 Cell 状态，不适用 |
| Flow | definition/拓扑 codec、派生 schedule、内部 Station 装配、active input 与 intake cache | build/open、资源布局、失败无副作用、`advance` 有界轮次签名 | `flow_lifecycle`（冷路径） | 尚无完整运行时，不适用 |
| Change + Store | 不适用 | 完整 Change entry、回放、事务、重批、reopen | `change_append_log` | `change_append_log_endurance` |

这里的“不适用”是显式边界，不是漏测。新增稳定运行路径、无界状态或跨层调度器时，必须先确定
成本边界和结果 oracle，再登记新的 benchmark 或 endurance target。

## 目录与 Cargo 约束

每个产品 crate 都采用相同骨架：

```text
crates/<crate>/
├── src/**/tests.rs              # 仅需要私有可见性的白盒测试
├── tests/correctness.rs         # 唯一公共正确性 Cargo target
├── tests/correctness/*.rs       # 按领域能力分模块，不按历史阶段分组
├── tests/fixtures/v<N>/         # 版本化持久化黄金字节（需要时）
├── benches/<scenario>.rs        # 一个可解释成本边界一个 target（需要时）
└── benches/support/             # 仅该 crate 的 fixture、Store root、oracle 和人类报告薄适配
```

跨 crate 接缝采用不可发布的下游 package：

```text
integration-tests/<seam>/
├── src/                         # 测试与 benchmark 共用的 fixture/oracle/workload
├── tests/correctness.rs         # 唯一公共组合正确性 target
├── tests/correctness/*.rs
└── benches/                     # 正常路径与必要的 fixed-window endurance
```

各 manifest 显式设置 `autotests = false` 和 `autobenches = false`，产品 library 还要设置
`[lib] bench = false`，并逐项声明允许的 target。
这样新增文件不会静默变成新的测试二进制，也不会复制 fixture 或改变执行矩阵。公共测试只通过
目标 crate 的公共 API；持久布局检查可以使用独立的磁盘格式适配器，但不得访问产品私有模块。

测试支持代码保持在所有者目录内，不为共享 fixture 扩张产品 API，也不创建携带产品语义的
“万能 test-utils” crate。`test-support/bench-protocol/` 是唯一的工作区级例外，只拥有严格环境
变量解析、主机指纹、typed JSONL、持续时间统计和 AB/BA 样本顺序；它不依赖产品 crate，也不拥有
fixture、Store 生命周期、workload、计时边界、结果 oracle 或人类报告。跨 test/bench 共用且包含
产品语义的数据仍留在 owner 的 `tests/`、`benches/support/` 或外部集成 package 的 `src/`。

## 所有权和依赖方向

- `crates/change/tests/` 与 `crates/change/benches/` 不得依赖 Store、Operation 或 Flow。
- `crates/store/tests/` 与 `crates/store/benches/` 不得依赖 Arrow、Change、Operation 或 Flow。
- Operation 正式依赖 Store，因此 Operation 的状态推进、rollback、commit 和 reopen 由
  `crates/operation/tests/correctness/` 拥有，不另建 `operation-store` package。
- Flow 是 Operation + Store 的产品组合根，因此 build/open/materialize 和稳定资源布局由
  `crates/flow/tests/correctness/` 拥有，不另建 `flow-store` package。
- Change 与 Store 是刻意独立的 sibling；唯一同时依赖两者的位置是
  `integration-tests/change-store/`。
- 产品 crate 不得依赖任何 `integration-tests/*`；外部集成 package 必须 `publish = false`。
- 将来出现真正的 Change 调度执行链时，再按实际组合根决定归 Flow/runtime 还是新增外部 seam，
  不提前创建空 package。

## 正确性分层

### 确定性公共契约

每次提交都运行。成功路径覆盖构造、边界值、事务、持久化、重新打开和资源所有权；失败路径
覆盖非法参数、畸形编码、错误数据类型、错误 Store、损坏 metadata、截断和尾随字节。断言稳定
错误类别及失败后的状态，不只检查 `is_err()`。

### 白盒不变量

只有公共 API 无法证明的不变量才留在 `src/**/tests.rs`，例如 decoder registry 完整性、私有
拓扑校验顺序、IPC buffer layout 和类型擦除绑定。能经公共 API 表达的测试必须下移到
`tests/correctness/`，避免实现重构同时击穿契约测试。

### 持久化兼容

稳定 magic、版本、tag、codec、物理布局和资源名必须有版本化黄金字节或原始布局断言，并覆盖
decode/reopen。黄金 fixture 是独立文件，不能只在测试中用同一套生产逻辑动态拼出 expected。
改变已有 fixture 需要迁移设计，不能用重新生成文件掩盖兼容性变化。

### 性质、变形与鲁棒性

性质测试使用确定性 seed，并在失败时打印 seed。Change 重点验证 roundtrip、投影等价、切片和
稳定重批；Store 重点验证分页/方向与 `BTreeMap` 模型一致、事务原子性和单调 offset；Operation
重点验证稳定 definition codec 与状态推进；Flow 重点验证任意合法 DAG 的顺序和拓扑约束。
任意输入 decoder 必须返回结果而非 panic、abort、无限循环或按未验证长度分配。

### 崩溃与长稳

进程级 crash test 属于确定性正确性，必须用子进程隔离并验证 durable commit 边界。Endurance
属于性能协议：它必须维持固定有效窗口、持续触发真实回收、记录资源峰值，并在 reopen 后完成
严格结果 oracle；二者不能互相替代。

## Change 数据规格

Change 正确性与性能沿用以下代表性形状。`logical / physical` 是顶层业务字段数与加入固定
`$dogpaddle.diff` 后的 Stream 顶层字段数；嵌套 persona 还必须单独报告 leaf fields：

| 数据形状 | logical / physical | 用途 |
| --- | ---: | --- |
| `diff_only_control` | 0 / 1 | 最小完整 Stream 固定成本 |
| `layout_v1_16` | 16 / 17 | v1 全部物理布局与 nullable 边界 |
| `fixed_event_8` | 8 / 9 | 常见固定宽度事件 |
| `mixed_event_16` | 16 / 17 | 数值、Boolean、Utf8、Binary 与 nullable 的主 anchor |
| `wide_numeric_64` | 64 / 65 | 多列 Schema、metadata 与 buffer 遍历 |
| `blob_event_4` | 4 / 5 | 128 B、1 KiB、8 KiB payload 轴 |
| `nested_event_8` | 8 / 9 | List/Struct 递归布局和完整子树投影 |
| `sliced_mixed_16` | 16 / 17 | 非零 Arrow offset、共享 buffer 和编码 |
| `heterogeneous` | 多种 | 同一 AppendLog 内不同 Schema、entry 大小与稳定重批 |

重点行数覆盖 1、7/8/9、63/64/65 等 bitmap 和对齐边界。性能默认行数锚点为 1、64、1024、
16384，宽 payload 锚点为 128 B、1 KiB、8 KiB；不执行所有维度的完整笛卡尔积，而是固定
代表性基线后逐轴扫描。组合层的完整 persona、有效 diff model、correctness 与 endurance 契约见
[`integration-tests/change-store/TESTING.md`](./integration-tests/change-store/TESTING.md)。

## 性能统一协议

### 计时边界

- fixture 构造、编码 seed、预热、结果校验和文件清理必须在计时外。
- 变更型样本使用新 Store；读取场景明确标注 warm committed。reopen 不等于 cold cache。
- durable 场景计量 begin、真实操作和 durable commit；rollback/body 场景必须在名称中声明。
- 完整解码和投影解码使用同一份输入并交错执行，减少顺序与温度偏差。
- 普通测试不得包含 wall-clock 断言；benchmark 也不得用耗时替代正确性 oracle。

### 指标

遍历完整数据的场景报告与语义相符的 operations/s、transactions/s、rows/s 或 encoded MiB/s，
并打印每事务工作量。O(1) metadata/view 操作只报告 ns/op，不能按输入总行数虚构吞吐。
常规基准保留每个样本的原始 ns，再给出 min/median/max；长稳协议报告 p50/p95/p99/max、
logical/allocated/peak bytes 和 reopen checksum。allocation calls/bytes 只有接入 allocator profiler
后才能报告，不能从耗时或 RSS 反推。

### 可复现环境

每个 benchmark 通过 `dogpaddle-bench-protocol` 输出具有稳定 discriminator 的 typed JSONL 逐样本
记录，并记录实际 rustc、OS/kernel、CPU、profile、git revision
与 dirty 状态；持久化基准还要记录实际文件系统路径和 durable sync mode。`smoke` 可使用临时目录，
`reference` 必须显式指定固定文件系统。正式回归只比较同一 rustc、profile、机器、文件系统和
workload 的原始配对样本；建立稳定基线前只报告结果，不凭空规定绝对 SLA。

标准 `cargo bench` 调用不需要额外配置，记录为 `cargo_profile=bench` 且
`cargo_profile_source=default`。Cargo 不会把 `--profile` 的名称传给 benchmark 进程；因此显式使用
`cargo bench --profile <name>` 时，必须同时设置 `DOGPADDLE_CARGO_PROFILE=<name>`。runner 将其
记录为 `cargo_profile_source=environment`；未配对该环境变量的自定义 profile 运行不属于有效
性能协议，不能进入 reference 基线。

## 验证层级

工作区提供两个规范入口：`cargo xtask check` 依次运行格式、debug/release correctness、Clippy 和
`-D warnings` Rustdoc；`cargo xtask bench-smoke` 使用代码内固定的缩小参数实际执行以下 10 个
release target。
benchmark smoke matrix 变更必须与 Cargo target 和本节同步评审，不能依赖个人 shell 历史。
`bench-smoke` 会先清除父进程全部 `DOGPADDLE_*` 变量，再逐 target 注入受审查配置，避免本地
workload filter、profile 或 Store 路径悄悄改变 PR gate。

### CI 分层

GitHub Actions 的 PR/push gate 使用固定 Ubuntu 24.04 和 `rust-toolchain.toml` 中的 Rust 1.96：

- `Workspace check` 执行规范入口 `cargo xtask check`；
- `Latest stable compatibility` 只补充运行最新 stable 的 workspace tests，不替代 MSRV gate；
- `Benchmark protocol smoke` 在 workspace gate 成功后实际运行 10 个 release target，只验证场景、
  oracle 和 typed machine protocol，绝不比较 wall-clock；
- 每周 `Endurance protocol` 用 GitHub hosted runner 执行受控 Store 与 Change + AppendLog 长稳并上传
  14 天原始日志。共享 runner 的 latency 不能进入正式性能基线。

workflow 使用只读 token、不可变 action commit SHA、并发取消和按 toolchain/lock/manifests 隔离的
Cargo cache。缓存只加速依赖与构建产物，不保存 benchmark 数据。正式 reference 仍必须在固定专用
机器和文件系统上由下述 runner 产生。

### 日常正确性

```bash
cargo test -p dogpaddle-change --test correctness
cargo test -p dogpaddle-store --test correctness
cargo test -p dogpaddle-operation --test correctness
cargo test -p dogpaddle-flow --test correctness
cargo test -p dogpaddle-change-store-integration --test correctness
cargo test --workspace
```

### 性能入口

```bash
cargo bench -p dogpaddle-change --bench change_core
cargo bench -p dogpaddle-change --bench change_codec
cargo bench -p dogpaddle-store --bench cell
cargo bench -p dogpaddle-store --bench ordered_map
cargo bench -p dogpaddle-store --bench append_log
cargo bench -p dogpaddle-store --bench append_log_endurance
cargo bench -p dogpaddle-operation --bench operation_core
cargo bench -p dogpaddle-flow --bench flow_lifecycle
cargo bench -p dogpaddle-change-store-integration --bench change_append_log
cargo bench -p dogpaddle-change-store-integration --bench change_append_log_endurance
```

PR 必须运行全量 correctness、格式化、Clippy 和文档检查，并实际执行缩小配置的 benchmark
protocol smoke，即：

```bash
cargo xtask check
cargo xtask bench-smoke
```

正式性能结果只由固定 reference runner 产生。

Change + AppendLog 的常规 reference 默认运行 5 个独立进程，endurance 默认运行 1 个；二者都要求
显式绝对 Store 路径和全新输出目录，并保存每次 stdout/stderr：

```bash
cargo xtask change-store-reference \
  --store-dir /absolute/path/on/reference-filesystem \
  --output-dir /absolute/new/result-directory

cargo xtask change-store-reference \
  --target endurance \
  --store-dir /absolute/path/on/reference-filesystem \
  --output-dir /absolute/new/endurance-result-directory
```

### 全工作区静态检查

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```
