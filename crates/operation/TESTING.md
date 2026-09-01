# dogpaddle-operation 测试说明

本 crate 拥有 Operation Definition、稳定 codec、类型化数据声明、materialize，以及当前具体
Operation 的状态推进测试。`dogpaddle-store` 是 Operation 的正式运行依赖，因此 Cell 状态的
commit、rollback、错误传播和 reopen 属于本 crate 的公共正确性，而不是额外的跨 crate 测试包。

Flow 负责完整 Station 资源名、资源创建顺序、Flow Definition 布局和 build/open 后的装配；这些
组合契约应在 `dogpaddle-flow` 的公共测试中验证。Operation 自身拥有 object-safe Change turn
接口、有序输入端口、`Idle`/`Commit`/`Complete` action、可选 output 和可 downcast 错误，因此这些
无需日志或 Station 的行为由本 crate 直接验证。`Commit` 后 durable input claim、同一 Change
重放和 `Complete` 后退休属于 Flow 与 Store 的组合契约，由 flow crate 验证。

## 目录职责

```text
crates/operation/
├── src/tests.rs                 # 私有 decoder registry 与类型擦除白盒测试
├── tests/correctness.rs         # 公共正确性唯一 Cargo target
├── tests/correctness/
│   ├── codec.rs                # 稳定编码、拒绝路径与 no-panic
│   ├── definition.rs           # Definition、声明、create/open/materialize
│   ├── source.rs               # SequenceSource 状态与事务语义
│   ├── transform.rs            # Count 状态与事务语义
│   ├── sink.rs                 # Discard 输入完成与无输出语义
│   └── support.rs              # 临时 Store 与测试 fixture 辅助
├── tests/fixtures/v1/           # Definition v1 黄金字节
└── benches/
    ├── operation_core.rs        # codec、事务体与 durable transaction
    └── support/mod.rs           # 本地 Store 生命周期、workload 字段与报告适配
```

benchmark 作为 dev target 使用零产品依赖的 `dogpaddle-bench-protocol`。共享 crate
统一严格解析 benchmark/Cargo profile 与正整数环境变量，采集 rustc、CPU、git 和文件系统
指纹，构造 typed JSONL record，并计算持续时间统计。本地 `benches/support/mod.rs` 只定义
Operation 的环境变量名与默认 workload、`BenchRoot`/`SampleStore` 生命周期、业务字段和
人类可读报告；fixture、计时边界与结果 oracle 仍由 `operation_core.rs` 拥有。

`src/tests.rs` 可以访问私有 `DECODERS`、`DataName` 和类型擦除容器，因此负责 decoder tag 与内建
Definition 集合双向一致、逻辑名合法唯一，以及实例按名称而非插入顺序绑定。duplicate、missing、
wrong class、不同 collection、不同 `Small`/`Large` 和 unconsumed 都在白盒层覆盖，公共层不重复
这些机械分支。

公共 correctness target 只从外部 crate 视角使用公开 API。`DataDeclaration`、`DataInstances`
虽然是 `#[doc(hidden)]`，却是 Flow 实际使用的 crate-to-crate 接缝，因此公共测试覆盖声明的精确
逻辑名以及 create → close → reopen → declaration open → materialize。Flow 的
`station/{index:08x}/...` 完整物理名不属于本 crate。

## 正确性口径

Definition codec 的 tag、payload 和完整字节是持久化兼容性边界。`tests/fixtures/v1/` 保存独立
可审查的十六进制黄金字节；测试要求 encode 精确匹配，decode 后重新 encode 仍逐字节相同。每个
合法 fixture 的所有严格前缀都必须拒绝，magic/version/tag/trailing bytes 分别锁定错误类别，固定
字节生成器额外证明任意探针不会引发 panic。

每个 Definition 的显式 `OperationKind` 都纳入公开或 registry 测试；kind 同时约束 nominal role、
非零 input arity 和 output 属性。每个有状态 Operation 覆盖固定 output Schema/diff、action 形状、
commit 后 reopen、显式
rollback、边界错误不改状态，以及错误后再次读取。SequenceSource 锁定无输入 commit 的
`Action::Commit(Some(_))`，Count 锁定有输入的 `Action::Complete(Some(_))`。Count 使用多行 Change 验证其当前
整批原子完成策略，并用稳定重批锁定展平 output 与最终状态逐项不变；带正、负和大幅 diff 的输入
锁定其“每行一个事件”的计数语义。SequenceSource 覆盖单行推进、含 `u64::MAX` 的最后成功输出和
后续 turn 稳定 `Idle`。无状态 Discard 锁定 `Sink` kind 和 `Action::Complete(None)`，
并拒绝缺失输入或非零端口。绑定到另一个 Store 的
`TransactionAccess` 必须透明返回 `StoreError::WrongStore`；同 placement、错误持久化 codec 也必须
安全返回 `StoreError::Codec`，并保持原始字节不变。Store 自己的事务中毒、物理 placement、
崩溃恢复和通用 Cell codec 由 `dogpaddle-store` 测试拥有，不在这里重复。

普通测试没有 wall-clock 断言，也不依赖测试执行顺序。

## `operation_core` 性能协议

唯一正常性能入口覆盖四类场景：

- Count、SequenceSource 与 Discard Definition 的公开 encode；
- 同一份预编码 Definition 的公开 decode；
- 已开始事务内的 N 次单行 `turn`，计时只包含 Operation 调用，随后 rollback；
- begin + N 次单行 `turn` + durable commit 的完整事务。

默认 `turns/transaction` 为 1、64、1024，分别观察单 turn 成本和事务摊销。codec fixture、Store 创建、
预热、期望值计算与状态校验均不计时；rollback body 与 durable workload 使用彼此独立的 Store，
durable 预热及每个计入统计的样本也各自使用新 Store，避免页复用、缓存和状态历史串扰样本。
每个 durable 预热或测量样本在计时后先完成状态校验，再立即 drop 其 `SampleStore`；
rollback body 的场景 Store 也在该场景验证完成后释放。runner 在输出样本前确认 run root 已无
任何 sample 目录，不会把所有 Store 积累到整次 benchmark 结束。

每个样本通过共享协议输出 typed JSONL，至少包含原始 `elapsed_ns`、operation 数、
transaction 数和 turns/transaction；每个场景还输出协议统一的 min/median/max summary record。
环境记录包含 rustc、OS/kernel、CPU、profile、git revision/state、文件系统和实际 Store 路径。
stdout 同时包含便于本机阅读的摘要与 JSONL；机器收集器只读取首字符为 `{` 的行。
environment、configuration、sample 和 summary 都由 typed record + validated `Fields` 构造，且带有
`"benchmark":"operation_core"`；本地 support 不拼接 JSON fragment。

Operation benchmark 的 Count/Sequence turn 固定处理或产生一行，因此 ns/operation 同时对应单行 turn 成本；
报告仍使用统一的 operation/transaction 字段。两个有状态 Operation 都只覆写固定大小 Cell，
Discard 无状态；
独立 endurance 会重复 Store 的页复用协议，因此当前明确不设置 Operation endurance target。出现
无界状态算子或真实 Change 调度循环后，应在拥有该组合生命周期的 runtime 集成层新增长稳协议。

环境变量：

| 变量 | 默认值 | 含义 |
| --- | ---: | --- |
| `DOGPADDLE_CARGO_PROFILE` | `bench` | 显式 `--profile` 时必须设为同名值 |
| `DOGPADDLE_OPERATION_BENCH_PROFILE` | `smoke` | `smoke` 使用临时目录；`reference` 强制固定文件系统 |
| `DOGPADDLE_OPERATION_BENCH_STORE_DIR` | 未设置 | smoke 可选；reference 必须设置为绝对、可创建的父目录 |
| `DOGPADDLE_OPERATION_BENCH_SAMPLES` | 9 | 每个场景的测量样本数 |
| `DOGPADDLE_OPERATION_BENCH_CODEC_OPERATIONS` | 100000 | 每个 codec 样本的调用数 |
| `DOGPADDLE_OPERATION_BENCH_BODY_TRANSACTIONS_PER_SAMPLE` | 512 | rollback body 样本聚合的事务数 |
| `DOGPADDLE_OPERATION_BENCH_DURABLE_TRANSACTIONS_PER_SAMPLE` | 64 | durable 样本中的提交事务数 |
| `DOGPADDLE_OPERATION_BENCH_WARMUP_TRANSACTIONS` | 4 | 每个 turn workload 的未报告预热事务数 |
| `DOGPADDLE_OPERATION_BENCH_TURNS_PER_TRANSACTION` | `1,64,1024` | 逗号分隔且不重复的事务内 turn 数 |

正式对比必须设置 `DOGPADDLE_OPERATION_BENCH_PROFILE=reference`，并把
`DOGPADDLE_OPERATION_BENCH_STORE_DIR` 显式指向固定文件系统上的绝对路径；目录不存在时 runner
会创建它，随后 canonicalize 并验证为目录。还必须固定 rustc、机器、同步模式和所有 workload
参数。environment 与 configuration JSON 都用 `profile` 记录 `smoke|reference`；environment
还单独记录声明的 Rust 构建 profile 与 debug assertions。默认
`cargo bench` 声明为 `bench`；使用 `--profile <name>` 时必须同时设置
`DOGPADDLE_CARGO_PROFILE=<name>`。未建立 reference 基线前只保存
并比较原始配对样本，不规定绝对 SLA。

## 命令

```bash
cargo test -p dogpaddle-operation
cargo test -p dogpaddle-operation --test correctness
cargo test -p dogpaddle-operation --release
cargo clippy -p dogpaddle-operation --all-targets -- -D warnings
cargo doc -p dogpaddle-operation --no-deps

cargo bench -p dogpaddle-operation --bench operation_core

# PR 级全工作区检查与固定缩小参数的 release benchmark smoke
cargo xtask check
cargo xtask bench-smoke
```

单个 target 命令用于本地迭代；PR 必须使用上述两个 xtask 入口。全工作区的测试所有权、
typed benchmark 协议、smoke matrix 与 reference 规则以根目录 [`TESTING.md`](../../TESTING.md) 为准。
