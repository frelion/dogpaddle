# Flow 测试与生命周期性能协议

本文件规定 `dogpaddle-flow` 的测试所有权、持久化兼容性验证和 `build/open` 生命周期基准。
Flow 是 Operation 与 Store 的产品组合根，因此真实资源创建、Operation 物化和重新打开测试属于
本 crate；它们不需要再放入 `integration-tests/flow-store`。当前尚无 `run`、边日志或可观察
Sink，所以本协议不伪造端到端运行测试，也不建立空的 `integration-tests/engine` package。

## 目录与所有权

```text
crates/flow/
├── src/build/tests.rs                 # 私有拓扑、Definition codec 和 CRC 白盒测试
├── src/flow/tests.rs                  # 私有 Flow 生命周期状态装配测试
├── src/stage/tests.rs                 # 私有 Stage/Operation 容器测试
├── tests/correctness.rs               # 唯一公共正确性 target
├── tests/correctness/
│   ├── lifecycle.rs                   # build/open 与 Store 独占
│   ├── validation.rs                  # 公共 Builder 错误及零文件副作用
│   ├── persistence.rs                 # 资源布局、发布、缺失资源和 reopen
│   ├── malformed.rs                   # 损坏 Definition、稳定错误和 no-panic
│   └── support.rs                     # 只供测试使用的 Store fixture
├── tests/fixtures/v1/                 # 完整 Flow Definition 黄金字节
└── benches/
    ├── flow_lifecycle.rs              # 唯一生命周期 benchmark
    └── support/mod.rs                 # benchmark 根目录和临时 sample 路径
```

`src/**/tests.rs` 可以访问私有 Definition、codec 和 Stage 字段，只验证无法通过公共 API 精确
表达的实现契约。`tests/correctness.rs` 作为独立下游 crate，只使用 Flow、Operation 和 Store 的
公共 API。公共持久化测试直接打开 Store 是有意的：Flow 拥有这些正常依赖，资源名称和类型又是
Flow 的磁盘兼容性边界。测试 fixture 和 helper 不进入产品库，也不为测试扩张公共 API。

benchmark 的本地 support 只拥有 Store 根目录与临时 sample 的生命周期；`flow_lifecycle.rs` 拥有
线性 DAG workload、计时边界、结果校验和人类摘要。严格 profile/配置解析、主机与文件系统指纹、
持续时间统计和 typed JSONL record/writer 统一来自 `dogpaddle-bench-protocol`，共享 crate 不创建或
打开 Flow，也不拥有 Store 生命周期。完整统一规则见[根目录测试协议](../../TESTING.md)。

## 正确性契约

公共正确性测试覆盖四组边界：

- **生命周期**：真实 `SequenceSource → Count` Flow 成功构建，声明顺序和 ID 保持不变，释放后
  可以重新打开；活动 Flow 独占 Store 路径。
- **纯校验**：空拓扑、非法或重复 ID、错误输入数量、外部 `StageRef`、空 source 列表、重复设置
  source、自环和间接环都返回稳定错误，且目标路径不存在。私有纯校验还穷举 1 到 5 个 Stage
  的全部零输入 Source / 一输入 Count 标号小图，以独立的逐路径 oracle 验证合法 DAG、直接/间接环
  分类、声明顺序和 source 绑定；已经存在的 Store 必须原样保留。
- **持久化**：v1 Definition 黄金字节来自实际 `FlowBuilder::build` 发布的 Cell；Flow/Stage state
  和内建 Operation data 使用稳定名称与精确类型；未发布、Definition 损坏、资源缺失和 Size
  不匹配均被拒绝；完整 Flow 可以重新打开。
- **鲁棒性**：带重新计算 CRC 的 magic、版本、UTF-8、source 引用和 Operation payload 变异必须
  到达并返回对应语义错误；确定性的截断、bit flip 和结构化垃圾输入调用 `Flow::open` 不得 panic，
  且必须在 Definition 解码阶段失败，不能由后续缺失资源错误冒充通过。

黄金 fixture 位于 `tests/fixtures/v1/sequence_source_count.hex`，包含 magic、版本、Stage 顺序、
Operation tag/payload、source 连接和 CRC。修改这些字节或稳定资源名时，必须先给出迁移设计，再
显式更新 fixture、资源布局和 reopen 测试，不能把测试改成只验证新编码自洽。

`DefinitionChangedDuringOpen` 保护两阶段 open 之间的变化。当前测试不通过竞态、sleep 或
wall-clock 猜测制造这个窗口；除非以后出现无需扩张公共 API 的确定性故障注入点，否则保留该
防御分支而不增加易抖动测试。

## `flow_lifecycle` 性能边界

此 benchmark 只回答持久化 Flow 的冷路径成本，不代表运行时吞吐。工作负载是一条由一个
`SequenceSource` 和 `stage_count - 1` 个 Count 组成的线性 DAG；`stage_count` 是唯一规模轴。

| scenario | 计时内 | 计时外 |
| --- | --- | --- |
| `fresh_durable_build` | 一次 `FlowBuilder::build`：拓扑校验、编码、fresh Store、全部资源创建及 durable Definition 发布 | 临时路径和 Builder 声明、结果校验、drop、重新打开校验、目录清理 |
| `warm_reopen` | 一次 `Flow::open`：两阶段 Definition 读取、解码、资源打开和 Operation 物化 | fixture 构建、preflight、预热、Stage ID 校验和 drop |

fresh build 的每个预热和样本使用独立 Store 路径。warm reopen 使用同一个已提交 fixture，并在
采样前完成 preflight 和显式预热，因此它是 warm committed reopen，不是 cold filesystem cache。
两种场景都使用 Store 固定的 durable MDBX sync。输出只报告每次生命周期操作的 ns，以及
`stage_count`；没有输入记录，因而不报告 rows/s、changes/s 或虚构的引擎吞吐。本阶段也不做
Flow endurance，长期日志空间和 GC 属于已存在的 Store/Change+Store 协议。

默认配置：

| profile | stage counts | samples | warmups | Store 根目录 |
| --- | --- | ---: | ---: | --- |
| `smoke` | `1,8,64` | 3 | 1 | 未配置时使用隔离临时目录 |
| `reference` | `1,64,1024` | 9 | 2 | 必须显式提供绝对路径 |

环境变量：

- `DOGPADDLE_FLOW_BENCH_PROFILE=smoke|reference`
- `DOGPADDLE_FLOW_BENCH_STORE_DIR=/absolute/path`
- `DOGPADDLE_FLOW_BENCH_STAGE_COUNTS=1,64,1024`
- `DOGPADDLE_FLOW_BENCH_SAMPLES=9`
- `DOGPADDLE_FLOW_BENCH_WARMUPS=2`

stdout 保留每个 scenario/stage count 的人类摘要；每个以 `{` 开头的机器记录均由共享 writer
生成，是 typed JSONL `environment`、`configuration`、`sample` 或 `summary`。Flow 将 workload
profile 与实际 benchmark 根目录交给协议记录，并补充 MDBX sync；configuration 记录本地的 stage
counts 与 setup/cache 口径，sample/summary 补充 `stage_count`。Cargo profile、环境指纹和 reference
比较规则不在本 crate 重复定义，统一遵循根目录协议。

## 命令

```bash
cargo test -p dogpaddle-flow
cargo test -p dogpaddle-flow --test correctness
cargo clippy -p dogpaddle-flow --all-targets -- -D warnings
cargo doc -p dogpaddle-flow --no-deps

cargo bench -p dogpaddle-flow --bench flow_lifecycle

DOGPADDLE_FLOW_BENCH_PROFILE=reference \
DOGPADDLE_FLOW_BENCH_STORE_DIR=/absolute/path/on/reference/filesystem \
cargo bench -p dogpaddle-flow --bench flow_lifecycle
```
