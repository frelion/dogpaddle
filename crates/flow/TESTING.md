# Flow 测试与生命周期性能协议

本文件规定 `dogpaddle-flow` 的测试所有权、持久化兼容性验证和 `build/open` 生命周期基准。
dogpaddle-flow 是 Operation 与 Store 的产品组合根，因此真实资源创建、Operation 物化和重新
打开测试属于本 crate；它们不需要再放入 `integration-tests/flow-store`。当前已有 Station output
log、只读 input capability、唯一 owned Change cache、确定性拓扑 schedule、上游有界 GC 和公共
有界轮次骨架，但尚无公共
`Flow::start`、Change 处理或可观察 Sink；`Flow::advance` 已公开，但会明确到达
`Station::process` 的 `todo!()`。因此本协议锁定其有界轮次签名和这个显式处理边界，不伪造端到端
运行测试，也不建立空的 `integration-tests/engine` package。

## 目录与所有权

```text
crates/flow/
├── src/build/tests.rs                 # 私有 FlowFactory 构建、拓扑、Definition codec 与 CRC 测试
├── src/open/tests.rs                  # 私有两阶段 reopen 与资源重新装配测试
├── src/flow/runtime.rs                # 运行态 Flow 数据与生命周期 API
├── src/flow/advance.rs                # 单轮调度协议与 Flow::advance
├── src/flow/tests.rs                  # 私有运行态 Flow 所有权、schedule 与生命周期测试
├── src/station/runtime.rs             # Station 装配与 process 边界
├── src/station/input.rs               # durable active/cursor、owned cache 与 intake
├── src/station/gc.rs                  # consumer cursor capability 与有界 output GC
├── src/station/protocol.rs            # Station outcome 与错误
├── src/station/tests.rs               # 私有 Station/Operation/数据 capability 装配测试
├── tests/correctness.rs               # 唯一公共正确性 target
├── tests/correctness/
│   ├── lifecycle.rs                   # build/open 与 Store 独占
│   ├── validation.rs                  # 公共 FlowFactory 声明错误及零文件副作用
│   ├── persistence.rs                 # 资源布局、发布、缺失资源和 reopen
│   ├── malformed.rs                   # 损坏 Definition、稳定错误和 no-panic
│   └── support.rs                     # 只供测试使用的 Store fixture
├── tests/fixtures/v1/                 # 完整 Flow Definition 黄金字节
└── benches/
    ├── flow_lifecycle.rs              # 唯一生命周期 benchmark
    └── support/mod.rs                 # benchmark 根目录和临时 sample 路径
```

`src/**/tests.rs` 可以访问私有 Definition、codec、Flow 和 Station 字段，只验证无法通过公共 API
精确表达的实现契约。`tests/correctness.rs` 作为独立下游 crate，只使用 FlowFactory、Flow、
Operation 和 Store 的公共 API。公共持久化测试直接打开 Store 是有意的：dogpaddle-flow 拥有
这些正常依赖，资源名称和类型又是 Flow 的磁盘兼容性边界。测试 fixture 和 helper 不进入产品
库，也不为测试扩张公共 API。

benchmark 的本地 support 只拥有 Store 根目录与临时 sample 的生命周期；`flow_lifecycle.rs` 拥有
线性 DAG workload、计时边界、结果校验和人类摘要。严格 profile/配置解析、主机与文件系统指纹、
持续时间统计和 typed JSONL record/writer 统一来自 `dogpaddle-bench-protocol`，共享 crate 不创建或
打开 Flow，也不拥有 Store 生命周期。完整统一规则见[根目录测试协议](../../TESTING.md)。

## 正确性契约

公共正确性测试覆盖四组边界：

- **生命周期**：真实 `SequenceSource → Count` Flow 成功构建，声明顺序和 ID 保持不变，释放后
  可以重新打开；活动 Flow 独占 Store 路径。
- **纯校验**：空拓扑、非法或重复 ID、错误输入数量、外部 `StationRef`、空 source 列表、重复设置
  source、自环和间接环都返回稳定错误，且目标路径不存在。私有纯校验还穷举 1 到 5 个 Station
  的全部零输入 Source / 一输入 Count 标号小图，以独立的逐路径 oracle 验证合法 DAG、直接/间接环
  分类、声明顺序和 source 绑定；已经存在的 Store 必须原样保留。
- **持久化**：v1 Definition 黄金字节来自实际 `FlowFactory::build` 发布的 Cell；Flow/Station state、
  Station output 和内建 Operation data 使用稳定名称与精确类型；terminal producer 仍有 output；
  未发布、Definition 损坏、资源缺失和 Size 不匹配均被拒绝；完整 Flow 可以重新打开。
- **运行期装配**：分离的读写事务启动 capability 归 Flow 长期持有，Station 只保存自己的 state、完整
  可选 output、有序只读 input logs、唯一 owned input-Change cache 及 output consumer cursor 的只读
  capability，不长期持有事务启动能力。
  私有测试从 Flow 一侧临时开始事务并验证同一 `TransactionAccess` 可以访问 Station output，而
  下游只能经 `ReadOnly<AppendLog<Vec<u8>>>` 观察同一日志。source 即使在 target 之后声明，build
  和 reopen 仍按 source ID 正确注入；二者派生相同的分层拓扑 schedule，同层保持声明顺序，且
  公共有界轮次的方法签名保持固定。build 在发布 Definition 的同一事务中，用稳定 key 和 4 字节
  value 初始化 active input，用稳定 port key 和 8 字节 value 初始化每个 cursor。私有测试验证
  `Station::intake` 经独立 RO snapshot 从 active input 循环查找、跳过空端口、只缓存第一个可用
  entry 的 input/offset/Change，cache hit 完全不访问 Store；缺失、错误长度或越界 active input，
  缺失或非 8 字节 cursor，以及非法 Change 均被拒绝。上游 GC 取全部 consumer edge cursor 的最小值
  并且单次至多删除 1024 个 entry。`Flow::advance` 已固定每个成功 turn 后触发全部不同的直接上游，
  但在 `process` 仍为 `todo!()` 时不伪造成功路径；
  `Station::process(&mut Transactions) -> Result<ProcessOutcome, StationError>` 仍为明确 `todo!()`，
  Operation 统一执行协议尚未定义，测试不伪造提交行为。
- **鲁棒性**：带重新计算 CRC 的 magic、版本、UTF-8、source 引用和 Operation payload 变异必须
  到达并返回对应语义错误；确定性的截断、bit flip 和结构化垃圾输入调用
  `FlowFactory::open` 不得 panic，且必须在 Definition 解码阶段失败，不能由后续缺失资源错误
  冒充通过。

黄金 fixture 位于 `tests/fixtures/v1/sequence_source_count.hex`，包含 magic、版本、Station 顺序、
Operation tag/payload、source 连接和 CRC。当前开发期允许破坏性更新 v1；修改这些字节或稳定
资源名时必须显式更新 fixture、资源布局和 reopen 测试，不能把测试改成只验证新编码自洽。

`DefinitionChangedDuringOpen` 保护两阶段 open 之间的变化。当前测试不通过竞态、sleep 或
wall-clock 猜测制造这个窗口；除非以后出现无需扩张公共 API 的确定性故障注入点，否则保留该
防御分支而不增加易抖动测试。

## `flow_lifecycle` 性能边界

此 benchmark 只回答持久化 Flow 的冷路径成本，不代表运行时吞吐。工作负载是一条由一个
`SequenceSource` 和 `station_count - 1` 个 Count 组成的线性 DAG；`station_count` 是唯一规模轴。

| scenario | 计时内 | 计时外 |
| --- | --- | --- |
| `fresh_durable_build` | 一次 `FlowFactory::build`：拓扑校验、编码、fresh Store、全部资源创建及 durable Definition 发布 | 临时路径和 Factory 声明、结果校验、drop、重新打开校验、目录清理 |
| `warm_reopen` | 一次 `FlowFactory::open`：两阶段 Definition 读取、解码、state/Operation/output 打开、Operation 物化和只读 input 注入 | fixture 构建、preflight、预热、Station ID 校验和 drop |

fresh build 的每个预热和样本使用独立 Store 路径。warm reopen 使用同一个已提交 fixture，并在
采样前完成 preflight 和显式预热，因此它是 warm committed reopen，不是 cold filesystem cache。
两种场景都使用 Store 固定的 durable MDBX sync。输出只报告每次生命周期操作的 ns，以及
`station_count`；没有输入记录，因而不报告 rows/s、changes/s 或虚构的引擎吞吐。本阶段也不做
Flow endurance；当前只锁定有界 GC 的正确性，不把它冒充完整运行吞吐。

默认配置：

| profile | station counts | samples | warmups | Store 根目录 |
| --- | --- | ---: | ---: | --- |
| `smoke` | `1,8,64` | 3 | 1 | 未配置时使用隔离临时目录 |
| `reference` | `1,64,1024` | 9 | 2 | 必须显式提供绝对路径 |

环境变量：

- `DOGPADDLE_FLOW_BENCH_PROFILE=smoke|reference`
- `DOGPADDLE_FLOW_BENCH_STORE_DIR=/absolute/path`
- `DOGPADDLE_FLOW_BENCH_STATION_COUNTS=1,64,1024`
- `DOGPADDLE_FLOW_BENCH_SAMPLES=9`
- `DOGPADDLE_FLOW_BENCH_WARMUPS=2`

stdout 保留每个 scenario/station count 的人类摘要；每个以 `{` 开头的机器记录均由共享 writer
生成，是 typed JSONL `environment`、`configuration`、`sample` 或 `summary`。Flow 将 workload
profile 与实际 benchmark 根目录交给协议记录，并补充 MDBX sync；configuration 记录本地的 station
counts 与 setup/cache 口径，sample/summary 补充 `station_count`。Cargo profile、环境指纹和 reference
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
