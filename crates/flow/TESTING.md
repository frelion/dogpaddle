# Flow 测试与性能协议

本文件规定 `dogpaddle-flow` 的测试所有权、持久化兼容性验证、`build/open` 生命周期基准和运行期
单轮调度基准。
dogpaddle-flow 是 Operation 与 Store 的产品组合根，因此真实资源创建、Operation 物化和重新
打开测试属于本 crate；它们不需要再放入 `integration-tests/flow-store`。当前已有 Station output
log、`Inbox`/owned `Claim`、确定性拓扑 schedule、Operation `Action::{Idle, Commit, Complete}`、
Complete 内联原子 ack/reclaim、`head == min(consumer cursors)`、强重放、per-output retained-byte
高水位和公共 `Flow::advance` 三态。尚无公共 `Flow::start` 或外部可观察 Sink；真实 Store 内执行链
仍由本 crate 验证，不建立重复的
`integration-tests/engine` package。

## 目录与所有权

```text
crates/flow/
├── src/build/tests.rs                 # 私有 FlowFactory 构建、拓扑、Definition codec 与 CRC 测试
├── src/build/open.rs                  # FlowFactory 两阶段 reopen 与资源重新装配
├── src/flow/runtime.rs                # 运行态 Flow 数据与生命周期 API
├── src/flow/advance.rs                # 单轮调度协议与 Flow::advance
├── src/flow/tests.rs                  # 私有运行态 Flow 所有权、schedule 与生命周期测试
├── src/station/runtime.rs             # Station 装配与 process 边界
├── src/station/input.rs               # Inbox/Claim、durable cursor 与内联 retention/reclaim
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
    ├── flow_lifecycle.rs              # build/open 生命周期 benchmark
    ├── flow_runtime.rs                # advance 稳态调度 benchmark
    └── support/mod.rs                 # benchmark 根目录和临时 sample 路径
```

`src/**/tests.rs` 可以访问私有 Definition、codec、Flow 和 Station 字段，只验证无法通过公共 API
精确表达的实现契约。`tests/correctness.rs` 作为独立下游 crate，只使用 FlowFactory、Flow、
Operation 和 Store 的公共 API。公共持久化测试直接打开 Store 是有意的：dogpaddle-flow 拥有
这些正常依赖，资源名称和类型又是 Flow 的磁盘兼容性边界。测试 fixture 和 helper 不进入产品
库，也不为测试扩张公共 API。

benchmark 的本地 support 只拥有 Store 根目录与临时 sample 的生命周期；`flow_lifecycle.rs` 拥有
线性 DAG 的 build/open workload，`flow_runtime.rs` 拥有 source/sink、Count chain、fan-out 和
capacity-pressure 的 `advance` workload；二者各自拥有计时边界、结果校验和人类摘要。严格
profile/配置解析、主机与文件系统指纹、持续时间统计和 typed JSONL record/writer 统一来自
`dogpaddle-bench-protocol`，共享 crate 不创建或打开 Flow，也不拥有 Store 生命周期。完整统一规则见
[根目录测试协议](../../TESTING.md)。

## 正确性契约

公共正确性测试覆盖四组边界：

- **生命周期**：真实 `SequenceSource → Count → Discard` Flow 成功构建，声明顺序和 ID 保持不变，释放后
  可以重新打开；活动 Flow 独占 Store 路径。
- **纯校验**：空拓扑、非法或重复 ID、错误输入数量、外部 `StationRef`、空 source 列表、重复设置
  source、遗漏/重复 output capacity、outputless Station 配置 capacity、自环、间接环、非 Source
  起点、非 Sink 终点和 Sink 作为上游都返回稳定错误，且目标路径
  不存在。多个 Source、多个 Sink 和多个合法分量可以构建。私有纯校验还穷举 1 到 5 个
  Source/Count 标号小图，并为每个叶节点连接 Discard，以独立的逐路径 oracle 验证合法 DAG、
  直接/间接环分类、声明顺序和 source 绑定；已经存在的 Store 必须原样保留。
- **持久化**：包含每 Station capacity 的新 v1 Definition 黄金字节来自实际 `FlowFactory::build`
  发布的 Cell；Flow/Station state、
  Station output 和内建 Operation data 使用稳定名称与精确类型；
  `OperationKind::has_output()` 为 true 的 Station 创建日志，Discard Sink 没有 output；
  未发布、Definition 损坏、资源缺失和 Size 不匹配均被拒绝；完整 Flow 可以重新打开。
- **运行期装配**：分离的读写事务启动 capability 归 Flow 长期持有，Station 只保存自己的 state、完整
  可选 output 与拥有有序 `InputPort` 和至多一个 owned `Claim` 的 `Inbox`，不长期持有事务启动能力。
  每个 InputPort 持有只读 input log、共享 `OutputRetention` 和 consumer slot；OutputRetention 组合
  producer output 与全部 consumer cursor 的只读 capability。
  私有测试从 Flow 一侧临时开始事务并验证同一 `TransactionAccess` 可以访问 Station output，而
  下游只能经 `ReadOnly<AppendLog<Vec<u8>>>` 观察同一日志。source 即使在 target 之后声明，build
  和 reopen 仍按 source ID 正确注入；二者派生相同的分层拓扑 schedule，同层保持声明顺序，且
  公共有界轮次的方法签名保持固定。build 在发布 Definition 的同一事务中，用稳定 key 和 4 字节
  value 初始化 active input，用稳定 port key 和 8 字节 value 初始化每个 cursor。私有测试验证
  输入准备经独立 RO snapshot 从 active input 循环查找、跳过空端口、只选择第一个可用 entry，并在
  Operation 调用前用独立短写事务把选中 port durable-pin 为 active input；已有 Claim 时完全不访问
  Store。缺失、错误长度或越界 active input，缺失或非 8 字节 cursor，以及非法 Change 均被拒绝。
  reopen 从 active/cursor 重建相同 `(port, offset, bytes)` 的完整 Change。`Station::process` 不在
  每个 turn 前重读 Store 校验 Claim；`Idle` 回滚 Operation 写入并保留 Claim，`Commit` 原子提交
  continuation 和可选 output 但不推进 cursor、轮转 active、回收 entry 或清除 Claim，`Complete`
  才验证 Claim 的 port/offset 与 durable active/cursor 相同，并原子提交状态、可选 output、cursor
  推进、active 轮转和必要的物理 head 前进，commit 后才清除 Claim。零输入 Source 通过相同的
  `turn(None, ...)` 和 `Action::Commit` 路径执行；返回 `Action::Complete` 会被拒绝。测试覆盖 Commit
  后同一完整 Change 重放、Complete 后退休、reopen 后 Claim 恢复、Complete 时 durable identity
  不匹配拒绝、output append 失败回滚、capacity 拒绝后的 Source/Commit/Complete 全事务回滚、
  oversize 空日志准入、物理 head 释放与无重复输出。

  build 后的每个已提交事务边界，每个 producer output 都必须满足
  `head == min(all consumer edge cursors)`，且全部 cursor 位于 `[head, tail]`；open 显式验证这些
  条件并拒绝损坏状态。单个 Complete 只推进一条 edge cursor 一个 offset，因此新的最小值最多从
  head 前进到 `head + 1`，只需在同一事务中至多回收一个 entry。fan-out、重复 source edge 和最慢
  consumer 测试证明所有 edge cursor 都参与最小值；cursor、active、head 和 retained bytes 在错误、
  背压或 commit 失败时一起回滚。没有独立回收 phase、补偿调度或回收 progress 状态。

  `Flow::advance` 在同一轮让拓扑下游观察上游已提交 output，并按 schedule 给每个 Station 至多一个
  turn。outcome 按 `Progressed > Backpressured > Idle` 聚合；Operation commit、durable pin 和 Complete
  内联 head 前进都算 progress，背压不得短路后续 Station。SequenceSource 的最终 `u64::MAX` 已提交
  但尚未消费时，reopen 后 source 的稳定 `Idle` 不得阻断下游退休该 Change。
- **鲁棒性**：带重新计算 CRC 的 magic、版本、UTF-8、source 引用和 Operation payload 变异必须
  到达并返回对应语义错误；确定性的截断、bit flip 和结构化垃圾输入调用
  `FlowFactory::open` 不得 panic，且必须在 Definition 解码阶段失败，不能由后续缺失资源错误
  冒充通过。

黄金 fixture 位于 `tests/fixtures/v1/sequence_source_count_discard.hex`，包含 magic、版本、Station
顺序、Operation tag/payload、每个 Station 的 output capacity、source 连接和 CRC。当前开发期允许
破坏性更新 v1；修改这些字节或稳定资源名时必须显式更新 fixture、资源布局和 reopen 测试，不能
把测试改成只验证新编码自洽。

`DefinitionChangedDuringOpen` 保护两阶段 open 之间的变化。当前测试不通过竞态、sleep 或
wall-clock 猜测制造这个窗口；除非以后出现无需扩张公共 API 的确定性故障注入点，否则保留该
防御分支而不增加易抖动测试。

## `flow_lifecycle` 性能边界

此 benchmark 只回答持久化 Flow 的冷路径成本，不代表运行时吞吐。工作负载是一条由一个
`SequenceSource`、`station_count - 2` 个 Count 和一个 Discard 组成的线性 DAG；`station_count`
是唯一规模轴，最小值为 2。

| scenario | 计时内 | 计时外 |
| --- | --- | --- |
| `fresh_durable_build` | 一次 `FlowFactory::build`：拓扑校验、编码、fresh Store、全部资源创建及 durable Definition 发布 | 临时路径和 Factory 声明、结果校验、drop、重新打开校验、目录清理 |
| `warm_reopen` | 一次 `FlowFactory::open`：两阶段 Definition 读取、解码、state/Operation/output 打开、Operation 物化和只读 input 注入 | fixture 构建、preflight、预热、Station ID 校验和 drop |

fresh build 的每个预热和样本使用独立 Store 路径。warm reopen 使用同一个已提交 fixture，并在
采样前完成 preflight 和显式预热，因此它是 warm committed reopen，不是 cold filesystem cache。
两种场景都使用 Store 固定的 durable MDBX sync。输出只报告每次生命周期操作的 ns，以及
`station_count`；没有输入记录，因而不报告 rows/s、changes/s 或虚构的引擎吞吐。本阶段也不做
Flow endurance。

默认配置：

| profile | station counts | samples | warmups | Store 根目录 |
| --- | --- | ---: | ---: | --- |
| `smoke` | `2,8,64` | 3 | 1 | 未配置时使用隔离临时目录 |
| `reference` | `2,64,1024` | 9 | 2 | 必须显式提供绝对路径 |

环境变量：

- `DOGPADDLE_FLOW_BENCH_PROFILE=smoke|reference`
- `DOGPADDLE_FLOW_BENCH_STORE_DIR=/absolute/path`
- `DOGPADDLE_FLOW_BENCH_STATION_COUNTS=2,64,1024`
- `DOGPADDLE_FLOW_BENCH_SAMPLES=9`
- `DOGPADDLE_FLOW_BENCH_WARMUPS=2`

stdout 保留每个 scenario/station count 的人类摘要；每个以 `{` 开头的机器记录均由共享 writer
生成，是 typed JSONL `environment`、`configuration`、`sample` 或 `summary`。Flow 将 workload
profile 与实际 benchmark 根目录交给协议记录，并补充 MDBX sync；configuration 记录本地的 station
counts 与 setup/cache 口径，sample/summary 补充 `station_count`。Cargo profile、环境指纹和 reference
比较规则不在本 crate 重复定义，统一遵循根目录协议。

## `flow_runtime` 性能边界

此 benchmark 回答预先构建的持久化 Flow 执行连续 `Flow::advance` 轮次的成本。每个 sample 计时内
只有固定次数的 `advance` 调用和 outcome 计数；Flow build、capacity-pressure backlog 注入与 reopen、
预热、状态校验和清理都在计时外。所有场景的每轮聚合结果都必须是 `Progressed`，否则样本无效。
当前内建 input Operation 只覆盖 Complete 路径，因此这个 benchmark 不冒充 retained-input
continuation 的性能数据。

| scenario | topology / 压力 | 本轮覆盖 |
| --- | --- | --- |
| `sink_steady` | `SequenceSource → Discard`，宽松 capacity | source Commit、sink Complete、cursor advance 与单 consumer 内联 reclaim |
| `chain_steady` | `SequenceSource → Count... → Discard` | 按 Station 数量扩展的拓扑调度、逐级 output、Complete 与 inline reclaim |
| `fanout_steady` | 一个 SequenceSource fan-out 到多个 Discard | per-edge cursor、min-cursor retention 与 fan-out 装配成本 |
| `capacity_pressure_steady` | source output capacity 为 1 byte，计时前注入 backlog | producer append 背压全事务回滚，同时下游 Complete/reclaim 使整轮继续 Progressed |

默认配置：

| profile | chain station counts | fan-outs | rounds/sample | samples | warmup rounds |
| --- | --- | --- | ---: | ---: | ---: |
| `smoke` | `3,8` | `1,4` | 32 | 3 | 4 |
| `reference` | `3,8,32` | `1,4,16` | 1024 | 9 | 64 |

`flow_runtime` 与 `flow_lifecycle` 共享 `DOGPADDLE_FLOW_BENCH_PROFILE` 和
`DOGPADDLE_FLOW_BENCH_STORE_DIR`；运行期规模还可用下列变量覆盖：

- `DOGPADDLE_FLOW_RUNTIME_BENCH_CHAIN_STATIONS=3,8,32`
- `DOGPADDLE_FLOW_RUNTIME_BENCH_FANOUTS=1,4,16`
- `DOGPADDLE_FLOW_RUNTIME_BENCH_ROUNDS_PER_SAMPLE=1024`
- `DOGPADDLE_FLOW_RUNTIME_BENCH_SAMPLES=9`
- `DOGPADDLE_FLOW_RUNTIME_BENCH_WARMUP_ROUNDS=64`

sample/summary 记录 scenario、topology、station count、fan-out、capacity mode、round 数与预期 outcome；
它只报告每组 advance 轮次的 duration，不从当前批量 Change 推导 rows/s。正式 reference 运行仍必须
使用显式绝对 Store 根目录。

## 命令

```bash
cargo test -p dogpaddle-flow
cargo test -p dogpaddle-flow --test correctness
cargo clippy -p dogpaddle-flow --all-targets -- -D warnings
cargo doc -p dogpaddle-flow --no-deps

cargo bench -p dogpaddle-flow --bench flow_lifecycle
cargo bench -p dogpaddle-flow --bench flow_runtime

DOGPADDLE_FLOW_BENCH_PROFILE=reference \
DOGPADDLE_FLOW_BENCH_STORE_DIR=/absolute/path/on/reference/filesystem \
cargo bench -p dogpaddle-flow --bench flow_lifecycle

DOGPADDLE_FLOW_BENCH_PROFILE=reference \
DOGPADDLE_FLOW_BENCH_STORE_DIR=/absolute/path/on/reference/filesystem \
cargo bench -p dogpaddle-flow --bench flow_runtime
```
