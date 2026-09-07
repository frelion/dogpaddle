# DogPaddle 验证规范

DogPaddle 只保留能够证明当前公共语义、持久化格式、事务边界或性能口径的证据。验证代码按产品所有权组织，不按 runner、实验阶段或历史入口组织；Git 历史是旧基础设施的唯一存档。

## 五类验证

1. `crates/<owner>/src/**/tests.rs`：必须访问私有状态的单元测试、故障注入和独立算法 oracle。
2. `crates/<owner>/tests/correctness.rs`：该产品 crate 唯一的公共测试 target，领域文件位于相邻的 `tests/correctness/`。
3. `integration-tests/<seam>/`：仅用于没有产品组合根的 sibling seam。当前只有 `integration-tests/change-store/`。
4. `system-tests/`：依赖真实 Java、Debezium 或 PostgreSQL 的系统验收。
5. `crates/<owner>/benches/`：由 workload owner 直接拥有的 benchmark。

不存在通用实验框架。准备合并的实验必须归入 correctness、system test 或 benchmark；否则留在临时分支。

产品 manifest 关闭自动 test、bench 和 example 发现，并显式声明保留的 target。产品 library 设置 `bench = false`。测试不得为了 fixture 扩张产品 API，也不得建立跨产品的测试 DSL、通用算子案例表或第二套运行模型。

## 证据所有权

- Change 拥有 Schema、Change、Projection、Arrow IPC 字节格式、互操作、损坏拒绝和稳定事件顺序。
- Store 拥有事务、能力边界、集合布局、分页、容量、reopen 和 crash consistency。
- Operation 拥有 Operation + Store：Definition、稳定 tag/payload、bind、materialize、运行协议、状态和算子语义。
- Flow 拥有 Flow + Operation + Change + Store：拓扑、全图 binding、资源装配、调度、claim、背压、回收、fail-stop、status 和 reopen。
- SQL 拥有 SQL + Flow + Operation：单语句 parser subset、端点参数、DataFusion coercion、LogicalPlan lowering 和 SQL 层 reopen。
- Debezium 拥有 connector-neutral runtime、bundle、checkpoint、delivery 和 ACK 生命周期。
- Change 与 Store 的外部组合只由 `integration-tests/change-store/` 证明。

能通过公共 API 证明的行为不得留在白盒测试中。删除测试前必须指出更低层或更靠近 owner 的替代证据；测试数量和代码行数不是正确性指标。

### Operation

Operation 的公共测试采用垂直所有权：每个内建算子各有一个文件，自己拥有 literal golden、kind、data declaration、bind、materialize、runtime 和 reopen 证据。跨算子文件只保留：

- `definition_codec`：外层 envelope、unknown tag 和通用损坏拒绝；
- `expression`：DataFusion Expr protobuf、精确 Schema binding 和 evaluate；
- `protocol`：`turn -> PreparedTurn -> AfterCommit` 与事务协议；
- `metamorphic`：稳定重批和独立模型。

生产 decoder registry、`src/tests.rs` 中的白盒手写 tag 列表和各算子文件中的公共 literal golden 必须是三份独立证据。不得建立 `BuiltinContractCase` 或从产品 registry 反向生成期望值。

Aggregate 的 owner 文件必须证明 tag `14` 与 `aggregate.groups/entries/control` 三资源、完整 Definition
roundtrip、精确 output Schema、exact-row admission 的整 turn rollback、Fold 与 Indexed 结果变化、MIN/MAX
当前值撤回后的 index reopen/重扫，以及不变结果不产生冗余 output。函数 descriptor、argument tuple layout
和 collision bucket codec 属于 Operation 私有实现，不在 Flow 或 SQL 复制 oracle。

### Flow

Flow correctness 按机制分为 `binding`、`topology`、`definition` 与运行期领域。Flow 只保留能证明全图机制的代表性算子：Project 的纯失败和 reopen/rebind、UnionAll 的多输入、Distinct 持久状态在 output 背压下的原子 rollback/reopen、SQLite Sink、PostgreSQL 运行资源，以及 temporal/decimal unary chain。算子自身的 payload、表达式和运行语义归 Operation，不在 Flow 逐个复制。

Flow 独有的 runtime、事务、背压、claim、reclaim、fail-stop 和 status 证据必须保留。

### SQL

SQL 只有一个公共 `correctness` target，并只通过 `SqlProgram::{parse,read,build,open}` 验证产品契约。护栏分四层：parser/endpoint 参数契约；所有拒绝路径不创建 Flow；Sequence→SQLite 的结果与精确目标列结构；真实 PostgreSQL 的端到端恢复。普通表和全部未支持节点必须在创建 Flow 路径前拒绝，AST 层还必须拒绝 DataFusion 可能擦除的 sampling、hint、row lock、typed alias 与 `LIMIT ALL`。

SQLite 结果矩阵必须覆盖别名与 qualified column、隐式 cast、CASE、TRY_CAST、算术、CTE fan-out、多 Scan、`UNION ALL` 的首分支列名、common type、nullable widening 和重复行语义，以及 `SELECT DISTINCT` 的最终 exact-row 结果；不得只断言 build 或一次 advance 成功。Distinct witness 必须跨 drop/open，证明已经提交的权重状态会恢复且后续重复不会再次输出。Aggregate witness 必须在一个非空 GROUP BY 中覆盖 `COUNT(*)`、同义 `COUNT(1)`、nullable `COUNT(expr)`、signed/unsigned SUM、signed/unsigned AVG、MIN/MAX 的最终关系并跨 drop/open；纯分组另有最终结果 witness。global aggregate、grouping sets、聚合 modifier/UDF、浮点 group key 与未支持参数类型必须证明不创建 Flow。不可达 Scan 声明也必须有同样的无目录副作用证据。公共链路同时验证固定 Station ID、64 MiB output capacity、drop/open 后持久 position，以及不同 SQL 调用 `open` 不会替换磁盘 Definition。每新增一种 SQL LogicalPlan lowering，都必须增加至少一个最终结果 witness；每新增一种明确拒绝的节点，都必须增加无目录副作用 witness。真实 PostgreSQL gate另外覆盖 `postgres_cdc → CTE/Filter/nullable UnionAll → postgres`、目标提交后本地结算前终止和 reopen 幂等重投。

`system-tests/postgres/check_sql.py --trace-output ...` 可在完整验收通过后导出该场景的 SQL、
宿主 I/O、SIGKILL、PostgreSQL 重放日志和关系快照，供 [持续 ETL 演示](docs/demo/README.md) 排版。
启用 trace 时，还会追加 28 次有界连续源表变更，每次完整目标关系与原生 PostgreSQL SQL oracle
核对，并记录推进前后快照。断言与故障边界仍由 SQL 系统验收拥有；视频间隔不作为性能证据。

### 私有测试拆分

每个源码模块目录只有一个 `tests.rs` 入口。超大模块可在同目录的 `tests/` 下按完整领域拆分；不要按每个生产源码文件建立镜像目录。当前较大的分区为：

- Station：`support`、`layout`、`claim`、`transaction`、`completion`；
- Change codec：`support`、`schema`、`projection`、`batch_layout`，精确 subprocess case 留在 `tests.rs`；
- SQLite Sink：`row`、`target`。

## 正确性证据准入

每个持久化协议至少具有：

1. literal golden 或独立 raw-layout 断言；
2. decode、open 和 reopen；
3. 不复用生产算法的语义或互操作 oracle；
4. malformed/corruption 拒绝，无 panic、无部分写入；
5. 精确资源名、类型、Size/codec 和失败后状态。

持久化定义或布局变更必须同时覆盖成功构建、纯校验失败无文件副作用、不完整构建、稳定编码、资源布局和重新打开。项目不识别、迁移或兼容未发布的旧格式；旧数据库直接删除并重建。

普通 correctness 测试不得依赖 wall-clock 断言、系统 Java 或外部 PostgreSQL。没有覆盖率或代码行数 CI 阈值。

## 性能所有权

`test-support/perf-context/` 只提供：

- `PerformanceProfile`；
- `RunRoot`；
- `HostEnvironment`；
- `require_release_build`。

共享层不得拥有 case、plan、registry、scheduler、统一结果 schema、validator 或 report。每个 owner 自己定义 workload、seed、预热、正确性断言和结果字段。

| Target | Runner 与必须保留的口径 |
| --- | --- |
| `change_core` | Criterion |
| `change_codec` | Change 自有五路旋转 runner |
| `cell` | Criterion |
| `ordered_map` | Store 自有 AB/BA paired runner |
| `append_log` | Store 自有 AB/BA/BA/AB counterbalanced runner |
| `append_log_endurance` | Store 自有 streaming runner |
| `flow_lifecycle` | Criterion |
| `flow_runtime` | Flow 自有逐采样 `advance` latency trace |
| `change_append_log` | Criterion |

自有 runner 的 stdout 只输出 owner-specific JSONL，stderr 只输出人类进度。失败前已经产生的样本必须保留。配对 benchmark 不得由两个独立 median 代替；Flow runtime 必须保留每次采样 `advance` 的原始 latency，预热只推进并校验，不进入计时或输出；endurance 必须流式写出样本。

Criterion 使用自身 raw samples 和 estimates，并把输出放在 `RunRoot` 管理的 target 目录。Criterion target 设置 `test = true`，使普通 workspace gate 能进入 test mode；自有 runner 设置 `test = false`，由明确的 smoke 命令执行。

性能环境只有两个入口：

- `DOGPADDLE_PERF_PROFILE=smoke|reference`：必填；
- `DOGPADDLE_PERF_ROOT=/absolute/path`：reference 必填且必须是绝对固定目录，smoke 可省略。

fixture、seed、预热和结果校验必须位于计时外。全部 runner 记录 profile、rustc、CPU、OS、git revision/dirty state 和实际结果目录。reference baseline 以采集提交为 epoch；不同 epoch、代码、rustc、机器、profile、文件系统或 workload 的数字不可直接比较。

## 标准命令

唯一工作区入口：

```bash
cargo xtask check
```

它等价于：

```bash
cargo fmt --all -- --check
cargo test --workspace --locked
cargo test --workspace --release --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

常用定向 correctness 命令：

```bash
cargo test -p dogpaddle-store --test correctness transaction::
cargo test -p dogpaddle-operation --test correctness aggregate::
cargo test -p dogpaddle-flow --test correctness runtime_corruption::
cargo test -p dogpaddle-sql --test correctness
cargo test -p dogpaddle-change-store-integration
```

性能 test mode 与 smoke：

```bash
cargo test --workspace --benches --locked

DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-change --bench change_codec
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench ordered_map
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench append_log
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-flow --bench flow_runtime
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench append_log_endurance
```

## 系统验收

普通 Cargo gate 保持离线。真实系统入口为：

```bash
system-tests/debezium-postgres/scripts/check.sh
python3 system-tests/postgres/check_cdc.py \
  --bundle /absolute/path/to/runtime-bundle \
  --postgres-bin /absolute/path/to/postgresql/bin
python3 system-tests/postgres/check_sink.py \
  --postgres-bin /absolute/path/to/postgresql/bin
python3 system-tests/postgres/check_sql.py \
  --bundle /absolute/path/to/runtime-bundle \
  --postgres-bin /absolute/path/to/postgresql/bin
```

`system-tests/debezium-postgres/host` 是独立 Cargo workspace 和 lockfile，只依赖 Debezium crate，作为真实外部消费者。它不进入根 workspace。脚本接口固定为：

```text
scripts/check.sh
scripts/run.sh --bundle ABSOLUTE_PATH --host ABSOLUTE_PATH [--artifacts-dir ABSOLUTE_PATH]
scripts/clean.sh
```

`check.sh` 完成本机全流程；`run.sh` 只运行已有产物，不构建、不下载。Debezium/JRE 上游源码字符串审计通过
`crates/debezium/scripts/audit-upstream-contract.sh` 只在 pin 升级时由升级者显式执行，不属于 D1 的
`check.sh`、`run.sh` 或日常 CI workflow。

根 workspace 中的 `system-tests/debezium-runtime/host` 只拥有 bundle lifecycle probe；`system-tests/postgres/hosts` 拥有 CDC、Sink、Sink recovery 和 SQL 四个 host。PostgreSQL 公共 support 只共享临时集群、端口、进程和日志，不被 D1 使用。
PostgreSQL 检查脚本在未提供 host 参数时显式构建该 package 的 release bins；CI 传入
`--host`（Sink 同时传 `--recovery-host`）以消费同一 workflow 的预构建 artifact。所有显式路径必须是绝对路径。

PostgreSQL CI 是单 workflow DAG：Linux runtime 和 native hosts 独立构建；D1、CDC、Sink、SQL 各自执行并始终上传独立日志；最终 required check 名称为 `PostgreSQL engine, scan and sink recovery`。四平台 runtime bundle workflow 保持独立，artifact 不跨 workflow 共享。

## 新增或删除验证

提交前回答：

1. 它锁住了哪个尚无证据的当前承诺或故障边界？
2. 最强 owner 是谁，能否扩展现有领域文件？
3. expected 是否独立于被测实现，失败能否定位到一个契约？
4. 它若只是更弱证据的重复，是否应替换旧测试？

golden、独立 model、malformed/no-panic、真实 reopen/crash 和 capability 证据不能仅为减少数量而删除。反之，无法归类或没有独立 claim 的验证不得进入主仓库。
