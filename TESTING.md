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
- Store 拥有事务、能力边界、集合布局、分页、容量、订阅位置与安全回收、reopen 和 crash consistency。
- Operation 拥有 Operation + Store：Definition、稳定 tag/payload、bind、materialize、运行协议、状态和算子语义。
- Flow 拥有 Flow + Operation + Change + Store：拓扑、全图 binding、subscription 装配、调度、claim、背压、fail-stop、status 和 reopen。
- SQL 拥有 SQL + Flow + Operation：单语句 parser subset、端点参数、DataFusion coercion、LogicalPlan lowering、Program identity 和 `start` 的自动构建/恢复选择。
- `dogpaddle` binary 只拥有 `run SQL_FILE [--state DIR]` 的参数、默认状态路径、短等待循环和 Ctrl-C 有界停止；它不复制 SQL 生命周期。
- Debezium 拥有 connector-neutral runtime、bundle、checkpoint、delivery 和 ACK 生命周期。
- Change 与 Store 的外部组合只由 `integration-tests/change-store/` 证明。

能通过公共 API 证明的行为不得留在白盒测试中。删除测试前必须指出更低层或更靠近 owner 的替代证据；测试数量和代码行数不是正确性指标。

### Operation

Operation 的公共测试采用垂直所有权：每个内建算子各有一个 owner module，自己拥有 literal golden、kind、data declaration、bind、materialize、runtime 和 reopen 证据；超大 owner 可用一个 façade 按独立行为域分卷。跨算子文件只保留：

- `definition_codec`：外层 envelope、unknown tag 和通用损坏拒绝；
- `expression`：DataFusion Expr protobuf、精确 Schema binding 和 evaluate；
- `protocol`：`AtomicOperation::apply` 完整消费契约，以及 `turn -> PreparedTurn -> AfterCommit` 事务协议；
- `metamorphic`：稳定重批和独立模型。

生产 decoder registry、`src/tests.rs` 中的白盒手写 tag 列表和各算子文件中的公共 literal golden 必须是三份独立证据。不得建立 `BuiltinContractCase` 或从产品 registry 反向生成期望值。

Aggregate 的 owner 文件必须证明 tag `14` 与 `aggregate.groups/entries/control` 三资源、完整 Definition
roundtrip、精确 output Schema、exact-row admission 的整 turn rollback、Fold 结果变化、分区有序索引的
MIN/MAX 首尾选择与 reopen，以及不变结果不产生冗余 output。函数 descriptor、argument tuple framing
和 group state codec 属于 Operation 私有实现，不在 Flow 或 SQL 复制 oracle。

EquiJoin 的 owner 文件必须用独立关系 oracle 覆盖 Inner、LeftSemi、LeftAnti、LeftOuter 与 FullOuter，
并证明 tag `16` 的 kind payload、Inner 三资源与其余 kind 四资源、NULL key、重复权重、同一 Claim 内的
presence 往返、outer nullability、正负 diff 边界、分页 rollback 和 Probe/Emit reopen；residual 还必须
覆盖逐完整行 support、`FALSE/NULL`、Probe/ClearShadow/Emit reopen 与 rollback。SQL 只证明
Right Join 的 swap + SchemaAlign、原生 residual 的方向改写，以及每种新增 LogicalPlan lowering
至少一个最终关系 witness，不复制 Join 状态机。

AsOfJoin 的 owner 文件必须用独立 winner/关系 oracle 覆盖 Inner、LeftOuter、LeftSemi 与
LeftAnti，并证明 tag `17`、`asof_join.left_rows/right_rows/continuation` 三资源、完整
Definition roundtrip 与精确 output Schema。运行证据覆盖 backward/forward/nearest、exact 开关、
两种 equidistant policy、空/多 equality partition、`Equal/NotDistinct`、单/多 order、NULL、tolerance 边界、
显式 tie-break 和三种 fallback、residual 跳过近候选、重复 multiplicity、两侧 insert/retract、旧负新正的
历史修正顺序、整批负前缀/溢出/歧义 rollback，以及候选与外层 left 双重分页中的 commit
rollback 和 reopen。SQL 只证明 DataFusion 原生 left-preserving `ASOF JOIN` 的四个不等方向、零/多等值键、
Schema/Program identity 和最终关系，不复制 Operation API 的 nearest、tolerance、tie 或 residual 状态机。

### Flow

Flow correctness 按机制分为 `binding`、`topology`、`definition` 与运行期领域。Flow 只保留能证明全图机制的代表性算子：Project 的纯失败和 reopen/rebind、UnionAll 的多输入、Distinct 持久状态在 output 背压下的原子 rollback/reopen、AsOfJoin 新增的双输入外层/候选双重 continuation 在真实资源路径上的 drop/open、SQLite Sink、PostgreSQL 运行资源，以及 temporal/decimal unary chain。算子自身的 payload、表达式和运行语义归 Operation，不在 Flow 逐个复制。

Flow 独有的 runtime、事务、背压、claim、subscription completion、fail-stop 和 status 证据必须保留。Definition
还必须覆盖 owner identity 的 Some/None 稳定编码，以及 open 在 Schema binding、资源打开和 materialize 前拒绝
identity mismatch。

### SQL

SQL 只有一个公共 `correctness` target，并只通过 `SqlProgram::{parse,read,start}` 验证产品契约。护栏覆盖 parser/endpoint 参数契约、所有拒绝路径不创建 Flow、Sequence→SQLite 的结果与精确目标列结构、Program identity、已有状态恢复和真实 PostgreSQL 端到端恢复。普通表和全部未支持节点必须在创建 Flow 路径前拒绝，AST 层还必须拒绝 DataFusion 可能擦除的 sampling、hint、row lock、typed alias 与 `LIMIT ALL`。

Endpoint 证据固定覆盖 `postgres_cdc(connection,table,publication[,bootstrap_spool_bytes][,CDC tuning...])`、
`mysql_cdc(connection,table[,bootstrap_spool_bytes][,CDC tuning...])` 和 `postgres(connection,table)`；CDC tuning
包含 connect/query timeout、retry limit/max delay、streaming heartbeat 和 snapshot fetch size，必须证明类型与范围校验在
Store/source I/O 前完成、准确映射到具体 connector、bootstrap heartbeat 不可覆盖、MySQL 未设置 fetch size 时不写 property，
且修改 tuning 后 Program identity 不变。默认 spool 必须等价于显式 1 GiB，旧的 split connection、runtime、engine、slot、sink
和 client-ID 参数必须作为 unknown parameter 拒绝。
连接 URL 的 secret 与 transient host/port 不改变 Program identity，database、qualified table、publication、spool
和查询语义必须改变 identity。CDC 测试只用绝对 `DOGPADDLE_DEBEZIUM_RUNTIME` 指向构建产物。

SQLite 结果矩阵必须覆盖别名与 qualified column、隐式 cast、CASE、TRY_CAST、算术、CTE fan-out、多 Scan、`UNION ALL` 的首分支列名、common type、nullable widening 和重复行语义，以及 `SELECT DISTINCT` 的最终 exact-row 结果；不得只断言构建或一次 advance 成功。Distinct witness 必须跨 drop/start，证明已经提交的权重状态会恢复且后续重复不会再次输出。Aggregate witness 必须在一个非空 GROUP BY 中覆盖 `COUNT(*)`、同义 `COUNT(1)`、nullable `COUNT(expr)`、signed/unsigned SUM、signed/unsigned AVG、MIN/MAX 的最终关系并跨 drop/start；纯分组另有最终结果 witness。global aggregate、grouping sets、聚合 modifier/UDF、浮点 group key 与未支持参数类型必须证明不创建 Flow。不可达 Scan 声明也必须有同样的无目录副作用证据。公共链路同时验证固定 Station ID、64 MiB output capacity、drop/start 后持久 position；语义相同但格式或 endpoint 参数顺序不同的 SQL 必须能恢复，语义或持久 endpoint 身份不同的 Program 必须因 owner identity mismatch 失败且不能替换磁盘 Definition。损坏、不完整、已存在但不可打开或被占用的状态不得触发自动重建。每新增一种 SQL LogicalPlan lowering，都必须增加至少一个最终结果 witness；每新增一种明确拒绝的节点，都必须增加无目录副作用 witness。真实 PostgreSQL SQL gate 另外覆盖 `postgres_cdc → CTE/Filter/nullable UnionAll → postgres`、目标提交后本地结算前终止和 start 幂等重投；CDC gate 使用预先写入的非空源，分别在 terminal capture commit 后 ACK 前和 2050 行快照中途 commit 后 ACK 前杀进程，验证 start 会恢复既有状态、丢弃未封口的私有 spool、完整重拍且只发布一次，再继续消费 WAL。

Join 的 SQL witness 必须覆盖 Inner、Left/Right/Full Outer 与 Left/Right Semi/Anti 的最终关系和 drop/start 恢复；
Right lowering 还要断言原 SQL 字段顺序与 nullability。所有 Join family 的 residual 必须证明 predicate
原生进入 `EquiJoin`，并覆盖同 key 下同时存在通过与不通过的候选；Cross/Natural/Using、纯非等值和
同侧 equality 必须在创建 Flow 前拒绝。

`system-tests/postgres/check_sql.py --trace-output ...` 可在完整验收通过后导出该场景的 SQL、
宿主 I/O、SIGKILL、PostgreSQL 重放日志和关系快照，供 [持续 ETL 演示](docs/demo/README.md) 排版。
启用 trace 时，还会追加 28 次有界连续源表变更，每次完整目标关系与原生 PostgreSQL SQL oracle
核对，并记录推进前后快照。断言与故障边界仍由 SQL 系统验收拥有；视频间隔不作为性能证据。

### 产品命令

`crates/dogpaddle/tests/correctness.rs` 只验证产品壳：唯一 `run` 子命令、SQL 旁
`.dogpaddle/<stem>` 默认状态、打印 canonical path、Ctrl-C 在当前有界轮次后成功退出，以及再次执行同一命令
恢复已有状态。SQL parsing、identity、构建和恢复语义继续由 SQL correctness 拥有，不在 binary 测试复制。

### 测试分卷

每个源码模块目录只有一个 `tests.rs` 入口。超大模块可在同目录的 `tests/` 下按完整领域拆分；不要按每个生产源码文件建立镜像目录。当前较大的分区为：

- Station：`support`、`claim`、`transaction`；
- Change codec：`support`、`schema`、`projection`、`batch_layout`，精确 subprocess case 留在 `tests.rs`；
- SQLite Sink：`row`、`target`。
- EquiJoin correctness：`family` 拥有五种关系语义与持久恢复，`inner_runtime` 拥有 Inner 热路径及 Join
  分页/预算分支；literal golden 只在 `family`。

## 正确性证据准入

每个持久化协议至少具有：

1. literal golden 或独立 raw-layout 断言；
2. decode、open 和 reopen；
3. 不复用生产算法的语义或互操作 oracle；
4. malformed/corruption 拒绝，无 panic、无部分写入；
5. 精确资源名、collection kind、codec 和失败后状态。

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
| `aggregate_extrema` | Operation 自有 Criterion：同组高 multiplicity、极值撤回、重复 MIN/MAX；两轮 turn/apply/sync commit/AfterCommit，fixture 与输出 oracle 不计时 |
| `equi_join` | Operation 自有 Criterion：纯等值 Inner/Semi/Full Outer 对照，residual 0/50/100% 选择率、Semi 同行 multiplicity 稳定快路径及 Semi/Full Outer partial transition；两个完整 Claim 的 Probe/ClearShadow/Emit、同步 commit 与 AfterCommit，fixture、seed 和结果校验不计时 |
| `equi_join_resources` | Operation 自有进程隔离 runner：同一动态 residual 的 0/50/100% 选择率、宽行、超过 1 MiB 的单候选活性逃生、分页边界、大 fanout、whole-Claim，以及窄/宽 FullOuter match-count 状态；分别输出 Rust allocator heap、Arrow array memory 和持久逻辑状态证据，RSS 明示 unavailable |
| `asof_join` | Operation 自有 Criterion：多 partition/少版本的左侧 lookup、单一大 partition、右侧尾部小修正与历史最坏修正、nearest+tolerance 和 residual 远候选回退；每次计时包含一对使关系回到初始态的完整 Claim、全部 Probe/Emit turns、同步 commit 和 AfterCommit，fixture、seed 与结果校验不计时 |
| `asof_join_resources` | Operation 自有进程隔离 runner：分页候选、宽/超大单行、whole-Claim、residual 远回退、右侧历史 rematch、双侧 NULL-order history N/2N，以及 port 1 的 empty-left distinct N/2N preload、same-key 高 multiplicity 和 active-overlay N/2N growth；分别输出一个完整 driving Claim 的 Rust allocator heap、每页产生的 Arrow array memory/行数、turn 数，以及独立 non-profile pass 扫描两个 rows map 得到的持久逻辑 entry/key+value bytes；RSS 明示 unavailable |
| `ordered_map` | Criterion；完整 owned-page 扫描 |
| `subscribed_log` | Criterion；大 payload status/消费、固定 fanout 跨 reopen 有界 churn |
| `flow_lifecycle` | Criterion |
| `flow_runtime` | Flow 自有逐采样 `advance` latency trace；包含同一 Project→Extend→Filter→Select→SchemaAlign 逻辑链的独立 Station 与线性多 Operation Station 对照 |
| `change_subscribed_log` | Criterion |

自有 runner 的 stdout 只输出 owner-specific JSONL，stderr 只输出人类进度。失败前已经产生的样本必须保留。需要旋转顺序的 benchmark 不得由多次独立运行的 median 代替；Flow runtime 必须保留每次采样 `advance` 的原始 latency，预热只推进并校验，不进入计时或输出。

`equi_join_resources` 的每个 case 必须在新子进程中建立 fixture、seed 与 driving Change，再启动一个 `dhat 0.3.3` profiler 覆盖恰好一个完整 Claim。heap 数字只表示经过 Rust global allocator 的 total/current/peak bytes 与 blocks，不包含 profiler 启动前的输入 Arrow/seed/fixture，也不包含 RocksDB native heap。输出 Arrow array memory 单独按每个 Change 的 `get_array_memory_size` 累计；FullOuter 状态另用不受 profiler 影响的独立 pass 在每次 commit 后扫描 `equi_join.match_counts`，只记录实际/影子 entry 数及 decoded key + `u64` 逻辑字节，不代表 WAL、LSM、cache、压缩或文件系统占用。portable runner 不采集平台单位不一致的 RSS，JSONL 中必须保留 `rss_bytes: null` 和原因，各口径不得互相替代。

`asof_join_resources` 遵循相同的新子进程/单完整 Claim `dhat 0.3.3` 边界。除候选分页、宽行、
whole-Claim、residual 和历史 rematch 外，它还成对比较 empty-left distinct N/2N、same-key 高
multiplicity、active-overlay N/2N，以及持久 NULL-order left/right history N/2N。NULL-left 对照固定
同一份 RHS Claim 并必须在两个 turn 内完成；NULL-right 对照固定同一条 left Claim 并必须在一个 turn
内完成。两者都必须自动断言 N/2N 的 claim 与 Rust heap 记录完全相同，证明双向 scan 都精确 seek 到
matchable marker，不能让永不匹配的历史放大另一侧更新。Heap 不包含
profiler 前建立的 fixture、seed 和输入 Arrow，也不包含 RocksDB native heap；output 单独累计
`RecordBatch::get_array_memory_size` 与 diff array，可能重复计入共享 buffer。持久状态在另一个不受 profiler 影响的
pass 中以 `OrderedMap<Vec<u8>, u64>` 观察句柄扫描 `asof_join.left_rows/right_rows`，记录解码
entry 数与 key + 8-byte positive weight；这是 codec 已锁定的逻辑大小，不是 RocksDB/WAL/LSM/cache/
压缩/文件系统占用。port 1 必须用 empty-left distinct N/2N preload 证明整批准入、turn continuation、
最终 right state 与撤回闭环，并用 same-key Claim 证明单 entry 的高 multiplicity；active-overlay N/2N
对照先 seed N 个不同 equality partition 的 left row，再由一个每 partition 一行的 N-row right Claim 驱动，
使每个事件只 rematch 一个 left 和一个 candidate。该 active case 的 non-profile oracle 必须在插入与撤回两向
校验每个 group 恰好一次、左右 group/order 对应及精确 `+1/-1` 权重，且最终状态回到 seed；paired 记录用于
同一次 benchmark 环境下比较 heap/turn 增长，test mode 只验证协议，不作为性能基线。RSS 仍必须明示
`null` 与 unavailable 原因，各口径不得互相替代。

Criterion 使用自身 raw samples 和 estimates，并把输出放在 `RunRoot` 管理的 target 目录。Criterion target 设置 `test = true`，使普通 workspace gate 能进入 test mode。`flow_runtime`、`equi_join_resources` 与 `asof_join_resources` 的自有 runner 同样设置 `test = true`；test mode 自动选择小规模 workload，后两者仍逐 case 启动隔离子进程。其他旋转型自有 runner 设置 `test = false`，由明确的 smoke 命令执行。

`flow_runtime` 的线性链对照固定使用相同的 SequenceScan→Project→Extend→Filter→Select→SchemaAlign→Discard 逻辑与数据：独立 Station 布局是 7 个 Station、6 个 durable output log 和 6 条 input edge；融合布局通过 `FlowFactory::append` 把五个 transform 放入 Scan Station，只保留 2 个 Station、1 个 durable output log 和 1 条 input edge。每个成功 `advance` 分别校验 7/2 次 Station commit、6/1 次 input completion 和 6/1 次 IPC Change append。每个采样只记录原始 `advance` latency 与 outcome；单行 source Change throughput 由消费者直接从 latency 推导，不在每条记录中重复保存派生值。这些 commit 与 append 是 Flow 协议层的语义计数，不是 RocksDB 内部计数。当前公共 API 只提供完整 `advance` 时长和 output 当前 retained bytes，不提供单次 Store transaction duration 或历史累计 IPC bytes，JSONL context 必须把两项记为 unavailable，不能用均摊延迟或当前 retained bytes 冒充。

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
cargo test -p dogpaddle --test correctness
cargo test -p dogpaddle-change-store-integration
```

性能 test mode 与 smoke：

```bash
cargo test --workspace --benches --locked

DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-change --bench change_codec
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench ordered_map
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench subscribed_log
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench aggregate_extrema
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench equi_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench equi_join_resources
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench asof_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench asof_join_resources
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-change-store-integration --bench change_subscribed_log
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-flow --bench flow_runtime
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

PostgreSQL CI 是单 workflow DAG：Linux runtime 和 native hosts 独立构建；D1、CDC、Sink、SQL 各自执行并始终上传独立日志；最终 required check 名称为 `PostgreSQL engine, scan and sink recovery`。四平台 runtime bundle workflow 保持独立，artifact 不跨 workflow 共享。推送与 workspace version 完全一致的 `v<version>` tag 时，同一 workflow 将 native `dogpaddle` 与已验证 runtime 组装为固定 `bin/`、`libexec/` 布局，为每个平台生成 tarball 和 SHA-256，并在全部 matrix job 成功后发布 GitHub Release。

## 新增或删除验证

提交前回答：

1. 它锁住了哪个尚无证据的当前承诺或故障边界？
2. 最强 owner 是谁，能否扩展现有领域文件？
3. expected 是否独立于被测实现，失败能否定位到一个契约？
4. 它若只是更弱证据的重复，是否应替换旧测试？

golden、独立 model、malformed/no-panic、真实 reopen/crash 和 capability 证据不能仅为减少数量而删除。反之，无法归类或没有独立 claim 的验证不得进入主仓库。
