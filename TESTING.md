# DogPaddle 验证规范

DogPaddle 只保留能够证明当前公共语义、持久化格式、事务边界或性能口径的证据。验证代码按产品所有权组织，不按 runner、实验阶段或历史入口组织；Git 历史是旧基础设施的唯一存档。

## 五类验证

1. `crates/<owner>/src/**/tests.rs`：必须访问私有状态的单元测试、故障注入和独立算法 oracle。
2. `crates/<owner>/tests/correctness.rs`：该产品 crate 唯一的公共测试 target，领域文件位于相邻的 `tests/correctness/`。
3. `integration-tests/<seam>/`：仅用于没有产品组合根的 sibling seam。当前无独立 seam package；Change IPC 的持久调用组合由 Flow 拥有。
4. `system-tests/`：依赖真实 Java、Debezium、PostgreSQL、MySQL 或其他外部服务的系统验收。
5. `crates/<owner>/benches/`：由 workload owner 直接拥有的 benchmark。

不存在通用实验框架。准备合并的实验必须归入 correctness、system test 或 benchmark；否则留在临时分支。

产品 manifest 关闭自动 test、bench 和 example 发现，并显式声明保留的 target。产品 library 设置 `bench = false`。测试不得为了 fixture 扩张产品 API，也不得建立跨产品的测试 DSL、通用算子案例表或第二套运行模型。

## 证据所有权

- Change 拥有 Schema、Change、Projection、Arrow IPC 字节格式、互操作、损坏拒绝和稳定事件顺序。
- Store 拥有事务、能力边界、Cell/OrderedMap/Queue 布局、分区与权重操作、分页、容量、reopen 和 crash consistency。
- Operation 拥有 Operation + Store：Definition、稳定 tag/payload、checked construct、运行协议、状态和算子语义。
- Flow 拥有 Flow + Operation + Change + Store：逻辑 DAG、全图 binding、持久调用栈、分页事务、背压、fail-stop、status 和 reopen。
- SQL 拥有 SQL + Flow + Operation：单语句 parser subset、端点参数、DataFusion coercion、LogicalPlan lowering、Program identity 和 `start` 的自动构建/恢复选择。
- `dogpaddle` binary 只拥有 `run SQL_FILE [--state DIR]` 的参数、默认状态路径、短等待循环和 Ctrl-C 有界停止；它不复制 SQL 生命周期。
- Debezium 拥有 connector-neutral runtime、bundle、checkpoint、delivery 和 ACK 生命周期。

能通过公共 API 证明的行为不得留在白盒测试中。删除测试前必须指出更低层或更靠近 owner 的替代证据；测试数量和代码行数不是正确性指标。

### Operation

Operation 的公共测试采用垂直所有权：每个内建算子各有一个 owner module，自己拥有 literal golden、kind、data declaration、checked construct、runtime 和 reopen 证据；超大 owner 可用一个 façade 按独立行为域分卷。跨算子文件只保留：

- `definition_codec`：外层 envelope、unknown tag 和通用损坏拒绝；
- `expression`：DataFusion Expr protobuf、精确 Schema binding 和 evaluate；
- `protocol`：Atomic 完整消费、Paged 的 `Resume/Progress` 与共享 `StepBudget`，以及 Source/Sink 的具体边界协议；
- `metamorphic`：稳定重批和独立模型。

生产 tag dispatch、`src/tests.rs` 中的白盒手写 tag 列表和各算子文件中的公共 literal golden 必须是三份独立证据。不得建立 `BuiltinContractCase` 或从产品 dispatch 反向生成期望值。

Aggregate 的 owner 文件必须证明 tag `14`、精确资源布局与 Schema、Definition roundtrip、共享 argument/moment 与 COUNT/SUM/AVG readout、MIN/MAX layout 复用、正权重 underflow 的整页 rollback、极值缓存重取与 reopen，以及不变结果不产生冗余 output。组权重归零仍须检查统计量一致性。私有 state codec 和表达式去重证据归 Operation。

EquiJoin 的 owner 文件用独立关系 oracle 覆盖 Inner、LeftSemi、LeftAnti、LeftOuter 和 FullOuter，固定 tag `16`、各 kind 的资源布局、NULL key、重复权重、residual 的 TRUE/FALSE/NULL、outer nullability、正负 diff 和溢出边界。用多个分页大小逐页 rollback/rebuild，证明 opaque Resume 不重复或漏过候选。晚期语义错误只回滚当前页，保留已提交页；reopen 必须再次到达确定性失败。SQL 只证明 lowering、Right swap/SchemaAlign 和最终关系。

AsOfJoin 的 owner 文件固定 tag `17` 与 `asof_join.left_rows/right_rows` 两个索引，覆盖 LeftOuter、Backward/Forward、exact 开关、零/多 Equal 键、单 order、NULL、重复 multiplicity、两侧 insert/retract 和歧义拒绝。独立 winner oracle 验证相邻版本定义的受影响区间、旧负新正顺序、每页 rollback/rebuild，以及 RHS 更新只在最后一页提交。产品只保留 SQL 可表达的 ASOF 语义；不维护 nearest、tolerance、residual 或 tie policy 扩展。

### Flow

Flow correctness 按机制分为 `binding`、`topology`、`definition` 与运行期领域。代表性算子覆盖融合尾部失败、UnionAll 多输入、Distinct 持久状态、ASOF 历史修正、同源自 Join、SQLite Sink 和 temporal/decimal unary chain；算子关系 oracle 归 Operation。

Flow 必须证明每个 Run/Send/入栈/出栈边界可 drop/open；父子 ordinal、port 必须对应调用边；子帧直接借用父页，root 直接借用 Source 队首，完成时同事务消费；拒绝损坏、超限与 orphan slot，且不改写已有状态。融合 head/tails 共享一笔小事务和预算，超限缩页时全部回滚；单事件仍超限须有限失败。首次合法输入的计算失败只保存初始 Run，多源及重开后均不能绕过该输入。本页计算与能接收的 Sink 入账同事务；背压或路由预算耗尽时保留结果及下一个消费者，重开后不重放已接收分支；本页内任一 Sink 错误须回滚同页计算和此前 Sink 入账。捕获与 Sink 排空仍获得服务。晚期 Join 错误由真实 SQLite 目标证明之前已提交页保留，失败位置可恢复。

Definition 覆盖逻辑 ID、owner identity 的 Some/None 编码、自动融合与深度上限；owner mismatch 在 binding 和资源打开前拒绝。commit、durability barrier、外部 ACK/delivery 不确定性必须 fail-stop。status 只读，并与 reopen 后一致。

### SQL

SQL 只有一个公共 `correctness` target，并只通过 `SqlProgram::{parse,read,start}` 验证产品契约。护栏覆盖 parser/endpoint 参数契约、所有拒绝路径不创建 Flow、Sequence→SQLite 的结果与精确目标列结构、Program identity、已有状态恢复和真实 PostgreSQL 端到端恢复。普通表和全部未支持节点必须在创建 Flow 路径前拒绝，AST 层还必须拒绝 DataFusion 可能擦除的 sampling、hint、row lock、typed alias 与 `LIMIT ALL`。

Endpoint 证据固定覆盖 `postgres_cdc(connection,table,publication[,bootstrap_spool_bytes][,CDC tuning...])`、
`mysql_cdc(connection,table[,bootstrap_spool_bytes][,CDC tuning...])`、`postgres(connection,table)`、
`clickhouse(connection,table)` 和 `doris(connection,table)`；CDC tuning
包含 connect/query timeout、retry limit/max delay、streaming heartbeat 和 snapshot fetch size，必须证明类型与范围校验在
Store/source I/O 前完成、准确映射到具体 connector、bootstrap heartbeat 不可覆盖、MySQL 未设置 fetch size 时不写 property，
且修改 tuning 后 Program identity 不变。默认 spool 必须等价于显式 1 GiB，旧的 split connection、runtime、engine、slot、sink
和 client-ID 参数必须作为 unknown parameter 拒绝。
连接 URL 的 secret 与 transient host/port 不改变 Program identity，database、qualified table、publication、spool
和查询语义必须改变 identity。CDC 测试只用绝对 `DOGPADDLE_DEBEZIUM_RUNTIME` 指向构建产物。

SQLite 结果矩阵必须覆盖别名与 qualified column、隐式 cast、CASE、TRY_CAST、算术、CTE fan-out、多 Scan、`UNION ALL` 的首分支列名、common type、nullable widening 和重复行语义，以及 `SELECT DISTINCT` 的最终 exact-row 结果；不得只断言构建或一次 advance 成功。Distinct witness 必须跨 drop/start，证明已经提交的权重状态会恢复且后续重复不会再次输出。Aggregate witness 必须在一个非空 GROUP BY 中覆盖 `COUNT(*)`、同义 `COUNT(1)`、nullable `COUNT(expr)`、signed/unsigned SUM、signed/unsigned AVG、MIN/MAX 的最终关系并跨 drop/start；纯分组另有最终结果 witness。global aggregate、grouping sets、聚合 modifier/UDF、浮点 group key 与未支持参数类型必须证明不创建 Flow。不可达 Scan 声明也必须有同样的无目录副作用证据。公共链路同时验证稳定逻辑 Operation ID、持久调用位置与 drop/start 后最终关系；语义相同但格式或 endpoint 参数顺序不同的 SQL 必须能恢复，语义或持久 endpoint 身份不同的 Program 必须因 owner identity mismatch 失败且不能替换磁盘 Definition。损坏、不完整、已存在但不可打开或被占用的状态不得触发自动重建。每新增一种 SQL LogicalPlan lowering，都必须增加至少一个最终结果 witness；每新增一种明确拒绝的节点，都必须增加无目录副作用 witness。真实 PostgreSQL SQL gate 另外覆盖 `postgres_cdc → CTE/Filter/nullable UnionAll → postgres`、已结算后的进程终止与 start 保持最终关系和技术 ID；目标提交后本地结算前的固定 Prepared 重投由直接 Sink 系统 gate 验证；CDC gate 使用预先写入的非空源，分别在 terminal capture commit 后 ACK 前和 2050 行快照中途 commit 后 ACK 前杀进程，验证 start 会恢复既有状态、丢弃未封口的私有 spool、完整重拍且只发布一次，再继续消费 WAL。

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

- Flow：`flow/advance/tests.rs` 只保留需要逐次驱动内部持久动作或注入边界故障的证据；
- Change codec：`support`、`schema`、`projection`、`batch_layout`，精确 subprocess case 留在 `tests.rs`；
- SQLite Sink：`row`、`target`。
- EquiJoin correctness：`family` 拥有五种关系语义与持久恢复，`inner_runtime` 拥有 Inner 热路径及 Join
  分页/预算分支；literal golden 只在 `family`。

源码 `tests.rs` 超过约 700 行且确有至少两个独立行为域时，才允许保留唯一 façade 并拆入 `tests/<behavior>.rs`。测试名称清楚描述行为；临时存储使用 `tempfile` 隔离。

## 正确性证据准入

每个持久化协议至少具有：

1. literal golden 或独立 raw-layout 断言；
2. decode、open 和 reopen；
3. 不复用生产算法的语义或互操作 oracle；
4. malformed/corruption 拒绝，无 panic、无部分写入；
5. 精确资源名、collection kind、codec 和失败后状态。

持久化定义或布局变更必须同时覆盖成功构建、纯校验失败无文件副作用、不完整构建、稳定编码、资源布局和重新打开。项目不识别、迁移或兼容未发布的旧格式；旧数据库直接删除并重建。

Change IPC 变更必须覆盖 self-contained Stream literal golden、标准 Arrow reader 互操作、顺序保持、零/多 RecordBatch 拒绝、截断和尾随字节；schema-bound 格式还必须覆盖 marker、Schema fingerprint、单 batch framing、owned decode 与 reopen。

普通 correctness 测试不得依赖 wall-clock 断言、系统 Java 或外部 PostgreSQL。没有覆盖率或代码行数 CI 阈值。

运行期 Schema guard 必须证明 output 违例回滚 head 与全部 tails，错误 input Schema 在恢复时被拒绝。Source 捕获提交和 durability barrier 必须先于原 Delivery 的 ACK；Delivery 的跨 connector、drop/replay、stop 失效由 Debezium 拥有。Sink 固定 Prepared 的持久化必须先于外部提交，外部提交后本地结算前的中断必须可幂等重投。旧的 Station、订阅、pin 与提交回调测试由这些 owner 证据替换。

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
| `change_codec` | Change 自有七路旋转 runner：self-contained/schema-bound encode 与 full decode，加三种 self-contained projected decode；同 fixture 记录两种格式 bytes 与节省比例 |
| `cell` | Criterion |
| `projection` | Operation 自有 Criterion：Select/SchemaAlign 的 8/128/512 列 × 1/256 行，加 2 列 × 1/256/65536 行的 identity/删列/Decimal/空投影；只计时 Atomic apply 与输出释放，构造、Store 事务创建、校验在计时外，无提交；保留完整行数、记录和 diff oracle |
| `aggregate_extrema` | Operation 自有 Criterion：同组高 multiplicity、极值撤回、保持历史口径的同 layout 重复 MIN/MAX、独立的多真实 layout MIN/MAX、单页多行、单页重复极值 key、页内净零 group/extrema 循环、多组已有 group 各一行更新与批量新 group ID 分配；按 case 计时完整 Atomic apply/`Transaction::commit`，有写才同步 WAL，fixture 与输出 oracle 不计时；多组已有 group case 使用 Int64 极值 key |
| `distinct` | Operation 自有 Criterion：同一 Change 内同 key 连续正负循环与两 key 交错循环；每轮完整 Atomic apply/`Transaction::commit`，有写才同步 WAL，fixture 和逐事件输出 oracle 不计时 |
| `cdc_bootstrap` | Operation 自有 Criterion：PG/MySQL 已封口快照逐条发布；未完成快照的常规 reset 固定用 257 条窄 entry 跨过 256-entry 批界，wide schema-bound entry 用同一 entry/row 布局配对发布与清理；计时一次只读 restore；publish 每 entry 包含 record 与同步 commit、published 只读/解码，以及 consume_published 的独立同步 commit；reset 每 batch 包含 record 与同步 commit，构造、seed、输出和最终持久状态 oracle 不计时；不启动外部 connector，不代表 capture、ACK 或端到端 CDC 吞吐 |
| `equi_join` | Operation 自有 Criterion：纯等值 Inner/Semi/Full Outer 对照，residual 0/50/100% 选择率、Semi 同行 multiplicity 稳定快路径及 Semi/Full Outer partial transition；两个完整输入 的全部分页 step 与同步 commit，fixture、seed 和结果校验不计时 |
| `equi_join_resources` | Operation 自有进程隔离 runner：同一动态 residual 的 0/50/100% 选择率、宽行、128 KiB 单候选、分页边界、大 fanout、whole-input、32 个计算 key 的整批准备，以及窄/宽 FullOuter match-count 状态；分别输出 Rust allocator heap、Arrow array memory 和持久逻辑状态证据，RSS 明示 unavailable |
| `buffered_sink` | Operation 自有 Criterion：SQLite durable buffer 的小批稳态 admission/drain、计入全部 admission 的多 entry 合批、独立计时 reopen + 首轮全 buffer 恢复校验、大 payload/小 event budget、受控的大 payload × multiplicity target-byte 分批，高 multiplicity/有限容量 churn，以及单个大 unit-weight entry 的多页 drain。常规 case 计时完整 admission、Prepared 持久化、sync、deliver 和 settle；恢复 case 只计时 reopen/bind 与首次完整 buffer 校验。fixture、初始化、预热、恢复样本的 durable staging/后续 drain 与目标关系 oracle 不计时，精确边界写入该次 `context.json` |
| `asof_join` | Operation 自有 Criterion：多 partition/少版本的左侧 lookup、单一大 partition、右侧尾部小修正与历史最坏修正、Backward 且允许 exact 的查找与历史修正；每次计时包含一对使关系回到初始态的完整输入、全部分页 step 和同步 commit，fixture、seed 与结果校验不计时 |
| `asof_join_resources` | Operation 自有进程隔离 runner：历史 lookup、RHS 历史修正、空影响区间和 NULL-order left；记录 Rust allocator heap、输出行数/正负事件、页数与重试数；关系 oracle 归 correctness |
| `ordered_map` | Criterion；完整 owned-page 扫描，以及已知存在 key 的直接 erase 与需要存在性结果的 checked remove 配对删除 |
| `flow_lifecycle` | Criterion |
| `flow_runtime` | Flow 自有逐采样 `advance` latency trace；包含 Select（选列）→Select（追加列）→Filter→Select→SchemaAlign、计数链和 fan-out；以相同逻辑输入与最终结果比较 |

自有 runner 的 stdout 只输出 owner-specific JSONL，stderr 只输出人类进度。失败前已经产生的样本必须保留。需要旋转顺序的 benchmark 不得由多次独立运行的 median 代替；Flow runtime 必须保留每次采样 `advance` 的原始 latency，预热只推进并校验，不进入计时或输出。

`equi_join_resources` 的每个 case 必须在新子进程中建立 fixture、seed 与 driving Change，再启动一个 `dhat 0.3.3` profiler 覆盖一个完整输入的全部分页计算。heap 数字只表示经过 Rust global allocator 的 total/current/peak bytes 与 blocks，不包含 profiler 启动前的输入 Arrow/seed/fixture，也不包含 RocksDB native heap。输出 Arrow array memory 单独按每个 Change 的 `get_array_memory_size` 累计；FullOuter 状态另用不受 profiler 影响的独立 pass 在每次 commit 后扫描 `equi_join.match_counts`，只记录实际 entry 数、零值 entry 数及 decoded key + `u64` 逻辑字节，不代表 WAL、LSM、cache、压缩或文件系统占用。portable runner 不采集平台单位不一致的 RSS，JSONL 中必须保留 `rss_bytes: null` 和原因，各口径不得互相替代。

`asof_join_resources` 逐 case 运行隔离进程，profile 只覆盖已准备好 fixture/input 之后的计算与同步提交。输出行数、正负事件数与分页计数作基本 witness；独立 winner/bag oracle 由 correctness 拥有。当前 runner 不记录 Arrow output bytes、RocksDB native heap 或 RSS，不将 Rust allocator heap 当作整个进程内存。

Criterion 使用自身 raw samples 和 estimates，并把输出放在 `RunRoot` 管理的 target 目录。Criterion target 设置 `test = true`，使普通 workspace gate 能进入 test mode。`flow_runtime`、`equi_join_resources` 与 `asof_join_resources` 的自有 runner 同样设置 `test = true`；test mode 自动选择小规模 workload，后两者仍逐 case 启动隔离子进程。其他旋转型自有 runner 设置 `test = false`，由明确的 smoke 命令执行。

`flow_runtime` 固定逻辑 workload，逐次记录完整有界 `advance` latency；预热、最终 backlog 排空、源捕获数量、计数状态和 reopen oracle 在计时外。单轮可能留下待处理调用，不把捕获数量当作该轮已完成数量。比较重构前后时必须匹配逻辑工作量与最终输入数；已删除的强制 Station 布局不构成新产品模式。实际 WAL sync、单事务 duration、累计 IPC bytes 没有观测 API 时必须标记 unavailable，不可用推算值冒充。

性能环境只有两个入口：

- `DOGPADDLE_PERF_PROFILE=smoke|reference`：必填；
- `DOGPADDLE_PERF_ROOT=/absolute/path`：reference 必填且必须是绝对固定目录，smoke 可省略。

fixture、seed、预热和结果校验必须位于计时外。全部 runner 记录 profile、rustc、CPU、OS、git revision/dirty state 和实际结果目录。reference baseline 以采集提交为 epoch；不同 epoch、代码、rustc、机器、profile、文件系统或 workload 的数字不可直接比较。

## 标准命令

唯一工作区入口：

```bash
cargo xtask check
```

工作区 `dev`/`test` 保留行号级调试信息，但不保留 `librocksdb-sys` 的 C++ DWARF，且关闭本地增量缓存；这使宽依赖/原生测试的冷编译可能变慢或失去原生 C++ 变量级调试。`release`/Criterion 配置不变。若确需原生调试，可用 Cargo profile 覆盖或临时独立 target 构建。修改 profile 会生成新的构建哈希，不会自动删除旧 `target/`：停止构建并确认无需保留全部 debug/test 缓存后，可先用 `cargo clean --profile dev --dry-run` 检查删除范围，再清理并重新构建；该命令可能删除整个 `target/debug`（包括 test-mode benchmark 产物），并非只清理旧哈希。不要无参数运行 `cargo clean`，它还会删除 `target/local-tools` 中独立下载的 JDK/Maven 和 benchmark/实验目录。新旧产物并存大小不是精简后的稳态大小；test-mode 与 release benchmark 也不能当作同一性能口径。

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
```

性能 test mode 与 smoke：

```bash
cargo test --workspace --benches --locked

DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-change --bench change_codec
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench ordered_map
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench projection
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench distinct
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench aggregate_extrema
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench equi_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench buffered_sink
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench cdc_bootstrap
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench equi_join_resources
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench asof_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench asof_join_resources
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-flow --bench flow_runtime
```

system-tests 统一拥有真实 JVM、Debezium bundle、PG/MySQL gate host 和 warehouse 容器脚本；gate-only host 不得伪装成产品 example。

system-tests 的稳定 host 布局：根 workspace 的 `system-tests/debezium-runtime/host` 只含 bundle probe，`system-tests/postgres/hosts` 只含原生 PG gate host，`system-tests/mysql/host` 只含 MySQL CDC gate host；独立 workspace `system-tests/debezium-postgres/host` 只依赖 Debezium 产品。`system-tests/postgres/support` 只能由 PG 脚本共享进程执行、临时 cluster、端口和日志基础设施，不得包含场景 oracle、SQL 或 host 协议，D1 和 MySQL gate 不得引用。`system-tests/warehouse-sinks` 只拥有锁定官方镜像的 Compose fixture 和真实 adapter gate，不新增 Rust host。

## 系统验收

普通 Cargo gate 保持离线。真实系统入口为：

```bash
system-tests/debezium-postgres/scripts/check.sh
python3 system-tests/postgres/check_cdc.py \
  --bundle /absolute/path/to/runtime-bundle \
  --postgres-bin /absolute/path/to/postgresql/bin
python3 system-tests/mysql/check_cdc.py \
  --bundle /absolute/path/to/mysql-capable-runtime-bundle \
  --engine podman
python3 system-tests/postgres/check_sink.py \
  --postgres-bin /absolute/path/to/postgresql/bin
python3 system-tests/postgres/check_sql.py \
  --bundle /absolute/path/to/runtime-bundle \
  --postgres-bin /absolute/path/to/postgresql/bin
system-tests/warehouse-sinks/check.sh
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

根 workspace 中的 `system-tests/debezium-runtime/host` 只拥有 bundle lifecycle probe；`system-tests/postgres/hosts` 拥有 CDC、Sink、Sink recovery 和 SQL 四个 host；`system-tests/mysql/host` 只拥有 MySQL CDC 的直接 Operation/Store 验收 host。PostgreSQL 公共 support 只共享临时集群、端口、进程和日志，不被 D1 或 MySQL gate 使用。
`system-tests/mysql/check_cdc.py` 每次使用随机名称的独立 MySQL 8.4 Compose 项目、数据卷和回环端口，不接触现有数据库。它用真正的 Debezium delivery 验证快照封口与 streaming 的 Store commit 后、原 Delivery ACK 前进程退出；重开检查私有 spool、精确有序输出、无重复和后继 binlog 事件；失败保留私有状态、丢弃可能泄露凭据的 host stderr，仅在终端显示脱敏的容器诊断，容器和数据卷仅清理本轮项目。bundle 必须包含 MySQL connector；可用 `--host` 指向已编译的绝对路径，容器 CLI 由 `--engine` 指定。此门禁不属于普通 Cargo gate。
`system-tests/warehouse-sinks/check.sh` 用锁定的官方镜像启动一次性 ClickHouse/Doris fixture，运行产品 crate 内标记为 ignored 的真实 adapter 测试，并在退出时删除容器和 volume；可用 `CONTAINER_ENGINE` 选择兼容 Compose 的容器 CLI。
PostgreSQL 检查脚本在未提供 host 参数时显式构建该 package 的 release bins；CI 传入
`--host`（Sink 同时传 `--recovery-host`）以消费同一 workflow 的预构建 artifact。所有显式路径必须是绝对路径。

PostgreSQL CI 是单 workflow DAG：Linux runtime 和 native hosts 独立构建；D1、CDC、Sink、SQL 各自执行并始终上传独立日志；最终 required check 名称为 `PostgreSQL engine, scan and sink recovery`。四平台 runtime bundle workflow 保持独立，artifact 不跨 workflow 共享。Linux release executable 在锁定 digest 的 manylinux 2.28 环境中构建，macOS release 显式使用 11.0 deployment target。每次 workflow 都组装并 smoke 最终产品 archive，审计目标架构、OS ABI、动态库引用和 compiler runtime 链接；推送与 workspace version 完全一致的 `v<version>` tag 时，在全部 matrix job 成功后发布这些已验证 archive、SHA-256 和 compatibility report，不重新构建。

macOS tag 直接发布经过相同 archive audit 和 smoke 的未签名产物，不需要 Apple Developer 凭据。用户环境中的 Gatekeeper 可能要求手动允许从互联网下载的 executable；这不属于 archive 的兼容性验证范围。bundle 内 Temurin Mach-O 保留 Eclipse Adoptium 的原始签名。

## Example 容器体验

`examples/` 下全部七个场景各自拥有 Compose 配置、公开 `.env`、初始化 SQL 和业务步骤。它们是用户体验环境，不是新增执行引擎或 gate host。验证使用已编译的产品 binary，数据库初始化直接挂载各自的 setup SQL，业务变更直接执行 `steps/*.sql`；跨库场景额外挂载只用于本地演示的 MySQL 账号初始化脚本。

2026-09-16 本机验收组合（macOS arm64，Podman 6.1.0 rootless machine，固定 PostgreSQL 16.15 / MySQL 8.4.6 多架构镜像 digest）：

| 引擎 / provider | 场景 | 验证范围 |
| --- | --- | --- |
| Podman / podman-compose 1.6.0 | 门店销售汇总 | 启动、健康、全部业务步骤、手动插入、重复 up、down/up 后 reopen 与离线变更、重置 |
| Podman / Docker Compose 5.5.1 | 成交报价匹配 | 启动、健康、全部业务步骤、重复 up、down/up 后 reopen 与离线变更、重置 |
| Podman / Docker Compose 5.5.1 | event-sync、order-etl、order-fulfillment | 启动、健康、全部业务步骤、down/up 保留卷、同 state reopen 与停机期间手动变更、空卷与新 state 恢复初始结果 |
| Podman / podman-compose 1.6.0 | customer-order-enrichment | 双库启动与健康、MySQL CDC/DML 权限、全部业务步骤、down/up 保留卷、同 state reopen 与停机期间修改客户、空卷与新 state 恢复初始结果 |
| Podman / Docker Compose 5.5.1 | payment-reconciliation | 双库启动与健康、MySQL CDC/DML 权限、全部业务步骤、down/up 保留卷、同 state reopen 与停机期间修正结算、空卷与新 state 恢复初始结果 |
| Docker Engine | 全部七个场景 | 当前机器未安装，未实跑；七份配置通过 Docker Compose 5.5.1 config 检查 |

本次产品运行使用本机已有的 `target/release/dogpaddle`，配同架构 Debezium runtime 的绝对覆盖路径，没有重新编译，也没有把它当作从 GitHub 下载的 archive 验收。release archive 的布局/打包验证仍归上面的发布 smoke。Linux/SELinux 与其他 provider 版本的端到端组合尚未实跑。

跨库实跑发现并补齐了读取 InnoDB 表标识所需的 `PROCESS` 权限，随后从空卷验证。另一个本机旧 runtime 只有 PostgreSQL connector，造成 MySQL `InvalidConfiguration`；切换已有完整 PostgreSQL/MySQL runtime 后通过，不修改产品代码或让用户手工补 JAR。七份 README 的 shell 代码块均通过 `sh -n` / `zsh -n`，续行符及步骤重定向另做人工复核。

后续修改这些配置时，从独立的空项目/数据卷和新 state 开始，按 example README 验证：

1. Compose config 能展开端口与初始化挂载；同一 shell 依次加载不同场景 `.env`，各场景仍分别使用自己的项目和端口。各 `.env` 不导出共享的 `COMPOSE_PROJECT_NAME`。
2. up 只创建数据库，不启动 DogPaddle；healthy 后源表和 publication 已存在，目标表尚不存在。容器内客户端和宿主 TCP 连接均可查询。
3. 启动产品后逐步写源表，比较有序目标查询与预期值。门店查询包含 COUNT/SUM/MIN/MAX，并以 `encode(average_order_cents, 'hex')` 验证 Float64 的原始位模式；ASOF 验证匹配时间、中间价和价格偏差；其他五例核对各 README 展示的业务字段（不是所有内部 Sink 列）。跨库步骤分别在对应的 PostgreSQL / MySQL 执行。
4. 自由写入额外源记录；重复 up 不覆盖它。停止产品后 down/up，保留两侧状态，停机期间再写源数据，重跑同一 state 能追上变化。
5. 先停止产品，再删除仅属于验收项目的数据库卷，同时移走旧 state。重新 up 并使用新 state，初始数据/结果恢复，验收追加数据不再出现。

数据库与 state 必须作为一组管理。验收通过 Compose project 覆盖与临时 state 隔离于用户默认体验项目；结束后删除仅由该次验收创建的数据卷/容器，不清理其他项目。普通 Cargo correctness/benchmark/Clippy 不需要 Docker 或 Podman；本轮纯 Compose/文档修改不重复运行未变动的 Rust gate。

## 新增或删除验证

提交前回答：

1. 它锁住了哪个尚无证据的当前承诺或故障边界？
2. 最强 owner 是谁，能否扩展现有领域文件？
3. expected 是否独立于被测实现，失败能否定位到一个契约？
4. 它若只是更弱证据的重复，是否应替换旧测试？

golden、独立 model、malformed/no-panic、真实 reopen/crash 和 capability 证据不能仅为减少数量而删除。反之，无法归类或没有独立 claim 的验证不得进入主仓库。
