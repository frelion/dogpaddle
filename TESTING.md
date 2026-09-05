# DogPaddle 测试协议

测试的目标不是覆盖实现行，而是用最少的独立证据锁住公共语义、持久格式、事务边界和性能口径。
同一行为只保留一个最强所有者；测试辅助代码不得发展成第二套产品模型。

## 四条证据轨道

| 轨道 | 回答的问题 | 主要手段 |
| --- | --- | --- |
| 公共契约 | 调用者能否观察到承诺的行为与错误？ | `tests/correctness`、真实公共 API、失败后状态断言 |
| 模型与变形 | 大量组合是否符合一个独立、可读的 oracle？ | 固定 seed、小模型、稳定重批、穷举小图 |
| 格式与损坏 | 稳定字节能否互操作、重开并安全拒绝损坏？ | golden/raw layout、标准 reader、truncation、no-panic |
| 事务与恢复 | commit、rollback、poison、进程死亡后是否只出现全旧或全新？ | 真实 MDBX、reopen、Store 层 SIGKILL、Flow 阶段恢复 |

性能测试是第五条独立轨道：它只测已经由 correctness 证明有效的 workload，不替代任何行为断言。

## 所有权

- `src/**/tests.rs` 只保存必须访问私有状态的不变量、错误注入和独立算法 oracle。能用公共 API
  证明的行为必须放到公共 target。
- 每个产品 crate 只有一个显式公共 target：`tests/correctness.rs`，领域文件位于
  `tests/correctness/`。
- Operation + Store 的组合由 Operation 拥有；Flow + Operation + Change + Store 的组合由 Flow
  拥有。
- Change 与 Store 是刻意独立的 sibling；它们唯一的外部接缝位于不可发布的
  `integration-tests/change-store/`。
- benchmark fixture、oracle 与计时边界归 workload 所有者。共享
  `test-support/bench-protocol/` 只拥有 profile、运行目录、主机指纹、typed JSONL、统计和完成记录。

产品 manifest 关闭自动 test/bench 发现，显式声明 target；产品 library 设置 `bench = false`。
测试不得为了注入 fixture 扩张产品 API，也不得引入万能 Store trait、persona DSL 或跨 crate test-utils。

## 证据所有权与目标矩阵

| 所有者 | 正确性所有权 | benchmark target |
| --- | --- | --- |
| Change | Schema、Change、Projection、IPC golden/interop/malformed；Date32、四种 Timestamp unit/timezone；Decimal128 的 full/projected/nested/标准 reader 与递归 value invariant | `change_core`、`change_codec` |
| Debezium | secret-safe config、runtime bundle/JVM singleton、owned delivery、opaque checkpoint golden/malformed/multi-partition restore、linear ACK、preview/actual offset 等价、handle lifecycle；四平台 public lifecycle 与真实 PostgreSQL recovery gate 独立运行 | 不适用 |
| Store | capability、事务、布局、集合、分页、容量、SIGKILL | `cell`、`ordered_map`、`append_log`、`append_log_endurance` |
| Operation | 统一 `turn → PreparedTurn → AfterCommit` 协议及 borrowed linear delivery 跨事务证据；可运行 QueueSource 示例共用代码的初始化回滚、未 ACK 重放、提交前后 reopen 完整输出序列；十二个内建 Definition、tag/golden、exact Schema bind/materialize；DataFusion Expr protobuf 与已承诺 operator/type evaluate；Project/Extend/Select/SchemaAlign 共享、空 Select/SchemaAlign runtime Schema guard、Filter null/混合 diff 重批；Date32/Timestamp/Decimal128 的 direct-copy、精确 cast 与组合比较；UnionAll 多端口/runtime Schema guard；RunningEventCount 状态 commit/rollback/reopen；SqliteSink 全部 v1 类型的 canonical row/hash、具体 mutation 批次及跨 SQLite/MDBX commit 的幂等重放；PostgresSink 的 tag12、非敏感资源边界、唯一 state 与 Prepared/receipt 协议 | `operation_core` |
| Flow | build、单次 Store setup 的 open、拓扑 Schema 传播、Project/Filter/Extend/Select/SchemaAlign/UnionAll/SqliteSink/PostgresSink 拒绝无建库副作用与 reopen 重绑定；PostgresSource/PostgresSink 精确运行资源；Date32/Timestamp/Decimal128 完整结构/表达式链两次 reopen；SQLite 表延迟初始化及端到端恢复；Claim 重放、Schema 违例回滚、`Turn::Idle`、Commit/Complete、AfterCommit commit-only 执行与 error/panic fail-stop/reopen、背压、reclaim、腐败状态 | `flow_lifecycle`、`flow_runtime` |
| Change + Store | full/projected owned entry decode、decode poison 后 forwarding/cursor 回滚 | `change_append_log` |

“不适用”不通过空 target 表示：D2 不建立 Criterion benchmark；真实 connector 的长稳、资源
占用与 WAL retention 由 D5 pinned gate 所有。

Debezium 的这一行是分层所有权，不表示所有行为都塞进 Rust 公共 correctness target：offline Rust
gate 证明 config、target manifest、JRE 关键资源、nested JAR closure、checkpoint/delivery codec 与
Rustdoc；Java component gate 证明 Engine handler、preview/actual、ACK 与 lifecycle；四个 native runner
用只存在于临时解包目录的确定性 connector，在没有系统 Java 的环境中经公共 API 证明
`open → start → poll(position 1) → Drop/原样重投 → ack → stop → checkpoint-only 重启 →
poll(position 2 witness) → ack → stop`、owned record 投影、pre-ACK checkpoint 和从已接受位置继续；
pinned PostgreSQL gate 最后只经公共 Rust API 证明同进程运行、unacked replay、checkpoint-only fresh
Engine restore 与 eventual LSN。确定性 connector 不进入正式 distribution 或 runtime archive，也不代替
真实 PostgreSQL 证据。
四层证据必须分别报告，只有全部通过才满足 D2 exit。

## PostgreSQL Source 的显式验收

Operation 公共 `correctness/postgres.rs` 拥有 tag11 canonical JSON golden、声明布局、exact Schema、
临时资源类型/脱敏、初始化与 checkpoint 恢复/回滚/reopen 不启动外部资源，以及损坏 checkpoint
拒绝；源码同目录 `tests.rs` 只拥有私有 Connect JSON 转换及大文本/binary/null 的稳定重批。
checkpoint codec 本身归 Debezium，不维护另一套 Source 编码。Flow 公共 `correctness/postgres.rs` 拥有资源
缺失/错误/重复/多余的无目录副作用、准确 Station ID、Schema 拒绝、build/open 与资源布局。

真实 Engine 与 PG 不进入普通 Cargo gate。显式执行：

```sh
python3 tools/check_postgres_cdc.py \
  --bundle /absolute/path/to/dogpaddle-debezium-runtime-aarch64-apple-darwin \
  --postgres-bin /absolute/path/to/postgresql/bin \
  --keep
```

需要 Python 3.9+、本机 PostgreSQL 15+ 的 `initdb/pg_ctl/postgres/psql` 和匹配 target 的已构建 runtime
payload。脚本创建独占临时 cluster、随机 loopback 端口和测试表/slot/publication，不连接已有服务；
无论成败都停止它自己的 cluster。`--keep` 或失败保留目录和日志，成功且无 `--keep` 才删除本次 fixture。

`crates/flow/examples/postgres_cdc.rs` 是公共 API JSONL host：Flow 模式证明 PG→SQLite 的完整
insert/update/delete、类型映射与进程 reopen；直接 Operation+Store 模式验证 checkpoint/output
单次原子提交、rollback/背压时二者均不变、提交后 ACK 前退出，以及 checkpoint-only fresh Engine。
2050 行单 PG 事务跨越 1024 条批量边界，首批提交后 ACK 前退出并 reopen，后继 witness 验证完整
事件顺序且无重漏。`flow-pg` 模式再证明同一个 PG 实例、同一个 database 内的 Source→PG Sink：
2050 行跨批、首批 PG 已提交而本地仍 Prepared 时进程终止、reopen、update/delete 与后继 witness，
publication 只包含源表，目标不会反馈进源。上述均为 correctness 验收，不是吞吐或延迟 benchmark。它不是产品故障注入
接口，也不是第二套运行层。结果必须报告实际 PG/Rust/runtime 版本；本机 PG17 证据不能冒充既有
Linux digest-pinned PG16.15 D1/D2 gate，D5 的长稳、fencing、升级与发布门仍独立开放。

## PostgreSQL Sink 的显式验收

Operation 公共 `correctness/postgres_sink.rs` 冻结 tag12 canonical JSON、非敏感 Definition、Sink/1
kind、唯一 `postgres_sink.state: Cell<Vec<u8>>`、exact Schema/target spec、精确
`PostgresSinkConfig` 资源类型，以及首 turn rollback/丢弃 completion 不访问目标。Flow 公共
`correctness/postgres_sink.rs` 拥有缺失/错误资源的无目录副作用、准确 Station ID、Schema 纯拒绝，
以及 build/open 离线装配和稳定 state 资源路径。源码同目录 `tests.rs` 拥有版本化 Initialize/Ready/
Prepared state codec 与 target SQL/row 编码的私有不变量。

普通 Cargo gate 不启动或连接 PostgreSQL。显式本机 gate 执行：

```sh
python3 tools/check_postgres_sink.py \
  --postgres-bin /absolute/path/to/postgresql/bin
```

脚本需要 Python 3.9+ 与本机 PostgreSQL 15+ 的 `initdb/pg_ctl/postgres/psql`，并自行构建公共
`SequenceSource → PostgresSink` Flow host，以及 Operation 所有的 `postgres_sink_recovery` 公共协议
host。只创建 loopback 临时 cluster、Flow 和 Store，不连接已有服务，无论成败都停止自己的 cluster。

- Flow：目标锁超时使 receipt/data 整笔回滚、AfterCommit fail-stop、reopen；PG 已提交而 MDBX
  仍 Prepared 时再次杀进程。最终三个 UInt64 最大值区间内的记录精确各出现一次，只留 receipt 3。
- Operation：16,385 重复行跨 1024 上限插入及完整撤回；Prepared 提交而 PG 尚未写入、PG 已写入而
  MDBX 尚未结算两个窗口；prepare/settlement rollback；不足额和非法负前缀拒绝无部分效果；普通
  planning error 后同一运行实例重试；最小 ID 匹配和交替正负事件。
- PG 参数类型与边界：NULL、嵌入 NUL 的 Utf8、UInt64 最大值、NaN payload、负零、各标量存储
  family，reopen 后精确撤回；零列与 1598 列，宽表按参数上限拆成同一事务内的小语句。
- 实际服务端 statement 日志锁住工作量：16,385 insert/delete 各 17 条批量 SQL，混合事件另各
  2 条；大额撤回各只做一次完整 admission count，所有 turn 后最多一条 live receipt。打印运行耗时
  只为诊断，不是吞吐 benchmark，也不把逻辑 receipt 数量当作 PG 文件大小的硬上限。

`.github/workflows/debezium-postgres.yml` 在产品 crate 或两份 gate 脚本变动时，先运行原 digest-pinned
D1/D2 gate，再复用其 native runtime payload 执行 Source、同 PG 往返和 Sink gate；native server 为
Ubuntu 包提供的 PG16，输出实际版本，不混同 pinned container 的证据。普通 Cargo gate 仍离线。
上述不代表 TLS、初始 snapshot、在线 Schema evolution、fencing 或生产长稳已经验收。

## Change + Store 的最小接缝

组合包只保留两个不能由 Change 与 Store 各自的证明组合推出的 witness：

1. nested、variable-width、非零 Arrow slice offset 的 full/projected decode 在 entry transaction
   结束后仍然 owned；
2. corrupt Change decode poison 同一 Store transaction，并回滚已经发生的 forwarding/cursor 写入。

稳定重批属于 Change/Operation；AppendLog paging、计费、truncate、reopen、crash 和 endurance
属于 Store。组合包不得重建这些矩阵。

## 持久化变更的最低证据

每个稳定协议至少需要：

1. 写侧 golden 或独立 raw layout 断言；
2. 读侧 decode/open/reopen；
3. 一个不复用生产算法的语义或互操作 oracle；
4. malformed/corruption 拒绝且无 panic、无部分写入；
5. 精确资源名、类型、size/codec 与失败后状态。

Expression golden 直接冻结当前精确 pin 的 DataFusion Expr protobuf bytes，并继续经过
`decode → exact Schema bind → materialize → turn`，验证 `create_physical_expr`、type/nullability 与
scalar/array evaluate。该格式不承诺跨 DataFusion 版本兼容；升级 DataFusion、`datafusion-proto` 或
Arrow 时必须更新当前 golden，并重跑 proto roundtrip、build/open/reopen 和执行语义。旧数据库直接
删除并重建；测试不约束旧 payload 或旧 manifest 的行为。

内建算子的 conformance 使用同一证据形状，不按算子复制测试框架：codec/postgres/postgres_sink 分区冻结十二个 tag、
canonical payload 与损坏拒绝，definition 分区覆盖 kind/arity/data 和 exact Schema 成功/拒绝，runtime
分区覆盖线性 turn、Action、diff/顺序、buffer sharing、错误与重批，Flow 组合根再覆盖提交前
completion 丢弃、提交后执行、post-commit fail-stop、纯失败无目录副作用、
稳定资源名、build/open/reopen 和运行期 Schema guard。RunningEventCount 的公共 API 与 data 资源名
采用当前简化基线：Definition tag 为 `2`，逻辑 data 名为 `running_event_count.count`。不保留旧 API
alias、资源路径 fallback 或迁移；正确性不约束旧库行为，只证明当前 golden、当前资源布局和当前
数据库的 build/open/reopen，旧数据库直接删除后重建。

SchemaAlign 的最低专用证据为 tag `9` golden、metadata canonical 编码与重复 key 构造拒绝、所有
表达式绑定同一原始 input、显式 `cast`/`try_cast`、non-null → nullable 放宽、nullable → non-null 纯拒绝、空字段 output、
直接列与 diff 共享，以及 Flow reopen。表达式断言必须标明“已承诺”“DataFusion 当前可规划但未承诺”
或“明确拒绝”；只有第一类可以进入用户 API 能力。Date32/Timestamp/Decimal128 首先由 Change 的
Schema/IPC/interop/malformed 轨道证明稳定传输；Operation 再用三个公共测试分别证明 direct-copy、
SchemaAlign 精确 cast/nullability 与 Filter 组合比较，且每个都走
`encode → decode → re-encode → bind → materialize → turn` 并核对 buffer/diff/顺序。Flow 组合根证明
`source → SchemaAlign → Project → Select → Extend → Filter → RunningEventCount → Discard` 在 build 与
两次 reopen 后最终 count 为 `3`。这一承诺严格限于 Date32、无 timezone 的 Millisecond Timestamp、
`Decimal128(10, 2)` 及测试中的 cast/comparison；其他时间/Decimal 运算、unit/timezone、舍入或 cast
不能从中推导。

Decimal128 value 证据独立于表达式算术：`Change::try_new`、full decode 和选中该字段的 projected
decode 递归拒绝任意 non-null slot 的 `|unscaled| >= 10^precision`，包括被 null List/Struct 祖先
遮蔽但物理存在的 non-null child；未选择字段的 value 不读取也不验证。测试同时覆盖顶层与嵌套、
构造与 codec 错误，不把这条 representability invariant 扩展为舍入或算术语义。

SQLite Sink 同时冻结 tag 10 Definition payload、版本化 pending bytes、canonical row 与 128-bit hash。
其恢复矩阵必须模拟 SQLite commit 成功后丢弃同一 turn 的 MDBX transaction，再 reopen 并重放
初始化、insert、delete 与完整 1024 项批次；目标表最终结果必须恰好一次，旧 pending 和 Flow claim
只能在外层 commit 后前进。锁、ID/完整性冲突或 SQLite commit 失败必须保留旧状态，解除故障后可以继续。
布局证据还覆盖零/1998/1999 列、标识符转义和冲突、全部 Arrow v1 类型、嵌套/nullable/浮点 bit pattern、
hash 碰撞、重复行最小 ID、正负 multiplicity、`i64::MIN`、ID 耗尽与稳定重批。Flow build/open 不得
打开 SQLite 或创建目标表；首次运行才允许初始化。

PostgreSQL Sink 同时冻结 tag12 canonical JSON、唯一版本化 state Cell，以及每个 Prepared intent 的
delivery/digest/frontier/continuation/mutations。离线测试必须证明 Definition 不持久化 runtime secret、
resource/Schema 错误先于建库、build/open 不访问 PG、rollback 或丢弃 `AfterCommit` 不产生远端动作。
真实 gate 必须在 PG receipt 与 mutations 同事务提交、MDBX 仍停在 Prepared 的窗口杀死进程，并证明
reopen 对相同 delivery 只验证 receipt 而不重复应用。单批上限固定为 1024；新 delivery 原子替换
已结算的旧 receipt，始终只保留最新一行。扩展类型或故障点时应增强同一脚本，而不是在普通 Cargo gate
偷偷依赖本机服务。

Store 层用一组多对象 transaction、drop/poison、snapshot、read-your-writes 与 SIGKILL 测试证明物理
原子性。上层只重复自己的协议阶段、对象组合和 reopen 义务，不为每个 crate 复制 SIGKILL harness。

Flow 公共 `correctness/status.rs` 证明只读 status 不启动外部资源、不推进游标，单一 snapshot 的
input/output counters 一致，能看到整轮 Progressed 下的 Station Backpressured，并在 reopen 后
保留 durable counters、清除内存 outcome。已有 fail-stop 私有故障测试同时验证 status 仍可读、
下一轮预检失败不会保留上一轮 outcome。

## Benchmark 协议

全部 10 个 target 是独立 release 进程，只有两个统一设置：

- `DOGPADDLE_BENCH_PROFILE=smoke|reference`：选择 owner 内固定、不可拼出非法组合的规模。
- `DOGPADDLE_BENCH_ROOT=/absolute/path`：reference 的固定文件系统根；smoke 默认使用临时目录。

不接受逐维环境变量。fixture、seed、预热、结果 oracle 和文件清理必须位于计时外。普通测试禁止
wall-clock 断言。持久化 reference 必须报告实际文件系统路径；所有 target 报告 rustc、OS/kernel、
CPU、Cargo profile、git revision/dirty state 和实际配置。

stdout 的 machine records 使用唯一的 typed JSONL `Record` 枚举。每个 target 必须依次产生：

1. 一个 `run`，声明环境、配置、按稳定 series 排序的完整 cases/observations 及各自精确数量；
2. 只携带紧凑 plan ID、连续 index 和 raw facts 的 `sample | observation`；
3. 唯一且位于末尾的 `completion`。

通用 validator 拒绝未知字段、非法 label、错误 identity/profile、非 canonical plan、越界或乱序 ID、
缺失/重复/额外记录，以及 completion 后的任何 machine record；它不解释 owner payload。
每个 target 邻接的 `<target>.plan.json` 另外冻结 smoke/reference 两档纯 Plan 的 case/observation 数量、
canonical byte length 与稳定 128-bit fingerprint。正常执行不读取 golden，而是消费预先冻结的同一组
plan IDs；`finish` 因而能拒绝漏跑。`cargo xtask bench-plan-check` 从 Cargo metadata 发现全部 target，
只构造两档 Plan、不创建 fixture 或开始计时，再与独立 golden 比较。`cargo xtask bench-smoke` 逐进程
真实执行 smoke workload，并同时验证 smoke golden 与完整输出；新增 target 不维护第二份 target 清单。

常规 benchmark 的 machine stream 只保留原始样本，进程内人类表格报告派生统计；pair/side 属于
run plan，两个 case 的相同 sample index 无损表达一一对应关系。Operation 的 per-operation 耗时由
`elapsed_ns / operations` 派生；Flow runtime 的总耗时、速率和分位数由 sample `elapsed_ns`、静态 work
counts 与 `round_latencies_ns` 派生，不重复写回 machine fields。AppendLog 长稳的 append/truncate
事务也是普通 duration cases；checkpoint observations 保留状态与文件大小，terminal observations 只补充
raw samples 无法导出的 wall elapsed 和最终 reopen checksum，其精确数量均在 run plan 中声明。
p50/p95/p99/max、吞吐、peak 和 tail spread 均从这些 raw facts 派生并显示在人类 summary。正式前后
对比只能使用相同代码协议版本、rustc、机器、profile、文件系统和 workload；不设置机器相关的 CI
wall-clock 阈值。

## Canonical gates

本地和 pinned MSRV CI：

```bash
cargo xtask check
cargo xtask bench-plan-check
cargo xtask bench-smoke
```

`cargo xtask check` 依次运行：

```bash
cargo fmt --all -- --check
cargo test --workspace --locked
cargo test --workspace --release --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

CI 另用最新 stable 运行一次 debug workspace tests，证明前向兼容。常用定向命令：

```bash
cargo test -p dogpaddle-store --test correctness transaction::
cargo test -p dogpaddle-flow --test correctness runtime_corruption::
cargo test -p dogpaddle-change-store-integration
cargo bench -p dogpaddle-flow --bench flow_runtime
```

`dogpaddle-debezium` 的普通 Rust gate 属于上述 workspace check，不能调用 Maven、下载 Java
artifact、联网或要求本机存在 JDK。Java 与 bundle gate 是显式命令：

```bash
crates/debezium/scripts/build-distribution.sh
crates/debezium/scripts/build-runtime-bundle.sh x86_64-unknown-linux-gnu
experiments/debezium-d1/scripts/run.sh
```

`build-distribution.sh` 只使用本机 Maven 与 JDK。
[`Debezium runtime bundles`](.github/workflows/debezium-runtime.yml) workflow 在 Ubuntu
上构建并测试一次 Java distribution，同时单独构建 test-only lifecycle connector；
Linux GNU x86_64/aarch64 与 macOS x86_64/aarch64 四个原生
runner 下载同一产物，再分别构建 Rust probe 和 runtime payload。每个 runner 将 archive 解压到含
空格的新路径，用 `crates/debezium/scripts/install-lifecycle-probe.sh` 把
`crates/debezium/bridge/probe/` 产出的确定性 connector 只注入该临时副本并重算
`debezium/SHA256SUMS`，然后清空 Java 相关环境、动态库搜索路径和系统 `PATH`，经
`crates/debezium/examples/bundled_runtime_probe.rs` 与公共 API 完成
`open → start → poll(position 1) → Drop/原样重投 → ack → stop → checkpoint-only 重启 →
poll(position 2 witness) → ack → stop`。它同时校验 topic、partition、timestamp、key、value、headers
和 pre-ACK checkpoint，并以第二条确定性记录证明新 Connector 从已接受位置继续而不是重放第一条。
上传的 runtime archive 不包含 probe connector、Rust probe
或其他宿主 executable。

真实 PostgreSQL 顺序、ACK、replay、checkpoint restore 与 eventual LSN 矩阵仍由 Linux x86_64
D1 gate 独立拥有。D1 分别只读挂载 Rust diagnostic host 与 runtime payload，并用 payload 内 JVM
在同一进程运行；它不依赖系统 Java。
[`Debezium PostgreSQL recovery`](.github/workflows/debezium-postgres.yml) workflow 在相关 PR、
`main` 变更、每周定时与手动触发时，于 Ubuntu 24.04 直接运行
`experiments/debezium-d1/scripts/run.sh`；它无论成功还是失败都执行 artifact 上传，其中包含当次
已产生的环境、checkpoint/fixture 状态与日志。
普通 Rust、Java component、四平台 bundle lifecycle 和 D1 PostgreSQL gate 都通过才构成 D2 证据。

## 新增或删除测试

新增测试前依次问：

1. 它锁住了哪个尚无证据的公共承诺或故障边界？
2. 最强所有者是谁？能否扩展现有 fixture/table/property，而不是新建 framework？
3. expected 是否独立于被测实现？失败时能否定位到一个契约？
4. 如果它只是更弱测试的重复，是否应替换旧测试而不是叠加？

代码重构后，若公共 correctness 已经更强地蕴含某个白盒 witness，应删除白盒版本。golden、独立
model、malformed/no-panic、真实 reopen/crash 和 compile-fail capability 证据不能仅为减少数量而删除。
