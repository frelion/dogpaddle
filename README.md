# DogPaddle

[![CI](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml)
[![Debezium runtime bundles](https://github.com/frelion/dogpaddle/actions/workflows/debezium-runtime.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/debezium-runtime.yml)
[![Debezium PostgreSQL recovery](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml)

**把可恢复的数据流，嵌进 Rust 进程。**

DogPaddle 是一个为 Rust 应用设计的嵌入式 Dataflow 引擎。它用 Arrow 和 DataFusion 处理数据，
并把静态 Flow、消费进度与算子状态保存在本地事务 Store 中。应用重启后，可以从已提交位置
重新打开并继续推进。

无需独立服务、控制面或流处理集群。

![同一个 PostgreSQL 内经 DogPaddle 持续增量同步](docs/assets/postgres-cdc-live.gif)

*真实进程录制：左侧向同一个 PostgreSQL 的 `source.orders` 写入 INSERT、UPDATE、DELETE；中间运行
`PostgresSource → PostgresSink`，从 WAL 捕获增量并以 Arrow Change 持久推进；右侧从
`target.orders` 查询同步结果。中途强制终止并重新打开 Flow host 后，后续变更继续到达目标。
publication 只包含 `source.orders`，写回不会形成 CDC 回环。停顿仅用于演示；该链路不执行初始快照。*

[查看演示 Flow](crates/flow/examples/postgres_sync_live.rs) ·
[重新生成录屏](tools/record_postgres_cdc_live.sh)

## 为什么是 DogPaddle

- **状态随 Flow 持久化**：拓扑、游标与算子进度统一保存在本地 MDBX，重启后从已提交位置继续。
- **先验证，再落盘**：完整 DAG 和 Arrow Schema 通过后才创建资源；不完整构建不会被打开为 Flow。
- **Arrow + DataFusion**：Arrow 承载批量差分，DataFusion 执行类型化、向量化表达式。
- **推进权留给应用**：每次 `Flow::advance` 只做有界工作，应用决定运行节奏，慢消费者自然形成软背压。

## 立即体验

```sh
demo_dir="$(mktemp -d /tmp/dogpaddle-demo.XXXXXX)"
cargo run --locked -q -p dogpaddle-flow --example sqlite_sink_live -- "$demo_dir" 10 0
sqlite3 -readonly -header -column "$demo_dir/events.sqlite" \
  'SELECT "$dogpaddle.id" AS id, number, square FROM even_squares ORDER BY id;'
```

需要 Rust 1.96+ 与 `sqlite3` 命令行。

## 已实现

当前共有 12 个内建算子，全部沿用同一套 Definition、Schema binding、Operation turn 与 Flow 调度协议。

| Source | Transform | Sink |
| --- | --- | --- |
| `SequenceSource` · `PostgresSource`（试点） | `RunningEventCount` · `Project` · `Filter` · `Extend` · `Select` · `SchemaAlign` · `UnionAll` | `SqliteSink` · `PostgresSink`（试点） · `Discard` |

`SqliteSink` 首次收到输入后延迟初始化普通 `STRICT` 表，不引入额外 SQLite 元数据表。
它支持 DogPaddle v1 的全部数据类型，并为 SQLite 与 MDBX 之间的提交窗口保存可重放批次；
在 Sink 独占目标表且数据库未被外部修改或替换的前提下，重放不会重复最终结果。

`PostgresSource → PostgresSink` 已形成首条 PostgreSQL 到 PostgreSQL 的持续增量链路。Source 借助
进程内 `dogpaddle-debezium` 从单表 WAL 捕获固定 Schema 事件，并在 checkpoint 与 Station output
同事务提交后才 ACK；Sink 将 exact relation 物化到独占的新目标表，每个 Prepared 批次把 receipt
与 mutation 放在同一个 PostgreSQL 事务中，使提交窗口可在 reopen 后重放收敛。Definition 只保存
非敏感 spec，连接配置与密码只在 build/open 时作为运行资源注入。
现有验收覆盖大批 insert/delete、提交前后进程终止与恢复，以及同一个 PG 数据库的完整往返；
PG Sink 按顺序批量写入，最多保留一条当前确认记录。目标列优先保留 Arrow 精确值，不是源表原生
SQL 类型的原样镜像；也不承诺整个源事务在目标侧原子可见。
这是无初始快照、无 TLS 与在线 Schema evolution 的试点；
[Source 使用与边界](crates/operation/README.md#postgresql-source-试点) ·
[Sink 使用与边界](crates/operation/README.md#operationsinkpostgressink)。

## 当前边界

- DogPaddle 目前仍是早期引擎内核，优先打磨持久化、恢复和 Schema 边界。
- 运行由宿主反复调用 `Flow::advance` 驱动；`Flow::status` 可只读查看各 Station 的游标、积压、容量、
  最近处理结果和是否需要 reopen。尚无 `Flow::start`、后台 runner 或中断控制。
- Operation 集合目前封闭。
- 一个 Store 路径同一时刻只允许一个活动 Flow。
- PostgreSQL 增量链路尚无初始全量、多表路由、TLS、在线 Schema evolution 或跨 Flow fencing；
  要物化完整关系，源表须在 slot 起点为空并从该起点开始写入。`PostgresSink` 只创建并独占新目标表，
  不支持 target spec 跨 Flow 接管/共享、外部改表/改数据或数据库替换恢复；生产加固仍属于 D5。
- `SqliteSink` 同样只创建并独占新目标表；尚无 MySQL 或通用外部 Sink。
- 当前是开发期 v1，持久格式显式版本化并经过 golden 测试，但不提供跨版本迁移承诺。

## 深入阅读

- [算子路线与语义边界](OPERATOR_ROADMAP.md)
- [Debezium Source D0–D7 路线图](DEBEZIUM_ROADMAP.md)
- [ADR-0001：在 Rust 宿主中嵌入 Debezium Engine](docs/adr/0001-embed-debezium-engine.md)
- [Debezium：自包含进程内 Engine 与 pre-ACK checkpoint](crates/debezium/README.md)
- [Flow：构建、运行与恢复](crates/flow/README.md)
- [Change：Arrow 差分与 IPC](crates/change/README.md)
- [Operation：定义、Schema 绑定与执行](crates/operation/README.md)
- [Store：MDBX 事务与集合](crates/store/README.md)
- [重新生成 PostgreSQL 增量同步录屏](tools/record_postgres_cdc_live.sh)
- [PostgreSQL Sink 真实验收](tools/check_postgres_sink.py)
- [重新生成 SQLite Sink 录屏](tools/record_sqlite_sink_live.sh)
- [SqliteSink 端到端测试](crates/flow/tests/correctness/sqlite_sink.rs)
- [正确性与性能测试](TESTING.md)
