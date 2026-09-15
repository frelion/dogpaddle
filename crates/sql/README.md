# dogpaddle-sql

`dogpaddle-sql` 是 `DogPaddle` 的产品编译入口。一个 `SqlProgram` 表示一条
`INSERT INTO sink(...) <query>`：SQL crate 负责解析、类型分析、翻译成 Operation，并把可以共用事务边界的 Operation 装进同一个 Station。执行和恢复仍由普通 `Flow` 完成。

它不维护第二套执行引擎，也不引入 Table、View、Catalog、后台 runner 或成本优化器。

## 一条 SQL 如何运行

```text
SQL 文件
  │
  ├─ syntax：验证一条 INSERT，并提取具体 source / sink
  ├─ plan：用 DataFusion 做名称解析与类型转换，再翻译受支持的 LogicalPlan
  ├─ assembly：形成 Operation DAG，并按结构规则融合 Station
  └─ program：生成身份，创建或恢复持久 Flow
          │
          ▼
      Flow::advance()
```

例如：

```sql
INSERT INTO sqlite(
    path => env('DOGPADDLE_QUICKSTART_SQLITE'),
    table => 'even_squares'
)
WITH numbers AS (
    SELECT CAST(sequence.value AS BIGINT) AS number
    FROM sequence(start => 0)
)
SELECT number, number * number AS square
FROM numbers
WHERE number % 2 = 0;
```

会得到两个 Station：

```text
sql/scan/00000000                              sql/sink
┌──────────────────────────────────────┐       ┌────────────┐
│ SequenceScan → 投影 → Filter → 投影  │══════▶│ SQLiteSink │
└──────────────────────────────────────┘       └────────────┘
             一笔 Store 事务                         独占
```

Station 是事务和持久化边界。同一 Station 内的线性 Operation 不需要中间持久队列；Station 之间通过持久 `SubscribedLog` 连接，因此进程退出后可以从已提交位置继续。

## 用户入口

安装后的产品命令只有：

```text
dogpaddle run SQL_FILE [--state DIR]
```

省略 `--state` 时，状态放在 SQL 文件旁的 `.dogpaddle/<stem>`。生产部署建议显式指定状态路径：

```sh
dogpaddle run /etc/dogpaddle/orders.sql --state /var/lib/dogpaddle/orders
```

命令会打印规范化后的状态路径并持续调用 `Flow::advance()`。`Progressed` 后立即继续；`Idle` 或 `Backpressured` 时短暂等待。`Ctrl-C` 只设置停止标记，当前有界 `advance` 返回后退出。运行错误直接退出；如果当前 Flow 必须 reopen，错误信息会提示重新执行同一命令。

没有 `check`、`status`、`init`、`reset`、`build` 或 `open` 子命令。

Rust 嵌入接口同样只有一个生命周期入口：

```rust,no_run
use dogpaddle_sql::SqlProgram;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let program = SqlProgram::read("orders.sql")?;
let mut flow = program.start("/var/lib/dogpaddle/orders")?;

let outcome = flow.advance()?;
# let _ = outcome;
# Ok(())
}
```

| API | 行为 |
| --- | --- |
| `SqlProgram::parse(sql)` | 纯解析 UTF-8 SQL；不读文件、不解析环境变量值、不连接外部系统 |
| `SqlProgram::read(path)` | 读取 UTF-8 SQL 文件并走同一解析路径 |
| `program.start(state)` | 状态路径不存在时编译并构建；存在时校验身份并恢复 |

`start` 不会在恢复失败后回退为构建。状态不完整、损坏、被占用或属于另一份 Program 时都会失败，已有目录和数据保持不动。低层 Rust 用户若要直接声明 Operation 和拓扑，使用 `dogpaddle-flow` 的 `FlowFactory::build/open`。

状态路径必须是 UTF-8。`start` 会创建缺失的父目录，并在派生外部资源身份前规范化路径。每次调用会把所有 endpoint 参数解析成一份临时快照；后续身份计算、发现和 build/open 都只读取这份快照，因此一次启动不会混用凭据轮换前后的值。

## Program 身份与恢复

SQL crate 为 Program 计算稳定的 32 字节身份，并通过 `FlowFactory::owner_identity` 原子写入 canonical Flow Definition。恢复时，Flow 在 Schema binding、资源打开和 Operation materialize 之前比较期望身份。

身份覆盖规范化查询、确定性装配 ABI、固定输出容量，以及会改变持久语义的 endpoint 参数。密码、用户名、主机、端口、runtime 位置和环境变量名称不进入身份；因此可以轮换凭据或连接地址，但不能用另一份查询、另一张表或不同的持久参数接管已有状态。SQL 原文、AST、LogicalPlan、凭据和环境引用都不持久化。

如果修改了查询语义、表身份、publication、spool 容量或装配规则，应使用新的状态路径。当前是开发期 v1，不读取或迁移旧布局。

## Endpoint 合同

参数必须写成 `name => value`。值只能是单引号字符串、非负整数或 `env('NAME')`；未知、缺失、重复、位置参数和其他命名运算符都会被拒绝。所有环境变量在 `start` 访问状态前解析，错误不回显解析后的秘密。

| 方向 | 函数 | 参数 |
| --- | --- | --- |
| Scan | `sequence` | `start` |
| Scan | `postgres_cdc` | `connection`, `table`, `publication`; 可选 `bootstrap_spool_bytes` 和 CDC 调优参数 |
| Scan | `mysql_cdc` | `connection`, `table`; 可选 `bootstrap_spool_bytes` 和 CDC 调优参数 |
| Sink | `sqlite` | `path`, `table` |
| Sink | `postgres` | `connection`, `table` |
| Sink | `clickhouse` | `connection`, `table` |
| Sink | `doris` | `connection`, `table` |
| Sink | `discard` | 无 |

`PostgreSQL` 示例：

```sql
INSERT INTO postgres(
    connection => env('TARGET_DATABASE_URL'),
    table => 'ops.current_orders'
)
SELECT *
FROM postgres_cdc(
    connection => env('SOURCE_DATABASE_URL'),
    table => 'sales.orders',
    publication => 'orders_publication',
    bootstrap_spool_bytes => 2147483648,
    heartbeat_interval_ms => 2000,
    snapshot_fetch_size => 4096
);
```

`MySQL` 示例：

```sql
INSERT INTO sqlite(path => '/var/lib/dogpaddle/orders.sqlite', table => 'orders')
SELECT *
FROM mysql_cdc(
    connection => env('SOURCE_DATABASE_URL'),
    table => 'shop.orders'
);
```

`connection` 必须解析为无 TLS 的数据库 URL，不能带 query 或 fragment：

```text
postgresql://user:password@127.0.0.1:5432/database
mysql://user:password@127.0.0.1:3306/database
clickhouse://user:password@127.0.0.1:8123/database
clickhouse+http://user:password@127.0.0.1:8123/database
doris://user:password@127.0.0.1:9030/database
```

URL 必须包含用户名和一个数据库路径段，用户名、密码和数据库名支持 percent encoding。数据库 Sink 只接受 numeric IP。`table` 必须恰好包含两个非空部分：`PostgreSQL` 使用 `schema.table`；`MySQL`、`ClickHouse` 和 `Doris` 使用 `database.table`，且 database 必须与 URL 一致。`ClickHouse` 与 `Doris` 当前只支持无 TLS endpoint。

`bootstrap_spool_bytes` 是私有快照队列的非零硬上限，默认 1 GiB。PostgreSQL 的容量要覆盖完整快照和快照封口前的 WAL 重叠；MySQL 的容量要覆盖完整快照，binlog 还必须保留到私有 spool 发布并追平完成。容量不足时当前 delivery 不提交也不 ACK，需要以更大容量和新状态重建。

两个 CDC Scan 都接受下面这些可选运行调优参数：

| 参数 | 作用 | 约束 |
| --- | --- | --- |
| `connect_timeout_ms` | 数据库连接超时；同时作用于启动前发现和 Debezium connector | `1..=2147483647` |
| `query_timeout_ms` | 数据库查询超时；同时作用于启动前发现和 Debezium connector | `1..=2147483000` |
| `retry_limit` | connector 启动成功后，Debezium 对可重试 polling 故障的最大重试次数；`0` 表示不重试 | `0..=2147483647` |
| `retry_max_delay_ms` | 上述 post-start polling 重试的最大退避间隔 | `301..=2147483647` |
| `heartbeat_interval_ms` | 持续捕获阶段的 heartbeat 间隔 | `1..=2147483647` |
| `snapshot_fetch_size` | 初始快照每次向 JDBC 请求的行数 | `1..=2147483647` |

省略这些参数时由对应 Scan 使用固定的产品默认值。PostgreSQL 的启动前发现与 Debezium 默认连接、查询超时均为 5 秒，快照 fetch size 为 10240。MySQL 的启动前发现默认连接、查询超时为 5 秒；Debezium 保持 30 秒连接超时、10 分钟查询超时以及未设置 fetch size 的流式读取行为。两者默认无限重试，最大退避 10 秒，持续捕获 heartbeat 为 1 秒。bootstrap 阶段为推进内部 checkpoint 固定使用 1 毫秒 heartbeat，不受用户参数影响。

启动前发现使用用户给出的精确毫秒值。Debezium 3.6 的 JDBC 查询超时实际以整秒执行，因此 connector 侧会向上取整到下一秒，避免 `1..999ms` 被 JDBC 解释成 `0`（无限等待）；PostgreSQL JDBC 的连接超时同样向上取整到整秒。MySQL 的发现阶段 query timeout 是 socket 读写等待上限，Debezium 阶段则是 JDBC statement 上限，两者共享预算但触发条件不同。

这些值只改变本次进程的连接、等待和读取批量，不进入 Program 身份或 Flow Definition。修改后重新执行同一 SQL 和 state path 即可生效。`snapshot.mode`、offset、schema history、事件表示、过滤、topic/slot/client identity、queue 和 delivery 上限仍由 `DogPaddle` 管理。

`retry_limit` 与 `retry_max_delay_ms` 映射 Debezium 的 connector error handler，只管理已进入 polling 后的可重试故障；它们不控制初始 task 启动，PostgreSQL 中也不控制 replication slot 创建。

`connect_timeout_ms` 与 `query_timeout_ms` 是单次数据库操作的预算，不是整个 connector 启动预算。为避免启动永久阻塞，`DogPaddle` 仍以固定 60 秒等待 connector 进入 polling；这高于当前 Debezium 任务管理的默认 40 秒，也为 `MySQL` 默认 30 秒连接超时留出调度余量。更大的连接或查询预算在启动后的重连与查询中仍会生效，但不会延长这 60 秒 readiness 边界。

SQL 不再接收 `runtime_bundle`、engine name、`PostgreSQL` slot、Sink ID 或 `MySQL` replication client ID。engine、slot 和 Sink ID 由 Program 身份、状态路径、endpoint 序号确定性派生；持久化的 `MySQL` engine name 再唯一确定 client ID，因此移动状态目录不会改变恢复身份。

CDC runtime 默认位于 executable 安装根下的 `libexec/dogpaddle/debezium`。源码树和系统测试可以设置 `DOGPADDLE_DEBEZIUM_RUNTIME` 覆盖它；该变量必须是绝对路径。一个进程只使用一个 canonical runtime，并共享其中唯一的 JVM。

## SQL v1 范围

支持：

- `SELECT`、`WHERE`、字段别名、非递归 CTE 和派生查询；
- `CAST`、`TRY_CAST`、`CASE`，以及当前表达式层支持的比较、布尔和算术表达式；
- `SELECT DISTINCT` 和 positional `UNION ALL`；
- Inner、Left/Right/Full Outer、Left/Right Semi、Left/Right Anti Join；
- left-preserving `ASOF JOIN ... MATCH_CONDITION (...) [ON ... | USING (...)]`；
- 非空 `GROUP BY`，`COUNT`、`SUM`、`AVG`、`MIN`、`MAX`，以及只有分组字段的查询。

每个普通 Join 至少有一个跨左右输入的等值 key。其余 `ON` 合取作为原生 residual 编译进 `EquiJoin`，
Inner、Outer、Semi 和 Anti 都以完整条件决定记录对是否匹配；predicate 的 `false` 与 `NULL` 都不匹配。
Right Join 通过交换输入复用 Left 语义，同时交换 residual 的端口 qualifier，再用同 Station 的
`SchemaAlign` 恢复 `DataFusion` 给出的字段顺序、名称、nullability 和 metadata。

`ASOF JOIN` 直接采用 `DataFusion` 的 Snowflake 风格语法，不建立另一套 SQL planner。每个 left row
选择至多一个 right row，没有匹配时仍保留 left row，并把 right 字段补为 NULL。`MATCH_CONDITION`
必须是跨左右输入的一次 `<`、`<=`、`>` 或 `>=` 比较：`>`/`>=` 选择向后最近值，`<`/`<=`
选择向前最近值，是否包含等号就是 exact-match 策略。可选 `ON` 只接受等值合取，`USING` 接受
同名等值分区；两者都省略时在全局分区中匹配。

ASOF equality 与 order 表达式绑定后只接受左右完全相同的稳定、扁平、非浮点 Arrow 类型：`Null`、
`Boolean`、`Int8`、`Int16`、`Int32`、`Int64`、`UInt8`、`UInt16`、`UInt32`、`UInt64`、`Utf8`、
`Binary`、`Date32`、任意单位与时区的 `Timestamp`，以及 `Decimal128`；`Float32`、`Float64` 和
其他类型都会在创建状态目录前被拒绝。普通 SQL equality 不匹配 NULL，NULL order 也永远没有
候选；完整 Rust API 另有 null-safe `NotDistinct`。

完整 `AsOfJoinDefinition` Rust API 的 backward/forward 支持非空 lexicographic order tuple；nearest
或 tolerance 则必须恰好只有一个可计算距离的 order，其类型限于上述整数、`Date32`、`Timestamp`
或 `Decimal128`。当前 `DataFusion` 原生 SQL node 固定只携带一个 order comparison；DogPaddle 的原生
ASOF SQL surface 中，`ON` 只接受 ordinary equality 合取，也没有 candidate residual、nearest、
tolerance、NULL-safe equality 或 tie-break 子句，因此 SQL 层不伪造这些扩展。SQL 中同一分区和
order 值若仍有多个不同的 right candidate，会确定性报错，而不会按到达顺序任意选择。没有
watermark 或 retention 合同时，ASOF 仍保存两侧关系并让 right 侧修正重配历史 left rows，因此
控制的是输出基数，不承诺有界状态。

明确拒绝：

- 普通表或未注册函数；
- `SELECT ALL`、`DISTINCT ON`、普通 `UNION` 和非 positional `UNION ALL`；
- Cross、Natural、普通等值 Join 的 `USING`，以及没有跨输入等值 key 的普通纯非等值 Join；
- 空 `GROUP BY` 的 global aggregate、grouping sets、aggregate modifier、聚合 UDF 和不支持的类型；
- Sort、Limit、Offset、Window、Values、EmptyRelation、DML、DDL、递归 CTE；
- 会在规划时丢失语义的 sampling、hint、row lock、typed alias 等语法。

`DataFusion` 只做 parser、`SqlToRel` 和 `TypeCoercion`。SQL crate 不运行 logical optimizer、physical planner 或 `SessionContext`。Projection 和 Union 分支最终都通过 `SchemaAlign` 精确保留分析后的 Arrow Schema。

## 智能装配规则

装配器统计每个 logical node 的直接消费边，再按确定性 postorder 处理。当前 Operation 只有同时满足以下条件时才追加到上游 Station：

1. 它是单输入 `AtomicTransform`；
2. 唯一上游只有这一条消费边，没有分叉；
3. 上游仍是所属 Station 的末项；
4. 该 Station 允许追加 atomic 尾链。

不满足时创建新 Station。分叉需要为每个消费者保留独立订阅位置；Join 作为双输入 `TurnTransform` 从新 Station 开始，但可以吸收后续单输入 Atomic；`ExclusiveTransform` 和 Sink 必须独占。

```text
直线： [Scan, Filter, Projection] ══持久边══> [Sink]

分叉： [Scan] ══> [Filter A]
             ╚══> [Filter B]

Join： [Left Scan]  ─┐
                     ├══> [Join, Projection] ══> [Sink]
      [Right Scan] ──┘
```

消费数按 edge 计算，同一 producer 接到同一个多输入 Operation 的两个端口也算两条。每个实际有输出的 Station 固定使用 64 MiB 持久队列；Scan ID 为 `sql/scan/{index:08x}`，实际新建的 Transform Station 使用稠密 `sql/transform/{index:08x}`，最终 Sink 为 `sql/sink`。

## 源码入口

1. [`src/program.rs`](src/program.rs)：`SqlProgram`、身份与 start 生命周期。
2. [`src/syntax.rs`](src/syntax.rs)：外层 INSERT 和 endpoint Table Function 的语法验证。
3. [`src/endpoint.rs`](src/endpoint.rs)：具体 endpoint 参数、连接解析、发现和运行资源。
4. [`src/plan.rs`](src/plan.rs)：`DataFusion` 规划与受支持节点的 lowering。
5. [`src/assembly.rs`](src/assembly.rs)：私有 logical arena、Station ID 与融合规则。
6. [`src/aggregate.rs`](src/aggregate.rs)：SQL 聚合函数 descriptor 与 lowering。

## 验证

```sh
cargo test -p dogpaddle-sql --test correctness
cargo test -p dogpaddle-sql --doc
```

产品命令的完整本地例子见仓库根 [`README`](../../README.md)。真实 `PostgreSQL` CDC、Sink、恢复和 SQL gate 见仓库根目录的 [`TESTING.md`](../../TESTING.md)。
