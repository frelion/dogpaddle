# dogpaddle-sql

`dogpaddle-sql` 是 DogPaddle 最上层的编译入口：它把一条受限的
`INSERT INTO sink(...) <query>` 编译成 `dogpaddle-flow` 已有的 Operation 和 Station，然后返回普通
`Flow`。它不执行另一套 SQL 引擎，也不引入 Table、View、Catalog 或后台服务。

## 十分钟理解 SQL 如何变成 Flow

### 1. 从一条 SQL 到可恢复流水线

先看随仓库发布的最小程序：

```sql
INSERT INTO sqlite(
    path => env('DOGPADDLE_QUICKSTART_SQLITE'),
    table => 'even_squares'
)
WITH numbers AS (
    SELECT CAST(sequence.value AS BIGINT) AS number
    FROM sequence(start => 0)
)
SELECT
    number,
    number * number AS square,
    CASE WHEN number >= 10 THEN 'large' ELSE 'small' END AS size
FROM numbers
WHERE number % 2 = 0;
```

这里 `sequence(...)` 是 Scan endpoint（数据入口），`sqlite(...)` 是 Sink endpoint（数据出口）。中间
查询会被翻译成 Filter、SchemaAlign 等已有 Operation。最终物理结构只有两个 Station：

```text
sql/scan/00000000                                      sql/sink
┌──────────────────────────────────────────────┐       ┌────────────┐
│ SequenceScan → 表达式/投影 → Filter → 投影   │══════▶│ SQLiteSink │
└──────────────────────────────────────────────┘       └────────────┘
                同一事务、没有中间落盘                    独占
```

**Station** 是 Flow 的事务和持久化边界。SQL 编译器会把安全的单输入 Operation 接到上游 Station
末尾，这就是这里的算子融合。Station 之间仍通过持久队列连接，所以进程退出后可以继续。

完整编译路径是：

```text
SQL 文本
  ↓ 解析 endpoint 与查询
DataFusion LogicalPlan（名称解析和类型转换）
  ↓ 只接受 DogPaddle 明确支持的节点
Operation DAG（SQL crate 内的临时有向图）
  ↓ 按结构规则划分 Station
FlowFactory
  ↓ build
持久化 Flow
```

DataFusion 只负责 SQL 解析、列解析和 type coercion（把兼容类型改写为明确的 cast）。它不会执行查询。
SQL crate 把认可的 LogicalPlan 节点逐个 lower（翻译）为 `dogpaddle-operation` 的 Definition；真正执行
仍由 `Flow::advance()` 完成。

### 2. 自动装配和融合规则

编译器先统计每个逻辑节点有多少条直接消费边，再按上游先于下游的固定顺序扫描节点。一个 Operation 只有同时满足
下面四点，才追加到上游 Station：

1. 它是单输入 `AtomicTransform`，也就是能在当前事务完整处理一条 Change；
2. 它的上游节点只被当前 Operation 消费一次，没有分叉；
3. 上游仍是所属 Station 的最后一个 Operation；
4. 上游 Station 的首 Operation 允许接 atomic 尾链。

不满足时就新建 Station。这是一条确定性的结构规则，不是成本优化器。

几个典型结果：

```text
直线： Scan → Filter → Projection → Sink
结果： [Scan, Filter, Projection] ══持久边══> [Sink]

分叉：            ┌→ Filter A
       Scan ──────┤
                  └→ Filter B
结果： [Scan] ══持久边══> [Filter A]
            ╚════持久边══> [Filter B]

Join： Left Scan ─┐
                  ├→ Join → Projection → Sink
      Right Scan ─┘
结果： [Left Scan] ─┐
                    ├══> [Join, Projection] ══> [Sink]
      [Right Scan] ─┘
```

分叉必须保留上游持久输出，让每个分支拥有独立订阅位置。Join 是双输入 `TurnTransform`，所以先创建新
Station；它可以跨多轮处理一条 Change，又允许后面的单输入 atomic Projection 融入。同一上游节点
接到同一多输入 Operation 的两个端口时，也按两条消费边计算。Sink 永远独占。

Operation 自己报告是否可融合。表达式实例如果不满足可重放要求，会报告 `ExclusiveTransform` 并形成
独立边界；SQL 编译器不会根据 tag、是否有状态或算子名称猜测。

每个实际有输出的 Station 使用固定 64 MiB 持久队列容量。只有真正新建的 Transform Station 才获得
稠密编号 `sql/transform/{index:08x}`；融合不会留下空 Station。最终分组直接写入当前 v1 Flow
Definition，`open` 不重新运行装配策略。

### 3. build、advance、open

产品公共面只有 `SqlProgram`：

```rust,no_run
use std::error::Error;

use dogpaddle_sql::SqlProgram;

fn main() -> Result<(), Box<dyn Error>> {
    let program = SqlProgram::read("flow.sql")?;
    let mut flow = program.build("/var/lib/dogpaddle/flow")?;

    flow.advance()?;
    drop(flow);

    let mut flow = program.open("/var/lib/dogpaddle/flow")?;
    flow.advance()?;
    Ok(())
}
```

| API | 发生什么 |
| --- | --- |
| `SqlProgram::parse(sql)` | 纯解析一段 UTF-8 SQL；不读文件、不读取环境变量值、不连接外部系统 |
| `SqlProgram::read(path)` | 读取一个 UTF-8 SQL 文件并调用同一解析路径 |
| `program.build(state_path)` | 解析参数、发现外部 Schema/目标、生成 Operation、装配 Station 并创建 Flow |
| `program.open(state_path)` | 先解析该 Program 的全部 endpoint 参数，再从磁盘恢复 Flow，并注入所需的临时运行配置 |

`Flow::advance()` 每次执行一轮有界调度。它不会永久占住当前线程；宿主决定循环、等待、停止和重启节奏。

磁盘中稳定编码的 Flow Definition 是恢复时的拓扑真相。SQL 文本、DataFusion LogicalPlan 和临时
Operation DAG 都不持久化。`open` 不重新编译查询，也不比较当前 Program 与磁盘中的查询、融合结果或
非敏感 endpoint 身份；当前 Program 只是按 endpoint 次序提供临时运行资源。传错 Program 时，一部分
声明可能被忽略，也可能因资源数量或类型不匹配而失败，不能把 `open` 当作身份校验。

`open` 会在读取 Flow 状态前解析当前 Program 的**全部** endpoint 参数，包括 `env(...)`、`sequence`
起点、SQLite 路径和最终不参与运行资源注入的身份参数。任何参数无法解析都会先失败。部署时应保留构建
所用的同一份 SQL；修改 SQL、endpoint 身份或装配规则后，应使用新状态目录或删除旧库重建。v1 没有
旧布局兼容路径。

## 跑通 Quickstart

完整 SQL 位于 [`examples/quickstart.sql`](examples/quickstart.sql)，宿主位于
[`examples/quickstart.rs`](examples/quickstart.rs)。在工作区根目录运行：

```sh
demo_dir="$(mktemp -d /tmp/dogpaddle-demo.XXXXXX)"
export DOGPADDLE_QUICKSTART_SQLITE="$demo_dir/results.sqlite"

cargo run --locked -q -p dogpaddle-sql --example quickstart -- \
  build crates/sql/examples/quickstart.sql "$demo_dir/flow" 12 0

sqlite3 -readonly -header -column "$DOGPADDLE_QUICKSTART_SQLITE" \
  'SELECT "$dogpaddle.id" AS id, number, square, size FROM even_squares ORDER BY id;'
```

第一次运行得到 `0, 2, 4, 6, 8`。进程退出后，用同一状态目录继续：

```sh
cargo run --locked -q -p dogpaddle-sql --example quickstart -- \
  open crates/sql/examples/quickstart.sql "$demo_dir/flow" 6 0
```

再次查询会新增 `10` 和 `12`，已有行不会重复。最后两个参数分别是调度轮数和每轮延迟毫秒数。
`sequence` 是持续 Scan，所以示例故意只运行有限轮。

## SQL 输入合同

一个 `SqlProgram` 只接受一条完整语句：

```text
INSERT INTO sink(name => value, ...)
WITH ...
SELECT ...
```

支持 SQL 注释、非递归 CTE、派生查询和末尾分号；拒绝多条语句。endpoint 参数必须使用
`name => value`，值只能是单引号字符串、非负整数或 `env('NAME')`。位置参数、重复参数、未知参数和
缺失参数都会报错。环境变量到 `build/open` 才解析，错误不会打印解析后的秘密。

### Endpoint

| 方向 | SQL 函数 | 必需参数 |
| --- | --- | --- |
| Scan | `sequence` | `start` |
| Scan | `postgres_cdc` | `engine_name`, `runtime_bundle`, `host`, `port`, `database`, `user`, `password`, `schema`, `table`, `slot`, `publication`, `bootstrap_spool_bytes` |
| Scan | `mysql_cdc` | `engine_name`, `runtime_bundle`, `host`, `port`, `database`, `user`, `password`, `replication_client_id`, `table`, `bootstrap_spool_bytes` |
| Sink | `sqlite` | `path`, `table` |
| Sink | `postgres` | `sink_id`, `host`, `port`, `database`, `user`, `password`, `schema`, `table` |
| Sink | `discard` | 无 |

`postgres_cdc` 和 `mysql_cdc` 会先把初始快照存入各自的私有持久 spool（缓冲队列），封口后再发布到
普通 Station 输出，然后继续 CDC。`bootstrap_spool_bytes` 是必填的非零 `u64`；容量不足时当前
delivery 不会 ACK，需要用更大容量和新状态目录重建。

PostgreSQL CDC 的 spool 必须容纳完整快照及封口前观察到的 WAL 重叠；它要求预先创建 publication，
并使用首启前不存在、之后由该 Scan 独占的 slot。MySQL 的 spool 必须容纳完整快照，并发变化留在
binlog 中，发布后再从封口位置继续；`replication_client_id` 必须是唯一的非零值，binlog 必须覆盖快照
和追平期间。两个源当前都要求固定
Schema，不支持 TLS 或在线 DDL。更完整的 connector、快照和 Sink 恢复合同见
[`dogpaddle-operation`](../operation/README.md)。

### Streaming SQL v1

支持：

- `SELECT`、`WHERE`、字段别名、非递归 CTE 和派生查询；
- `CAST`、`TRY_CAST`、`CASE`，以及当前表达式层可绑定的比较、布尔和算术表达式；
- `SELECT DISTINCT`；
- `UNION ALL`；
- `JOIN` / `INNER JOIN ... ON`，条件必须是一个或多个跨左右输入的等值表达式；
- 非空 `GROUP BY`，以及 `COUNT`、`SUM`、`AVG`、`MIN`、`MAX`；只有分组字段而没有聚合调用也合法。

Join 的复合键只要一个分量为 `NULL` 就不匹配。v1 Join key 必须是两侧精确同类型、可 canonical 编码的
扁平非浮点值。`SUM/AVG` 参数必须绑定为 `Int64` 或 `UInt64`；分组字段不能包含 `Float32/Float64`。
`SELECT DISTINCT` 使用 DogPaddle 完整行 identity，浮点值按原始位模式区分。

在创建状态目录前会拒绝：

- 普通表、外连接、Cross/Natural/Using Join、非等值 Join、不能化为等值合取的剩余条件和普通 `UNION`；
- `SELECT ALL`、`DISTINCT ON`、Sort、Limit、Window 和 Values；
- 无分组的全局 Aggregate、grouping sets，以及聚合调用的 `DISTINCT`、`FILTER`、`ORDER BY` 和 null treatment；
- 递归 CTE、标量或相关子查询、UDF、时间函数、随机函数和 session variable；
- 没有明确 lowering 的其他 DataFusion LogicalPlan 节点。

SQL v1 有意只接受能准确映射到现有增量 Operation 的计划。具体类型和表达式矩阵以
[`dogpaddle-operation`](../operation/README.md#表达式边界) 为准。

## 一个更真实的例子

[`examples/fulfillment.sql`](examples/fulfillment.sql) 持续读取 PostgreSQL `sales.orders` 的 WAL，计算
订单金额和折扣，筛选已付款订单，通过 `UNION ALL` 分配履约中心，再写入 PostgreSQL
`ops.fulfillment_queue`。这个例子展示了三件事：

- 多段 CTE 只是 SQL 的可读结构；能安全串联的转换仍会融合进同一 Station；
- `UNION ALL` 和分叉会形成真实的持久边界；
- CDC 和 PostgreSQL Sink 的连接配置在 build/open 时注入，恢复进度在 Flow 状态目录中。

该程序的本机 PostgreSQL 崩溃恢复验收由根目录 `system-tests/postgres/check_sql.py` 执行。

## 从哪里开始读源码

1. [`src/program.rs`](src/program.rs)：`SqlProgram` 的 parse/read/build/open 生命周期。
2. [`src/lower.rs`](src/lower.rs)：DataFusion 规划，以及每种受支持节点如何变成 Operation。
3. [`src/compiler.rs`](src/compiler.rs)：临时 Operation DAG 和完整的 Station 融合规则。
4. [`src/endpoint.rs`](src/endpoint.rs)：endpoint 参数、环境变量和运行资源。
5. [`src/aggregate.rs`](src/aggregate.rs)：SQL 聚合名称到 Aggregate Operation 的唯一描述表。

继续追执行路径时，从 [`dogpaddle-flow`](../flow/README.md) 的 `Flow::advance` 开始；具体算子状态和
增量语义见 [`dogpaddle-operation`](../operation/README.md)。

## 验证

```sh
cargo test -p dogpaddle-sql --test correctness
cargo test -p dogpaddle-sql --doc
```

correctness suite 覆盖 quickstart 的 build/open、确定性 Flow Definition、CTE 分叉、Station 融合、
Join 后缀融合、Union、Distinct、Aggregate、参数错误和拒绝路径。真实 PostgreSQL CDC → SQL →
PostgreSQL Sink gate 及全工作区规则见 [`TESTING.md`](../../TESTING.md)。
