# DogPaddle

**用一份 SQL 持续处理数据库变化，并把结果写回数据库。**

DogPaddle 是一个本地运行、持久化进度的流处理引擎。SQL 描述数据入口、计算和结果表；同一条命令负责首次创建状态，也负责进程重启后的恢复。

```text
PostgreSQL / MySQL 变化  →  SQL 筛选、计算、聚合、Join  →  PostgreSQL / SQLite
```

项目仍处于早期开发阶段，PostgreSQL 与 MySQL 接入均为固定 Schema 的试点能力。

[快速上手](#快速上手) · [SQL 入口](#sql-入口) · [当前能力](#当前能力) · [架构](#架构) · [文档](#文档)

[![CI](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml)
[![PostgreSQL 恢复测试](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml)

## 快速上手

从 [GitHub Releases](https://github.com/frelion/dogpaddle/releases) 下载与你的平台匹配的压缩包：

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

压缩包同时包含 DogPaddle 和固定版本的 Debezium/JRE runtime。解压后可直接运行，不需要安装 Rust 或系统 Java：

```sh
tar -xzf dogpaddle-v0.1.0-aarch64-apple-darwin.tar.gz
./dogpaddle-v0.1.0-aarch64-apple-darwin/bin/dogpaddle run orders.sql
```

发布页同时提供每个压缩包的 `.sha256` 文件。

仓库中的 [quickstart.sql](crates/sql/examples/quickstart.sql) 会持续生成递增数字，筛选偶数并写入 SQLite。它不需要 PostgreSQL 或 Java。

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

准备仓库指定的 Rust 1.96，然后运行：

```sh
git clone https://github.com/frelion/dogpaddle.git
cd dogpaddle

demo_dir="$(mktemp -d /tmp/dogpaddle-demo.XXXXXX)"
export DOGPADDLE_QUICKSTART_SQLITE="$demo_dir/results.sqlite"

cargo run --locked -p dogpaddle -- \
  run crates/sql/examples/quickstart.sql --state "$demo_dir/state"
```

`sequence` 是持续数据源，按 `Ctrl-C` 后 DogPaddle 会在当前有界调度轮结束时退出。随后查看结果：

```sh
sqlite3 -readonly -header -column "$DOGPADDLE_QUICKSTART_SQLITE" \
  'SELECT number, square, size FROM even_squares ORDER BY number LIMIT 10;'
```

再次执行完全相同的 `dogpaddle run` 命令会从已提交的位置继续。状态损坏、不完整、正在被另一进程占用，或 SQL 与该状态不匹配时，命令会报错；它不会删除或偷偷重建已有状态。

## SQL 入口

产品命令只有一个：

```text
dogpaddle run SQL_FILE [--state DIR]
```

省略 `--state` 时，状态目录是 SQL 文件旁的 `.dogpaddle/<文件名去掉扩展名>`。命令启动后会打印规范化后的状态路径。生产环境建议显式指定状态目录，例如：

```sh
dogpaddle run /etc/dogpaddle/orders.sql --state /var/lib/dogpaddle/orders
```

数据库密码放在进程环境变量中，SQL 只保存环境变量名：

```sql
INSERT INTO postgres(
    connection => env('TARGET_DATABASE_URL'),
    table => 'ops.current_orders'
)
SELECT *
FROM postgres_cdc(
    connection => env('SOURCE_DATABASE_URL'),
    table => 'sales.orders',
    publication => 'orders_publication'
);
```

连接值是无 TLS 的数据库 URL，不能带 query 或 fragment：

```text
postgresql://user:password@127.0.0.1:5432/database
mysql://user:password@127.0.0.1:3306/database
```

DogPaddle 从 SQL 身份、状态路径和端点位置确定性派生 connector、PostgreSQL slot、Sink ownership 和 MySQL replication client ID，用户无需配置这些内部标识。CDC runtime 随产品安装在 `<安装根>/libexec/dogpaddle/debezium`。

Rust 应用也可以直接控制每一轮调度：

```rust,no_run
use dogpaddle_sql::SqlProgram;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let program = SqlProgram::read("orders.sql")?;
let mut flow = program.start("/var/lib/dogpaddle/orders")?;

loop {
    let outcome = flow.advance()?;
    // 宿主根据 outcome 决定立即继续、等待或停止。
    # let _ = outcome;
    # break;
}
# Ok(())
}
```

SQL 层的公共生命周期只有 `SqlProgram::parse`、`SqlProgram::read` 和 `SqlProgram::start`。需要直接装配 Operation 的底层用户仍可使用 `FlowFactory::build/open`。

## 当前能力

| 需求 | 当前支持 |
| --- | --- |
| 读取数据 | `sequence`；PostgreSQL WAL / MySQL binlog 单表 CDC 与一致初始快照 |
| 筛选和计算 | `SELECT`、`WHERE`、别名、算术与布尔表达式、`CASE`、`CAST`、`TRY_CAST` |
| 查询结构 | 非递归 CTE、派生查询、`SELECT DISTINCT`、`UNION ALL` |
| Join | Inner、Left/Right/Full Outer、Left/Right Semi、Left/Right Anti 等值连接 |
| 聚合 | 非空 `GROUP BY`；`COUNT`、`SUM`、`AVG`、`MIN`、`MAX`；纯分组 |
| 输出 | SQLite、PostgreSQL、Discard |
| 恢复 | 本地持久化拓扑、订阅位置与算子状态；相同 SQL 自动恢复 |

主要边界：

- 每个文件只接受一条 `INSERT INTO sink(...) <query>`。不支持交互式查询、Sort、Limit、Window、普通 `UNION`、`DISTINCT ON` 或无分组全局聚合。
- Join 至少包含一个跨输入等值 key。Inner Join 可附加 residual 条件；Outer、Semi、Anti 只接受等值合取。Cross、Natural、Using 和纯非等值 Join 尚未实现。
- PostgreSQL / MySQL CDC 使用固定 Schema。PostgreSQL 要求预先配置 publication 和 `REPLICA IDENTITY FULL`；MySQL binlog 必须覆盖快照、spool 发布和追平阶段。两者暂不支持 TLS、在线 DDL 或多表路由。
- `bootstrap_spool_bytes` 可选，默认 1 GiB。它必须能容纳完整快照；PostgreSQL 还要容纳快照封口前的 WAL 重叠。
- SQLite / PostgreSQL 结果表由 DogPaddle 独占，必须是新目标，不能与其他写入者共享。
- v1 持久格式不提供迁移或兼容层。修改 SQL 语义、端点身份或装配规则后，应使用新状态目录和新目标。

## 架构

```text
SQL / dogpaddle run
        ↓ parse · plan · assemble
Flow：静态 DAG、Station、调度、事务边界
        ↓
Operation：Scan / Transform / Sink 的状态与执行语义
        ↓                         ↓
Change：Arrow 差分批次          Store：RocksDB 事务与持久集合
```

SQL 编译器会把一条直线上的单输入原子算子装入同一个 Station，使它们在一笔事务中执行并省去中间持久队列。分叉、多输入边界、要求独占的算子和 Sink 会形成新的 Station。分组是确定性的结构规则，不引入成本优化器或第二套执行引擎。

## 文档

- [SQL、端点与智能装配](crates/sql/README.md)
- [Flow 的十分钟运行骨架](crates/flow/README.md)
- [Operation 的构建与执行](crates/operation/README.md)
- [Change 数据模型](crates/change/README.md)
- [Store 事务状态](crates/store/README.md)
- [Debezium 拉取与恢复](crates/debezium/README.md)
- [构建、测试与性能验证](TESTING.md)
- [算子路线图](OPERATOR_ROADMAP.md)
- [Debezium 接入路线图](DEBEZIUM_ROADMAP.md)
