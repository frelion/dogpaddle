# DogPaddle

**一份 SQL，把同一个 PostgreSQL 里的订单实时变成履约队列。**

业务后端照常写 `sales.orders`。DogPaddle 读取 WAL，计算应付金额、筛选可履约订单、划分优先级并
分配履约中心，再把 `ops.fulfillment_queue` 写回同一个数据库。业务代码无需双写。

![DogPaddle 将 PostgreSQL 订单实时转换为履约队列](docs/assets/fulfillment-hero.png)

**计价**　数量 × 单价并应用折扣　→　**准入**　`paid` 且应付金额 ≥ `$100`　→　
**分级**　应付金额 ≥ `$400` 为 `priority`　→　**路由**　`cn-east / cn-south` 到 `CN-HUB`，
其他地区到 `GLOBAL-HUB`

https://github.com/user-attachments/assets/2b348985-6795-41dd-8c34-f302994cb385

*30 秒真实 PostgreSQL 演示：`INSERT` 让订单进入队列，`UPDATE` 重算金额与优先级，`DELETE`
撤回派生结果；目标写入后强杀宿主，再从同一 state 恢复。画面中的每张表都来自系统验收的实时查询；
这次恢复没有重复结果，后续订单继续处理。*

[查看完整 SQL](crates/sql/examples/fulfillment.sql) · [运行本地 Quickstart](#快速上手) ·
[复现真实演示](#复现真实演示)

> [!NOTE]
> PostgreSQL 接入目前是早期试点：从空源表和新的 replication slot 开始，不包含已有数据的初始快照；
> 生产加固仍在进行。

[![CI](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml)
[![Debezium PostgreSQL recovery](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml)

## 快速上手

准备仓库固定的 Rust 1.96 和 `sqlite3` 命令行，然后在仓库根目录运行：

```sh
demo_dir="$(mktemp -d /tmp/dogpaddle-demo.XXXXXX)"
export DOGPADDLE_QUICKSTART_SQLITE="$demo_dir/results.sqlite"

cargo run --locked -q -p dogpaddle-sql --example quickstart -- \
  build crates/sql/examples/quickstart.sql "$demo_dir/flow" 12 0

sqlite3 -readonly -header -column "$DOGPADDLE_QUICKSTART_SQLITE" \
  'SELECT "$dogpaddle.id" AS id, number, square, size FROM even_squares ORDER BY id;'
```

第一次运行会得到 `0, 2, 4, 6, 8`。现在用同一份 SQL 和同一个 state 目录恢复：

```sh
cargo run --locked -q -p dogpaddle-sql --example quickstart -- \
  open crates/sql/examples/quickstart.sql "$demo_dir/flow" 6 0

sqlite3 -readonly -header -column "$DOGPADDLE_QUICKSTART_SQLITE" \
  'SELECT "$dogpaddle.id" AS id, number, square, size FROM even_squares ORDER BY id;'
```

结果会继续增加 `10` 和 `12`。`sequence` 是持续 Scan；quickstart 宿主按参数执行有限轮
`Flow::advance` 后主动退出，以便直接看到恢复行为。实际应用自行决定推进频率和停止时机。

[查看 quickstart SQL](crates/sql/examples/quickstart.sql) ·
[查看 quickstart 宿主](crates/sql/examples/quickstart.rs)

## 一个文件就是一条完整 Flow

quickstart 使用的文件没有 Pipeline DDL、Table、View、Catalog 或旁路配置：

```sql
-- One statement defines Scan -> Transform -> Sink.
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

这条语句直接形成：

```text
sequence(...)  →  CTE / CAST / WHERE / CASE  →  sqlite(...)
     Scan                    Transform                 Sink
```

`build` 先完成 DataFusion 分析、全图 Schema binding 和拓扑校验，再创建 canonical Flow。
`open` 从磁盘恢复已经提交的 Definition、进度和算子状态，并注入当前运行需要的连接资源。
凭据可以通过 `env('NAME')` 读取，错误不会打印解析后的秘密。

## 为什么是 DogPaddle

- **SQL 直接描述数据去向**：一个文件覆盖 Scan、转换和 Sink，Rust 宿主只负责生命周期。
- **恢复是 Flow 的基本语义**：Flow Definition 持久保存；游标与算子进度按提交边界恢复，外部 Sink
  通过可重放状态收敛。
- **先验证，再落盘**：SQL、DAG 和 Arrow Schema 全部通过后才创建 state 目录。
- **Arrow + DataFusion**：变化以 Arrow 批次流动，表达式使用 DataFusion 的类型与向量执行语义。
- **运行节奏属于应用**：一次 `Flow::advance` 只做有界工作，慢消费者通过持久输出形成软背压。

Rust 中的宿主接口保持很小：

```rust
use dogpaddle_sql::SqlProgram;

let program = SqlProgram::read("flow.sql")?;
let mut flow = program.build("./flow-state")?;
flow.advance()?;

drop(flow);
let mut flow = program.open("./flow-state")?;
flow.advance()?;
```

## 当前能力

| 层 | 已实现 |
| --- | --- |
| SQL Scan | `sequence(...)`、`postgres_cdc(...)`（试点） |
| SQL Transform | `SELECT`、`WHERE`、字段别名、非递归 CTE、派生查询、`CAST`、`TRY_CAST`、`CASE`、`UNION ALL` |
| SQL Sink | `sqlite(...)`、`postgres(...)`（试点）、`discard()` |
| 运行与恢复 | canonical Flow Definition、持久游标、算子状态、软背压、`Flow::status`、build/open/reopen |
| 数据模型 | Arrow Schema、批量差分 Change、完整自描述 Arrow IPC Stream |

## 当前边界

- DogPaddle 仍是早期引擎内核，持久化和恢复已有系统验收，生产加固仍在进行。
- SQL v1 是明确受限的 streaming SQL 子集。Join、Aggregate、普通 `UNION`、Distinct、Sort、Limit、
  Window、表达式子查询和依赖函数 registry 的函数会在创建 state 目录前被拒绝。
- 一个 SQL 文件只接受一条直接写入一个 Sink 的 `INSERT ... SELECT`；没有 DDL、Catalog、查询结果返回
  或自动运行循环。
- `open` 以磁盘中的 canonical Flow Definition 为准。修改 SQL 不会热更新已有 Flow；拓扑变更需要新的
  state 目录，并为独占 Sink 使用新的目标。
- 一个 state 路径同一时刻只允许一个活动 Flow。SQLite 和 PostgreSQL Sink 都独占自己创建的目标表。
- PostgreSQL 试点必须从空源表和匹配的新 slot 起点开始；它没有初始全量，不能直接接管已有数据的
  非空业务表。目前也没有多表路由、TLS、DNS endpoint、在线 Schema evolution 或跨 Flow fencing。
- `PostgresSink` 创建并独占无损 Arrow 关系表，不镜像源表 DDL；当前文本值按 bytes 保存，所以示例
  查询使用 `convert_from(...)`。完整约束见 [Operation 文档](crates/operation/README.md)。
- 当前持久格式经过 golden 与 reopen 测试，但开发期 v1 不提供跨版本迁移承诺。

## 复现真实演示

准备 `cargo`、Python 3.9+、`uv`、本机 PostgreSQL 可执行文件和已经构建的
[固定版本 Debezium runtime bundle](crates/debezium/README.md#runtime-bundle)，然后运行：

```sh
docs/tools/record_fulfillment_demo.sh \
  --bundle /absolute/path/to/runtime-bundle \
  --postgres-bin /absolute/path/to/postgresql/bin
```

该命令先运行[真实 PostgreSQL 系统验收](system-tests/postgres/check_sql.py)，捕获八个 source/target
快照；全部通过后才生成 README 海报和 `target/demo/fulfillment-demo.mp4`。渲染依赖由 `uv` 按固定版本
安装，不进入 Rust 产品依赖；Linux 还需要 Chromium 的系统运行库。

## 深入阅读

- [SQL：语法、生命周期与可运行 quickstart](crates/sql/README.md)
- [Flow：构建、运行与恢复](crates/flow/README.md)
- [Operation：算子、Schema 绑定与外部端点](crates/operation/README.md)
- [Change：Arrow 差分与 IPC](crates/change/README.md)
- [Store：MDBX 事务与集合](crates/store/README.md)
- [Debezium：自包含进程内 Engine 与 pre-ACK checkpoint](crates/debezium/README.md)
- [算子路线与语义边界](OPERATOR_ROADMAP.md)
- [Debezium Scan D0–D7 路线图](DEBEZIUM_ROADMAP.md)
- [正确性、系统验收与性能测试](TESTING.md)
