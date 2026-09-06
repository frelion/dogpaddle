# dogpaddle-sql

`dogpaddle-sql` 用一条 `INSERT INTO ... SELECT ...` 定义一条完整、可恢复的 `DogPaddle`
`Flow`。`Scan`、转换和 `Sink` 全部在 SQL 文件中声明；它不引入 `Table`、`View`、`Catalog`、独立服务或另一套运行时。

## 实时订单履约队列

仓库中的
[`fulfillment.sql`](https://github.com/frelion/dogpaddle/blob/main/crates/sql/examples/fulfillment.sql)
持续读取业务后端写入 `sales.orders` 的 WAL，并把可履约订单写回同一个 `PostgreSQL` 数据库中的
`ops.fulfillment_queue`。一条 SQL 完成四段业务转换：

- 将数量统一为 `BIGINT`，计算 `subtotal_cents = quantity × unit_price_cents`。
- 应用折扣，得到实际应付金额 `payable_cents`。
- 只保留已付款且应付金额不少于 10000 cents 的订单；达到 40000 cents 时进入 `priority` 通道。
- 用 `UNION ALL` 将 `cn-east` / `cn-south` 订单分配到 `CN-HUB`，其余地区分配到
  `GLOBAL-HUB`；高价值全球订单标记 `export_review`。

```text
business backend ── INSERT / UPDATE / DELETE ──▶ sales.orders
                                                       │ WAL
                                                       ▼
postgres_cdc ──▶ subtotal ──▶ discount ──▶ eligibility ──▶ routing
                                                            │
                                              CN-HUB / GLOBAL-HUB
                                                            │
                                                            ▼
                                                ops.fulfillment_queue
```

订单从 `new` 更新为 `paid` 后会进入队列；数量或折扣变化会实时重算金额和优先级；删除源订单会撤回
目标结果。[无声持续演示](https://github.com/frelion/dogpaddle/blob/main/docs/assets/fulfillment-continuous.mp4)
固定展示源表、完整 `fulfillment.sql` 文件和目标表，连续执行 28 次真实新增、修改和删除。
中间从读写端点到最后一行 SQL 全程可见，无省略或滚动。
付款状态、数量、价格、折扣与地区反复变化，可以持续观察筛选、金额重算、优先级和路由变化。
视频的呈现间隔经过编辑，每次完整结果均与 `PostgreSQL` 原生 SQL 核对。
可用
[`record_fulfillment_demo.sh`](https://github.com/frelion/dogpaddle/blob/main/docs/tools/record_fulfillment_demo.sh)
在本机真实 `PostgreSQL` 上重新生成：

```sh
docs/tools/record_fulfillment_demo.sh \
  --bundle /absolute/path/to/runtime-bundle \
  --postgres-bin /absolute/path/to/postgresql/bin
```

命令生成无声视频与封面，并将完整 SQL、原始时间戳和执行输出保存在 `target/demo/`。
环境要求和证据说明见
[录制说明](https://github.com/frelion/dogpaddle/blob/main/docs/demo/README.md)。

当前 `PostgreSQL` 试点从空源表和匹配的新 slot 起点开始，不执行已有数据的初始快照。
`PostgresSink` 创建并独占无损 Arrow 关系表，不镜像源表 DDL；文本值当前按 bytes 保存，查询时使用
`convert_from(...)`。

## Quickstart

仓库自带的
[`quickstart.sql`](https://github.com/frelion/dogpaddle/blob/main/crates/sql/examples/quickstart.sql)
从持续的 `sequence(...)` Scan 中选择偶数、计算平方并写入
`SQLite`：

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

在工作区根目录运行：

```sh
demo_dir="$(mktemp -d /tmp/dogpaddle-demo.XXXXXX)"
export DOGPADDLE_QUICKSTART_SQLITE="$demo_dir/results.sqlite"

cargo run --locked -q -p dogpaddle-sql --example quickstart -- \
  build crates/sql/examples/quickstart.sql "$demo_dir/flow" 12 0

sqlite3 -readonly -header -column "$DOGPADDLE_QUICKSTART_SQLITE" \
  'SELECT "$dogpaddle.id" AS id, number, square, size FROM even_squares ORDER BY id;'
```

第一次运行得到 `0, 2, 4, 6, 8`。进程已经正常退出；使用同一个 state 目录重新打开：

```sh
cargo run --locked -q -p dogpaddle-sql --example quickstart -- \
  open crates/sql/examples/quickstart.sql "$demo_dir/flow" 6 0
```

再次查询会看到新增的 `10`、`12`，已有行不重复。示例的最后两个参数分别是推进轮数和每轮延迟毫秒数。
`sequence` 是持续 Scan，所以
[`quickstart.rs`](https://github.com/frelion/dogpaddle/blob/main/crates/sql/examples/quickstart.rs)
有意只执行有限轮；真实宿主同样通过 `Flow::advance` 控制运行节奏。

## 嵌入 Rust

产品 API 只有一个程序对象和四个生命周期入口：

```no_run
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

| API | 行为 |
| --- | --- |
| `SqlProgram::parse(sql)` | 纯解析一段 UTF-8 SQL；不访问文件、网络或环境变量 |
| `SqlProgram::read(path)` | 读取并解析一个 UTF-8 SQL 文件 |
| `SqlProgram::build(state_path)` | 解析端点参数、发现外部 `Schema`/目标、分析 SQL、lower 为 `Operation` DAG 并创建 `Flow` |
| `SqlProgram::open(state_path)` | 从 state 目录恢复 canonical `Flow`，并注入 SQL 中声明的运行连接资源 |

`open` 不重新编译或替换已持久化拓扑。请用构建时的同一份 SQL 恢复；修改查询或端点身份后，使用新的
state 目录，并为独占 Sink 使用新的目标。

## SQL 合同

一个文件只包含一条完整流水线语句：

```text
INSERT INTO sink(name => value, ...)
WITH ...
SELECT ...
```

- 支持 SQL 注释、非递归 CTE、派生查询和末尾分号；拒绝多条语句。
- 端点只接受具名参数 `name => value`。
- 参数值只接受单引号字符串、非负整数或 `env('NAME')`。
- 位置参数、重复参数、未知参数和缺失参数都会报错。
- 环境变量在 `build/open` 时解析；错误不会显示解析后的秘密。

V1 端点：

| 类型 | SQL 函数 | 必需参数 |
| --- | --- | --- |
| Scan | `sequence` | `start` |
| Scan | `postgres_cdc` | `engine_name`, `runtime_bundle`, `host`, `port`, `database`, `user`, `password`, `schema`, `table`, `slot`, `publication` |
| Sink | `sqlite` | `path`, `table` |
| Sink | `postgres` | `sink_id`, `host`, `port`, `database`, `user`, `password`, `schema`, `table` |
| Sink | `discard` | 无 |

## Streaming SQL v1

支持：

- `SELECT`、`WHERE` 和字段别名
- `SELECT DISTINCT`，按完整输出记录的 `DogPaddle` exact-row identity 去重
- 非递归 CTE 与派生查询
- `CAST`、`TRY_CAST`、`CASE`
- 现有表达式执行层可接受的比较、布尔与算术表达式
- `UNION ALL`

在创建 state 目录前明确拒绝：

- 普通表、Join、Aggregate 和普通 `UNION`
- `SELECT ALL`、`DISTINCT ON`、Sort、Limit、Window、Values
- 递归 CTE、标量子查询和相关子查询
- UDF、时间、随机数、session variable
- 依赖 `DataFusion` function registry 的 scalar、aggregate 或 window 函数
- 任何没有显式 lowering 的 `LogicalPlan` 节点

`DataFusion` 负责 SQL 解析、名称解析和 type coercion；分析得到的显式表达式与精确 `Schema` 被 lower
为现有 `Filter`、`SchemaAlign`、`UnionAll` 和 `Distinct` Operation。`DataFusion` 不执行 `Flow`，
`DogPaddle` 也不维护第二套表达式 AST 或 SQL 执行引擎。

全行去重沿用 `DogPaddle` 的 exact-row identity：null 使用 canonical 表示，浮点值按原始位模式区分，
不应用外部数据库的 collation。

## 持久化与测试

SQL 文本和 `DataFusion` `LogicalPlan` 不写入磁盘。state 目录中的 canonical `Flow Definition` 是 reopen
的唯一拓扑真相，`Operation Definition`、游标和状态继续沿用 `Flow` 的稳定编码与事务协议。

离线公共测试：

```sh
cargo test -p dogpaddle-sql --test correctness
```

测试直接编译并执行随 crate 发布的 `examples/quickstart.sql`，覆盖 build、SQLite 结果、drop/open
和无重复恢复；完整 matrix 还覆盖参数错误、隐式 coercion、CTE fan-out、`UNION ALL` Schema、
`SELECT DISTINCT` 的最终结果与状态恢复，以及拒绝路径。
真实 `PostgreSQL` CDC → SQL transforms → `PostgreSQL Sink` 的崩溃恢复 gate 位于
[`system-tests/postgres/check_sql.py`](https://github.com/frelion/dogpaddle/blob/main/system-tests/postgres/check_sql.py)。
工作区统一 gate 和数据规格见
[`TESTING.md`](https://github.com/frelion/dogpaddle/blob/main/TESTING.md)。
