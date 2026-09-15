# DogPaddle

**一条 SQL，持续维护数据库中的结果。**

```text
PostgreSQL / MySQL  ── 一致快照 + CDC ──▶  SQL  ──▶  PostgreSQL / SQLite / ClickHouse / Doris
```

DogPaddle 是一个本地运行、可恢复的增量 SQL 引擎。它先读取源表的一致快照，再持续消费 WAL 或 binlog；源记录被新增、修改或删除时，筛选、Join、聚合和时间匹配产生的结果随之改变。进程重启后，从已提交的位置继续。

下面完整展开成交分析与门店经营两个场景：先介绍业务、源表与 SQL，再用动图展示源数据变化如何影响结果。SQL 启动后持续运行，演示中的后续操作只修改源表。完整建表语句、连接配置与逐步操作都保存在 [`examples/`](examples/)，可以使用 release 包中的 `dogpaddle` 在本地复现；其他业务示例见后文的「更多可运行场景」。

[![CI](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml)
[![PostgreSQL 恢复测试](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml)

## 成交分析：历史报价变化后，重新匹配已有成交

分析成交价格时，需要知道成交发生时最近的市场报价，再将成交价与买卖报价的中间价比较。成交和报价分别写入数据库，历史报价还可能被补录、修正或撤回，因此已生成的分析结果也需要更新。

### 两张源表

两表都在 PostgreSQL 中。`market.fills` 以 `trade_id BIGINT` 为主键，其他字段为非空的 `symbol TEXT`、`executed_at TIMESTAMP`、`side TEXT` 和 `price_cents BIGINT`：

| trade_id | symbol | executed_at | side | price_cents |
| --- | --- | --- | --- | --- |
| 9001 | ACME | 09:30:00.500 | BUY | 10018 |

`market.quotes` 以 `quote_id BIGINT` 为主键，其他字段为非空的 `symbol TEXT`、`quoted_at TIMESTAMP`、`bid_cents BIGINT` 和 `ask_cents BIGINT`：

| quote_id | symbol | quoted_at | bid_cents | ask_cents |
| --- | --- | --- | --- | --- |
| 1 | ACME | 09:30:00.100 | 10000 | 10020 |

表中的时间均为 `2026-09-15`，这里只显示时分秒。价格以整数分存储；两个时间字段表示成交、报价的业务发生时间。初始报价的中间价为 `10010` 分，这笔买入成交比它高 `8` 分。

### 实时 SQL

`ON` 按证券代码分组，`ASOF JOIN` 为每笔成交选择**不晚于成交时刻的最近报价**。`arrival_mid_cents` 计算买卖报价的中间价；买入时用成交价减中间价，卖出时反向相减，得到本例的 `slippage_cents` 价格偏差。结果写入 `analytics.execution_quality`，保留成交与匹配报价的时间，便于核对。

没有符合条件的报价时，成交仍保留，报价及依赖报价的计算列为空。本例用可整除的报价演示中间价计算，SQL 中的价格运算保持整数分单位。

```sql
INSERT INTO postgres(
    connection => env('DOGPADDLE_EXECUTION_QUALITY_TARGET'),
    table => 'analytics.execution_quality'
)
SELECT
    fills.trade_id,
    fills.symbol,
    fills.executed_at,
    fills.side,
    fills.price_cents AS execution_price_cents,
    quotes.quoted_at,
    (quotes.bid_cents + quotes.ask_cents) / 2 AS arrival_mid_cents,
    CASE
        WHEN fills.side = 'BUY'
            THEN fills.price_cents - (quotes.bid_cents + quotes.ask_cents) / 2
        ELSE (quotes.bid_cents + quotes.ask_cents) / 2 - fills.price_cents
    END AS slippage_cents
FROM postgres_cdc(
    connection => env('DOGPADDLE_FILLS_SOURCE'),
    table => 'market.fills',
    publication => 'dogpaddle_fills'
) AS fills
ASOF JOIN postgres_cdc(
    connection => env('DOGPADDLE_QUOTES_SOURCE'),
    table => 'market.quotes',
    publication => 'dogpaddle_quotes'
) AS quotes
MATCH_CONDITION (fills.executed_at >= quotes.quoted_at)
ON fills.symbol = quotes.symbol;
```

### 看同一笔成交如何重新匹配

动图先展示完整 SQL 并启动 DogPaddle，随后左栏显示成交和报价，右栏显示目标结果。成交 `9001` 始终不变，依次观察三次报价变更：

1. 补录 `09:30:00.400` 的报价，中间价为 `10012`。它更接近成交时间，右侧改配到这条报价，偏差从 `8` 变为 `6`。
2. 将这条报价的买卖价修正为 `10012 / 10016`。匹配时间不变，中间价变为 `10014`，偏差变为 `4`。
3. 删除这条报价。成交重新匹配 `09:30:00.100` 的原报价，偏差回到 `8`。

![补录、修正和删除历史报价，使已有成交的匹配报价与价格偏差持续变化](docs/assets/readme-trade-asof.gif)

[本地复现](examples/trade-quote-asof/) · [建表与初始数据](examples/trade-quote-asof/setup.sql) · [完整 SQL](examples/trade-quote-asof/pipeline.sql) · [逐步变更](examples/trade-quote-asof/steps/) · [终端录制](docs/assets/readme-trade-quote-asof.cast)

## 门店经营：改价和退款后，修正销售汇总

门店看板需要显示当前已付款订单的数量、金额合计、平均金额和最小／最大金额。订单会从待付款变为已付款，也可能改价或退款；本例约定退款订单不再计入这份汇总。

### 源表与初始数据

PostgreSQL 的 `sales.store_orders` 以 `order_id BIGINT` 为主键，另外包含非空的 `store_id BIGINT`、`status TEXT` 和 `amount_cents BIGINT`。金额以分存储：

| order_id | store_id | status | amount_cents |
| --- | --- | --- | --- |
| 3001 | 11 | paid | 12000 |
| 3002 | 11 | paid | 18000 |
| 3003 | 12 | pending | 25000 |

### 实时 SQL

先用 `WHERE status = 'paid'` 筛选订单，再按门店分组。`COUNT/SUM/AVG/MIN/MAX` 分别计算订单数、金额合计、平均值和两端值，写入 `analytics.store_sales`。每个仍有已付款订单的门店对应一条当前汇总。

初始时，门店 `11` 有两笔订单：合计 `30000` 分，平均 `15000` 分，最小 `12000` 分，最大 `18000` 分。门店 `12` 的订单尚未付款，因此没有汇总行。

```sql
INSERT INTO postgres(
    connection => env('DOGPADDLE_STORE_SALES_TARGET'),
    table => 'analytics.store_sales'
)
SELECT
    store_id,
    COUNT(*) AS order_count,
    SUM(amount_cents) AS revenue_cents,
    AVG(amount_cents) AS average_order_cents,
    MIN(amount_cents) AS smallest_order_cents,
    MAX(amount_cents) AS largest_order_cents
FROM postgres_cdc(
    connection => env('DOGPADDLE_STORE_SALES_SOURCE'),
    table => 'sales.store_orders',
    publication => 'dogpaddle_store_sales'
)
WHERE status = 'paid'
GROUP BY store_id;
```

### 看改价和退款如何修正汇总

动图先展示 SQL 并启动 DogPaddle，随后左栏显示订单，右栏显示门店汇总。右栏为便于阅读，展示订单数、合计、最小值和最大值；平均值也由 SQL 写入目标表。

1. 订单 `3003` 付款，门店 `12` 出现一条汇总。
2. 订单 `3002` 从 `18000` 分改为 `22000` 分，门店 `11` 的合计从 `30000` 变为 `34000`，最大值也变为 `22000`。
3. 订单 `3003` 退款，门店 `12` 的汇总消失。
4. 门店 `11` 的剩余订单退款，目标表变为空。最后一笔已付款订单退出后，该分组不再出现在查询结果中。

![订单付款创建门店汇总，改价更新合计与最大值，退款撤回贡献并移除空分组](docs/assets/readme-store-sales.gif)

[本地复现](examples/store-sales-summary/) · [建表与初始数据](examples/store-sales-summary/setup.sql) · [完整 SQL](examples/store-sales-summary/pipeline.sql) · [逐步变更](examples/store-sales-summary/steps/) · [终端录制](docs/assets/readme-store-sales-summary.cast)

## 更多可运行场景

下面的 examples 同样保留建表、完整 SQL 和逐步变更脚本：

| 场景 | 源表与计算逻辑 | 可以观察的变化 |
| --- | --- | --- |
| [业务事件同步](examples/event-sync/) | PostgreSQL 事件表原样写入仓库表 | 新增、修正和删除反映到目标副本 |
| [订单履约](examples/order-etl/) | 筛选已付款订单，按数量与单价计算金额，再按金额分配处理通道 | 付款后进入结果、改量后重算、取消后撤回 |
| [客户订单关联](examples/customer-order-enrichment/) | PostgreSQL 订单与 MySQL 客户按客户 ID 连接 | 已付款订单关联客户当前等级，相关资料变化后更新结果 |
| [支付对账](examples/payment-reconciliation/) | PostgreSQL 业务支付与 MySQL 渠道结算做全外连接 | 结算迟到、金额不符和修正后的对账状态变化 |
| [完整订单履约](examples/order-fulfillment/) | 付款筛选、折扣计算、地区路由与 `UNION ALL` | 订单变更，以及进程中断后的恢复验收 |

## 中断后继续

DogPaddle 把输入位置、拓扑和算子状态保存在本地 state 目录。重新执行同一条命令会打开已有状态；状态损坏、不完整、被其他进程占用或属于另一份 SQL 时会报错，不会删除或重新构建已有状态。

[订单履约演示](docs/demo/)包含一次真实的进程终止和恢复，恢复后继续处理订单的新增、修改和删除。演示使用的仍是仓库中的[完整 pipeline.sql](examples/order-fulfillment/pipeline.sql)。

## 快速开始

不连接外部数据库的 quickstart 使用内置 `sequence` 数据源，筛选偶数并持续写入 SQLite。源码构建需要 Rust 1.96：

```sh
git clone https://github.com/frelion/dogpaddle.git
cd dogpaddle

example_dir="$(mktemp -d /tmp/dogpaddle-quickstart.XXXXXX)"
export DOGPADDLE_QUICKSTART_SQLITE="$example_dir/results.sqlite"

cargo run --locked -p dogpaddle -- \
  run crates/sql/examples/quickstart.sql --state "$example_dir/state"
```

按 `Ctrl-C` 后查看结果：

```sh
sqlite3 -readonly -header -column "$DOGPADDLE_QUICKSTART_SQLITE" \
  'SELECT number, square, size FROM even_squares ORDER BY number LIMIT 10;'
```

再次执行相同的 `dogpaddle run` 命令会从已提交位置继续。

生产发行包可以从 [GitHub Releases](https://github.com/frelion/dogpaddle/releases) 下载；其中包含固定版本的 Debezium 和 JRE，无需安装 Rust 或系统 Java，也不执行系统安装。解压 `.tar.gz` 后直接运行其中的 `bin/dogpaddle`，并保留完整目录，使它能从同一安装根加载 `libexec/dogpaddle/debezium`。安装目录可以只读，运行状态写入 `--state` 指定的目录。

原生发行包当前覆盖 `x86_64`、`aarch64` 的 GNU/Linux 与 macOS。Linux 以 glibc 2.28 为最低用户态 ABI，并静态链接 DogPaddle 使用的 C++ compiler runtime；macOS 以 macOS 11.0 为最低 deployment target。每个 archive 都在发布前检查 OS ABI、包内动态库引用和 CPU 架构，并在没有系统 Java、空 `PATH`、只读安装目录下完成首次构建与 reopen smoke。JRE 中未被 DogPaddle headless 路径加载的桌面模块可能仍声明 X11 或 ALSA 动态库；它们记录在随 release 发布的 `compatibility.txt` 中，不属于 DogPaddle 运行路径。

macOS archive 不做 Developer ID 签名或 Apple notarization。若 Gatekeeper 阻止从互联网下载的 executable，用户需要在系统设置中手动允许运行。

产品命令只有：

```text
dogpaddle run SQL_FILE [--state DIR]
```

## 当前能力与边界

| 维度 | 当前支持 |
| --- | --- |
| 数据源 | PostgreSQL CDC、MySQL CDC；一致初始快照与后续 WAL/binlog |
| SQL | `SELECT`、`WHERE`、表达式、普通 `JOIN`、动态 `ASOF JOIN`、`GROUP BY`、`DISTINCT`、`UNION ALL` |
| 普通 Join | Inner、Left/Right/Full Outer、Left/Right Semi、Left/Right Anti，以及 residual 条件 |
| 聚合 | 非空分组上的 `COUNT`、`SUM`、`AVG`、`MIN`、`MAX` |
| 目标端 | PostgreSQL、SQLite、ClickHouse、Doris |
| 恢复 | 本地持久化输入位置、拓扑和算子状态 |

DogPaddle 仍处于早期开发阶段。当前每个 SQL 文件只接受一条 `INSERT INTO sink(...) <query>`；CDC 读取单表并要求固定 Schema，暂不支持 TLS、在线 DDL 或多表路由。数据库目标关系由 DogPaddle 独占。当前 v1 状态格式不提供迁移；修改查询语义或持久 endpoint identity 时应使用新的 state 路径。完整约束见 [SQL 文档](crates/sql/README.md)。

## 文档

- [全部可运行 examples](examples/)
- [SQL、端点与运行方式](crates/sql/README.md)
- [完整订单履约与恢复演示](docs/demo/)
- [构建、测试与性能验证](TESTING.md)
- [底层 Flow API](crates/flow/README.md)
