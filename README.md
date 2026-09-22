# DogPaddle

**一条 SQL，持续维护数据库中的结果。**

```text
PostgreSQL / MySQL  ── 一致快照 + CDC ──▶  SQL  ──▶  PostgreSQL / SQLite / ClickHouse / Doris
```

DogPaddle 是一个本地运行、可恢复的增量 SQL 引擎。从 PostgreSQL 或 MySQL 读取一致快照后，持续消费 WAL / binlog，把源表的新增、修改和删除反映到查询结果中。

[![CI](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/ci.yml)
[![PostgreSQL 恢复测试](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml/badge.svg)](https://github.com/frelion/dogpaddle/actions/workflows/debezium-postgres.yml)

## 成交分析：随历史报价修正结果

为每笔成交匹配当时最近的报价，计算成交价与报价中间价的偏差。历史报价被补录、修正或删除时，已有成交的分析结果随之更新。

### 两张源表

PostgreSQL 成交表 `market.fills`：

| trade_id | symbol | executed_at | side | price_cents |
| --- | --- | --- | --- | --- |
| 9001 | ACME | 09:30:00.500 | BUY | 10018 |

报价表 `market.quotes`：

| quote_id | symbol | quoted_at | bid_cents | ask_cents |
| --- | --- | --- | --- | --- |
| 1 | ACME | 09:30:00.100 | 10000 | 10020 |

时间均为 `2026-09-15`，价格以整数分存储。初始报价中间价为 `10010` 分，买入成交价比它高 `8` 分。

### 实时 SQL

`ASOF JOIN` 按证券代码匹配不晚于成交时刻的最近报价；无匹配时保留成交，报价相关列为空。价格偏差在买入时为成交价减中间价，卖出时反向计算；本例使用整数除法。

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

成交 `9001` 不变：补录更近的报价，偏差从 `8` 变为 `6`；修正报价后变为 `4`；删除后重新匹配原报价，回到 `8`。动图左侧是源表，右侧是结果。

![补录、修正和删除历史报价，使已有成交的匹配报价与价格偏差持续变化](docs/assets/readme-trade-asof.gif)

[本地复现](examples/trade-quote-asof/) · [完整 SQL](examples/trade-quote-asof/pipeline.sql)

## 门店经营：随订单变化更新汇总

按门店汇总已付款订单的数量、总额、均值和最小／最大金额。付款和改价更新汇总，退款撤回贡献。

### 源表与初始数据

PostgreSQL 订单表 `sales.store_orders`，金额以分存储：

| order_id | store_id | status | amount_cents |
| --- | --- | --- | --- |
| 3001 | 11 | paid | 12000 |
| 3002 | 11 | paid | 18000 |
| 3003 | 12 | pending | 25000 |

### 实时 SQL

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

动图左侧是订单，右侧是汇总：付款创建分组，改价更新合计与最大值，退款撤回贡献。最后一笔已付款订单退出后，对应门店的汇总行消失。均值也写入目标表，动图中未展示。

![订单付款创建门店汇总，改价更新合计与最大值，退款撤回贡献并移除空分组](docs/assets/readme-store-sales.gif)

[本地复现](examples/store-sales-summary/) · [完整 SQL](examples/store-sales-summary/pipeline.sql)

## 更多可运行场景

[examples](examples/) 提供七个场景的 Docker／Podman Compose 环境、SQL 和逐步变更脚本，可配合发行包在本地复现。

| 场景 | 源表与计算逻辑 | 可以观察的变化 |
| --- | --- | --- |
| [业务事件同步](examples/event-sync/) | PostgreSQL 事件表原样写入仓库表 | 新增、修正和删除反映到目标副本 |
| [订单履约](examples/order-etl/) | 筛选已付款订单，按数量与单价计算金额，再按金额分配处理通道 | 付款后进入结果、改量后重算、取消后撤回 |
| [客户订单关联](examples/customer-order-enrichment/) | PostgreSQL 订单与 MySQL 客户按客户 ID 连接 | 已付款订单关联客户当前等级，相关资料变化后更新结果 |
| [支付对账](examples/payment-reconciliation/) | PostgreSQL 业务支付与 MySQL 渠道结算做全外连接 | 结算迟到、金额不符和修正后的对账状态变化 |
| [完整订单履约](examples/order-fulfillment/) | 付款筛选、折扣计算、地区路由与 `UNION ALL` | 订单变更，以及进程中断后的恢复验收 |

## 快速开始

使用内置 `sequence` 数据源，筛选偶数并持续写入 SQLite，无需外部数据库。源码构建需要 Rust 1.96：

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

输入位置和计算状态保存在 `--state` 目录，再次执行同一条命令即可从已提交位置继续。已有状态无法恢复时会报错，不会自动删除或重建。见[中断与恢复演示](docs/demo/)。

也可下载 [GitHub Releases](https://github.com/frelion/dogpaddle/releases) 中的发行包，内含 Debezium 和 JRE，无需 Rust 或系统 Java。保留解压后的完整目录，运行：

```text
bin/dogpaddle run SQL_FILE [--state DIR]
```

发行包支持 `x86_64` / `aarch64`，要求 GNU/Linux glibc 2.28+ 或 macOS 11.0+，兼容性详情见随 release 发布的 `compatibility.txt`。macOS 包未经签名或公证，可能需要在系统设置中允许运行。

## 当前能力与边界

| 维度 | 当前支持 |
| --- | --- |
| 数据源 | PostgreSQL CDC、MySQL CDC；一致初始快照与后续 WAL/binlog |
| SQL | `SELECT`、`WHERE`、表达式、普通 `JOIN`、动态 `ASOF JOIN`、`GROUP BY`、`DISTINCT`、`UNION ALL` |
| 普通 Join | Inner、Left/Right/Full Outer、Left/Right Semi、Left/Right Anti，以及 residual 条件 |
| 聚合 | 非空分组上的 `COUNT`、`SUM`、`AVG`、`MIN`、`MAX` |
| 目标端 | PostgreSQL、SQLite、ClickHouse、Doris |
| 恢复 | 本地持久化输入位置、拓扑和算子状态 |

DogPaddle 仍处于早期开发阶段。当前每个 SQL 文件只接受一条 `INSERT INTO sink(...) <query>`；CDC 读取单表并要求固定 Schema，暂不支持 TLS、在线 DDL 或多表路由。数据库目标关系由 DogPaddle 独占。当前 v1 状态格式不提供迁移；修改查询语义或持久 endpoint identity 时应使用新的 state 路径。分页 Join 的晚期错误可能保留已交付的部分结果；重启继续失败位置。完整约束见 [SQL 文档](crates/sql/README.md)。

## 文档

- [全部可运行 examples](examples/)
- [SQL、端点与运行方式](crates/sql/README.md)
- [完整订单履约与恢复演示](docs/demo/)
- [构建、测试与性能验证](TESTING.md)
- [底层 Flow API](crates/flow/README.md)
