# DogPaddle examples

这里的每个目录都是一条可以独立运行的 DogPaddle SQL 管道。根 README 中展示的业务场景直接引用这些文件；示例保留完整 SQL、建表语句和业务变更，可以复现相同的变化路径。README 新增的 ASOF 与门店聚合动图直接由这些文件录制。

| 示例 | 源 | 目标 | 主要行为 |
| --- | --- | --- | --- |
| [`event-sync`](event-sync/) | PostgreSQL | PostgreSQL | 初始快照以及源记录的新增、修改和删除 |
| [`order-etl`](order-etl/) | PostgreSQL | PostgreSQL | 已付款订单进入结果，改价后重新计算，取消后撤回 |
| [`order-fulfillment`](order-fulfillment/) | PostgreSQL | PostgreSQL | 订单 ETL 的完整恢复验收：折扣、分支路由、崩溃与 reopen |
| [`customer-order-enrichment`](customer-order-enrichment/) | PostgreSQL + MySQL | PostgreSQL | 客户资料变化后，已有订单的派生结果随之更新 |
| [`payment-reconciliation`](payment-reconciliation/) | PostgreSQL + MySQL | PostgreSQL | 两侧记录迟到、金额不一致和修正后的动态对账 |
| [`trade-quote-asof`](trade-quote-asof/) | PostgreSQL | PostgreSQL | 成交匹配当时最近报价，历史报价补录后重新匹配 |
| [`store-sales-summary`](store-sales-summary/) | PostgreSQL | PostgreSQL | 门店销售的分组统计随改价和退款持续修正 |

## 共同约定

这些示例使用空数据库，连接必须是无 TLS、没有 query 或 fragment 的 numeric-IP URL。PostgreSQL 需要启用 logical replication；MySQL 需要启用 binlog、`ROW` 格式和 `FULL` row image。发行包已经包含 DogPaddle 所需的 JRE 与 Debezium runtime。

进入对应示例目录后，运行方式相同：

1. 准备空的数据库实例，并按示例 README 执行目录中的 `setup*.sql`（MySQL setup 会创建示例数据库）；
2. 导出 `pipeline.sql` 中使用的环境变量；
3. 启动 `dogpaddle run pipeline.sql --state ./state`；
4. 在另一个终端按编号执行 `steps/*.sql`，每一步后查询目标关系；
5. 使用同一 SQL 和 state 目录再次启动即可恢复。

示例的 source table identity 和 state 属于同一次运行。若删除并重建 source table，应同时使用新的 state 目录；DogPaddle 不会用旧状态接管一个新建的源表。
