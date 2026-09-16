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

全部七个 example 均提供 `compose.yaml`、`.env` 和独立 README，自动准备 PostgreSQL，跨库场景同时准备 MySQL。进入任意目录后启动数据库、加载 `.env`，再运行 release 包中的 `dogpaddle run pipeline.sql`。首次体验可以从 [门店销售汇总](store-sales-summary/) 开始。

## 容器环境

全部 Compose 使用固定版本及多架构 digest 的官方 PostgreSQL 16.15 镜像，跨库场景另用 MySQL 8.4.6。每例的 Compose project、端口变量、数据卷和默认 `.dogpaddle/pipeline` state 独立。`.env` 是可提交的公开本地演示配置，不要填入生产凭据。所有端口只绑定 `127.0.0.1`：

| Example | PostgreSQL 端口 | MySQL 端口 / 库名 |
| --- | --- | --- |
| store-sales-summary | 55432 | — |
| trade-quote-asof | 55433 | — |
| event-sync | 55434 | — |
| order-etl | 55435 | — |
| order-fulfillment | 55436 | — |
| customer-order-enrichment | 55437 | 53306 / crm |
| payment-reconciliation | 55438 | 53307 / dogpaddle |

PostgreSQL 库名均为 `dogpaddle`；两种数据库的演示用户名/密码均为 `dogpaddle / dogpaddle`，客户端关闭 SSL。各例 README 给出完整连接与查询命令。

- Docker：安装 Docker Engine/Desktop 与 Compose，使用 `docker compose up -d --wait`。
- Podman：安装 Podman 和 Compose provider，macOS 需要 Linux 虚拟机；首次执行 `podman machine init`，未运行时执行 `podman machine start`，已经运行则无需重复启动。Linux 通常无需 machine。随后使用 `podman compose up -d`，再通过 `podman compose ps` 确认所有服务 `healthy`。本运行流程面向 release 支持的 GNU/Linux 与 macOS。
- [`podman compose`](https://docs.podman.io/en/latest/markdown/podman-compose.1.html) 调用外部 provider。可用 `podman compose version` 检查；同时装了多个 provider 时，可通过 `PODMAN_COMPOSE_PROVIDER=podman-compose` 选择。无需为了本例切换 rootful 模式。

数据库健康检查包含 TCP 就绪与源表可查询；如果初始化失败，先查看 `compose logs`，不要重复手动灌入 `setup*.sql`。官方数据库镜像只在空数据卷上执行初始化脚本，已有卷会保留你的修改。MySQL 演示账号由各例 `setup-mysql-user.sql` 初始化，具备 [Debezium 所需 CDC 权限](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-creating-user) 和源库行编辑权限。实际验证组合与覆盖项见 [TESTING.md](../TESTING.md#example-容器体验)。

可直接用 GUI 连接数据库，或用 `compose exec postgres psql -U dogpaddle -d dogpaddle` 打开容器里的客户端。环境只准备业务数据，不启动 DogPaddle，不自动执行 `steps`；你可以自由修改源表数据，目标由 DogPaddle 独占维护。不同终端不共享 shell 环境变量，但 `compose exec` 不要求先加载 `.env`。

同一次体验保留同一个引擎、Compose project、数据库卷及 state。`down` 保留数据；完整重置必须同时删除本例数据卷和 state，具体命令见各例 README。停止 DogPaddle 后再停止或重置数据库。复制槽 WAL 保留预算为 256 MiB，超过预算可能使槽失效，需要完整重置；该预算在检查点生效，不是磁盘硬配额，长期不用请停止数据库。同名 example 的不同 checkout 默认共享 Compose 项目，不能同时作为独立实例运行，具体隔离方式见各例末尾。

MySQL binlog 保留一天，在自动清理时生效而非磁盘硬配额。停止消费后持续写入或长时间停机可能使所需日志过期，此时同样需要完整重置。

## 自备数据库

这些示例使用空数据库，连接必须是无 TLS、没有 query 或 fragment 的 numeric-IP URL。PostgreSQL 需要启用 logical replication；MySQL 需要启用 binlog、`ROW` 格式和 `FULL` row image。发行包已经包含 DogPaddle 所需的 JRE 与 Debezium runtime。

进入对应示例目录后，运行方式相同：

1. 准备空的演示数据库：纯 PostgreSQL 场景仅用 `psql` 执行 `setup.sql`；跨库场景用 `psql` 执行 `setup-postgres.sql`，用 `mysql` 执行 `setup-mysql.sql`（会创建业务库）。不要用 glob 批量执行 `setup*.sql`。`setup-mysql-user.sql` 仅供 Compose 本地演示自动初始化，不在自有实例执行；自备数据库的 CDC 用户与权限由你配置，MySQL 还需 `PROCESS` 权限读取 InnoDB 表标识；
2. 导出 `pipeline.sql` 中使用的环境变量；
3. 启动 `dogpaddle run pipeline.sql --state ./state`；
4. 在另一个终端按编号执行 `steps/*.sql`，每一步后查询目标关系；
5. 使用同一 SQL 和 state 目录再次启动即可恢复。

示例的 source table identity 和 state 属于同一次运行。若删除并重建 source table，应同时使用新的 state 目录；DogPaddle 不会用旧状态接管一个新建的源表。
