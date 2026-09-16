# 跨库支付对账

PostgreSQL 保存业务支付，MySQL 保存渠道结算。FULL OUTER JOIN 保留两侧记录，标记等待结算、金额不符、匹配成功以及缺少业务支付；结算迟到或金额修正后状态持续更新。

## 1. 启动数据库

准备已解压的 [DogPaddle release](https://github.com/frelion/dogpaddle/releases) 和 Docker Compose 或 Podman + Compose provider。保留 release 的 `bin/` 与 `libexec/` 完整目录，无需安装 Rust、Java 或本机数据库客户端。支持 GNU/Linux 与 macOS。

从仓库根目录进入本例：

```sh
cd examples/payment-reconciliation
```

选择一个引擎，同一次体验始终使用它：

```sh
# Docker Compose
docker compose up -d --wait

# 或 Podman
podman compose up -d
podman compose ps
```

macOS 的 Podman 需要 Linux 虚拟机；未启动时先执行 `podman machine start`，首次尚未创建时执行 `podman machine init` 再启动。Linux 通常无需 machine。Podman 的 Compose provider 安装与选择见 [公共说明](../README.md#容器环境)。

等待两个服务均显示 `healthy` 再继续；尚未就绪可以再次执行 `ps`，失败时查看 `podman compose logs`。首次空卷自动加载 [PostgreSQL 建表](setup-postgres.sql)、[MySQL 建表](setup-mysql.sql) 和 [MySQL 演示账号权限](setup-mysql-user.sql)，再次 `up` 保留修改，不重新初始化。环境不会自动启动 DogPaddle 或执行业务步骤。

### 数据库连接

| 数据库 | 地址 | 库名 | 用户 / 密码 |
| --- | --- | --- | --- |
| PostgreSQL（源与目标） | `127.0.0.1:55438` | `dogpaddle` | `dogpaddle / dogpaddle` |
| MySQL（源） | `127.0.0.1:53307` | `dogpaddle` | `dogpaddle / dogpaddle` |

关闭客户端 SSL，端口仅监听本机，凭据仅供本地演示。可以在 DataGrip、DBeaver 中自由查看数据。MySQL 已开启 ROW/FULL binlog、区分表名大小写，并配置 CDC 所需权限。目标表在 DogPaddle 启动后创建。

## 2. 运行实时 SQL

在本例目录加载 [.env](.env)，运行 [pipeline.sql](pipeline.sql)：

```sh
set -a
. ./.env
set +a

/absolute/path/to/release/bin/dogpaddle run pipeline.sql
```

替换为实际 release binary 路径，保持终端运行。默认 state 位于本例 `.dogpaddle/pipeline`。源码编译产物需额外设置绝对 `DOGPADDLE_DEBEZIUM_RUNTIME`；完整 release 包无需设置。

初始目标结果：7001（12500 分）、7002（8900 分）均为 awaiting_settlement。

## 3. 查看数据和自由操作

另一个终端进入本例目录，使用容器内客户端（无需重新加载环境变量；Docker 用户将下文 `podman compose` 换为 `docker compose`）：

```sh
podman compose exec postgres psql -U dogpaddle -d dogpaddle
```

查询 PostgreSQL 源表：

```sql
SELECT * FROM payments.payment_orders ORDER BY payment_id;
```

查询目标，文本列用 `convert_from` 从当前 Sink 的字节存储转换为可读文本：

```sql
SELECT business_payment_id, settlement_payment_id, expected_cents,
       settled_cents, convert_from(reconciliation_status, 'UTF8') AS status
FROM reconciliation.payment_status ORDER BY business_payment_id NULLS LAST;
```

在 psql 查询后输入 `\watch 1` 每秒刷新，`Ctrl-C` 停止刷新，`\q` 退出。

MySQL 可在另一个终端打开，输入 `exit` 退出：

```sh
podman compose exec mysql mysql -u dogpaddle -pdogpaddle --ssl-mode=DISABLED dogpaddle
```

```sql
SELECT * FROM dogpaddle.settlements ORDER BY payment_id;
```

可以自由对源表执行 `INSERT/UPDATE/DELETE`。保持表结构、主键、publication 和 CDC 配置不变；目标表由 DogPaddle 独占维护，只查询，不手动改写。

也可以在 shell 中逐条执行现成步骤，每条后观察结果：

```sh
# 7001 结算到账，变为 matched。
podman compose exec -T mysql mysql -u dogpaddle -pdogpaddle --ssl-mode=DISABLED dogpaddle \
  < steps/01-settlement-arrives.sql

# 7002 到账 7900，变为 amount_mismatch。
podman compose exec -T mysql mysql -u dogpaddle -pdogpaddle --ssl-mode=DISABLED dogpaddle \
  < steps/02-wrong-amount.sql

# 7002 修正为 8900，变为 matched。
podman compose exec -T mysql mysql -u dogpaddle -pdogpaddle --ssl-mode=DISABLED dogpaddle \
  < steps/03-correct-amount.sql

# 出现 7999 / 4300，标记 missing_business_payment。
podman compose exec -T mysql mysql -u dogpaddle -pdogpaddle --ssl-mode=DISABLED dogpaddle \
  < steps/04-orphan-settlement.sql
```

步骤可选，不维护进度。预期值只适用于初始数据及上述顺序，手动修改后以实际数据为准。

## 停止、恢复与重置

先在 DogPaddle 终端按 `Ctrl-C`，再停止数据库：

```sh
podman compose down
```

数据库卷和 state 都保留。下次在同一目录重新 `up`、等待健康、加载 `.env`，重跑同一 DogPaddle 命令即可恢复。

**清空重新体验**：先停止 DogPaddle，在本例目录执行以下命令。它会删除本例两个数据库的数据卷和默认 state，含手动写入的数据；两侧必须同时重置：

```sh
podman compose down --volumes && rm -rf -- ./.dogpaddle/pipeline
```

然后从步骤 1 开始。自定义 `--state` 也需清理本例对应路径或使用新路径。初始化失败时不要重灌已有卷，查看 logs 后按此流程重置。

端口冲突时编辑 `.env` 的 `DOGPADDLE_RECONCILIATION_PG_PORT` / `DOGPADDLE_RECONCILIATION_MYSQL_PORT`，重新加载后再启动。同一 shell 可切换不同 example，它们使用场景独立变量。

同名 example 的多个 checkout 默认共享 Compose `name`，不能当成独立实例；任何一份执行 `down --volumes` 都会删除该共享数据。需要额外实例时同时设置独立的 Compose `name`、端口和新的 state，不混用 `COMPOSE_PROJECT_NAME` / `-p` 覆盖。切换容器引擎也需新的 state。

PostgreSQL 复制槽 WAL 保留预算为 256 MiB（检查点生效，非磁盘硬配额）。MySQL binlog 配置为保留一天（自动清理时生效，也非磁盘硬配额）。DogPaddle 停止过久且持续写入可能导致所需日志失效，此时需完整重置。长期不用请停止数据库。
