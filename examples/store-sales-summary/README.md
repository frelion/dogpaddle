# 门店销售汇总

按门店持续汇总已付款订单。付款、改价和退款后，订单数、金额合计、平均值和最小／最大值随之更新；没有已付款订单的门店退出结果。

## 1. 启动数据库

准备已解压的 [DogPaddle release](https://github.com/frelion/dogpaddle/releases) 和 Docker Compose 或 Podman + Compose provider。保留 release 的完整目录，包含 `bin/` 与 `libexec/`，无需安装 Rust、Java 或本机数据库客户端。

在仓库根目录进入本例：

```sh
cd examples/store-sales-summary
```

选择一个容器引擎，同一次体验始终使用它：

```sh
# Docker Compose
docker compose up -d --wait

# 或 Podman；macOS 请先启动 Podman machine
podman compose up -d
podman compose ps
```

Podman 等待 `ps` 显示 `healthy` 再继续；还未就绪时可再次执行 `ps`。若一直未就绪，查看 `podman compose logs postgres`。Docker 也可用相同的 `ps/logs` 命令诊断。Podman 的 `compose` 由外部 provider 执行，安装要求和验收组合见 [公共说明](../README.md#容器环境)。

首次启动会执行 [setup.sql](setup.sql)，完成建表、CDC publication 和初始数据加载。再次启动保留已有数据，不重新执行 setup。数据库启动后即可自由连接；它不会自动运行 DogPaddle 或业务步骤。

| 连接项 | 默认值 |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `55432` |
| Database | `dogpaddle` |
| User / Password | `dogpaddle` / `dogpaddle` |
| SSL | 关闭 |

可以用 DataGrip、DBeaver 直接连接。源表与目标表位于同一个演示数据库的不同 schema 中；目标表在 DogPaddle 启动后创建。端口只监听本机，以上为公开演示凭据。

## 2. 运行实时 SQL

在本例目录加载 [.env](.env)，然后用已下载的 binary 启动 [pipeline.sql](pipeline.sql)：

```sh
set -a
. ./.env
set +a

/absolute/path/to/release/bin/dogpaddle run pipeline.sql
```

将 binary 路径替换为实际解压路径，保持这个终端运行。默认 state 位于本例的 `.dogpaddle/pipeline`。本机源码构建的 binary 需要另外配置绝对 `DOGPADDLE_DEBEZIUM_RUNTIME`，release 包不需要。

初始结果：门店 11 有 2 笔已付款订单，合计 30000 分，平均 15000 分，最小 12000、最大 18000。门店 12 尚无汇总。

## 3. 自由查看和修改

在另一个终端进入本例目录，使用容器内自带的客户端（无需加载环境变量）：

```sh
podman compose exec postgres psql -U dogpaddle -d dogpaddle
# Docker 用户将 podman compose 换为 docker compose，下同。
```

查看源表：

```sql
SELECT * FROM sales.store_orders ORDER BY order_id;
```

查看目标：

```sql
SELECT store_id, order_count, revenue_cents,
       smallest_order_cents, largest_order_cents
FROM analytics.store_sales ORDER BY store_id;
```

在 psql 查询后输入 `\watch 1` 可每秒刷新，`Ctrl-C` 停止刷新，`\q` 退出。

此查询与 README 动图一样显示五个整数列。SQL 也计算了 `average_order_cents`；当前 PostgreSQL Sink 将 Float64 按原始大端位模式存为 `bytea`，直接查询该列会显示字节，不是格式化的小数。

可以自由对源表执行 `INSERT/UPDATE/DELETE`，再观察目标变化。保持源表结构、主键和 publication 不变；目标表由 DogPaddle 维护，仅查询，不手动改写。

也可以在另一个 shell 逐条执行已有步骤，每执行一条就观察一次结果：

```sh
# 订单 3003 付款：门店 12 出现，合计 25000 分。
podman compose exec -T postgres psql -U dogpaddle -d dogpaddle \
  -v ON_ERROR_STOP=1 < steps/01-pay-order.sql

# 订单 3002 改价：门店 11 合计 34000，平均 17000，最大值 22000。
podman compose exec -T postgres psql -U dogpaddle -d dogpaddle \
  -v ON_ERROR_STOP=1 < steps/02-correct-price.sql

# 订单 3003 退款：门店 12 消失。
podman compose exec -T postgres psql -U dogpaddle -d dogpaddle \
  -v ON_ERROR_STOP=1 < steps/03-refund-largest-order.sql

# 门店 11 剩余订单退款：目标表变空。
podman compose exec -T postgres psql -U dogpaddle -d dogpaddle \
  -v ON_ERROR_STOP=1 < steps/04-refund-last-store-order.sql
```

这些预期值基于初始数据和给定顺序；手动改过数据后，以你的实际数据为准。步骤执行不是强制流程，重复执行也不会重置环境。

## 停止、恢复与重置

先在 DogPaddle 终端按 `Ctrl-C`，再停止数据库：

```sh
podman compose down
```

数据卷与本地 state 都保留。下次重新 `up`、等待健康、加载 `.env`，然后在同一目录运行同一条 DogPaddle 命令即可恢复。

**重新体验初始数据**时，先停止 DogPaddle，在本例目录执行下面两步。它们会删除本例数据库数据卷和默认 state，包括你手动修改的数据；不要只清理其中一边：

```sh
podman compose down --volumes && rm -rf -- ./.dogpaddle/pipeline
```

再从步骤 1 开始。若曾使用自定义 `--state`，也必须清理对应的本例 state 或改用新的路径。

端口冲突时，在启动前编辑 `.env` 的 `DOGPADDLE_STORE_SALES_PORT`，重新加载 `.env` 再执行 `up` 与 DogPaddle。所有场景使用独立端口变量，同一 shell 可以切换场景。

Compose 文件的 `name` 标识本例容器与数据卷。默认一次只运行一份本例：不同 checkout 的同名 example 仍共享这个项目，`down --volumes` 会删除这份共享数据，复制目录不会创建独立实例。需要额外实例时，同时改为独立的 Compose `name`、端口和新的 state；不要混用 `COMPOSE_PROJECT_NAME` 或 `-p` 覆盖。不同容器引擎的数据卷独立，切换引擎也必须使用新的 state。

本例将复制槽 WAL 保留预算设为 256 MiB（检查点时生效，不是磁盘硬配额）。DogPaddle 停止后大量写入可能使复制槽失效；这时需要按上面的完整重置流程重新体验。长期不用时请停止数据库。
