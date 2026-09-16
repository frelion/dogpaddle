# 成交与历史报价匹配

为每笔成交匹配同一证券、不晚于成交时间的最近报价，再计算成交价与报价中间价的偏差。历史报价补录、修正或删除后，已有成交的匹配结果随之更新。

## 1. 启动数据库

准备已解压的 [DogPaddle release](https://github.com/frelion/dogpaddle/releases) 和 Docker Compose 或 Podman + Compose provider。保留 release 的完整目录，包含 `bin/` 与 `libexec/`，无需安装 Rust、Java 或本机数据库客户端。

在仓库根目录进入本例：

```sh
cd examples/trade-quote-asof
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
| Port | `55433` |
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

初始结果：成交 9001 匹配 09:30:00.100 的报价，中间价 10010 分，价格偏差 8 分。

## 3. 自由查看和修改

在另一个终端进入本例目录，使用容器内自带的客户端（无需加载环境变量）：

```sh
podman compose exec postgres psql -U dogpaddle -d dogpaddle
# Docker 用户将 podman compose 换为 docker compose，下同。
```

查看源表：

```sql
SELECT * FROM market.fills ORDER BY trade_id;
SELECT * FROM market.quotes ORDER BY quote_id;
```

查看目标：

```sql
SELECT trade_id,
       timestamp 'epoch' + quoted_at * interval '1 microsecond' AS quoted_at,
       execution_price_cents, arrival_mid_cents, slippage_cents
FROM analytics.execution_quality ORDER BY trade_id;
```

在 psql 查询后输入 `\watch 1` 可每秒刷新，`Ctrl-C` 停止刷新，`\q` 退出。 PostgreSQL Sink 按整数微秒保存时间戳，上述查询将其转换回可读时间。

可以自由对源表执行 `INSERT/UPDATE/DELETE`，再观察目标变化。保持源表结构、主键和 publication 不变；目标表由 DogPaddle 维护，仅查询，不手动改写。

也可以在另一个 shell 逐条执行已有步骤，每执行一条就观察一次结果：

```sh
# 补录 .400 的报价：成交 9001 改配，中间价 10012，偏差 6。
podman compose exec -T postgres psql -U dogpaddle -d dogpaddle \
  -v ON_ERROR_STOP=1 < steps/01-backfill-closer-quote.sql

# 修正报价：匹配时间不变，中间价变为 10014，偏差 4。
podman compose exec -T postgres psql -U dogpaddle -d dogpaddle \
  -v ON_ERROR_STOP=1 < steps/02-correct-quote.sql

# 删除该报价：重新匹配 .100，中间价 10010，偏差恢复为 8。
podman compose exec -T postgres psql -U dogpaddle -d dogpaddle \
  -v ON_ERROR_STOP=1 < steps/03-retract-bad-quote.sql
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

端口冲突时，在启动前编辑 `.env` 的 `DOGPADDLE_TRADE_ASOF_PORT`，重新加载 `.env` 再执行 `up` 与 DogPaddle。所有场景使用独立端口变量，同一 shell 可以切换场景。

Compose 文件的 `name` 标识本例容器与数据卷。默认一次只运行一份本例：不同 checkout 的同名 example 仍共享这个项目，`down --volumes` 会删除这份共享数据，复制目录不会创建独立实例。需要额外实例时，同时改为独立的 Compose `name`、端口和新的 state；不要混用 `COMPOSE_PROJECT_NAME` 或 `-p` 覆盖。不同容器引擎的数据卷独立，切换引擎也必须使用新的 state。

本例将复制槽 WAL 保留预算设为 256 MiB（检查点时生效，不是磁盘硬配额）。DogPaddle 停止后大量写入可能使复制槽失效；这时需要按上面的完整重置流程重新体验。长期不用时请停止数据库。
