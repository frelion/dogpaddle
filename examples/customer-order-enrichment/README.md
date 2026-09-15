# Customer order enrichment

已付款订单保存在 PostgreSQL，客户分层保存在 MySQL。客户从 `standard` 升级为 `vip` 时，已有订单的派生结果随之替换；新增订单后，它也会关联当前客户等级，随后继续响应等级变化。

MySQL 必须启用 binary log、`ROW` 格式、`FULL` row image，并使用 `lower_case_table_names=0`。连接用户需要 Debezium MySQL connector 所需的读取和复制权限。

```sh
export DOGPADDLE_ORDERS_SOURCE='postgresql://dogpaddle:dogpaddle@127.0.0.1:5432/dogpaddle'
export DOGPADDLE_CUSTOMERS_SOURCE='mysql://dogpaddle:dogpaddle@127.0.0.1:3306/crm'
export DOGPADDLE_CUSTOMER_ORDERS_TARGET="$DOGPADDLE_ORDERS_SOURCE"

psql "$DOGPADDLE_ORDERS_SOURCE" -f setup-postgres.sql
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < setup-mysql.sql
dogpaddle run pipeline.sql --state ./state
```

在第二个终端执行客户和订单两侧的变化：

```sh
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < steps/01-upgrade-customer.sql
psql "$DOGPADDLE_ORDERS_SOURCE" -f steps/02-create-paid-order.sql
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < steps/03-promote-customer.sql
```

查看当前结果：

```sh
psql "$DOGPADDLE_CUSTOMER_ORDERS_TARGET" -c \
  'SELECT order_id, customer_id, convert_from(segment, '\''UTF8'\'') FROM analytics.enriched_orders ORDER BY order_id'
```
