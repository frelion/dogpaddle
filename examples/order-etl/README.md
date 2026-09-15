# Paid-order ETL

这份管道对应根 README 的订单 ETL 动图：待付款订单不会进入目标；付款、修改数量和新增订单会持续改变金额与处理通道。把订单改回非付款状态或删除时，结果会被撤回。

```sh
export DOGPADDLE_ORDER_ETL_SOURCE='postgresql://dogpaddle:dogpaddle@127.0.0.1:5432/dogpaddle'
export DOGPADDLE_ORDER_ETL_TARGET="$DOGPADDLE_ORDER_ETL_SOURCE"
psql "$DOGPADDLE_ORDER_ETL_SOURCE" -f setup.sql
dogpaddle run pipeline.sql --state ./state
```

在第二个终端执行动图中的变化：

```sh
psql "$DOGPADDLE_ORDER_ETL_SOURCE" -f steps/01-pay-and-reprice.sql
psql "$DOGPADDLE_ORDER_ETL_SOURCE" -f steps/02-create-paid-order.sql
psql "$DOGPADDLE_ORDER_ETL_SOURCE" -f steps/03-increase-quantity.sql
psql "$DOGPADDLE_ORDER_ETL_SOURCE" -f steps/04-cancel.sql
```

查看当前履约关系：

```sh
psql "$DOGPADDLE_ORDER_ETL_TARGET" -c \
  'SELECT order_id, amount, convert_from(lane, '\''UTF8'\'') AS lane FROM ops.fulfillment ORDER BY order_id'
```

[`order-fulfillment`](../order-fulfillment/) 在同一业务背景上增加折扣、地区路由、`UNION ALL` 和崩溃恢复验收。
