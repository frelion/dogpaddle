# Store sales summary

DogPaddle 为每个门店维护已付款订单的 `COUNT`、`SUM`、`AVG`、`MIN` 和 `MAX`。订单付款、改价和退款时，目标中的同一分组被替换；门店没有已付款订单后，对应分组消失。

```sh
export DOGPADDLE_STORE_SALES_SOURCE='postgresql://dogpaddle:dogpaddle@127.0.0.1:5432/dogpaddle'
export DOGPADDLE_STORE_SALES_TARGET="$DOGPADDLE_STORE_SALES_SOURCE"

psql "$DOGPADDLE_STORE_SALES_SOURCE" -f setup.sql
dogpaddle run pipeline.sql --state ./state
```

在第二个终端逐步执行：

```sh
psql "$DOGPADDLE_STORE_SALES_SOURCE" -f steps/01-pay-order.sql
psql "$DOGPADDLE_STORE_SALES_SOURCE" -f steps/02-correct-price.sql
psql "$DOGPADDLE_STORE_SALES_SOURCE" -f steps/03-refund-largest-order.sql
psql "$DOGPADDLE_STORE_SALES_SOURCE" -f steps/04-refund-last-store-order.sql
```

查看当前门店汇总：

```sh
psql "$DOGPADDLE_STORE_SALES_TARGET" -c \
  'SELECT store_id, order_count, revenue_cents, smallest_order_cents, largest_order_cents FROM analytics.store_sales ORDER BY store_id'
```
