# Order fulfillment

已付款且达到最低金额的订单进入履约队列。订单付款后结果出现，数量和折扣变化会替换金额与处理通道，取消订单会撤回结果。国内订单与其他地区订单通过 `UNION ALL` 路由到不同履约中心。

```sh
export DOGPADDLE_FULFILLMENT_SOURCE='postgresql://dogpaddle:dogpaddle@127.0.0.1:5432/dogpaddle'
export DOGPADDLE_FULFILLMENT_TARGET="$DOGPADDLE_FULFILLMENT_SOURCE"
psql "$DOGPADDLE_FULFILLMENT_SOURCE" -f setup.sql
dogpaddle run pipeline.sql --state ./state
```

在第二个终端逐步执行：

```sh
psql "$DOGPADDLE_FULFILLMENT_SOURCE" -f steps/01-pay.sql
psql "$DOGPADDLE_FULFILLMENT_SOURCE" -f steps/02-reprice.sql
psql "$DOGPADDLE_FULFILLMENT_SOURCE" -f steps/03-cancel.sql
```

查看结果：

```sh
psql "$DOGPADDLE_FULFILLMENT_TARGET" -c \
  'SELECT order_id, payable_cents, convert_from(fulfillment_center, '\''UTF8'\''), convert_from(handling_lane, '\''UTF8'\'') FROM ops.fulfillment_queue ORDER BY order_id'
```

完整的崩溃恢复和持续变更验收使用的也是这份 [`pipeline.sql`](pipeline.sql)，见[演示与录制说明](../../docs/demo/)。
