# Trade execution quality with ASOF

每笔成交匹配相同证券在成交时刻之前最近的一条报价。示例先让成交匹配 `09:30:00.100` 的报价，再补录一条 `09:30:00.400` 的历史报价；已有成交随之改配。修正和删除这条报价时，成交质量结果再次变化。

```sh
export DOGPADDLE_FILLS_SOURCE='postgresql://dogpaddle:dogpaddle@127.0.0.1:5432/dogpaddle'
export DOGPADDLE_QUOTES_SOURCE="$DOGPADDLE_FILLS_SOURCE"
export DOGPADDLE_EXECUTION_QUALITY_TARGET="$DOGPADDLE_FILLS_SOURCE"

psql "$DOGPADDLE_FILLS_SOURCE" -f setup.sql
dogpaddle run pipeline.sql --state ./state
```

在第二个终端逐步补录、修正和撤回报价：

```sh
psql "$DOGPADDLE_QUOTES_SOURCE" -f steps/01-backfill-closer-quote.sql
psql "$DOGPADDLE_QUOTES_SOURCE" -f steps/02-correct-quote.sql
psql "$DOGPADDLE_QUOTES_SOURCE" -f steps/03-retract-bad-quote.sql
```

每一步后查看成交匹配的报价和滑点：

```sh
psql "$DOGPADDLE_EXECUTION_QUALITY_TARGET" -c \
  'SELECT trade_id, timestamp '\''epoch'\'' + quoted_at * interval '\''1 microsecond'\'' AS quoted_at, arrival_mid_cents, slippage_cents FROM analytics.execution_quality ORDER BY trade_id'
```

PostgreSQL Sink 按 Arrow 的整数表示保存时间戳；上面的查询将微秒值转换回 PostgreSQL 时间。
