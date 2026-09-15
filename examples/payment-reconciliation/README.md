# Payment reconciliation

业务支付保存在 PostgreSQL，渠道结算保存在 MySQL。`FULL OUTER JOIN` 保留两侧尚未匹配的记录；结算迟到或金额被修正时，同一个对账结果会从等待、异常变为匹配。

MySQL 必须启用 binary log、`ROW` 格式、`FULL` row image，并使用 `lower_case_table_names=0`。连接用户需要 Debezium MySQL connector 所需的读取和复制权限。

```sh
export DOGPADDLE_PAYMENTS_SOURCE='postgresql://dogpaddle:dogpaddle@127.0.0.1:5432/dogpaddle'
export DOGPADDLE_SETTLEMENTS_SOURCE='mysql://dogpaddle:dogpaddle@127.0.0.1:3306/dogpaddle'
export DOGPADDLE_RECONCILIATION_TARGET="$DOGPADDLE_PAYMENTS_SOURCE"

psql "$DOGPADDLE_PAYMENTS_SOURCE" -f setup-postgres.sql
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < setup-mysql.sql
dogpaddle run pipeline.sql --state ./state
```

在第二个终端依次执行 `steps` 中的文件：

```sh
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < steps/01-settlement-arrives.sql
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < steps/02-wrong-amount.sql
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < steps/03-correct-amount.sql
mysql --host=127.0.0.1 --user=dogpaddle --password=dogpaddle < steps/04-orphan-settlement.sql
```

查看每笔支付当前的对账状态：

```sh
psql "$DOGPADDLE_RECONCILIATION_TARGET" -c \
  'SELECT business_payment_id, settlement_payment_id, expected_cents, settled_cents, convert_from(reconciliation_status, '\''UTF8'\'') FROM reconciliation.payment_status ORDER BY business_payment_id NULLS LAST'
```
