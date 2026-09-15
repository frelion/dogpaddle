# Event synchronization

这个示例从一致快照开始，把 `sales.events` 的当前关系写入 `warehouse.events_sync`，随后持续同步新事件。修改或删除源记录时，目标关系也会执行对应变化。

```sh
export DOGPADDLE_EVENT_SYNC_SOURCE='postgresql://dogpaddle:dogpaddle@127.0.0.1:5432/dogpaddle'
export DOGPADDLE_EVENT_SYNC_TARGET="$DOGPADDLE_EVENT_SYNC_SOURCE"
psql "$DOGPADDLE_EVENT_SYNC_SOURCE" -f setup.sql
dogpaddle run pipeline.sql --state ./state
```

在第二个终端依次执行三次新增，再修改和删除已有记录：

```sh
psql "$DOGPADDLE_EVENT_SYNC_SOURCE" -f steps/01-queue.sql
psql "$DOGPADDLE_EVENT_SYNC_SOURCE" -f steps/02-pay.sql
psql "$DOGPADDLE_EVENT_SYNC_SOURCE" -f steps/03-ship.sql
psql "$DOGPADDLE_EVENT_SYNC_SOURCE" -f steps/04-update.sql
psql "$DOGPADDLE_EVENT_SYNC_SOURCE" -f steps/05-delete.sql
```

查看目标关系：

```sh
psql "$DOGPADDLE_EVENT_SYNC_TARGET" -c \
  'SELECT event_id, convert_from(payload, '\''UTF8'\'') AS payload FROM warehouse.events_sync ORDER BY event_id'
```
