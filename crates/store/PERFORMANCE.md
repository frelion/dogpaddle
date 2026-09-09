# dogpaddle-store 性能口径

Store 自己拥有 workload、fixture、seed、预热、正确性断言和结果字段。工作区共享的
`dogpaddle-perf-context` 只解析 profile、管理结果目录、采集主机环境并拒绝非 release 实测；这里没有
中央 case registry、plan、fingerprint、结果 schema 或 validator。

## Targets

### `cell`

- `hot_get_one_tx`：在一个事务中重复读取已预热的 `Cell<u64>`；
- `read_update_commit`：每次 read-modify-write 都提交一个 durable transaction。

### `ordered_map`

这个 target 只测量当前唯一的 `OrderedMap<u64, Vec<u8>>`。场景分别回答：

- `bulk_put_commit`：一个 durable transaction 中顺序写入完整 map；
- `point_get`：一个只读 snapshot 中按固定伪随机序列读取热 key；
- `ascending_scan` / `descending_scan`：使用真实 item/byte limit 和 continuation 扫描完整 map；
- `wide_scan`：8 KiB value 的有界分页与完整 owned decode；
- `station_step`：同一事务更新 `Cell` 与八个 map entry，呈现一个 durable Station step；
- `durable_hot_overwrite`：每次提交覆盖同一个 key，单独呈现 WAL + sync commit 成本。

`OrderedMap` 只有一个物理实现，因此不运行形式配对或两套重复 fixture。这个 target 也不为编码表示、
无关命名空间和事务失败路径复制同一组成本矩阵。底层 RocksDB 压力、compaction 与 endurance 应由
专门实验拥有。

### `subscribed_log`

- `snapshot_status`：预热后比较 1 KiB 与大 payload 的只读 snapshot/status；
- `peek_ack_commit`：完整 owned payload 读取、确认与同步提交，补充输入在计时外；
- `fanout_append_ack_churn`：三个 subscriber 分批推进，快订阅者排空后保留慢订阅者 backlog，
  每个 burst 在计时外 reopen 并验证 position、retained bytes 和 payload，再计时确认最后一个订阅者。

smoke 使用 1 MiB 大 entry、4 KiB churn entry 和 4-entry burst；reference 使用 64 MiB、64 KiB 和
64-entry burst。payload 使用记录在 context 中的固定伪随机 seed。原生 sample/estimate 保留在
Criterion 目录；这里只测有界 churn，不声称覆盖长期 compaction 稳态或物理空间回收时延。

## 输出

三个 target 都使用 Criterion。原生 raw samples 与 estimates 写到该次 `RunRoot` 的
`criterion/`，相邻 `context.json` 记录：

- benchmark 与 `smoke|reference` profile；
- 实际 workload、scan limit 和固定随机种子；
- rustc、OS/kernel、CPU、git revision 和 dirty state；
- 结果文件系统；
- RocksDB、WAL enabled 与 `sync=true` 的 durable write 模式。

fixture 创建、数据填充、预热和 oracle 位于计时外。`ordered_map` 的读场景使用真正的只读
snapshot；写场景通过唯一 `Transactions` capability 提交。字段只属于当前 target，不形成跨
target ABI。

## 运行

快速 smoke：

```bash
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench cell
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench ordered_map
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-store --bench subscribed_log
```

正式 reference 必须指定固定的绝对目录：

```bash
DOGPADDLE_PERF_PROFILE=reference \
DOGPADDLE_PERF_ROOT=/absolute/path/on/reference-filesystem \
cargo bench --locked -p dogpaddle-store --bench ordered_map
```

不同 baseline epoch、提交、rustc、机器、profile、文件系统或 workload 的结果不可直接比较。工作区
分类和准入规则见根目录 [`TESTING.md`](../../TESTING.md)。
