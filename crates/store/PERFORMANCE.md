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
- `wide_scan_owned` / `wide_scan_projected`：比较 8 KiB value 的完整 owned decode 与只读取必要字节；
- `station_step`：同一事务更新 `Cell` 与八个 map entry，呈现一个 durable Station step；
- `durable_hot_overwrite`：每次提交覆盖同一个 key，单独呈现 WAL + sync commit 成本。

`OrderedMap` 只有一个物理实现，因此不运行形式配对或两套重复 fixture。这个 target 也不为编码表示、
无关命名空间和事务失败路径复制同一组成本矩阵。底层 RocksDB 压力、compaction 与 endurance 应由
专门实验拥有。

## 输出

两个 target 都使用 Criterion。原生 raw samples 与 estimates 写到该次 `RunRoot` 的
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
```

正式 reference 必须指定固定的绝对目录：

```bash
DOGPADDLE_PERF_PROFILE=reference \
DOGPADDLE_PERF_ROOT=/absolute/path/on/reference-filesystem \
cargo bench --locked -p dogpaddle-store --bench ordered_map
```

不同 baseline epoch、提交、rustc、机器、profile、文件系统或 workload 的结果不可直接比较。工作区
分类和准入规则见根目录 [`TESTING.md`](../../TESTING.md)。
