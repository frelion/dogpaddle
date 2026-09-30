# dogpaddle-store 性能口径

Store 自己拥有 workload、fixture、seed、预热、正确性断言和结果字段。工作区共享的
`dogpaddle-perf-context` 只解析 profile、管理结果目录、采集主机环境并拒绝非 release 实测；这里没有
中央 case registry、plan、fingerprint、结果 schema 或 validator。

## Targets

### `cell`

- `hot_get_one_tx`：在一个事务中重复读取已预热的 `Cell<u64>`，随后结束无写入事务而不进入 WAL；
- `read_update_commit`：每次 read-modify-write 都提交一个 durable transaction。

### `ordered_map`

除点查、分页和同步写入外，`bulk_remove_checked_commit` 与 `bulk_erase_known_commit` 使用相同的预填充
map 和删除集合，区分“需要返回存在性”的 remove 与“调用方已经证明存在”的无条件 erase 成本。

这个 target 只测量当前唯一的 `OrderedMap<u64, Vec<u8>>`。场景分别回答：

- `bulk_put_commit`：一个 durable transaction 中顺序写入完整 map；
- `point_get`：一个只读 snapshot 中按固定伪随机序列读取热 key；
- `ascending_scan` / `descending_scan`：使用真实 item/byte limit 和 continuation 扫描完整 map；
- `wide_scan`：8 KiB value 的有界分页与完整 owned decode；
- `station_step`：同一事务更新 `Cell` 与八个 map entry，呈现一个 durable computation step；
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
- RocksDB、WAL enabled 与 `sync=true` 的普通 `Transactions` durable write 模式。

这些 owner target 不使用 `DurabilityBatch`，因此仍逐笔呈现同步写成本。Flow 调度轮共享 barrier 的收益由
`flow_runtime` benchmark 负责；两者的 computation 语义 commit 数与 RocksDB WAL sync 次数不能互相替代。

fixture 创建、数据填充、预热和 oracle 位于计时外。`ordered_map` 的读场景使用真正的只读
snapshot；写场景通过唯一 `Transactions` capability 提交。字段只属于当前 target，不形成跨
target ABI。

## 2026-09-30：集合简化后的 smoke 对照

基线为 `2a8dd332e3ce6710e6314f0562317eb904f1ad77`，新版为
`codex/durable-call-stack` 本轮实现。两版均独立构建相同 `ordered_map` target，
使用 Rust 1.96.0 release、Apple M5、同一 APFS 文件系统，顺序运行且无并行测试负载。
每版使用现有 smoke 配置：10 个 Criterion 样本、20 ms 预热、50 ms 测量。
下表为 Criterion median point estimate，单位为微秒；每个 workload 的 oracle 均通过。

| workload | 旧版 | 新版 |
| --- | ---: | ---: |
| bulk_put_commit / 4 entries | 30.563 | 33.792 |
| bulk_remove_checked_commit / 4 | 29.042 | 30.229 |
| bulk_erase_known_commit / 4 | 22.958 | 25.354 |
| point_get / 4 | 0.841 | 0.871 |
| ascending_scan / 4 | 1.400 | 1.410 |
| descending_scan / 4 | 1.798 | 1.803 |
| wide_scan / 2 | 1.372 | 1.450 |
| station_step / 1 | 41.063 | 37.333 |
| durable_hot_overwrite / 1 | 31.167 | 24.979 |

这些短 smoke 样本只作回归筛查，尤其同步写入受文件系统波动影响；不据此声称稳定的吞吐提升。
本轮还将实际 range lower bound 传给 RocksDB iterator，避免 descending 空分区越过下界扫描
其他分区的 tombstone。现有双方向、边界与分页 oracle 验证其正确性，上述非空小 map 场景
不覆盖该病理工作量，不能用它们量化这项修复的收益。

本地 raw samples、estimates、context 和输出保存在
`/tmp/dogpaddle-final-performance-20260930/store-before/`、`store-after/` 及相邻日志。
Flow 的持久调用栈成本由其 [性能记录](../flow/PERFORMANCE.md) 单独报告。

最终清理将 Cell/Map 的无界点读合入 pinned bounded 读取路径后，又顺序重跑旧版与当前版的
`ordered_map` smoke。全部 oracle 通过；`point_get/4` 的 Criterion time point estimate 为
837.82 ns → 842.90 ns，ascending 为 1.3721 µs → 1.4134 µs，descending 为
1.7670 µs → 1.7956 µs。这仍只作短时回归筛查，不作提升声明。完整输出在
`/tmp/dogpaddle-final-store-bench-before.log`、`/tmp/dogpaddle-final-store-bench-after.log`。

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
