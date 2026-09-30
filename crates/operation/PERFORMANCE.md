# Operation 性能

## Buffered Sink

`buffered_sink` 是本 owner 的真实 SQLite Sink 对照，包含 input admission、Store
同步提交、target delivery 和 settlement。fixture、初始化和关系 oracle 不计时。
`restore_validation` 计 reopen/bind 和首次 drain，包括完整恢复校验与首批
delivery/settlement；staging 和后续 drain 不计时。
测试模式只证明可执行，不能用来比较 release 性能。

```bash
DOGPADDLE_PERF_PROFILE=reference \
  DOGPADDLE_PERF_ROOT=/absolute/path/to/results \
  cargo bench --locked -p dogpaddle-operation --bench buffered_sink
```

每轮在独立目录保存 `context.json`、Criterion samples/estimates 和
`target_storage.json`。后者在计时外通过真正的 Sink 交付 65,536 份 64-byte 字符串行，
记录 SQLite 表、rowid 和 hash index 的 page allocation；不包含 Store、WAL 或 heap。
smoke/test 使用各自的较小行数，不能与 reference 空间结果混合。

`large_unit_entry` 把 65,536 个不同 unit-weight rows 放进一个 IPC entry，覆盖正负两个
方向的 1024-event 切片，防止每页反复解码和扫描原始 prefix。它仍包含同步 I/O、精确 lookup
和 target SQL，不能把整轮时间解释成单独的游标定位成本。

## 事件位置架构的确定性变化

非空 Ready 控制从最多 59 bytes 变为固定 34 bytes。仍有后续输入的 1024-mutation
Prepared，旧 codec 是 `2 + 49 + 49 + 8 + 5 + 16 × 1024 = 16,497` bytes；
新 codec 是 `68 + 8 × negatives` bytes：纯正事件 68 bytes，纯负事件 8260 bytes。
这衡量逻辑控制数据，不代表整个数据库、WAL 或 allocator 的等比例缩减。

SQLite INTEGER PRIMARY KEY 继续使用 rowid alias。signed event ID 映射保持顺序，但初期
负 rowid 使用 9-byte varint，比小正 rowid 大，因此原生整数宽度相同不代表磁盘占用相同。
该代价应与控制数据缩减分别报告，不能用压缩后的 Prepared 遮盖目标空间增长。

当前恢复直接比较借用的 Arrow 行，不重复分配出生行的 canonical payload；prepare 在已
验证的队列中直接构造临时 mutations，不再调用完整的持久计划恢复路径。
有界逻辑计费和真实 heap、native allocation、WAL、RSS 是不同口径；本 Criterion target
未直接测量这些运行时资源。精确约束与恢复信任边界见 [Sink 契约](docs/sinks.md)。

## 2026-10-01 同机 reference 对照

Apple M5、aarch64 macOS/Darwin 25.6、APFS、Rust 1.96.0，release build；每个场景
10 samples、2 秒 warmup、5 秒目标 measurement。大 entry 每轮时间超过 1 秒，Criterion
自动使用 10 次整轮测量。baseline 与 candidate 使用相同 benchmark instrumentation，
顺序执行全套场景；最终这两轮期间没有并发构建、容器验收或 worktree 归档。

baseline 产品代码是 `3becbf94932b36bc328a5352f3b15151ebccadd8`；基线快照
`00415e90ab458dfae45f77d28c83f94833a925f3` 仅增添本次的 benchmark instrumentation。
candidate 是相同基线加本次事件位置产品 diff，测量时为未提交工作树。
原始 context、samples、estimates 和日志保存于
`/tmp/dogpaddle-event-address-performance/paired-before`、`paired-after` 及同级日志；
具体 run 目录分别为 `dogpaddle-buffered-sink-run-k9C4a8` 与
`dogpaddle-buffered-sink-run-Q6Bf4J`。

表中列出整轮中位数，单位 ms；变化为 candidate / baseline - 1，负值表示耗时减少。
它描述此次样本，不是跨机器保证。

| 场景 | baseline | candidate | 变化 |
| --- | ---: | ---: | ---: |
| steady_small_admission_drain | 1.674 | 1.728 | +3.2% |
| multi_entry_batch / 64 | 2.313 | 2.243 | -3.0% |
| restore_validation / 64 | 9.635 | 9.731 | +1.0% |
| large_payload_small_event | 16.401 | 16.359 | -0.3% |
| large_payload_multiplicity_target_slicing | 28.796 | 28.573 | -0.8% |
| high_multiplicity_finite_capacity_churn | 13.450 | 13.374 | -0.6% |
| large_unit_entry | 1002.365 | 1065.033 | +6.3% |

小批中位数的 95% bootstrap CI 为旧 `[1.658, 1.693]`、新 `[1.717, 1.740]` ms；
大 entry 为旧 `[998.317, 1015.216]`、新 `[1061.004, 1075.336]` ms，不能宣称这两个
场景没有回归。大 entry 旧样本还包含一次严重高值：mean 为 1261.380 ms，95% CI
`[1000.844, 1777.389]`；新 mean 为 1068.268 ms，95% CI `[1062.779, 1074.847]`。
均值下降不能据此解释为提速；本表保留全部原始样本，用中位数呈现典型耗时。
其他场景的小幅变化与样本抖动应一并考虑，不据此作普遍性能提升结论。

相同 65,536 行空间 fixture：旧目标 1939 pages / 7,942,144 bytes，新目标
2122 pages / 8,691,712 bytes，page size 均为 4096、free pages 均为 0，
**目标空间增加 9.4%**。这与负 rowid 的 varint 成本一致；它没有测量 Store 控制数据
缩减后的总系统空间，不能将两种口径相减并宣称总体省空间。

本次结果证明确定性的控制数据缩减与正式场景可执行，未证明全面无性能回归。
四库恢复与重放行为另由 correctness 和系统验收覆盖；本表只量化 SQLite，未对
PostgreSQL、Doris、ClickHouse 的吞吐或 native memory 作性能保证。
