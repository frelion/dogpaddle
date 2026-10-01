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

## 单 Queue CDC bootstrap：2026-10-01 reference 对照

已封口快照现在直接消费原 input Queue，删除 spool → published 的逐 entry 搬运。
对 N 个 bootstrap entries，确定性地消除 N 次逻辑 payload 读取、N 次 payload 重写和
N 笔 publication 事务；消费及其事务仍保留。本 owner benchmark 对每笔事务同步提交，
但生产 Flow 的维护事务可共享 WAL barrier，不能推导出减少 N 次 WAL sync。
封口与原真实 Delivery 的数据、
checkpoint 同事务提交，额外只更新 phase，不把整个快照放进一个大事务。

`cdc_bootstrap` 的历史 case 名 `publish` 保留用于配对，它计时一次只读 restore、
全部 front 读取/Change 解码和消费者同步提交。旧版还计入每条 input 的搬运、
publication 同步提交及维护 ACK；新版直接读取原队列。构造、encoding/seed、
关系及持久状态 oracle、teardown 都不计时。没有外部 connector I/O，不能解释为
capture、真实 ACK、Flow 计算或端到端 CDC 吞吐的提速。

窄场景为 32 entries × 256 rows，同步提交由 64 次降为 32 次；wide 场景为
1 entry × 32,768 rows，由 2 次降为 1 次。reset 仍是每事务 discard 至多 256
entries；257-entry 场景两次提交，wide reset 一次提交，算法没有改变。

```bash
DOGPADDLE_PERF_PROFILE=reference \
  DOGPADDLE_PERF_ROOT=/absolute/path/to/results \
  cargo bench --locked -p dogpaddle-operation --bench cdc_bootstrap
```

Apple M5、aarch64 macOS/Darwin 25.6、APFS、Rust 1.96.0，release build；每场景
10 samples、20 ms warmup、5 秒目标 measurement，前后顺序执行全套八个场景。
两轮测量期间没有并发构建、容器验收或 worktree 归档；MySQL VM 在测量前已停止。
baseline 是干净的 `e97f63bcea927df1f735e40a4bbd48767f0e2ed8`，candidate 是同一
基线加本次单 Queue diff，测量时未提交。workload、schema-bound entry 编码和
结果 oracle 相同；恢复后的终态分别为 Streaming 和空 Sealed，后者直到首份
成功的 streaming record 才写入 Streaming。

原始 context、samples、estimates 与日志保存在
`/tmp/dogpaddle-sealed-source-performance/before`、`after` 及同级日志；run 目录
分别为 `dogpaddle-cdc-bootstrap-run-mWyji0` 和 `dogpaddle-cdc-bootstrap-run-yVT2ua`。
下表是整轮中位数，单位 µs；变化为 candidate / baseline - 1。

| 场景 | baseline | candidate | 变化 |
| --- | ---: | ---: | ---: |
| postgres / publish | 2404.395 | 1042.125 | -56.7% |
| postgres / publish_wide | 361.173 | 99.575 | -72.4% |
| postgres / reset | 421.516 | 408.106 | -3.2% |
| postgres / reset_wide | 61.093 | 55.226 | -9.6% |
| mysql / publish | 2533.787 | 1043.424 | -58.8% |
| mysql / publish_wide | 328.245 | 102.023 | -68.9% |
| mysql / reset | 420.982 | 411.250 | -2.3% |
| mysql / reset_wide | 67.842 | 56.532 | -16.7% |

publication 中位数的 95% bootstrap CI，单位 µs：

| 场景 | baseline CI | candidate CI |
| --- | ---: | ---: |
| postgres / publish | [2338.506, 2560.304] | [1016.677, 1063.768] |
| postgres / publish_wide | [336.104, 392.222] | [95.185, 108.625] |
| mysql / publish | [2441.670, 2701.947] | [1024.758, 1092.433] |
| mysql / publish_wide | [325.856, 343.999] | [97.247, 110.177] |

本次八个场景未见耗时回归；reset 变化仍应结合样本与恢复读取成本理解，不能从
未变的删除算法推出普遍 reset 提速。这里没有直接测量 WAL、总磁盘、heap 或 RSS，
也未测量新增 phase 控制读写对常规 streaming 吞吐的影响。

新增私有行为证据覆盖封口事务回滚、restore 前的持久可见性、隐藏 input 消费拒绝、
大于 64 MiB 的 Sealed backlog、data/heartbeat 背压零写、首份 Streaming phase
与 checkpoint 回滚、较小 bootstrap 配额下的 Streaming restore，以及提前启动
源失败不改 input。真实 PostgreSQL CDC/SQL 与 MySQL CDC gate 均通过，包括
terminal commit-before-ACK、消费者回滚、部分快照 reset 和重开的有序后继事件；
MySQL 还证明已封口 input 无需 poll/record 即可在一次消费者事务中提交。

三个独立 Agent 分别复审了完整 diff 的抽象、恢复/事务与性能/资源风险；确认的问题
已修复，并通过 `cargo xtask check` 与 `cargo build --workspace --locked`。
唯一 input resource 是开发期 v1 layout 变化，受影响状态必须重建。提前 poll
streaming 可能更早遇到源错误并 fail-stop，原封口 input/checkpoint 保留；
具体恢复与容量边界见 [CDC 契约](docs/cdc.md)。

## ASOF 页内 winner 复用：2026-10-01 reference 对照

历史修正的 before/after winner 每页各严格解码一次，每个 left 只解码一次供
`-old,+new` 使用。复用只减少执行和临时分配；每条输出仍预扣原有的完整嵌套值与
Arrow 重建费用，Scalar 复制与最终 Arrow 构造没有免账，不靠增加页大小取得收益。
没有新增持久缓存、输出表示或执行层，索引、Resume 与最后一页 RHS 提交边界不变。

baseline 产品为 `a3dc2dc`，测量快照 `b4cfb68` 只增加相同的 ASOF benchmark
instrumentation。candidate 为 `35e9662` 加当前 ASOF diff。Apple M5、aarch64
Darwin 25.6、APFS、Rust 1.96.0、release；顺序执行，无并发 Cargo、容器或归档。
每个 Criterion case 为 10 samples、100 ms warmup、5 s measurement；包含完整正负
输入、确定性缩页重试和每页同步 commit，fixture/seed/oracle 在计时外。
reference 宽 winner case 为 128 个 left 与 64 KiB RHS 字符串。

以下为整轮中位数，单位 ms；变化为 candidate / baseline - 1。

| 场景 | baseline | candidate | 变化 |
| --- | ---: | ---: | ---: |
| partitioned_lookup | 3.5538 | 3.2342 | -9.0% |
| global_partition_lookup | 0.0533 | 0.0521 | -2.3% |
| right_tail_small_rematch | 0.0712 | 0.0819 | +15.0% |
| right_historical_full_rematch | 0.3241 | 0.1932 | -40.4% |
| right_wide_winner_rematch | 63.0524 | 47.8234 | -24.2% |

宽 winner 中位数 95% CI 为旧 `[61.6964,64.8942]`、新 `[46.0040,51.0359]` ms。
小尾部场景为旧 `[0.0705,0.0721]`、新 `[0.0690,0.1043]` ms，candidate 波动较大；
保留其较慢的中位数，不从区间重叠宣称全面无回归。

独立进程 `asof_join_resources` 的 4096-left 历史修正，页数均 16、输出均 8192、
失败重试均 0，持久索引 entry 数和编码逻辑字节相同。Rust allocator 分配次数由
119,289 降为 45,249（-62.1%），累计分配字节由 13,948,191 降为 9,574,155
（-31.4%）；peak 为 238,963 → 239,044 bytes，基本持平。单 lookup 累计分配不变，
peak 为 3113 → 3213 bytes；空区间和 NULL-order 场景各项相同。
这些数字排除 fixture/input/seed 与 RocksDB native heap，未测 RSS，不能解释为
进程内存下降 31.4%。

原始证据位于 `/tmp/dogpaddle-arrow-join-performance/`：Criterion baseline 使用
`before-retried/dogpaddle-asof-join-run-8K1qsB`，candidate 使用
`after/dogpaddle-asof-join-run-AjzmE0`；resources 使用
`before/dogpaddle-asof-join-resources-run-AQT7g5` 与
`after/dogpaddle-asof-join-resources-run-FvfJOR`。初次未支持缩页的宽 winner 失败记录
保留于 `before-asof.log`，不混入此表。

原 Arrow JoinOutput 原型因净增代码、宽行最小原子退化与嵌套 List scratch 漏账而
淘汰，备份位于 `/tmp/dogpaddle-arrow-join-rejected`；其测试不作为本候选证据。
当前候选经过三份独立完整 diff 审查，修复重复输出的重建准入缺口，并以公共宽 winner
缩页与输出 payload 证据防止再次漏账；`cargo xtask check` 和工作区构建通过。

冻结到 `fa7f43d` 后又顺序运行全部 ASOF reference 与两个 Join resource target。
采集时 tracked diff 为空，唯一 untracked 文件为用户历史提案；源码状态证据为
`frozen-source.patch` 与 `frozen-source-status.txt`。五个场景的冻结结果如下；单位为 ms，
变化相对上表 baseline，区间为冻结版本的 95% CI。

| workload | 冻结中位数 | 95% CI | 变化 |
| --- | ---: | --- | ---: |
| global single lookup | 0.0565 | `[0.0526,0.0567]` | +6.0% |
| partitioned single lookup | 3.2281 | `[3.1874,3.2943]` | -9.2% |
| historical correction | 0.1909 | `[0.1887,0.1930]` | -41.1% |
| small tail | 0.0729 | `[0.0722,0.0734]` | +2.4% |
| wide winner | 48.4836 | `[47.5342,48.6963]` | -23.1% |

历史修正资源计数与上表 candidate 完全相同。global 单次查找与 baseline 区间重叠，
不能据此声称稳定回归；小尾部仍略慢，不撤销原较慢样本。原始结果保存在同一证据目录的
`frozen/dogpaddle-asof-join-run-tNQdYf` 与
`frozen/dogpaddle-asof-join-resources-run-arOD0T`。

EquiJoin 的 12 个 reference resource case 全部完成，包括此前被错误链阻断的
`full_outer_wide_state`：257 个 64 KiB 候选、32 个提交页、256 个输出。
修复 `BudgetExceeded` 的标准 source 后，runner 能按既有规则缩页；此前失败发生在
首个 256-item 尝试，不是已证明最小一项不能执行。结果位于
`frozen/dogpaddle-equi-join-resources-run-vdrMOy`。该 target 直接调用 Operation，
不编码 Flow Frame；64 KiB 行加 canonical framing 仍可能超过 Flow 的 64 KiB Resume
上限，这项结果不能充作该宽行 Flow workload 的成功证据。公共 Flow 回归使用约
8 KiB 嵌套候选，已证明旧错误包装失败、修复后缩页完成，并覆盖逐轮重开。
