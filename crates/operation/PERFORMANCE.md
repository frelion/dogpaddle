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

## 三个远程 Sink 共享 Arrow Schema：2026-10-01 构造内存对照

PostgreSQL、ClickHouse、Doris 的 RowCodec 直接持有 `SchemaRef`，删除三套
Layout、ColumnLayout、StorageType，不再另存每列名称及存储类型/nullable 的镜像。
DDL、catalog 校验与行编码仍按原规则从 Arrow Field 派生；此改动不改变持久字节、
目标布局、SQL 或幂等协议，也不减少 PostgreSQL 必要的 typed parameter value。

以下数据来自仓库外临时程序，仅调用公开 `OperationDefinition::construct`。
baseline 为 `e80c0e70a35ce1329f9b057da176a0c37c374782`，candidate 为同一基线加
本次 Sink Schema diff；两个构建与工作区共有的 351 项依赖精确锁定相同版本和来源。
三个后端分别使用 1 列与 1597 列 nullable `Int64`，每列名称 34 UTF-8 bytes；
每版本、每场景运行一个新进程，共 12 个进程，不是重复采样的耗时基准。

DHAT 只追踪预建 Schema、Definition、Config 之后的构造分配，包括 StoreSetup
draft 和 PostgreSQL 缓存 SQL。程序不发布 Store，不访问数据库、网络或 JVM。
存活值在 Operation 与 draft 尚未释放时读取；下表单位为 bytes，箭头为旧 → 新。

| 后端 / 列数 | 存活字节 | 存活块数 | 峰值字节 | 累计分配字节 |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL / 1 | 5740 → 5626 | 33 → 31 | 6843 → 6745 | 18924 → 18554 |
| PostgreSQL / 1597 | 1226172 → 1069650 | 1629 → 31 | 2671949 → 2515443 | 5736169 → 5317759 |
| ClickHouse / 1 | 1901 → 1819 | 20 → 18 | 2289 → 2223 | 4880 → 4670 |
| ClickHouse / 1597 | 120005 → 14587 | 1616 → 18 | 378633 → 273231 | 1013896 → 777534 |
| Doris / 1 | 1809 → 1727 | 19 → 17 | 2253 → 2187 | 4788 → 4578 |
| Doris / 1597 | 119913 → 14495 | 1615 → 17 | 378597 → 273195 | 1013804 → 777442 |

宽表中 PostgreSQL 少 156522 存活字节，ClickHouse 与 Doris 各少 105418 字节；
三者均少 1598 个存活分配块。这是上述字段规格的实际结果，不是任意 Schema 的
固定分配次数公式。所有 12 个进程在释放 Operation 与 draft 后，追踪存活字节和
块数均为零。构造 oracle 只检查返回 Sink 且没有输出 Schema，不是逐行或端到端验收。
这些数字不包括预建输入对象、native heap 或 RSS，不说明行吞吐、目标 I/O 或重连速度。

原始结果与临时程序保存在 `/tmp/dogpaddle-sink-schema-memory/`，包括
`baseline.json`、`candidate.json`、`summary.json` 与 `source-context.json`。
程序 source SHA256 为
`754cfb927ca381f3be5e8e35d71d332255a727863796db9bf247860bab894fae`，
测量 Cargo.lock SHA256 为
`4d35f0727efabafd13d9163bdba29dce19243587b42b00960b5ba5416eafb4cc`。

## EquiJoin 从 Rows 推导纯 presence：2026-10-01 对照

无 residual 的非 Inner 不再声明 key-count 资源，也不读写其 16-byte value；
presence 和 first/last distinct-row 边界由真实 Rows 分区推导。所有 kind 的当前事件
最后一页直接写回已检查的 after 权重，省去重复点读。residual qualifying support
仍保留，Rows key/value 和 Resume 不变；受影响的开发期状态直接重建。
最终生产 Rust（排除私有测试模块）净减少 16 行，主要收益是退休派生持久事实及维护机制。

Apple M5、macOS arm64、Rust 1.96，同一锁文件与原 `equi_join` reference fixture：
1024 个同 key 候选、256 head items、4 MiB 页预算、同步提交；每 case 10 samples，
100 ms warmup、5 s measurement。baseline 为 `7502703`，最终 candidate 为同一
基线加本轮 diff；benchmark 只改上下文说明，fixture、计时与输出行数/方向 oracle 未改。
两个保存的 release binary 按 baseline/candidate/candidate/baseline 顺序运行全部 11 cases。
下表为每轮 Criterion mean 的变化；正值表示变慢，CI 栏表示该轮两侧 95% CI 是否重叠。

| case | 正序变化 | CI 重叠 | 反序变化 | CI 重叠 |
| --- | ---: | :---: | ---: | :---: |
| full_outer_first_last_match | +3.29% | 否 | -2.65% | 否 |
| full_outer_residual_partial_transition | -7.58% | 否 | -13.93% | 否 |
| inner_first_last_match | +1.47% | 否 | -6.73% | 否 |
| inner_residual_full_selectivity | +1.22% | 是 | -29.14% | 否 |
| inner_residual_half_selective | +1.39% | 是 | -2.22% | 否 |
| inner_residual_zero_selectivity | +0.95% | 是 | -4.28% | 否 |
| left_semi_first_last_match | +3.00% | 否 | -7.61% | 否 |
| left_semi_presence_stable | -8.24% | 否 | -2.12% | 是 |
| left_semi_residual_left_presence_stable | -3.48% | 是 | +7.45% | 否 |
| left_semi_residual_partial_transition | -8.68% | 否 | -24.83% | 否 |
| left_semi_residual_presence_stable | -6.12% | 否 | -23.34% | 否 |

先前把事件准入逻辑展开在 driver 内的 candidate，完整 ABBA 的 Inner residual
zero/half/full 分别出现 +12.23%/+7.79%/+4.87% 和 +59.11%/+14.37%/+19.64%，
全部 CI 分离。用同一对原二进制只过滤这三个 case 重测后，变化为
+1.66%/+1.20%/+1.05% 和 +0.07%/+0.02%/-0.39%；仅 half 正序 CI 分离。
最终代码把事件准入恢复为借用同一 partition 的小函数；读写、prefix 生命周期和预算未改。
原始负证据仍保留；结果对运行上下文敏感，尚未分离顺序、前序 workload 与 host 状态
的影响，不能证明内联是原因，也不构成普遍提速、无性能回归或 RSS 改善的承诺。

仓库外相同公开 API 程序核实三个 Inner residual selectivity 的插入/撤回各用四页，
每页消费 256 work items，输出行数、方向和 More/Done 与原版本逐页相同；
新版本每页恰多 44 bytes 剩余预算。单页时钟读数仅作诊断，不作为吞吐对照。
两组无 residual 宽 equality 测试在原 8 MiB/1 MiB 页预算均完成；新版本接受了
4 MiB/512 KiB 的四个首段探测，但四个续段仍拒绝，不代表整个 workload 已适配小页。

完整与过滤的原始 samples、estimates、比较、源码 patch 与 binary SHA 保存在
`/tmp/dogpaddle-join-partition-presence-performance/`；最终对照为
`helper-comparison.json`，原展开版本为 `comparison.json`，过滤对照为
`filtered-comparison.json`。逐页程序与 oracle 保存在 `/tmp/dogpaddle-join-page-witness/`。

## ASOF 原始 row 后缀与借用 winner：2026-10-01 对照

索引只给 equality/order 分段，canonical row 原样作为末尾后缀；winner 只保存
key 和 row 起点，直接借用后缀。每个 key 少 row 内零字节数量加两个终止字节，
同时退役 row 解转义和 winner 的第二份 row Vec。两侧资源改为
`asof_join.left_index/right_index`，受影响的开发期旧布局直接重建；Definition
字节不变。原准备、scalar、输出重建和分页预算保留。最终生产 Rust 净减少 3 行，
本轮主要收益是表示和执行成本。

Apple M5、macOS arm64、Rust 1.96，同一锁文件。baseline 为 `034fbba`，
candidate 为同一基线加本轮源码；原五个 `asof_join` Criterion fixture、计时和
输出行数/方向 oracle 逐字未改。reference profile：256 partitions、1024 versions，
历史修正 128 left rows；wide winner 为 64 KiB 非零 UTF-8、128 left rows。
每 case 10 samples、100 ms warmup、5 s measurement；保存的 release binaries
按 baseline/candidate/candidate/baseline 运行。下表为 Criterion mean 的变化，
负数表示耗时减少；CI 栏为两侧 mean 的 95% CI 是否重叠。

| case | 正序变化 | CI 重叠 | 反序变化 | CI 重叠 |
| --- | ---: | :---: | ---: | :---: |
| global_partition_lookup | -0.76% | 是 | -4.36% | 否 |
| partitioned_lookup | -2.82% | 是 | -4.75% | 否 |
| right_historical_full_rematch | -8.55% | 否 | -9.84% | 否 |
| right_tail_small_rematch | -4.04% | 否 | -1.51% | 否 |
| right_wide_winner_rematch | -57.48% | 否 | -59.50% | 否 |

wide winner 的 mean 为 46.585 → 19.808 ms、49.244 → 19.946 ms；
其它场景收益较小，两个 lookup 正序 CI 重叠，不构成所有 workload 的提速承诺。

`asof_join_resources` 另用 smoke profile 的 512 history rows，按相同 ABBA
顺序运行六个独立子进程，共 24 个进程。两种新增 fixture 的 RHS payload 为
64 KiB 全零 Binary，left payload 为空；lookup 的 RHS history 只有一行。
旧四个 fixture、两个版本的输入、profiler 范围和输出 oracle 相同，仅资源名
随当前布局改变。fixture、seed 和 driving input 在 DHAT 之前建立；以下
Rust allocation 与 peak 只包含完整分页 input 的执行，不含 RocksDB native heap。
每版本的两轮数字完全一致。逻辑状态为两个 map 的 encoded key+value 总长。

| case | 逻辑状态 bytes 旧 → 新 | 累计 Rust allocation bytes 旧 → 新 | Rust peak bytes 旧 → 新 |
| --- | ---: | ---: | ---: |
| left_lookup_history | 51823 → 40532 | 6243 → 5790 | 3213 → 3017 |
| left_lookup_zero_payload | 131282 → 65698 | 598190 → 270214 | 396429 → 199653 |
| right_empty_interval | 51604 → 40402 | 2514 → 2415 | 1135 → 1110 |
| right_historical_interval | 51630 → 40414 | 1180809 → 1139900 | 234948 → 228904 |
| right_null_left_history | 33641 → 25168 | 2572 → 2350 | 1087 → 999 |
| right_zero_payload_rematch | 314539 → 171486 | 850813321 → 402859512 | 1844459 → 1643094 |

两版本六个场景的页数、输出行数和失败重试数均相同。零 payload 历史修正
各为 64 页、1024 输出行、315 次失败重试；累计分配包含这些重试，
不能将它解释为常驻内存。lookup 的 logical bytes 接近减半是这一零字节
payload 的结果，不代表任意编码或数据库文件大小减半。benchmark oracle
只检查行数和方向；逐值关系、rollback/reopen 和坏后缀由 correctness 拥有。
未测量 RSS、native heap、WAL、物理磁盘或跨数据库端到端吞吐。

原始 samples、estimates、24 份 resource 结果、fixture-only baseline patch、
源码和 binary SHA 位于 `/tmp/dogpaddle-asof-row-suffix-performance/`，
对照为 `comparison.json` 与 `resource-comparison.json`。初次格式化的 module 路径错误
与首次编译的借用生命周期错误均已修复，负证据保留；上述对照仅使用修复后的 release binary。

## 空 List 子类型 shape 准入：2026-10-01 对照

保留 canonical row 的全行 framing/逻辑费用预验与原实际 decoder，只在 present
空 List 分支调用已有 shape 计费函数；该函数从 `admit_null_array` 改名为
`admit_array_shape`，算法不变。生产 Rust 净增加 3 行，不增加类型、缓存、codec、
状态或重试机制。canonical bytes、row hash 与资源布局不变；空子类型的形状费用
补计可能缩小合法页，或使最小行报 `BudgetExceeded`。逻辑费不是 Arrow 所有对象的
精确物理成本，不构成严格 RSS 上限；Boolean 等语义仍由实际 decoder 检查。

同一 Apple M5/macOS arm64/Rust 1.96 与锁文件，baseline 为 `fcca35f`，
candidate 为同一基线加本轮修复。原 11 个 `equi_join` Criterion fixture、计时与
输出行数/方向 oracle 未改，reference 为 1024 fanout，每 case 10 samples、100 ms
warmup、5 s measurement。保存的 release binaries 按 baseline/candidate/
candidate/baseline 运行。下表为 mean 变化，负数表示耗时减少；CI 栏为两侧
mean 的 95% CI 是否重叠。

| case | 正序变化 | CI 重叠 | 反序变化 | CI 重叠 |
| --- | ---: | :---: | ---: | :---: |
| full_outer_first_last_match | +2.59% | 否 | +0.12% | 是 |
| full_outer_residual_partial_transition | +0.68% | 是 | -2.03% | 是 |
| inner_first_last_match | +0.32% | 是 | -1.54% | 是 |
| inner_residual_full_selectivity | +3.56% | 否 | -0.37% | 是 |
| inner_residual_half_selective | +0.92% | 是 | +1.23% | 是 |
| inner_residual_zero_selectivity | +2.71% | 否 | +1.02% | 是 |
| left_semi_first_last_match | +0.59% | 是 | +0.62% | 是 |
| left_semi_presence_stable | -1.76% | 是 | -0.66% | 是 |
| left_semi_residual_left_presence_stable | -4.15% | 是 | +2.87% | 否 |
| left_semi_residual_partial_transition | +1.98% | 是 | +0.26% | 是 |
| left_semi_residual_presence_stable | +2.41% | 否 | +0.15% | 是 |

正反序结果混合；五个比较出现耗时增加且 CI 分离，反序最大为 +2.87%。
这一对照不能证明普遍提速或无性能回归。14 个 `equi_join_resources` smoke
fixture 各按 ABBA 在独立子进程运行，共 56 份记录；每 case 的四份完整对象
完全相同，包括 Rust 分配、页数、方向、输出和逻辑状态。原 12 个 fixture
不变；新增两个 fixture 逐字应用到 baseline：8 个候选、4 个 qualifying，
分别为含 NULL 的 512-element List，以及 16 个空 `List<Struct<16 fields>>`。
二者均一页输出 4 个正行；累计/峰值 Rust bytes 分别为 420587/95276、
981883/118804。fixture、seed、input 在 profiler 前建立；benchmark 检查
行数与方向，逐值关系由 correctness 和下述公开 Operation 程序验证。

另用仓库外相同公开 Operation API 程序运行 8 个场景的 ABBA，共 32 个独立
进程；所有进程成功退出，每版本各场景的两份完整记录相同。profiler 包含
begin、step、全部失败重试和事务 drop，成功 output 保留至统计；fixture、
逐值 oracle、完整 Rows/Resume 校验和只读 reopen 在 profiler 外。所有尝试
都回滚，因此这一对照不代表 commit 吞吐或端到端 SQL 性能。

| 场景 | 累计 Rust bytes 旧 → 新 | Rust peak bytes 旧 → 新 | 结果与尝试次数 |
| --- | ---: | ---: | --- |
| 512-element 非空 List | 58810 → 58810 | 44735 → 44735 | 两版本成功后回滚，各 1 次 |
| 16 个空 inner List、16 个 Struct fields | 134758 → 134758 | 90495 → 90495 | 两版本成功后回滚，各 1 次 |
| 1024 个空 inner List、256 个 Struct fields | 105580054 → 116403 | 80152767 → 11967 | 旧版漏费准入 1 次；新版 Budget 失败 9 次，最终 head=1 |
| 128 KiB 前缀后的 late Budget | 6072819 → 6072819 | 673823 → 673823 | 两版本 Budget 失败，各 9 次 |
| 坏 UTF-8 / trailing / nonnull NULL / truncated | 每 case 69494–69606，旧新相同 | 每 case 68502–68507，旧新相同 | 两版本对应错误，各 1 次 |

宽空子类型把小 canonical row 放大为大量 Arrow shape；补计后在 owned
重建前拒绝。80 MB → 12 KB 是不同准入结果的失败路径改善，不是等工作量
吞吐提升。其余七个场景的完整分配和结果记录与旧版完全相同。未测量 RSS、
RocksDB native heap、WAL、磁盘或跨数据库吞吐。

原先尝试过单次递归边解码边计费，生产 Rust 减少 37 行，但实际失败成本更高，
已全部撤回。相同 32 进程的旧/单次解码 ABBA 中，宽空 shape 新版九次 Budget
失败累计分配 877213635 bytes，旧版漏费成功一次为 105580054 bytes；两者
准入结果不同。等失败结果的 late Budget 各九次仍从 6072819/673823 增至
7256499/805343 累计/峰值 bytes，四种晚期坏编码也重复物化了前缀。普通成功
仅节省 64 bytes。最终保留费用预验，拒绝用少 37 行换取明显失败放大。

原始 samples、estimates、56 份 owner resource 记录、32 份最终公开 API 记录、
源码上下文与 binary SHA 保存在 `/tmp/dogpaddle-canonical-row-decoder-performance/`，
完整对照为 `final-comparison.json`。撤回的源码、二进制与 32 份负证据独立保存为
`rejected-single-pass-product5.patch`、`single-pass-witness-runs.json`；临时程序
错误文本 oracle 与 trait-object 编译修复记录也保留，最终对照只使用修复后的
同一程序和成功构建的二进制。

## 直接 Arrow Join 输出：2026-10-02 配对对照

Join 候选与输出现在直接追加到 Arrow builders，退休 owned ScalarValue 行、
canonical 输出副本、两种 Join 的 Scalar 输出容器和 NULL Scalar 缓存。
原 canonical bytes、hash、Store layout 与 Resume 不变；实际 Arrow payload、
nested shape 和 diff buffers 仍逐批准入。ASOF 仅需 row suffix 时共用原 framing
解析器跳过两个索引 header，不重复分配 partition/order。

Apple M5、aarch64 macOS/Darwin 25.6、APFS、Rust 1.96.0；release build。
CPU targets 每场景 10 samples、100 ms warmup、5 秒目标 measurement；
EquiJoin 和 ASOF 都计完整 insert/retract 输入、所有分页和同步提交，ASOF 还计失败
尝试及确定性缩页。fixture、seed、关系 oracle 和 teardown 不计时。资源 targets
使用 smoke profile，不能与 reference CPU 的绝对规模混算。

基线是 `4f9e394b1cfe98c2b4ffe7aab87c4abf6e8dacef`；candidate 是其冻结的
未提交产品 diff。四个 benchmark 源文件、workload 和 oracle 未改。每个二进制由
fresh=false 的 release compiler artifact 复制到独立目录并验证 SHA；运行顺序为
baseline、candidate、candidate、baseline，期间没有 Cargo、native gate 或容器工作。
原始 build/source context、全部 samples/estimates、80 条 resource 记录和 32 次
同源 rollback witness 位于 `/tmp/dogpaddle-arrow-relation-pages-performance-v3`；
`comparison.json` 保留完整数据和 CI，`runs.json` 记录实际退出码。基线复制前的
原 build context 在 `/tmp/dogpaddle-arrow-relation-pages-performance`，复制上下文
保存其来源并再次核验二进制。

| target | baseline binary SHA-256 | candidate binary SHA-256 |
| --- | --- | --- |
| equi_join | `9aad6e7afd073a1bdff021f1da2a44833b4c6811e0b02ffd584ad493d7fd123c` | `9301bef7d528df9e4df65dcb30ffab8885f5e73618edc51bcc55fdb693d5c962` |
| asof_join | `0f715cf8dca9ee189aa79061b495d969155996be7e48518a49aea7849a5b6abb` | `6ec24029076bdef30ccd1016ebd510f8317a4adb3aa7cb9b78bd738b7263f0de` |
| equi_join_resources | `f7fe1fce44114fe78dae6b24e080e5675c102da59087d1beee941fc62c5fe1e5` | `e5ce41ba5da3fdac7eb5308742b287aa6d0f9f979e67e23c2dca1096c2898f8f` |
| asof_join_resources | `ad99c5314e1e444c5a5a95d85f0aca5fbeeec82fba3465c171b027b08351cd8e` | `6299eb540502a61f0f741ea4a519e004c65a562f2cba14c20084b78f02f45ec9` |

下表中位数单位 µs；首轮是 baseline → candidate，反向轮是 candidate → baseline。
变化为 candidate / baseline - 1，负值表示耗时下降；CI 为中位数 95% bootstrap CI。
两轮 CI 的完整端点保存在原始 estimates，表中逐轮标出是否重叠。

| 场景 | 首轮 A / C | 首轮变化 | 反向 A / C | 反向变化 | CI 重叠：首 / 反 |
| --- | ---: | ---: | ---: | ---: | --- |
| equi_join/full_outer_first_last_match | 557.041 / 572.725 | +2.82% | 568.418 / 571.669 | +0.57% | 否 / 是 |
| equi_join/full_outer_residual_partial_transition | 3026.139 / 2984.860 | -1.36% | 3010.894 / 2935.973 | -2.49% | 是 / 是 |
| equi_join/inner_first_last_match | 454.834 / 422.576 | -7.09% | 473.336 / 416.873 | -11.93% | 否 / 否 |
| equi_join/inner_residual_full_selectivity | 643.775 / 551.086 | -14.40% | 659.338 / 555.726 | -15.71% | 否 / 否 |
| equi_join/inner_residual_half_selective | 543.518 / 476.330 | -12.36% | 563.099 / 485.676 | -13.75% | 否 / 否 |
| equi_join/inner_residual_zero_selectivity | 442.179 / 399.592 | -9.63% | 459.894 / 407.704 | -11.35% | 否 / 否 |
| equi_join/left_semi_first_last_match | 443.703 / 362.227 | -18.36% | 446.372 / 364.141 | -18.42% | 否 / 否 |
| equi_join/left_semi_presence_stable | 45.339 / 44.923 | -0.92% | 45.335 / 44.971 | -0.80% | 是 / 是 |
| equi_join/left_semi_residual_left_presence_stable | 45.238 / 47.395 | +4.77% | 47.309 / 45.008 | -4.86% | 是 / 是 |
| equi_join/left_semi_residual_partial_transition | 2922.241 / 2856.582 | -2.25% | 2857.849 / 2877.013 | +0.67% | 是 / 是 |
| equi_join/left_semi_residual_presence_stable | 43.170 / 43.333 | +0.38% | 43.430 / 43.352 | -0.18% | 是 / 是 |
| asof_join/global_partition_lookup | 50.996 / 46.508 | -8.80% | 50.745 / 50.374 | -0.73% | 否 / 是 |
| asof_join/partitioned_lookup | 3111.243 / 2946.148 | -5.31% | 3111.060 / 2922.737 | -6.05% | 是 / 否 |
| asof_join/right_historical_full_rematch | 166.449 / 145.018 | -12.88% | 164.998 / 145.793 | -11.64% | 否 / 否 |
| asof_join/right_tail_small_rematch | 68.927 / 64.998 | -5.70% | 68.925 / 68.237 | -1.00% | 否 / 是 |
| asof_join/right_wide_winner_rematch | 23904.519 / 13393.261 | -43.97% | 28532.351 / 14513.309 | -49.13% | 否 / 否 |

纯 FullOuter 首轮 +2.82% 且 CI 不重叠，反向 +0.57% 且重叠；不能宣称全面无
CPU 回归。其余小幅、异号或 CI 重叠的变化不作普遍提速结论。Inner residual 和
ASOF 历史/宽 winner 的两轮改善均有同向且不重叠的 CI，但仍仅描述本机样本。

资源表单位 bytes，A / C 分别为基线与 candidate；累计包含完整 driving input 的
所有成功与失败尝试。每版本两轮 resource JSON 完全一致；两版本关系输出数和
记录到的逻辑 Store 条目数与字节统计一致。allocator 只覆盖 Rust global allocator，排除 fixture、
seed、input Arrow、RocksDB/native heap；没有 RSS 采样。

| 场景 | 累计 A / C | 峰值 A / C | 页 A / C |
| --- | ---: | ---: | ---: |
| equi_join_resources/selectivity_zero | 59951 / 13119 | 31784 / 8856 | 1 / 1 |
| equi_join_resources/selectivity_half | 101711 / 20207 | 31784 / 9672 | 1 / 1 |
| equi_join_resources/selectivity_full | 141647 / 24559 | 47736 / 10952 | 1 / 1 |
| equi_join_resources/wide_zero | 2113231 / 1564895 | 1322904 / 1054008 | 1 / 1 |
| equi_join_resources/wide_full | 4235759 / 2604175 | 1597208 / 1055080 | 1 / 1 |
| equi_join_resources/wide_candidate | 798666 / 400474 | 397403 / 264555 | 1 / 1 |
| equi_join_resources/page_boundary | 517399 / 79839 | 152174 / 37691 | 2 / 2 |
| equi_join_resources/large_fanout | 1546862 / 246278 | 188169 / 37745 | 4 / 4 |
| equi_join_resources/batch | 1502507 / 1502219 | 292265 / 292121 | 2 / 2 |
| equi_join_resources/computed_keys | 3000400 / 3000112 | 159808 / 159664 | 2 / 2 |
| equi_join_resources/full_outer_state | 276573 / 54405 | 72885 / 21467 | 1 / 1 |
| equi_join_resources/full_outer_wide_state | 64377747 / 53033459 | 2183334 / 1628849 | 8 / 8 |
| equi_join_resources/nested_list_values | 420587 / 129679 | 95276 / 57024 | 1 / 1 |
| equi_join_resources/nested_empty_child_shape | 981883 / 46127 | 118804 / 13540 | 1 / 1 |
| asof_join_resources/left_lookup_history | 5790 / 4766 | 3017 / 2078 | 1 / 1 |
| asof_join_resources/right_historical_interval | 1139900 / 149156 | 228904 / 56664 | 2 / 2 |
| asof_join_resources/right_empty_interval | 2415 / 2271 | 1110 / 966 | 1 / 1 |
| asof_join_resources/right_null_left_history | 2350 / 2206 | 999 / 855 | 1 / 1 |
| asof_join_resources/left_lookup_zero_payload | 270214 / 135902 | 199653 / 132677 | 1 / 1 |
| asof_join_resources/right_zero_payload_rematch | 402859512 / 196376802 | 1643094 / 2298688 | 64 / 32 |

EquiJoin 全部完整输入的 current Rust heap 都回到零；输出 Arrow capacity 字节可能
不同，原始 resource 记录保留了该差异。nested empty child 累计 -95.30%、峰值
-88.60%；宽 FullOuter 累计 -17.62%、峰值 -25.40%，均保持原页数、输出行数及所记录状态统计。
ASOF 宽 rematch 同一 4 MiB 逻辑预算下页数 64 → 32、失败尝试 315 → 124，累计
-51.25%，但峰值 1,643,094 → 2,298,688（+39.90%）：每页容纳更多真实输出。
仍逐输出计 winner payload，未删除真实 Arrow 费用；不能据此承诺全面降低峰值或
进程 RSS。其他五个 ASOF 资源场景的失败尝试都为零，两版本相同。

另一个配对 witness 使用同一份临时 Rust 源码与独立完整输出/rollback oracle，
源码 SHA-256 为 `950ae40ed574fa5e9cf3d74c14a464c88b0c12c0d6884066ea3a3ae7076ff5c5`。
它不是通用实验框架或产品 API；成功和拒绝的每次尝试都回滚，再比较完整 raw left
Rows、空 right Rows、原 Resume 字节并 readonly reopen。成功输出逐字段匹配源 Arrow。
每版本每场景两次，32 次全部成功；下表各版本两次测量相同。

| witness | 结果 A / C | 尝试 A / C | 累计 A / C | 峰值 A / C | current A / C |
| --- | --- | ---: | ---: | ---: | ---: |
| nonempty | 成功并回滚 / 成功并回滚 | 1 / 1 | 58810 / 35353 | 44735 / 13511 | 6819 / 7131 |
| small_empty | 成功并回滚 / 成功并回滚 | 1 / 1 | 134758 / 47801 | 90495 / 13275 | 5931 / 9327 |
| shape_gap | 预算拒绝 / 成功并回滚 | 9 / 1 | 116403 / 566249 | 11967 / 147531 | 6 / 105711 |
| late_budget | 预算拒绝 / 成功并回滚 | 9 / 1 | 6072819 / 3044729 | 673823 / 1331239 | 6 / 658395 |
| utf8 | 同类语义拒绝 / 同类语义拒绝 | 1 / 1 | 69505 / 67809 | 68503 / 66975 | 36 / 36 |
| trailing | 同类语义拒绝 / 同类语义拒绝 | 1 / 1 | 69507 / 67811 | 68504 / 66976 | 37 / 37 |
| null | 同类语义拒绝 / 同类语义拒绝 | 1 / 1 | 69606 / 67910 | 68507 / 66979 | 76 / 76 |
| truncated | 同类语义拒绝 / 同类语义拒绝 | 1 / 1 | 69494 / 67798 | 68502 / 66974 | 26 / 26 |

shape_gap（256 child fields、1024 outer elements）和 late_budget（60,000 UInt64
elements、128 KiB prefix）现在在 4 MiB 内合法成功。两版本做了不同的工作，不能
把该表的 peak/current 或耗时变化当相同拒绝路径的回归/提速。nonempty/small_empty
的 retained 输出 heap 略增，current 不是泄漏断言；四种 malformed 的 current 是仍
持有的错误文本 outcome String。所有源码、lock、binary SHA 和实际退出码由 paired-witness context
与日志保存；基线 witness 的全部产品源逐字节核对为 4f9e394。

两个先前实现没有交付：S0 canonical 输出缓冲使宽 FullOuter 累计分配 +60.33%，
宽 ASOF +131.34%；S1 虽整批准入后再复制，仍有宽 FullOuter +60.19%，ASOF
历史/宽 winner CPU +29%～49%（两轮 CI 不重叠）。原始负结果保留在
`/tmp/dogpaddle-arrow-relation-pages-performance` 和 `-performance-v2`。
最终直接 Arrow 路径删除这些额外复制和只取后缀时的 allocating index decode；
没有通过改 workload、放宽 framing 或恢复 phantom 费用来抹掉负结果。
correctness 另证明更紧预算仍拒绝并回滚真实 payload，以及单候选可重试和完整结果；
whole-workspace gate 和 native SQL 组合验证分别记录实际执行，不能把 smoke/test
mode 当 release 性能证据。

## 2026-10-02 Aggregate 参数拥有状态地址

本轮删除统计、layout、slot 到参数的三张反查表，以及重复的调用 ADT；唯一参数直接拥有可选角色地址，调用仍使用 `AggregateCall<usize>`。生产代码净减 96 行，持久 `GroupState` codec、dense 地址、entries 分区与逻辑费用不变。跨参数同时非法时首个错误的顺序不承诺；逐事件校验、Store poison 与整页回滚仍保留。

性能原始证据位于 `/tmp/dogpaddle-aggregate-argument-owner-performance-v2`：同一最终 owner harness 编译 A/C，依次 reference A1/C1/C2/A2，四次实际退出码均为 0。基线只将四个 Aggregate 生产文件还原为 `1cdf5e0`，构建后逐字节恢复当前实现；共享 harness、lock、其余源码 SHA 均核对相同。Cargo artifact 均为 release、fresh=false，binary、源码、host、raw samples、估计值与命令保存在 source-context/runs/comparison JSON。旧九个 workload 和计时界不变；新增 64 个 COUNT 参数 ×512 行及 2 个统计/64 个极值参数 ×512 行，两次 apply/commit 回到 seed。每次 apply 仍使用 64 MiB 逻辑预算。

下表为 median 的相对耗时，正数表示增时；“CI重叠”分别列两轮 95% median 区间是否相交。不能将区间相交解释为精确无回归。

| case | C1/A1 | C2/A2 | CI重叠：第一/第二轮 |
| --- | ---: | ---: | --- |
| `distinct_layout_min_max` | -2.81% | +4.29% | 否/否 |
| `extrema_retraction` | +1.35% | -1.38% | 是/是 |
| `many_count_arguments_one_turn` | +1.65% | -0.05% | 否/是 |
| `many_existing_groups_one_turn` | +2.94% | -0.41% | 是/是 |
| `many_new_groups_one_turn` | -4.74% | -0.23% | 是/是 |
| `many_rows_one_turn` | +0.27% | +3.34% | 是/是 |
| `repeated_extrema_key_one_turn` | +1.69% | -0.06% | 否/是 |
| `repeated_min_max` | -1.37% | -1.90% | 是/是 |
| `same_group_high_multiplicity` | -1.29% | -4.29% | 是/是 |
| `sparse_statistics_many_extrema_one_turn` | +0.15% | -0.27% | 是/是 |
| `zero_net_group_extrema_cycles_one_turn` | +1.59% | -0.99% | 否/否 |

没有一致的热运行提速或增时结论。COUNT-heavy 第一轮增时 1.65% 且 CI 不重叠，第二轮 −0.05% 且重叠；distinct layout 第一轮 −2.81%、第二轮 +4.29%，两轮均不重叠，方向相反。净删代码不等于加速；上述负结果没有删去。

临时私有 `size_of` witness 使用实际 Rust 1.96/aarch64 类型，执行退出码 0，随后移除临时 module。记录位于 `/tmp/dogpaddle-aggregate-argument-owner-performance/compiled-sizes.log` 和 `compiled-sizes-context.json`。旧 argument 80 B、新 argument 136 B；旧 statistic/layout/方向 slot 分别 16/48/8 B，调用 enum 新旧均为 16 B。下表只比较每个唯一参数的绑定记录及旧反查记录，不包括固定数组 header、表达式内部 heap、Arc 所指的 Field/表达式堆对象、统计状态或 RSS。

| 参数角色 | 旧绑定记录 | 新绑定记录 | 变化 |
| --- | ---: | ---: | ---: |
| COUNT-only | 96 B | 136 B | +40 B |
| MIN-only | 136 B | 136 B | 0 B |
| MIN/MAX | 144 B | 136 B | −8 B |
| 统计 + MIN/MAX | 160 B | 136 B | −24 B |

全部结果包含完整 Atomic apply 和同步 `Transaction::commit`；fixture、绑定、完整输出 oracle 与返回输出的释放不计时。新增 NULL/count 与 extrema 小样例、512 行批次检查全部输出列及 diff。新 case 测热运行的记录跨度和稀疏角色遍历，未隔离冷缓存延迟；没有测绑定构造、大量新 group 的统计初始化、allocator 或 RSS，不能据此声称这些方面改善。持久状态和历史保留不增大；COUNT-only 冷态记录增长是本轮保留的明确代价。

## 2026-10-02 Sink 只保留输入与目标进度

本轮删除持久 Prepared、负事件 ID 清单、准备后的恢复重建和普通 drain 的第二次 Store 提交。SQLite/PG 在目标事务中以单例 F 表示下一个未提交事件；ClickHouse/Doris 以 birth/death 的绝对事件位置作 occurrence version。共享协议不再保存第二份执行计划。按七个产品 crate 的 Rust src 与 bridge main Java 统计，排除完整 `cfg(test)` 项和专用测试文件，基线 `49dbde9` 的 33,976 行变为 34,052 行：共享接口/协议净减 310 行，适配器的事务、目录、可见性与 deadline 检查净增 386 行，全产品 **净增 76 行**。本轮减少概念和一次持久提交，不宣称总代码减少。

SQLite owner benchmark 原始证据位于 `/tmp/dogpaddle-occurrence-delivery-performance`。在同一 Rust 1.96/aarch64、lock 和 filesystem 下保存 baseline/candidate release 二进制，reference 顺序为 A1/C1/C2/A2，四次实际退出码均为 0。各七个 case、输入、profile、预热与测量配置相同；基线执行原准备协议，candidate 执行新接口。常规 case 仍计完整 admission、全部同步 Store commit、目标交付与结算，恢复 case 仍计 reopen/bind、完整 buffer 恢复校验及首次交付/结算；没有把剩余业务搬到计时外。初始化与完整关系 oracle 仍在计时外。构建、source/binary/lock SHA、host context、命令、raw samples、估计值与实际退出码保存在 build-context/runs/comparison JSON。原生测试结束后停止本轮自建数据库并恢复原先停止的 Podman VM，再串行测量；未测 allocator 或进程 RSS。Clippy 的三处语法/局部 annotation 修复后重新构建，最终 benchmark 二进制与原实测 candidate 逐字节相同；`/tmp/dogpaddle-occurrence-delivery-performance-v2/measured-binary-postcondition.json` 保留最终源码与 binary SHA 闭环，未把原运行重新标成新测量。

下表为 median 相对耗时，负数表示耗时下降；“CI重叠”分别列两轮 95% median 区间是否相交。

| case | C1/A1 | C2/A2 | CI重叠：第一/第二轮 |
| --- | ---: | ---: | --- |
| `high_multiplicity_finite_capacity_churn` | -16.01% | -12.55% | 否/否 |
| `large_payload_multiplicity_target_slicing` | -19.50% | -25.38% | 是/否 |
| `large_payload_small_event` | -16.38% | -18.29% | 否/否 |
| `large_unit_entry` | -30.36% | -30.57% | 是/否 |
| `multi_entry_batch/64` | +22.93% | -5.15% | 否/是 |
| `restore_validation/64` | +0.99% | +0.44% | 是/是 |
| `steady_small_admission_drain` | -36.50% | -43.13% | 否/是 |

高 multiplicity 与大 payload 单事件两轮均改善且 CI 不重叠；其它改善仅描述本机样本。多 entry 合批首轮明确增时、反向方向相反，不能删去首轮负结果，也不能宣称全面无回归。恢复校验仍扫描原完整保留输入，不作恢复提速结论。每个普通交付少一次真实 Store commit，target 多了 F 的原子更新；这不是无代价的机械优化。

warehouse lookup 的单独原生资源对照位于 `/tmp/dp-ch-history-lookup-_1r2jfn0`。使用固定 ClickHouse 25.8.31.9、相同 UInt64-version 物理布局与完整逻辑行，在原 64 MiB/five-second 配额下比较旧 live-only lookup 和新 MAX(version)+live IDs：hot history、64 行 hash collision、8 个 64 KiB 宽行的四次完整查询及独立完整结果 oracle 均成功。新增历史 MAX 让 JOIN build rows 分别从 1→100,001、64→16,704、8→40；两个查询的 SelectedRows/Bytes 相同，不能把有界返回解释成有界扫描或宣称新增物理扫描字节已被测得。宽行首次 candidate 查询峰值约 59.66 MiB，仅剩约 4.34 MiB 配额余量；顺序预热明显影响峰值，单组 ABBA 不支持稳定内存或耗时比例。

固定的更强宽行 history17 保留为负证据：原完整历史 payload control 先触发 64 MiB 拒绝；另一个明确缩小 scope 的独立 lookup 对照保留公共完整 bag 与 domain/count/hash 检查，旧 lookup 和新 lookup 均以实际 241 退出。后者位于 `/tmp/dp-ch-wide17-lookup-5xg5owz4`，不是整体通过，也不是 candidate 独有回归；没有加配额或继续调参掩盖失败。MAX 含删除历史和 tombstone 保留会随关系历史增长，返回最多 1024 IDs 不限制数据库查询内存。

实际产品适配器证据位于 `/tmp/dp-prefix-warehouse-product-5qp72yu_`：ClickHouse/Doris 六项测试全部退出 0，包括 signed-ID/full-row rebinding 拒绝、历史 lookup、宽 SQL 事务分片，以及已提交长前缀重新切短/切后缀的完整 FIFO oracle。真实 PG 17.10 gate 位于 `/tmp/dp-occurrence-postgres-c5x9ise1`，退出 0，覆盖缺失/错布局/越界 F、实际 UNIQUE 回滚、未知提交重开、16,385 行分批和两连接锁后重读；角色默认 Repeatable Read 已读回，adapter 显式 ReadCommitted。

Doris 4.1.3 的独立真实发布故障证据保存在 `/tmp/dogpaddle-doris-occurrence-native`，同 txn/label 的后续状态回读保存在 `/tmp/dp-podman-native-6qytk18x/commands.json`：standalone INSERT 实际 COMMITTED 被拒，随后同 txn/label 发布为 VISIBLE；显式 VALUES 事务 COMMIT 实际客户端超时，随后同 txn/label VISIBLE 且完整关系正确。没有观测到最终显式 COMMIT 的 OK COMMITTED envelope，不能声称覆盖该分支；该故障 harness 也不证明网络故障与产品 Store settlement 的组合。实际产品 strict VISIBLE parser 对空、PREPARE、COMMITTED 与 malformed envelope 拒绝。同步 mysql 驱动的五秒是接受预算及 socket 空闲 timeout，不能保证协议 read、连接 Drop 或产品停止在五秒内返回；未知或迟到结果保留输入并在重开后重新强读。单 FE/BE 验证不代表 follower/failover 或全规模资源保证。
