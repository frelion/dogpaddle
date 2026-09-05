# Change × AppendLog 性能报告（2026-08-27，重构前存档）

> 此报告使用已经删除的 benchmark 协议和 workload epoch，仅作为历史设计记录。它不转换为当前格式，
> 也不得与 `dogpaddle-perf-context` 体系产生的结果直接比较。

## 结论摘要

在本机 Apple M5、APFS、Rust 1.96、release benchmark、MDBX durable sync 下：

- 主 anchor 的 sparse projection 只选择 3/16 个业务列、11.79% Arrow array bytes；相对 full
  replay 的跨进程中位加速为 **1.97×**，45/45 个配对样本获胜。
- Change encode + AppendLog durable append 相对预编码 append 多耗时约 **44.7%**；编码成本不可忽略。
- 把事务从 1 Change/commit 增加到 128 Changes/commit，生产吞吐从 3.34M 提升到
  **9.91M rows/s（2.96×）**。
- 1 row/Change 只有约 95K rows/s；1024 rows/Change 达到 **5.56M rows/s**。完整 IPC Stream 的
  固定成本说明生产端不应逐行持久化 Change。
- 受控异构 endurance 的完整串行协议达到 **5.53M rows/s**；30 cycles 内 producer/full consumer/
  projected consumer/truncate 的跨进程 p50 分别为 0.717/0.288/0.170/0.105 ms。
- 3 个 endurance 进程经过 6 次周期 reopen 后 checksum 完全一致；最终 MDBX allocated amplification
  为 **1.12× retained encoded bytes**。

绝对时间在连续运行时跨进程波动较大，主 anchor 各场景的 run-median spread 为 42%–80%。因此本次
绝对吞吐适合描述当前机器量级，不适合直接设回归阈值；counterbalanced 配对比值明显更稳定。

## 环境与口径

| 项目 | 值 |
| --- | --- |
| revision | `3c89e184907904537ee938acbda8da3a1239878b`，clean |
| CPU | Apple M5，10 logical workers |
| OS / filesystem | macOS arm64，APFS `/dev/disk3s5` |
| rustc | 1.96.0 |
| Cargo profile | `bench`，debug assertions disabled |
| Store | MDBX durable sync，固定 `/private/tmp` APFS 路径 |
| 常规样本 | 3 个 clean 独立进程；每 case 3 warmups + 15 samples |
| Endurance 样本 | 3 个 clean 独立进程；每模式 30 cycles |

常规 target 完整执行 41 个单轴 case。运行期间仓库从等价 dirty tree 提交为上述 revision，因此提交前
两轮原始数据不进入本报告统计。

## 主 Anchor

`mixed_event_16` 每 row 有 16 个业务顶层列；加 `$dogpaddle.diff` 后为 17 个物理顶层列。
每个 Change 1024 rows，每事务 32 Changes，每 sample 8 个 durable transactions，共 262,144 rows、
256 Changes 和 80.03 MiB 输入 IPC。sparse projection 选择 3 列，投影后 IPC 为 10.98 MiB/sample。

下表的时间是 3 个独立进程各自 15-sample median 的中位数；范围也是这 3 个 run median 的范围。

| 场景 | 时间中位数 | run 范围 | rows/s |
| --- | ---: | ---: | ---: |
| preencoded append + durable commit | 26.56 ms | 15.87–26.96 ms | 9.87M |
| encode + append + durable commit | 37.71 ms | 23.86–42.03 ms | 6.95M |
| warm full replay | 14.98 ms | 8.81–20.77 ms | 17.50M |
| warm sparse projected replay | 7.81 ms | 4.03–9.65 ms | 33.57M |
| reopened-first full replay | 22.02 ms | 21.53–32.63 ms | 11.91M |
| projected decode → re-encode → output + cursor durable | 14.38 ms | 13.14–23.09 ms | 18.23M |

配对结果比绝对时间稳定：

- preencoded/integrated 时间比为 0.691（run 范围 0.626–0.694），即集成编码路径约 1.447× 时间；
- full/projected replay 时间比为 1.971×（run 范围 1.905–2.113），projected 45/45 获胜。

## 投影收益

数字是 full/projected 的跨进程中位加速；“bytes”包含始终保留的 diff Array。identity 是噪声对照。

| Persona | diff-only | key-only | sparse | payload-only | dense | identity |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mixed_event_16 | 2.90× | 2.92× | 1.86× | 1.51× | 1.25× | 1.00× |
| blob_event_4 | 2.86× | 2.62× | 2.18× | 0.98× | 2.22× | 0.98× |
| nested_event_8 | 3.50× | 3.35× | 1.80× | 1.59× | 1.30× | 0.98× |

除 blob payload-only 和 identity 外，所有投影 case 均 45/45 获胜。blob dense 虽选择 3/4 列，实际只
读取 9.72% array bytes，因此仍有 2.22× 收益；payload-only 只选 1/4 列却读取 93.05% bytes，几乎
没有收益。这证明性能判断必须看 array-byte selectivity，不能只看列数。

分页没有改变结论：1-entry、约 1 MiB、约 16 MiB page 的 projected 加速分别为 1.97×、1.95×、
1.87×，三组均 45/45 获胜。

## 分批与数据形状

### Changes / durable transaction

固定 256 Changes、262,144 rows 和 80.03 MiB 输入：

| Changes/tx | transactions | encode+append+commit | rows/s |
| ---: | ---: | ---: | ---: |
| 1 | 256 | 78.40 ms | 3.34M |
| 8 | 32 | 42.04 ms | 6.24M |
| 32 | 8 | 32.24 ms | 8.13M |
| 128 | 2 | 26.45 ms | 9.91M |

durable commit 的摊销非常明显。32→128 仍有约 22% 吞吐提升，但事务也扩大到约 40 MiB 输入，生产
配置需要结合延迟与失败重试成本选择，而不是只追求最大 batch。

### Rows / Change

固定 32,768 rows 和 2 次 durable commit：

| rows/Change | Changes/sample | encode+append+commit | rows/s |
| ---: | ---: | ---: | ---: |
| 1 | 32,768 | 344.71 ms | 0.095M |
| 64 | 512 | 10.40 ms | 3.15M |
| 1024 | 32 | 5.89 ms | 5.56M |
| 16,384 | 2 | 6.16 ms | 5.32M |

1024 附近已基本摊平 IPC 固定成本；继续扩大到 16,384 没有可见收益。当前数据支持把 1024 作为默认
物理批次，而不是逐行或无限扩大 Change。

### Binary payload

`blob_event_4` 固定 2048 rows、32 Changes、4 durable commits：

| payload/row | 时间 | rows/s | encoded MiB/s |
| ---: | ---: | ---: | ---: |
| 128 B | 0.814 ms | 2.52M | 417 |
| 1 KiB | 1.829 ms | 1.12M | 1,142 |
| 8 KiB | 6.865 ms | 0.298M | 2,344 |

payload 增大后 row throughput 下降，但 byte throughput 上升，说明固定事务/IPC 成本逐渐被内存复制和
MDBX 写带宽取代。

### Schema 量级

在 1024 rows/Change、32 Changes/tx、8 transactions 下，integrated producer 的代表性吞吐为：固定
8 列 24.19M rows/s、heterogeneous 8.07M、nested 8 列 6.79M、layout-v1 16 列 5.91M、wide numeric
64 列 4.27M。不同 Schema 的 encoded bytes 不同，这些数字用于容量量级，不应当被解释为只由列数
造成的单变量差异。

## Representative Endurance

这次运行不是 weekly `full`。配置为：1024 base rows/Change、8 Changes/cycle、30 cycles、256 B base
payload、32 MiB retained window、8 entries/8 MiB consumer page、每 5 cycles close/reopen。每个模式
运行 3 个独立 Store。

| 模式 | producer p50/p95 | full consumer p50/p95 | projected p50/p95 | truncate p50/p95 |
| --- | ---: | ---: | ---: | ---: |
| heterogeneous | 0.717/0.840 ms | 0.288/0.336 ms | 0.170/0.218 ms | 0.105/0.135 ms |
| homogeneous | 0.712/0.882 ms | 0.255/0.314 ms | 0.134/0.159 ms | 0.097/0.113 ms |

| 模式 | measured rows/entries | 串行 protocol | rows/s | input MiB/s | retained | peak allocated | amplification |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| heterogeneous | 215,100 / 240 | 38.88 ms | 5.53M | 1,663 | 31.81 MiB / 119 entries | 35.66 MiB | 1.12× |
| homogeneous | 245,760 / 240 | 36.36 ms | 6.76M | 1,866 | 31.94 MiB / 113 entries | 36.06 MiB | 1.12× |

异构 entry 长度 min/p50/p95/max 为 432 B / 168,472 B / 547,640 B / 547,640 B；同构 entry 固定
296,408 B。异构 projected consumer p50 比 full 快 1.69×，同构快 1.90×。

每个 run 周期 reopen 6 次，3 个 run 的最终 checksum 分别在同一模式内完全一致：heterogeneous
`0xc3ad37ce06d25919`，homogeneous `0xfd2ff42da1b196b2`。所有 cursor、GC byte-window、raw IPC、full
decode、顺序与 relation oracle 均通过。

## 风险与下一步

1. 当前机器连续运行的 absolute run medians 波动较大，不能据此设置 CI wall-clock gate。正式回归机
   应固定电源/温度/后台负载，并考虑 case-order counterbalancing 或 run 间冷却。
2. 配对投影结果跨进程稳定，可以先作为优化方向与人工回归参考，但仍不建议立即硬编码阈值。
3. 生产默认建议从约 1024 rows/Change、至少 32 Changes/transaction 开始；128 Changes/transaction
   吞吐更高，但应结合可接受的事务大小与重试代价。
4. weekly 仍需在专用磁盘运行 500-cycle、512 MiB retained 的 `full` endurance；本报告没有把受控
   representative workload 冒充长期空间稳定性结论。

## 原始数据

- 常规 clean runs：`/tmp/dogpaddle-change-store-report-20260827-normal-extra/` 中 run 02–04；
- representative endurance：`/tmp/dogpaddle-change-store-report-20260827-endurance/`；
- 提交前 dirty runs 保留用于审计，但未进入统计：
  `/tmp/dogpaddle-change-store-report-20260827-normal/` 和 normal-extra run 01。

所有机器记录均为 typed JSONL；常规 clean runs 合计 3,870 raw samples、258 summaries、126 paired
summaries，endurance 合计 180 cycle samples 和 6 summaries。
