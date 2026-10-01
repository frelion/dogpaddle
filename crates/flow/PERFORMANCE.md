# Flow 性能记录

## 2026-09-30：第三轮，直接路由并借用源队首

当前实现把新页直接交给可接收的 Sink，只有调用挂起才持久保存 pending 页。
root 直接读取 Source 队首，完成时同事务消费；child 直接读取父 pending 页。
因此普通直线调用不需要激活事务、输入副本、落盘后再读回的 Send 页，也不写空帧 tombstone。
首次计算安全失败时单独保存初始 Run，保持多源和重开后的失败位置。

最终审查修正后，同机重新运行两轮 reference，第二轮反转版本顺序；每版本、场景共 18,432 个计时样本。
测量期间没有并行 Cargo、系统验收或容器负载。基线、硬件、工作量和默认融合计数链
与下方初版说明相同。每轮每个场景处理 9,280 个输入，构造、最终 oracle 和重开不计时。
下表为完整 `advance` 延迟中位数，单位微秒：

| 场景 | 旧版 | 重构初版 | 当前第三轮 | 当前 / 旧版 |
| --- | ---: | ---: | ---: | ---: |
| Sequence → Discard | 31.125 | 70.959 | 35.458 | 1.14 |
| 五项纯转换链 → Discard | 34.084 | 72.458 | 39.459 | 1.16 |
| 1 个 RunningEventCount | 32.042 | 73.542 | 39.542 | 1.23 |
| 14 个 RunningEventCount | 54.250 | 96.458 | 61.333 | 1.13 |
| 62 个 RunningEventCount | 140.209 | 183.250 | 149.959 | 1.07 |
| 4 路 Discard fan-out | 60.125 | 87.458 | 37.333 | 0.62 |
| 16 路 Discard fan-out | 274.730 | 不可比，有积压 | 36.333 | 0.13 |

当前全部场景在计时结束前已完成全部捕获输入，Source 队列和调用栈为空，状态与重开 oracle
均通过。初版 16 路仍有积压，不能拿其单轮延迟作为同工作量吞吐基线。
第三轮收回直线场景初版的大部分固定开销，但仍比旧版慢约 7%–23%；不声称全面性能提升。
分叉收益来自把多个终端的入账合入本页事务，仍受每页和每轮预算限制。
这些 Sequence → Discard 微基准不代表真实 CDC、数据库目标或进程 RSS。

本地最终原始样本、各次 oracle 与摘要在 `/tmp/dogpaddle-final-performance-after-review-20260930/`，
文件名为 `0-*`、`1-*` 和 `summary.json`；临时对照脚本为
`/tmp/dogpaddle-final-compare.py`。第三轮审查前的重复测量另存于
`/tmp/dogpaddle-iteration-three-performance-20260930/`。另保留第二轮直接路由、尚未借用源队首的
证据 `/tmp/dogpaddle-iteration-two-performance-20260930/`：直线链只比初版改善约 4%–10%，
这个结果促使继续删除根输入搬运。上述临时证据不属于产品协议。

## 2026-09-30：持久调用栈初版

这次重构删减执行协议，并为分页、恢复和融合尾链建立统一边界。初版实现仍有明显的
`advance` 固定开销回退；下面的结果不能作为性能提升的证据。

基线为 `2a8dd332e3ce6710e6314f0562317eb904f1ad77`，新版为
`codex/durable-call-stack` 的本轮未提交实现。两者均使用 Rust 1.96.0 release、Apple M5、
macOS Darwin 25.6.0、同一 APFS 文件系统。测量期间没有并行 Cargo、系统验收或容器负载。
这些是单机微基准，源为 Sequence，目标为 Discard，不代表真实 CDC 或外部数据库吞吐。

### 相同逻辑工作量与默认融合

reference 每个场景预热 64 次，再采集 9 组、每组 1,024 次完整 `advance` 延迟；
下表是全部 9,216 个样本的中位数，单位为微秒。构造、状态读取和恢复校验在计时外。
每个可比较场景均确认计时结束时已经完成全部 9,280 个输入，没有调用帧或源队列积压，
计数状态正确，重开成功。

| 场景 | 旧版 | 新版 | 新 / 旧 |
| --- | ---: | ---: | ---: |
| Sequence → Discard | 30.250 | 70.625 | 2.33 |
| 五项纯转换链 → Discard | 32.417 | 72.417 | 2.23 |
| 1 个 RunningEventCount | 31.875 | 73.792 | 2.32 |
| 14 个 RunningEventCount | 53.416 | 96.208 | 1.80 |
| 62 个 RunningEventCount | 137.875 | 182.625 | 1.32 |
| 4 路 Discard fan-out | 60.250 | 85.834 | 1.42 |

纯转换链的两版均为 Select → 追加 `next = value + 1` → Filter → Select → SchemaAlign，
最终保留 `source_value` 与 `derived_value` 两列。计数链两版均采用默认自动融合。

旧仓库的 `flow_runtime` 计数链基准显式调用 `materialize`。直接拿它与新版自动融合比较，
会把布局变化误计为执行器加速。因此计数链基线另用同一旧版 API 构造
Sequence → N 个 RunningEventCount → Discard，省略全部 `materialize`；确认只有两个 Station，
所有计数算子与源融合。每个场景断言源位置为 9,279、Sink 订阅位置和 tail 均为 9,280、
保留日志为空、全部 N 个计数均为 9,280，重开后再次验证。

新旧原有 reference runner 分别完成一次全场景运行；默认融合计数链基线另运行一次。
这不是随机交替的多轮 reference 实验，不据此给出跨机器的精确回退比例。
另外完成了五轮交替 smoke 对照，每版每场景共 15 个计时样本；可直接比较的 Sink、
纯转换链与 2 路 fan-out 均出现同方向回退。smoke 的旧计数链使用强制物化布局，
不纳入上述默认融合结论。

### 有积压的轮次

16 路 fan-out 的新版 reference 在计时结束时仍有待处理帧，
`caught_up_before_drain = false`。其单轮中位数为 154.063 微秒，旧版为 268.334 微秒，
但新版每轮完成的工作量更少，**不能将这个差值解释为吞吐提升**。
停止源后，未计时的排空和重开校验确认最终结果完整。

当前 runner 记录捕获数量、排空前栈深度及是否完成全部已捕获输入。
只有排空前的栈、源队列和计数 oracle 均通过，才可将采样时段与完整输入工作量关联。
`advance` 延迟不等于单个 Store 事务耗时；本轮没有测量 WAL sync 次数、累计 IPC 字节、
单笔事务时长或进程 RSS，也不从调度代码推算这些数值。

### 重跑与原始证据

```sh
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-flow --bench flow_runtime
DOGPADDLE_PERF_PROFILE=reference DOGPADDLE_PERF_ROOT=/absolute/perf-root \
  cargo bench --locked -p dogpaddle-flow --bench flow_runtime
```

本次本地原始证据保存在 `/tmp/dogpaddle-final-performance-20260930/`：
`flow-paired.jsonl`、`flow-reference-before.jsonl`、`flow-reference-after.jsonl`、
`flow-reference-baseline-fused.jsonl` 及相邻统计摘要。临时默认融合基线源码保存在工作树的
`target/baseline-fused-countchain/`，只依赖旧仓库的产品 crate 与性能上下文；原仓库 tracked
文件没有修改。这些本地临时证据不属于持久格式或仓库测试协议。

## Tagged Operation plan：2026-10-01 启动对照

Operation Definition 统一为稳定名称标记的 canonical JSON，删除数字 tag 目录、逐算子
decode 与 Payload 镜像；Flow 的图格式及运行状态协议保持原有机制，嵌入的 Operation
字节属于开发期 v1 变化，旧状态直接重建。该简化不意味着启动时间必然降低。

同机 Apple M5、aarch64 Darwin 25.6、APFS、Rust 1.96.0 release，
`flow_lifecycle` reference 使用 30 samples、2 s warmup、5 s measurement。
baseline 产品为 `a3dc2dc`（benchmark-only 快照 `75febde`），candidate 为 `35e9662`
加 ASOF 页内复用与 Aggregate API 收敛；生命周期 workload 不执行 ASOF。
构建/重开单独计时，fixture 和结果验证在计时外；两轮无并发 Cargo、容器或归档。

整轮中位数如下，单位 ms；变化为 candidate / baseline - 1。

| 场景 / Operation 数 | baseline | candidate | 变化 |
| --- | ---: | ---: | ---: |
| fresh_durable_build / 2 | 4.4797 | 4.6088 | +2.9% |
| fresh_durable_build / 64 | 4.6786 | 4.8567 | +3.8% |
| fresh_durable_build / 1024 | 7.6577 | 8.0396 | +5.0% |
| warm_reopen / 2 | 5.8089 | 5.8704 | +1.1% |
| warm_reopen / 64 | 6.0327 | 5.5462 | -8.1% |
| warm_reopen / 1024 | 7.6393 | 7.6750 | +0.5% |

fresh build 的中位数 95% CI：2 个 Operation 为旧 `[4.4461,4.5363]`、
新 `[4.5950,4.6219]`；1024 个为旧 `[7.5981,7.6905]`、新 `[8.0070,8.0583]`。
保留本轮 0.13–0.38 ms 的启动回归。warm reopen 的各自区间大体重叠，不宣称普遍
提速或完全无回归；未测构建分配/RSS，也不据此推断 steady-state throughput。

原始 context、samples、estimates 位于 `/tmp/dogpaddle-definition-performance/`：
`before/dogpaddle-flow-lifecycle-run-naFgvB` 与
`after/dogpaddle-flow-lifecycle-run-0kNpR1`，相邻日志保留全部输出。
完整工作区 debug/release correctness、benchmark test mode、Clippy、Rustdoc 和最终
构建通过；独立审查覆盖 raw plan 的 owner 上限、纯 binding 前置拒绝与只读恢复。

## 声明顺序构建：2026-10-01 启动对照

图的声明顺序已经是合法拓扑顺序，构建和恢复现在直接沿用它。删除第二次拓扑排序、
构建时的 encode/decode 往返与运行实例重排；持久图仍完整校验，CRC 使用已有标准实现。
本轮不改变合法图的 ordinal、资源名和融合边界，Sink 的轮转顺序统一为声明顺序。

同机 Apple M5、aarch64 Darwin 25.6、APFS、Rust 1.96.0 release，
`flow_lifecycle` reference 使用 30 samples、2 s warmup、5 s measurement。
baseline 为 `29d713a`，candidate 为 `9e25a27`；两份产品 source patch 均为空。
context 的 dirty 来自未跟踪的本地历史计划，未进入编译输入。
两个版本使用同一 benchmark，测量期间没有并行 Cargo、容器或系统验收。
fixture 及路径、Operation 数量/ID 验证在计时外；fresh build 另外在计时外真实 reopen
并验证，warm_reopen 计时包含实际 open。全部六个场景通过。

下表为中位数及其 95% CI，单位 ms；变化为 candidate / baseline - 1。

| 场景 / Operation 数 | baseline [95% CI] | candidate [95% CI] | 变化 |
| --- | ---: | ---: | ---: |
| fresh_durable_build / 2 | 4.5597 [4.5358, 4.6245] | 4.6525 [4.6024, 4.6700] | +2.0% |
| fresh_durable_build / 64 | 4.8284 [4.7879, 4.8483] | 4.7152 [4.6819, 4.7340] | -2.3% |
| fresh_durable_build / 1024 | 7.9839 [7.9393, 8.0036] | 6.9011 [6.8709, 6.9214] | -13.6% |
| warm_reopen / 2 | 5.8708 [5.6700, 6.0975] | 5.8496 [5.6059, 6.0049] | -0.4% |
| warm_reopen / 64 | 6.0504 [5.7961, 6.2964] | 5.9338 [5.8302, 6.1622] | -1.9% |
| warm_reopen / 1024 | 7.6062 [7.4734, 7.7804] | 7.1188 [6.9339, 7.3406] | -6.4% |

1024 个 Operation 的新建和重开均改善；小图的新建中位数增加 2.0%，区间重叠，
保留这项结果，不宣称所有规模都提速。小图重开区间同样重叠。
本轮同时改变排序、构建往返和 CRC，不能把收益单独归因于其中一项。
没有测量分配、进程 RSS 或 steady-state throughput。

原始 context、samples、estimates 与 benchmark/oracle 执行日志位于
`/tmp/dogpaddle-ordered-flow-performance/`：
`before/dogpaddle-flow-lifecycle-run-mLA0E2` 与
`after/dogpaddle-flow-lifecycle-run-AzGT3B`；相邻 revision 与 product patch 记录保留版本证据。
当前组合通过完整工作区 debug/release correctness、benchmark test mode、Clippy、Rustdoc
及 workspace build；独立审查覆盖声明顺序、1024-node/port 准入、只读恢复和资源绑定。

## 唯一类型化计划：2026-10-01 启动对照

Flow 直接持久保存 canonical JSON 计划；删除逐节点二进制包装、Operation envelope
与独立 Operation codec。运行帧、计算事务、Source ACK 和 Sink Prepared 协议不变。
开发期 v1 图字节改变，旧状态直接重建；三个固定图由 218/490/1031 字节变为
251/614/1139 字节。这项简化减少概念和代码，并未缩小所有持久计划。

同机 Apple M5、aarch64 Darwin 25.6、APFS、Rust 1.96.0 release，未修改的
`flow_lifecycle` reference 使用 30 samples、2 s warmup、5 s measurement。
baseline 为 `5f34c3c`；candidate 为产品等价的 `0a27785` 加本轮 source diff。
benchmark SHA-256 为 `f33eec4c516f23e65f0230f6a1cd58c33475b2dfbc26a80574a73ad9eba8f486`。
计时包含实际 build/open 和 Store 成本，fixture、factory、ID/count 校验、drop 及
fresh build 的额外 reopen 在计时外；不等同于 codec CPU 时间或 steady-state throughput。
测量时没有并行 Cargo、容器或系统验收，桌面负载未隔离。

首轮按 baseline → candidate；为核实共同漂移，再按 candidate → baseline。
下表保留两轮全部中位数与各自 95% CI，单位 ms；变化为 candidate / baseline - 1。

| 场景 / Operation 数 | 首轮 baseline [95% CI] | 首轮 candidate [95% CI] | 变化 | 反向 baseline [95% CI] | 反向 candidate [95% CI] | 变化 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fresh_durable_build / 2 | 4.4809 [4.4777, 4.4969] | 4.9279 [4.8120, 5.0989] | +9.98% | 4.5204 [4.5061, 4.5613] | 4.6513 [4.6351, 4.6775] | +2.89% |
| fresh_durable_build / 64 | 4.6174 [4.5658, 4.6410] | 5.1935 [5.0124, 5.4294] | +12.48% | 4.6917 [4.6394, 4.7394] | 4.8123 [4.7651, 4.8302] | +2.57% |
| fresh_durable_build / 1024 | 6.7866 [6.7565, 6.8500] | 7.7582 [7.6745, 7.8563] | +14.32% | 6.8696 [6.8436, 6.9341] | 6.9785 [6.9662, 7.0100] | +1.59% |
| warm_reopen / 2 | 5.6845 [5.5217, 5.9571] | 6.2895 [6.2029, 6.4736] | +10.64% | 5.9437 [5.7987, 6.1501] | 5.9431 [5.7624, 6.2308] | -0.01% |
| warm_reopen / 64 | 5.8612 [5.6529, 6.0198] | 6.7697 [6.6011, 6.9755] | +15.50% | 5.9770 [5.7306, 6.2358] | 6.0963 [5.8625, 6.3550] | +2.00% |
| warm_reopen / 1024 | 7.0366 [6.8976, 7.2456] | 7.5603 [7.3890, 7.7605] | +7.44% | 7.1993 [7.0383, 7.4264] | 7.2934 [7.1811, 7.4683] | +1.31% |

首轮六项均回退 7.44%–15.50%，各自 median CI 不重叠；反向轮为 -0.01%–+2.89%。
反向轮 fresh build 仍增加 1.59%–2.89%，三组 CI 不重叠；warm reopen 三组 CI 重叠。
同一 candidate 第二次运行也比第一次低约 4%–10%，证明存在运行间漂移，尚不能
将漂移归因于具体 IO、热状态或 codec。保留首轮回归和反向轮的 build 回归，不宣称
普遍提速、完全无回归或把较好的单轮当作唯一结论。未测分配、native heap 或 RSS。

原始 context、samples、estimates、源码 patch 与实际编译路径日志位于
`/tmp/dogpaddle-one-flow-plan-performance/`：首轮 `before/dogpaddle-flow-lifecycle-run-FF34UE`
与 `after/dogpaddle-flow-lifecycle-run-xluEpZ`，反向 `reverse-after/dogpaddle-flow-lifecycle-run-psc2zw`
与 `reverse-before/dogpaddle-flow-lifecycle-run-lKkpfW`；`paired-order-summary.json`
保留从每组 30 个样本独立复算的中位数；95% CI 取原 Criterion estimates。
完整工作区 debug/release correctness、benchmark test mode、Clippy、Rustdoc 和 workspace build 通过。
同一新编译 release host 通过真实 PostgreSQL CDC、SQL、Sink/recovery 与 MySQL CDC 验收。

## 构造后释放计划：2026-10-01 内存与运行对照

Flow 装配消费 Definition，每个运行节点直接拥有 ID、inputs、Operation、output codec
与 pending Delivery；退休常驻完整计划及 Operation/codec/pending 平行数组。CLI 在
start 后也释放 SqlProgram。此轮生产代码净减 5 行，主要收益是删除常驻重复表示，
没有改变持久 Definition、Frame、事务、ACK 或 Sink Prepared。

allocator 对照使用相同临时程序与锁定依赖：有限 Sequence → 64 个 Select → Discard，
每个 Select 有一个含 16,389 ASCII 字节的独立字符串字面量，总计 66 个节点。
baseline 为 `d2c90b9`，candidate 为相同产品树的 `ddb7ee0` 加本轮 diff；
Apple M5、aarch64 Darwin 25.6、APFS、Rust 1.96.0 release，dhat 0.3.3。
每个 case 是独立进程，build 在创建 factory 前启 profiler；open 在 profiler 启动前 seed/drop，
随后启 profiler 并实际 open。调用者不保留 Definition clone。
下表在 build/open 和 ID/count/depth 校验后、首次 advance 前读取 Rust global allocator，
peak 与累计分配也只截止此观察点，单位 byte。

| case | baseline live | candidate live | 释放 | baseline peak | candidate peak | baseline 累计分配 | candidate 累计分配 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| build | 3,270,849 | 1,154,969 | 2,115,880 (64.69%) | 4,711,033 | 3,608,564 | 23,071,288 | 23,074,456 |
| open | 3,275,508 | 1,155,660 | 2,119,848 (64.72%) | 4,708,174 | 3,605,705 | 16,422,625 | 16,425,793 |

两例 live blocks 均减少 388，观察点 peak 约减 23.4%；累计分配反而各增加 3,168 byte。
约 1.15 MB 的 live 分配仍包含必要的已编译字面量，不宣称所有表达式内存都释放。
随后真实执行至 Idle/空栈、drop 并 reopen 检查数量和空栈；这不是逐值输出 oracle。
drop 后两版 build 都剩 157 byte/4 blocks，open 都为 0。未测 RocksDB native heap、
JVM 或进程 RSS；64 个宽字面量的比例不代表所有 SQL。build 与 open 的初始化口径
不同，不互作性能对照。该内存见证也不证明运行吞吐。

为检查更宽 RuntimeNode 的访问成本，再运行未修改的 `flow_runtime` reference。
每个场景 64 轮预热，9 个 sample × 1024 次完整有界 advance；fixture、最终排空、
source 数量、counter 和 reopen oracle 在计时外。两版各场景捕获数都为 9,280，
计时结束前均已追平，排空与重开 oracle 一致。无并行 Cargo、容器或系统验收。
下表 median 为 9 个 sample 中位数的中位数，p95 对全部 9,216 次原始 latency
按 (n−1)×0.95 线性插值；单位 µs，仅描述这组采样，不作为统计置信区间。

| 场景 | baseline median | candidate median | 变化 | baseline p95 | candidate p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sink | 37.000 | 36.833 | -0.45% | 49.291 | 49.635 |
| PureChain | 39.958 | 39.583 | -0.94% | 52.750 | 51.750 |
| CountChain(1) | 38.667 | 39.083 | +1.08% | 51.125 | 51.292 |
| CountChain(14) | 62.167 | 61.708 | -0.74% | 75.375 | 75.625 |
| CountChain(62) | 149.708 | 149.645 | -0.04% | 164.250 | 163.375 |
| Fanout(4) | 36.625 | 35.938 | -1.88% | 49.333 | 48.166 |
| Fanout(16) | 37.125 | 35.208 | -5.16% | 49.469 | 48.386 |

保留 CountChain(1) 中位数 +1.08% 与部分 p95 的增加，不宣称所有场景提速或
无回归。最长计数链基本持平；fan-out 16 的本轮中位数降低约 5.16%，没有额外
吞吐计时，也未观测单事务 duration、实际 WAL sync 或累计 IPC bytes。

原始 4 份 allocator JSONL、编译 log 与空 stderr 位于
`/tmp/dogpaddle-runtime-nodes-memory-{baseline,candidate}-{build,open}.*`；
相同临时源码在 `/tmp/dogpaddle-runtime-plan-memory/src/main.rs`，SHA-256 为
`62cb4ed22d52c1bc796f774eb365b847219cd4bf41785bcba8041bc1eba972b2`。
`/tmp/dogpaddle-runtime-nodes-performance/` 保留源码 patch、版本/context、memory-summary，
以及 `before.jsonl`、`after.jsonl` 的全部运行 latency、7 个 oracle 与 completion；
`runtime-summary.json` 保留每个 sample 的中位数及全量 p50/p95/p99。
完整工作区 debug/release correctness、benchmark test mode、Clippy、Rustdoc 与 workspace build 通过。
新编译的 release hosts 通过真实 PostgreSQL CDC、SQL 与 Sink/recovery 三个验收入口。
