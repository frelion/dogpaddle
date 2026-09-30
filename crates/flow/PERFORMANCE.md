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
