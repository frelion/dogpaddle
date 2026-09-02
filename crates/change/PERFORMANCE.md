# dogpaddle-change 性能协议

本文件只定义 Change 单体 benchmark 的 workload、计时边界和配置。统一的环境/profile
规则、typed JSONL、统计与 reference 比较协议见[根目录测试协议](../../TESTING.md)。机械协议由内部
crate `dogpaddle-bench-protocol` 提供；Change benchmark 仍在本地拥有 Arrow fixture、结果 oracle、
尺寸预检和五种 codec case 的交错顺序。

## 工作负载

| 名称 | Schema 形状 | 主要问题 |
| --- | --- | --- |
| `diff_only` | 零 logical column | 每个完整 Stream 的固定成本 |
| `narrow_fixed` | `UInt64 + Int64` | 固定宽度 rows/s |
| `wide_projectable` | `id + Binary + tail` | 跳过宽 payload 的收益 |
| `mixed_nullable` | Bool、Float64、Utf8、Binary、Null | bitmap、offset 与值校验 |
| `nested` | List 与 Struct | 递归 layout 和完整子树 |
| `sliced` | 非零 Arrow offset 的 `id + Binary + tail` | sliced buffer 的编码和选择性解码 |

`smoke` 只测 4 rows/Change 和 16 B 宽 payload；`reference` 分别测 1、64、1024、16384
rows/Change 和 1 KiB 宽 payload。该矩阵刻意不覆盖全部
Arrow 类型，因为类型全集属于正确性测试，性能测试只选择不同成本原型。

`change_core` 测量 `Change::try_new`、`ChangeProjection::try_new`、`try_slice` 和 `try_project`。
只有逐行验证 diff 的 `try_new` 报告 rows/s；Schema 绑定、切片和零复制投影只报告每次操作延迟，
不把未扫描的行数或编码字节伪装成吞吐。

`change_codec` 测量 `encode_change`、`decode_change`，以及 diff-only、narrow、identity
`decode_change_projected`。projection 在计时外创建，所有 decode 使用同一份预编码字节。每个
case 在正式采样前独立预热；随后以 sample 为外层循环交错执行并轮换首个 case，保留同一 sample
index 下可配对的原始结果。结果等价验证位于计时外。

## 配置

benchmark 只有 `smoke` 与 `reference` 两套代码内固定矩阵，不接受逐维覆盖。快速协议检查：

```bash
DOGPADDLE_BENCH_PROFILE=smoke cargo bench -p dogpaddle-change --bench change_codec
```

正式 reference 使用 `DOGPADDLE_BENCH_PROFILE=reference`；Change 不落盘，因此不要求 root。

输出保留 rows/Change、changes/sample 和逐样本耗时；每个 fixture 的 encoded bytes/Change 作为一条
独立 raw observation 保存，避免把 fixture facts 重复写进每个 sample。统一 reporter 从验证后的
artifact 派生 min/median/p95 与 operations/s。它们是 warm single-thread CPU 数量级，不是 Store、磁盘或 Flow 吞吐。
真实持久化路径由 `dogpaddle-change-store-integration` 独立测量。

stdout 的机器流由单一 `Record` 枚举生成：自包含 `run` 先声明稳定 series、精确样本数与静态 work
facts，随后是只含 case ID、index、elapsed 的 raw `sample`，最后是 `completion`。统计只由统一
reporter 从验证后的 raw artifact 派生。启动时仍由 Change fixture 用 checked arithmetic 预检尺寸，并在分配
前拒绝超过 Arrow i32 offset 容量的 Binary、Utf8 或 List 配置。

相邻的 `change_core.plan.json` 与 `change_codec.plan.json` 分别冻结 smoke/reference 的纯 Plan
fingerprint；`cargo xtask bench-plan-check` 不构造 Arrow fixture 即可验证两档矩阵。
