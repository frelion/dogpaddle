# dogpaddle-change 性能协议

本文件定义可复现的 Change 单体基准，不把某台开发机的一次运行包装成产品吞吐承诺。正式基线
必须记录 git revision、实际 rustc、声明的 Cargo profile、CPU、OS/arch/kernel 和负载参数，并在固定机器
上保存原始配对样本。默认 `cargo bench` 记录 `bench`；使用 `--profile <name>` 时
必须同时设置 `DOGPADDLE_CARGO_PROFILE=<name>`，完整协议见根目录 `TESTING.md`。

## 工作负载

| 名称 | Schema 形状 | 主要问题 |
| --- | --- | --- |
| `diff_only` | 零 logical column | 每个完整 Stream 的固定成本 |
| `narrow_fixed` | `UInt64 + Int64` | 固定宽度 rows/s |
| `wide_projectable` | `id + Binary + tail` | 跳过宽 payload 的收益 |
| `mixed_nullable` | Bool、Float64、Utf8、Binary、Null | bitmap、offset 与值校验 |
| `nested` | List 与 Struct | 递归 layout 和完整子树 |
| `sliced` | 非零 Arrow offset 的 `id + Binary + tail` | sliced buffer 的编码和选择性解码 |

默认分别测 1、64、1024、16384 rows/Change；宽 payload 默认为 1 KiB。该矩阵刻意不覆盖全部
Arrow 类型，因为类型全集属于正确性测试，性能测试只选择不同成本原型。

`change_core` 测量 `Change::try_new`、`ChangeProjection::try_new`、`try_slice` 和 `try_project`。
只有逐行验证 diff 的 `try_new` 报告 rows/s；Schema 绑定、切片和零复制投影只报告每次操作延迟，
不把未扫描的行数或编码字节伪装成吞吐。

`change_codec` 测量 `encode_change`、`decode_change`，以及 diff-only、narrow、identity
`decode_change_projected`。projection 在计时外创建，所有 decode 使用同一份预编码字节。每个
case 在正式采样前独立预热；随后以 sample 为外层循环交错执行并轮换首个 case，保留同一 sample
index 下可配对的原始结果。结果等价验证位于计时外。

## 配置

| 环境变量 | 含义 |
| --- | --- |
| `DOGPADDLE_CARGO_PROFILE` | 显式 `--profile` 的同名声明；标准 `cargo bench` 留空 |
| `DOGPADDLE_BENCH_CHANGE_ROWS` | 逗号分隔的 rows/Change |
| `DOGPADDLE_BENCH_CHANGE_PAYLOAD_BYTES` | wide payload 每行字节数 |
| `DOGPADDLE_BENCH_CHANGE_WORKLOADS` | 逗号分隔的 workload 名称 |
| `DOGPADDLE_BENCH_CHANGE_TARGET_ROWS` | 每个样本的目标总行数 |
| `DOGPADDLE_BENCH_CHANGE_MAX_CHANGES` | 每个样本最多执行的 Change 数 |
| `DOGPADDLE_BENCH_SAMPLES` | 每个 workload 的正式样本数 |

例如运行一个快速 release protocol smoke：

```bash
DOGPADDLE_BENCH_CHANGE_ROWS=64 \
DOGPADDLE_BENCH_CHANGE_WORKLOADS=narrow_fixed,wide_projectable \
DOGPADDLE_BENCH_CHANGE_TARGET_ROWS=1024 \
DOGPADDLE_BENCH_CHANGE_MAX_CHANGES=16 \
DOGPADDLE_BENCH_SAMPLES=1 \
cargo bench -p dogpaddle-change --bench change_codec
```

输出包含 rows/Change、changes/sample、encoded bytes/Change、每 Change 的 min/median/max、
rows/s 和 encoded MiB/s。它们是 warm single-thread CPU 数量级，不是 Store、磁盘或 Flow 吞吐。
真实持久化路径由 `dogpaddle-change-store-integration` 独立测量。

控制台摘要之后有一个以明确起止标记包围的 CSV block，逐样本包含 workload、scenario、sample、
elapsed ns、operations、rows/Change 和 encoded bytes/Change。运行头同时打印 rustc、OS、arch、
kernel、git revision 和 working-tree 状态；reference runner 应保存完整 stdout，而不是只复制摘要。启动时会用 checked
arithmetic 预检 fixture 尺寸，并在分配前拒绝超过 Arrow i32 offset 容量的 Binary、Utf8 或 List
配置。

在建立 reference baseline 前不设置绝对 rows/s 门槛。后续回归比较必须固定 toolchain 与
`Cargo.lock`，交错运行 baseline/candidate，保存每个原始样本，并用配对比而不是两次独立最小值
判断变化。
