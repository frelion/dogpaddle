# Change + AppendLog 测试协议

这个 package 只验证 `dogpaddle-change` 与 `dogpaddle-store` 的公共装配接缝。Change 的
IPC 私有布局、完整畸形输入语料和 Schema 拒绝矩阵仍属于 Change；通用 AppendLog 布局、
崩溃边界、分页算法和事务实现仍属于 Store。组合测试只保留无法由任一产品 crate 单独证明的
契约。

## Workload 描述

每个测试与 benchmark case 都从稳定 persona 构造，并携带可机器读取的 descriptor。
`logical_columns` 表示业务 Schema 的顶层字段数；`physical_columns` 还包含固定的
`$dogpaddle.diff`，因此恒为 `logical_columns + 1`。嵌套 Schema 另外报告 leaf field 数，
不能把一个 Struct 错算成一个简单标量列。

| persona | logical / physical | 主要目的 |
| --- | ---: | --- |
| `diff_only_control` | 0 / 1 | 完整 Stream 与 entry 固定成本 |
| `layout_v1_16` | 16 / 17 | v1 全部物理布局与 nullable 边界 |
| `fixed_event_8` | 8 / 9 | 固定宽度事件 |
| `mixed_event_16` | 16 / 17 | 数值、Boolean、Utf8、Binary 与 nullable 的主 anchor |
| `wide_numeric_64` | 64 / 65 | 多字段 Schema、metadata 与 buffer 遍历 |
| `blob_event_4` | 4 / 5 | 128 B、1 KiB、8 KiB payload 轴 |
| `nested_event_8` | 8 / 9 | List/Struct 完整子树 |
| `sliced_mixed_16` | 16 / 17 | 非零 Arrow offset |
| `heterogeneous` | 多种 | 一个 AppendLog 内的异构 Schema 与变长 entry |

生成器使用确定性 seed。常规性能输入从空关系开始采用 `insert_only`；churn 输入必须先插入，
再撤回完全相等的记录，并由独立 relation model 验证每个记录的任意处理前缀权重非负。不能用
没有声明初始关系的首条负 diff 冒充有效变化流。

## Correctness 矩阵

唯一的 `correctness` target 使用 table-driven、pairwise 矩阵，而不是执行所有维度的完整
笛卡尔积：

- 重点 rows/Change 为 1、7、8、9、63、64、65；
- projection 包含 diff-only、key-only、非相邻 sparse、dense、payload-only 和 identity；
- 每个代表 persona 覆盖 append、durable commit、close、reopen、raw-byte equality、full decode
  equality 与独立 Arrow projection oracle；
- heterogeneous 序列使用不同 Schema、rows 和 payload，覆盖按 item/byte limit 的多页追赶、
  cursor resume、truncate 和再次 reopen；
- 同一展平事件序列使用不同 Change 分区，物理 `(offset, row_index)` 可以变化，事件顺序不得变化；
- 一个 callback 已写 output/cursor 后遇到损坏 Change，整个事务必须 poison 且原子回滚。

正确性 oracle 不使用 benchmark checksum 代替 equality。投影 expected 直接从输入 Schema 字段和
Arrow columns 组装，不能只调用被测的 `Change::try_project` 生成 expected。组合层只保留一个
代表性 malformed Stream 和一个 projection Schema mismatch；详细 decoder 负向矩阵不在这里复制。

## 性能矩阵

常规 target 使用一个主 anchor 加逐轴 sweep。主 anchor 为 `mixed_event_16`；Schema、
rows/Change、Changes/transaction、payload、entry bytes、projection 和 replay page limit 每次只改变
一个轴。Changes/transaction sweep 固定总 Changes/sample 并执行多个事务；rows/Change sweep 固定
总 rows/sample。相互比较的 preencoded/integrated 与 full/projected 使用相同输入、相同 sample
index 和 counterbalanced 顺序。

端到端 headline 只包括：

1. encode + append + durable commit；
2. 多页 full replay；
3. 多页 projected replay；
4. reopen 后第一次 replay（不称 cold cache）；
5. scan + projected decode + re-encode output + cursor update + durable commit。

preencoded append 只是 attribution control。raw `append_entry` forwarding 的性能属于 Store；组合
correctness 仍验证完整 Change Stream 原样转发。

每个机器记录必须包含 persona、diff model、logical/physical/leaf 字段数、nullable/variable/nested
字段数、rows/Change、Changes/transaction、transactions/sample、encoded entry min/p50/max、
encoded bytes/transaction、projection 字段与 Arrow value-byte 选择率、ScanLimit 和 lifecycle。
严格结果校验、fixture、预热与清理全部在计时外。

## Endurance

`homogeneous_control` 保留等长 entry，提供低噪声存储回归；`heterogeneous_pipeline` 才是主要真实
长稳场景。后者轮换 persona、rows 和 payload，producer 计时包含 encode、append 与 durable commit，
consumer 分页 full/projected decode 并 durable commit cursor，GC 只能回收两个已提交 cursor 都已
处理的 entry。

因为 Stream 不可切分，稳定 byte window 的合法范围是
`target - max_entry_bytes < retained_encoded_bytes <= target`；不得再断言每个周期删除 entry 数等于
新增数。协议周期性 close/reopen 后继续，最终逐 offset 验证重建的原字节、full decode、顺序和
relation model。

## 执行层级

- PR：完整 deterministic correctness；`cargo xtask bench-smoke` 只验证所有场景、oracle 与机器协议，
  不设置 wall-clock 门槛。
- Nightly：anchor 与全部逐轴 sweep。
- Reference：固定机器、rustc、profile 和文件系统，保存原始配对样本；没有稳定历史前只报告结果。
- Weekly：homogeneous 与 heterogeneous endurance，记录 producer/consumer/truncate p50/p95/p99/max、
  logical/allocated/peak bytes 和 reopen oracle。

正式比较必须匹配完整 workload descriptor。不同列数、entry 大小、projection、页大小或 lifecycle
的结果不是同一个 benchmark，不能合并成笼统的“Change 性能”。
