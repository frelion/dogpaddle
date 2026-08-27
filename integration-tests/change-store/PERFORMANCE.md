# Change + AppendLog 性能协议

本协议测量真实装配 `AppendLog<Vec<u8>>`，每个 entry 保存一个完整、自描述的 Arrow IPC
Change Stream。它不把 Change codec、通用 AppendLog 和组合流水线压成一个含混数字；结果只在
相同 git revision、rustc、Cargo profile、机器、文件系统和完整 workload descriptor 下可比较。
当前没有绝对吞吐 SLA。

## 一行到底有多少列

机器记录中的 `business_columns` 是业务顶层列数，`physical_columns` 还包含固定、非 null 的
`$dogpaddle.diff`，所以恒为 `business_columns + 1`。例如主 anchor `mixed_event_16` 的每行有
16 个业务列，IPC RecordBatch 有 17 个顶层物理列。`nested_event_8` 的 Struct/List 在顶层仍各算
一列，因此同时单独报告 leaf、nullable、variable-width 和 nested field 数，不能只看一个“列数”。

完整 persona 表和类型目的见 [`TESTING.md`](./TESTING.md)。每个 sample 的 typed JSONL 都记录
persona、Schema 名称、业务/物理/leaf 列统计、类型摘要、行数、entry 大小和投影选择率，因此任何
吞吐数字都能还原到具体数据形状。

## 常规矩阵：`change_append_log`

主 anchor 是 `mixed_event_16`。reference 使用 anchor 加单轴 sweep，不执行所有维度的笛卡尔积：

| 轴 | reference 取值 | 固定条件 |
| --- | --- | --- |
| Schema | 8 个 concrete persona 与 `heterogeneous`，加 anchor | anchor 的 rows/事务/payload |
| rows/Change | 1、64、1024、16384 | 32768 rows/sample、2 次 durable commit |
| Changes/transaction | 1、8、32、128 | 256 Changes/sample，事务数反向变化 |
| Binary payload | 128 B、1 KiB、8 KiB | `blob_event_4`、64 rows/Change、8 Changes/tx、4 tx |
| projection | 6 个合法 profile | `mixed_event_16`、`blob_event_4`、`nested_event_8` |
| replay page | 1 entry、约 1 MiB、约 16 MiB | 同一 committed 输入、sparse projection |

默认 anchor 是 1024 rows/Change、32 Changes/transaction、reference 8 transactions/sample。
`smoke` 使用 7 个代表 case，确保 Schema、rows、Changes/transaction、payload、projection、分页和
全部 headline 路径都真正执行；它只验证协议与 oracle，不形成性能基线。

### 场景与计时边界

| scenario | 计时区间 | 角色 |
| --- | --- | --- |
| `preencoded_append_durable_commit` | begin + append 预编码 entry + durable commit | 写入 attribution control |
| `encode_append_durable_commit` | begin + Change encode + append + durable commit | 生产端 headline |
| `multi_page_full_replay` | 已打开事务内的 scan + full decode body | 全量回放 headline |
| `multi_page_projected_replay` | 同一输入/页限制的 scan + projected decode body | 选择性回放 headline |
| `reopened_first_full_replay` | Store open + log open + begin + 第一次 full replay | reopen 后首次访问，不称 cold cache |
| `project_decode_reencode_append_cursor_durable` | 每页 begin + projected decode + re-encode output + append + cursor set + commit | 真实 consumer headline |

preencoded/integrated 和 full/projected 使用相同输入与 sample index，并按 counterbalanced 顺序配对。
每个 warmup 和正式变更型 sample 使用新的 Store；读取 fixture 是已提交的 warm input。fixture 构造、
seed、预热、严格 raw/Arrow/output/cursor/reopen oracle 和 Store 清理全部在计时外。集成 encode 场景
在时钟停止后才释放临时编码副本，allocator 析构不污染 durable 时间。

投影同时记录：所选/总业务列 min/p50/max、列选择率，以及包含 diff 的所选/全部 Arrow array
buffer bytes min/p50/max 和字节选择率。列选择率不能代替字节选择率。

### 常规配置

| 环境变量 | smoke / reference 默认值 | 含义 |
| --- | ---: | --- |
| `DOGPADDLE_CHANGE_STORE_BENCH_ROWS_PER_CHANGE` | 1024 | anchor 每个 Change 的行数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_CHANGES_PER_TX` | 32 | anchor 每事务 Change 数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_TRANSACTIONS_PER_SAMPLE` | 2 / 8 | anchor 每样本事务数，至少 2 |
| `DOGPADDLE_CHANGE_STORE_BENCH_PAYLOAD_BYTES` | 256 | anchor 每行 Binary payload 字节 |
| `DOGPADDLE_CHANGE_STORE_BENCH_SAMPLES` | 7 / 15 | 正式样本数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_WARMUPS` | 1 / 3 | 每个场景预热数 |
| `DOGPADDLE_CHANGE_STORE_BENCH_MAX_WORKING_SET_BYTES` | 512 MiB | fixture、编码副本与 Arrow 数据硬预算 |

启动时先用 checked arithmetic 检查总行数、event ID、Arrow i32 offset 和估算工作集，再构造大
fixture。环境变量只改变 anchor；reference 的显式 sweep 仍保持上表中的去混杂约束。

## 长稳矩阵：`change_append_log_endurance`

默认依次运行两个互相隔离的 Store：

- `heterogeneous_pipeline` 是主要真实场景，轮换 concrete persona，并确定性改变 rows、payload 和
  entry 长度；
- `homogeneous_control` 固定为 `blob_event_4` 等长 entry，提供低噪声存储对照。

每个周期的 producer 编码并 durable append 一批 Change。full 与 projected consumer 使用独立、
持久化 cursor 分页追赶；每页事务包含 begin、scan/decode、cursor set 和 durable commit。GC 只能
回收到两个 durable cursor 的较小值，并按完整 IPC entry 的 encoded bytes 维护窗口；AppendLog 的
8-byte offset 只计入 scan admission，不冒充 persisted Change payload。
Stream 不可切分，因此窗口不变量是：

```text
target - max_entry_bytes < retained_encoded_bytes <= target
```

删除条数不需要等于追加条数。协议按配置周期 close/reopen Store、log 和两个 cursor 后继续；最终
再次 reopen，逐 offset 验证 retained raw bytes、full Arrow equality、稳定顺序，以及消费全过程的
relation checksum。

### 长稳计时边界

| 指标 | scope | 计时区间 |
| --- | --- | --- |
| producer | cycle transaction | Change encode + begin + append_batch + durable commit |
| full/projected consumer | page transaction | begin + page scan/decode + cursor set + durable commit |
| truncate | cycle transaction | begin + 分步 truncate_before + durable commit |

文件 `stat`、计划队列维护、严格 oracle、reopen 验证和 JSON 输出在计时外。逐 cycle 记录四类 latency、
页数、cursor、removed/retained entry 与 bytes、reopen 标记、MDBX logical/allocated bytes；summary
报告 p50/p95/p99/max、文件峰值、写放大和最终 checksum。`protocol_ns` 只聚合上述计时区间；包含
fixture 与 bookkeeping 的 `wall_ns` 只用于观察。

### 长稳配置

| 环境变量 | smoke | full |
| --- | ---: | ---: |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_ROWS_PER_CHANGE` | 256 | 4096 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_CHANGES_PER_CYCLE` | 8 | 32 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_CYCLES` | 16 | 500 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_PAYLOAD_BYTES` | 128 | 1024 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_RETAINED_BYTES` | 4 MiB | 512 MiB |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_TRUNCATE_ITEMS` | 64 | 4096 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_CONSUMER_PAGE_ITEMS` | 8 | 16 |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_CONSUMER_PAGE_BYTES` | 32 MiB | 128 MiB |
| `DOGPADDLE_CHANGE_STORE_ENDURANCE_REOPEN_INTERVAL_CYCLES` | 4 | 25 |

`DOGPADDLE_CHANGE_STORE_ENDURANCE_WORKLOAD_MODE` 可取 `all`（默认）、
`heterogeneous_pipeline` 或 `homogeneous_control`。工作集预算默认 1 GiB，总编码写入预算默认 1 TiB，
分别由 `_MAX_WORKING_SET_BYTES` 与 `_MAX_TOTAL_WRITTEN_BYTES` 覆盖。它们是配置防护，不是 MDBX 文件
或可用磁盘配额。

## 运行与保存基线

PR 使用受审查的固定缩小矩阵；xtask 会清除父进程继承的 `DOGPADDLE_*` 环境变量：

```bash
cargo xtask bench-smoke
```

正式常规 reference 默认启动 5 个独立进程；长稳默认 1 个。runner 强制固定绝对文件系统路径、
`reference` profile（长稳同时使用 `full`），并把每次原始 stdout/stderr 单独保存到一个全新目录：

```bash
cargo xtask change-store-reference \
  --store-dir /absolute/path/on/reference-filesystem \
  --output-dir /absolute/new/result-directory

cargo xtask change-store-reference \
  --target endurance \
  --store-dir /absolute/path/on/reference-filesystem \
  --output-dir /absolute/new/endurance-result-directory
```

机器输出使用 `dogpaddle-bench-protocol` 的 typed JSONL environment/configuration/sample/summary/
pair_summary，以及 endurance 的 validated `cycle_sample`/`endurance_summary` extension。共享协议只
拥有严格配置、主机指纹、JSONL 与统计；本 package 保留 workload、Store 生命周期、计时和 oracle。

正式比较必须匹配 git/rustc/CPU/profile/filesystem 和所有 case 字段。不要比较不同 Schema、列数、
payload、entry size、projection、page limit 或 lifecycle 的数字，也不要把 reopen 称为 cold cache。
性能统一规则见根目录 [`TESTING.md`](../../TESTING.md)。
