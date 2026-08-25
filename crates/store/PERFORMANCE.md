# dogpaddle-store 性能报告

本报告只评价 `dogpaddle-store` 的数据结构和事务内执行路径，不规定 Flow、Operation、调度器或
事务批次将来应当如何组织。报告同时保留正收益、未检测到收益和回归门禁结果，避免把底层
microbenchmark 外推成上层吞吐承诺。

报告生成于 2026-08-25。完整基准可以通过文末命令重新运行。

## 结论摘要

| 优化或数据对象 | 本机结果 | 结论 |
|---|---:|---|
| `AppendLog::append_batch`，128 B body | `4.363x`，9/9 配对胜 | 明确收益 |
| `AppendLog::append_batch`，1 KiB body | `1.887x`，9/9 配对胜 | 明确收益 |
| `AppendLog::append_batch`，8 KiB body | `1.209x`，9/9 配对胜 | 明确但随 value 变宽而减小 |
| 128 B、10k 条、一个 durable transaction | `2.135x`，9/9 配对胜 | 收益穿透一次 durable commit |
| 1 KiB / 8 KiB durable total | `1.042x`（5/9）/ `1.023x`（6/9） | 没有检测到稳定改善 |
| cursor prefix GC | paired median `1.181x`，5/5 进程级配对胜 | 20k 条的一次 truncate 总延迟约降低 15% |
| `Cell<u64>` committed hot get | paired median `1.204x`，5/5 配对胜 | 延迟约降低 17% |
| `OrderedMap<_, Vec<u8>, _>` point get | 胜负混合，paired median 约 `1.02x–1.04x` | 未观察到稳定回退，也不能归因为加速 |
| `OrderedMap::scan` | 本轮没有修改 | 仍是 owned scan，不能称为零拷贝 |

底层收益最明确的三条路径是：AppendLog 批量追加的事务体、AppendLog 连续前缀删除，以及固定
宽度 value 的 clean point get。durable commit 的磁盘延迟足以掩盖 1 KiB 和 8 KiB append 的
CPU/body 改善，这是实测负结果，不应删掉。

## 环境与指标口径

| 项目 | 值 |
|---|---|
| CPU | Intel Core i5-14400F，16 个逻辑 CPU |
| 系统 | Linux x86_64，WSL2 6.6.87.2 |
| 临时目录文件系统 | `/tmp`，ext4，设备 `/dev/sdd` |
| Rust | rustc 1.98.0，release benchmark profile |
| MDBX | durable sync，单线程执行 |
| 读取 | warm cache |
| 变更样本 | 每个样本使用新临时 Store，构造和校验在计时外 |

本报告使用两种比较：

1. **同二进制配对**：新的 AppendLog benchmark 对相同 records 交错执行 scalar 与 batch，采用
   ABBA/BAAB 顺序，同时报告 paired ratio 和胜出次数。body workload 在活动事务内计时，随后
   rollback；durable workload 两侧都包含 begin、访问、写入和一次 commit。
2. **优化前后进程级配对**：GC、Cell 和 Map 保存优化前后的 release benchmark 二进制，按平衡
   顺序交错运行。每个进程内部仍取多个样本的中位数，最终比较五对进程中位数。

`records/s` 和 `encoded MiB/s` 都是按逻辑编码大小计算的吞吐，不是 MDBX 物理写放大或磁盘带宽。
当前 WSL2 文件系统的 durable 延迟会出现整轮漂移；因此结论以配对结果和胜出次数为主，不用一次
全局最小值除以另一次全局最小值。

## `AppendLog<T>`

### 批量追加事务体

记录编码为 `[diff: i64][key: u64][payload]`。每个 workload 都处理 10,000 条 typed records；
scalar 逐条调用 `append`，batch 调用一次 `append_batch`。body 计时不包含 transaction begin、
access、commit 或 rollback，但包含 value 编码和所有 MDBX cursor 写入。

| record | scalar body 中位数 | batch body 中位数 | paired speedup | batch wins |
|---:|---:|---:|---:|---:|
| 128 B | 5.652 ms | 1.292 ms | **4.363x** | 9/9 |
| 1 KiB | 10.417 ms | 5.493 ms | **1.887x** | 9/9 |
| 8 KiB | 55.585 ms | 46.535 ms | **1.209x** | 9/9 |

收益来自 Store 内部减少的操作数量：一批只读取和验证一次 `head/tail`，复用一个 MDBX cursor，
按单调 offset 使用 `APPEND | NO_OVERWRITE`，最后只更新一次 metadata。value 越大，复制 value
本身所占比例越高，因此固定的 cursor/metadata 节省占比越小。

### 包含一次 durable commit

同样是 10,000 条 records，但计时包含 transaction begin、访问、写入和一个 durable commit。
paired speedup 是每一对 scalar/batch 耗时比的中位数，不一定等于两列独立中位数的商。

| record | scalar 总耗时中位数 | batch 总耗时中位数 | paired speedup | batch wins | 判定 |
|---:|---:|---:|---:|---:|---|
| 128 B | 8.857 ms | 4.149 ms | **2.135x** | 9/9 | 稳定改善 |
| 1 KiB | 168.766 ms | 167.615 ms | 1.042x | 5/9 | 未检测到稳定改善 |
| 8 KiB | 439.575 ms | 125.853 ms | 1.023x | 6/9 | 未检测到稳定改善 |

8 KiB 两列独立中位数看似相差很大，但样本存在秒级 I/O 尖峰，配对比仅 1.023x 且只赢 6/9。
因此不能把独立中位数的商包装成稳定收益。这个例子也说明为什么报告必须保留 paired ratio。

这组数据只证明“一个 AppendLog collection、一个包含 10k records 的事务”。它没有测量任何
Flow Source，也没有证明任意上层 batch 都会获得相同比例。

### Prefix GC

GC A/B 固定为 20,000 条 128 B records，一次 `truncate_before` transaction 删除全部前缀。
优化前逐 key delete；优化后一次 `set_range`，随后用 cursor `del(CURRENT)` 和
`get_current` 连续删除。

| pair | before 进程中位数 | after 进程中位数 | before / after |
|---:|---:|---:|---:|
| 1 | 5.609 ms | 3.890 ms | 1.442x |
| 2 | 4.706 ms | 3.985 ms | 1.181x |
| 3 | 4.417 ms | 3.741 ms | 1.181x |
| 4 | 4.484 ms | 3.695 ms | 1.214x |
| 5 | 5.077 ms | 4.422 ms | 1.148x |

五对全部改善，paired speedup 中位数为 **1.181x**，对应总延迟约降低 **15%**；单对观察范围约
为 13%–31%。包含 append 和两次 durable transaction 的 steady workload 没有检测到稳定变化，
因为 GC 只是总成本的一部分，且该 workload 的 append 仍使用 scalar API。

### 读取和投影的当前数量级

当前实现 warm scan 10,000 条记录的中位平均成本如下。此表不是本轮优化前后比较，只用于说明
AppendLog 读取路径的数量级。

| record | 只投影 `diff` | 完整 owned decode | 完整解码 / 投影 |
|---:|---:|---:|---:|
| 128 B | 62 ns/条 | 89 ns/条 | 1.44x |
| 1 KiB | 78 ns/条 | 139 ns/条 | 1.79x |
| 8 KiB | 99 ns/条 | 875 ns/条 | 8.84x |

投影避免完整 payload materialization，record 越宽收益越大。这里的逻辑 GiB/s 不能解释成磁盘
带宽，因为数据是 warm cache，projection 只解析记录头部。

## `Cell<T>`

### Cow value decode A/B

旧 point get 先让 MDBX 生成 `Vec<u8>`，再解析 `u64`；新路径允许 committed clean page 以
`Cow::Borrowed` 进入 decoder，固定宽度 value 直接从 `as_ref()` 解析。每个进程运行 1,000,000
次同事务 warm
`Cell<u64>::get`，下面列出五对进程中位数。

| pair | before | after | before / after |
|---:|---:|---:|---:|
| 1 | 48.262 ms | 40.072 ms | 1.204x |
| 2 | 48.181 ms | 38.981 ms | 1.236x |
| 3 | 48.100 ms | 45.316 ms | 1.061x |
| 4 | 50.221 ms | 41.131 ms | 1.221x |
| 5 | 45.447 ms | 39.370 ms | 1.154x |

五对全部改善，paired speedup 中位数为 **1.204x**，即 hot-get 延迟约降低 **17%**。当前完整
Cell benchmark 的 100,000 次 warm get 中位数约为 41.8 ns/次；read-update-durable-commit
约为 0.938 ms/事务。后者由 commit 主导，本报告不声称 Cow 让 durable update 提升 20%。

dirty page 不会被不安全地借用：libmdbx 会产生 `Cow::Owned`，decoder 对两种 variant 必须返回
相同逻辑值。MDBX 允许保守地把 clean page 也返回为 Owned，所以具体 variant 是性能行为而非
Store 的正确性承诺。

## `OrderedMap<K, V, SIZE>`

### 当前读取数量级

当前默认 benchmark 使用 100,000 个 `u64 -> Vec<u8>` 条目，value 为 64 B。最新完整运行的
warm read/scan 中位数如下：

| workload | `Small` | `Large` | 说明 |
|---|---:|---:|---|
| random point get | 342 ns/次 | 279 ns/次 | 返回 owned 64 B `Vec<u8>` |
| ascending scan | 133 ns/条 | 129 ns/条 | owned batch，batch size 1024 |
| descending scan | 133 ns/条 | 128 ns/条 | owned batch，batch size 1024 |
| overwrite + rollback | 215 ns/次 | 190 ns/次 | 同一事务反复覆盖 |
| 8 个 Small 背景 namespace 下 point get | 363 ns/次 | 286 ns/次 | target workload 不变 |

`Small` 共享主 B+Tree，`Large` 使用独立 named table。这里的差异只描述当前数据规模和访问形状；
Size 仍然由对象用途决定，不是根据某一行纳秒数据动态选择。

### Cow 回归门禁

本轮 point-get 改造也经过优化前后进程级 A/B，但现有 Map benchmark 的 value 是 `Vec<u8>`：
无论旧路径还是新路径，公开返回 owned `Vec<u8>` 都必须复制一次。因此它只能验证没有稳定回退，
不能直接证明 Map 获得了 Cow 加速。

| workload | paired median before/after | after wins | 判定 |
|---|---:|---:|---|
| typed key, `Small` | 1.024x | 4/5 | 未观察到稳定回退 |
| typed key, `Large` | 1.043x | 4/5 | 未观察到稳定回退 |
| byte key, `Small` | 约 1.030x | 3/5 | 胜负混合，视为无变化 |
| byte key, `Large` | 约 1.019x | 3/5 | 胜负混合，视为无变化 |

要单独证明 OrderedMap 的 Cow 正收益，需要增加优化前后同形状的
`OrderedMap<u64, u64, SIZE>` 基准。本轮没有伪造缺失的 before 数据，也没有把 Cell 的比例直接
复制给 Map。

OrderedMap scan 本轮完全没有优化：raw scan 仍会物化 owned key/value batch，再由 typed layer
解码。报告不会称其为 borrowed 或 zero-copy scan。

### Durable 写入波动

同机完整运行中，10 万条批量写入曾从 `Small/Large ≈ 36/25 ms` 漂移到约 `202/103 ms`；单条
durable overwrite 也出现约 0.8–2.7 ms/事务的整轮变化。这个波动来自当前 WSL2 `/tmp` 的 I/O
历史，远大于 codec 微优化。生产磁盘判断必须在目标机器上独立复测。

## 正确解读这些收益

- 可以说：AppendLog typed batch 明显减少同事务内 metadata/cursor 开销。
- 可以说：cursor prefix GC 在固定单事务 workload 中稳定降低了总延迟。
- 可以说：固定宽度 value 的 committed hot get 可以避免临时 `Vec`，Cell 实测约降低 17% 延迟。
- 不可以说：Flow、Source 或 Stage 吞吐已经提高相同比例；本报告没有测这些上层策略。
- 不可以说：1 KiB/8 KiB durable append 已稳定变快；配对结果不支持。
- 不可以说：OrderedMap scan 已零拷贝；本轮没有修改该路径。
- 不可以说：所有 `StoreValue` 都会更快；返回 `Vec`/`String` 时仍需拥有结果。

## 重新运行

```bash
cargo bench -p dogpaddle-store --bench cell
cargo bench -p dogpaddle-store --bench ordered_map
cargo bench -p dogpaddle-store --bench append_log
```

常用环境变量及 workload 解释见 [`README.md`](./README.md#性能)。做回归比较时必须固定机器、
文件系统、Rust profile、records、record bytes、transaction 数与 codec；至少同时查看 paired
ratio、胜出次数、min/median/max，并保留没有收益的结果。
