# dogpaddle-store 性能报告

本报告只评价 `dogpaddle-store` 的数据结构和事务内执行路径，不规定 Flow、Operation、调度器或
事务批次将来应当如何组织。报告同时保留正收益、未检测到收益和回归门禁结果，避免把底层
microbenchmark 外推成上层吞吐承诺。

表格中的历史报告生成于 2026-08-25。完整基准可以通过文末命令重新运行。

## 当前 runner 与历史基线

当前四个 benchmark target 遵循[工作区测试协议](../../TESTING.md)。共享
`dogpaddle-bench-protocol` 提供严格设置解析、环境指纹、typed JSONL、统计和配对顺序；Store
自己的 `benches/support` 只保留 benchmark root、样本路径与人类格式薄适配。`ordered_map` 和
`append_log` 的入口负责场景编排，并各自把 fixture、measure、oracle、report 放在同名子目录，
不增加 Cargo target，也不把产品语义放进共享协议 crate。统一配置见根目录
[`TESTING.md`](../../TESTING.md)。

`append_log_endurance` 由统一 `smoke`/`reference` profile 选择固定 workload、工作集和总写入预算、逐 cycle
append/truncate 记录与 reopen checksum。所有 target 都输出完整环境指纹、计时外 oracle、隔离
样本 Store，以及 `smoke`/`reference` 文件系统档位。

下文 2026-08-25 数值是旧 runner 在明确记录的 WSL2 `/tmp` 环境中得到的历史优化证据，应保留
用于解释设计决策；它们不能直接作为新 runner 的回归基线。建立新基线时必须在固定 reference
目录重新运行，保存 stdout 中所有 `record=sample` 与 `record=pair_summary` JSONL，再从同一协议
的原始配对样本比较。

## 结论摘要

| 优化或数据对象 | 本机结果 | 结论 |
|---|---:|---|
| `AppendLog::append_batch`，128 B body | `4.363x`，9/9 配对胜 | 明确收益 |
| `AppendLog::append_batch`，1 KiB body | `1.887x`，9/9 配对胜 | 明确收益 |
| `AppendLog::append_batch`，8 KiB body | `1.209x`，9/9 配对胜 | 明确但随 value 变宽而减小 |
| 128 B、10k 条、一个 durable transaction | `2.135x`，9/9 配对胜 | 收益穿透一次 durable commit |
| 1 KiB / 8 KiB durable total | `1.042x`（5/9）/ `1.023x`（6/9） | 没有检测到稳定改善 |
| cursor prefix GC | paired median `1.181x`，5/5 进程级配对胜 | 20k 条的一次 truncate 总延迟约降低 15% |
| `AppendLog` 64 MiB 固定窗口长稳 | 处理 1 GiB 后文件为 80/96/112 MiB，后半程波动均为 0% | 前缀 GC 后 MDBX 页可稳定复用 |
| `Cell<u64>` committed hot get | paired median `1.204x`，5/5 配对胜 | 延迟约降低 17% |
| `OrderedMap<_, Vec<u8>, _>` point get | 胜负混合，paired median 约 `1.02x–1.04x` | 未观察到稳定回退，也不能归因为加速 |
| `OrderedMap::scan`，64 B full decode | paired median `1.296x–1.426x`，39/40 进程级配对胜 | owned batch 改为 bounded Cow + visitor 后稳定改善 |
| `OrderedMap::scan`，64 B projection | `1.185x–1.204x`，10/10 进程胜 | 小 value 也能跳过完整 value materialization |
| `OrderedMap::scan`，8 KiB projection | `5.813x–6.307x`，10/10 进程胜 | wide value 的列裁剪收益明确 |

底层收益最明确的路径包括 AppendLog 批量追加的事务体、AppendLog 连续前缀删除、固定宽度
value 的 clean point get，以及 OrderedMap committed scan 的 borrowed admission 与投影。durable
commit 的磁盘延迟足以掩盖 1 KiB 和 8 KiB append 的 CPU/body 改善，这是实测负结果，不应删掉。

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

本报告使用三种比较：

1. **同二进制配对**：新的 AppendLog benchmark 对相同 records 交错执行 scalar 与 batch，采用
   ABBA/BAAB 顺序，同时报告 paired ratio 和胜出次数。body workload 在活动事务内计时，随后
   rollback；durable workload 两侧都包含 begin、访问、写入和一次 commit。
2. **优化前后进程级配对**：GC、Cell 和 Map 保存优化前后的 release benchmark 二进制，按平衡
   顺序交错运行。每个进程内部仍取多个样本的中位数，最终比较五对进程中位数。
3. **固定窗口长稳**：AppendLog 对每种记录宽度连续运行 960 个 epoch；每个 epoch 先用一个
   durable transaction 追加约 1 MiB，再用另一个 durable transaction 删除等量前缀。报告给出
   两次连续完整运行的范围，并在每种宽度结束后重开 Store、逐条校验整个保留窗口。

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

### 固定窗口长稳与空间复用

长稳基准使用 128 B、1 KiB 和 8 KiB 三种完整编码宽度。每种宽度先填充 64 MiB 保留窗口，再执行
960 个 `append_batch + durable commit`、`truncate_before + durable commit` epoch；稳态阶段累计
追加并回收 960 MiB，加上初始窗口后每种宽度总共写入 1 GiB。batch 的目标编码大小为 1 MiB。

两次连续完整运行得到完全一致的空间结果：

| record | 保留条目 | seed | final | sampled peak | 已分配空间 / payload | 后半程波动 |
|---:|---:|---:|---:|---:|---:|---:|
| 128 B | 524,288 | 80 MiB | 80 MiB | 80 MiB | 1.25x | 0.00% |
| 1 KiB | 65,536 | 96 MiB | 96 MiB | 96 MiB | 1.50x | 0.00% |
| 8 KiB | 8,192 | 112 MiB | 112 MiB | 112 MiB | 1.75x | 0.00% |

这里同时读取了逻辑文件大小和 ext4 实际分配 block；本机两者相等。固定窗口形成后，所有 64-epoch
checkpoint 都保持不变，所以本轮结果不是“文件增长得较慢”，而是在处理后续 960 MiB 时没有增加
文件高水位。三组文件尺寸都以 16 MiB 为增量，MDBX 在本机表现出的映射/增长粒度相对 64 MiB
payload 仍然可见；不要把这三个空间放大比例外推到不同窗口大小或其他文件系统。

事务延迟与协议吞吐在两次连续运行中的范围如下。协议吞吐只累计 append 与 GC transaction，
不包含预填充和最终校验；每个 epoch 含两次 durable commit。

| record | append p50 | append p99 | append max | GC p50 | GC p99 | GC max | encoded throughput |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 128 B | 2.045–2.290 ms | 4.524–5.335 ms | 6.898–7.430 ms | 2.026–2.166 ms | 4.298–5.775 ms | 4.953–9.528 ms | 206.7–223.4 MiB/s |
| 1 KiB | 12.071–13.938 ms | 29.136–32.219 ms | 228.335–262.873 ms | 1.259–1.319 ms | 14.353–14.834 ms | 17.319–19.557 ms | 55.5–70.7 MiB/s |
| 8 KiB | 1.702–2.034 ms | 25.792–33.575 ms | 229.905–251.141 ms | 0.796–0.902 ms | 10.484–12.041 ms | 18.400–18.794 ms | 101.1–151.4 MiB/s |

1 KiB 和 8 KiB append 都出现约 228–263 ms 的最大延迟，且两轮总体吞吐有明显波动；报告保留这些
尾峰，不用平均值掩盖。128 B 的提交延迟更集中。三种宽度在两轮中都成功关闭并重新打开 Store，
持久化 bounds 与预期一致，整个 64 MiB 保留窗口的 offset、diff、key、长度和 payload 均逐条通过
校验。这个结果支持“当前协议可稳定复用页”，但不证明断电恢复延迟、冷缓存读取或 Flow 端到端性能。

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

当前默认 benchmark 使用 100,000 个 `u64 -> Vec<u8>` 条目，常规 value 为 64 B。下表取五个
after 进程各自 9 个样本中位数的中位数；scan page 同时受 1024 items 和 4 MiB 约束。

| workload | `Small` | `Large` | 说明 |
|---|---:|---:|---|
| random point get | 326 ns/次 | 270 ns/次 | 返回 owned 64 B `Vec<u8>` |
| ascending full scan | 86.8 ns/条 | 89.2 ns/条 | visitor 中执行 `decode_owned` |
| descending full scan | 86.9 ns/条 | 89.5 ns/条 | visitor 中执行 `decode_owned` |
| `u64 -> u64` ascending full scan | 85.2 ns/条 | 87.7 ns/条 | after-only 绝对数量级 |
| overwrite + rollback | 206 ns/次 | 179 ns/次 | 同一事务反复覆盖 |
| 8 个 Small 背景 namespace 下 point get | 385 ns/次 | 298 ns/次 | target workload 不变 |

`Small` 共享主 B+Tree，`Large` 使用独立 named table。这里的差异只描述当前数据规模和访问形状；
Size 仍然由对象用途决定，不是根据某一行纳秒数据动态选择。

### owned batch 到 bounded Cow visitor 的 A/B

旧实现先构造 public owned key/value batch，再由 typed layer 解码。新实现先在私有 cursor 内完成
range、续传和 item/byte admission，把有界 `Cow` 页带出 cursor，随后才调用 visitor。下面只比较
旧、新二进制中完全同形的 100,000 条、64 B value full-decode workload；业务 checksum、方向、
事务边界、1024 items 与 4 MiB page limit 均相同。

冻结的 release 二进制按 Before→After / After→Before 平衡顺序运行五对，每个进程内部取 9 个
样本。paired speedup 是每一对进程中位数的 `before / after`，范围保留全部五对而不是挑最好值。

| workload | before median | after median | paired speedup | after wins | 五对范围 |
|---|---:|---:|---:|---:|---:|
| byte key ascending，`Small` | 13.426 ms | 10.088 ms | **1.338x** | 5/5 | 1.228–1.425x |
| byte key ascending，`Large` | 13.227 ms | 10.662 ms | **1.296x** | 5/5 | 1.066–1.442x |
| byte key descending，`Small` | 12.669 ms | 9.498 ms | **1.369x** | 5/5 | 1.126–1.461x |
| byte key descending，`Large` | 12.612 ms | 10.127 ms | **1.314x** | 4/5 | 1.000–1.393x |
| typed key ascending，`Small` | 12.640 ms | 8.683 ms | **1.398x** | 5/5 | 1.064–1.487x |
| typed key ascending，`Large` | 12.209 ms | 8.919 ms | **1.326x** | 5/5 | 1.246–1.465x |
| typed key descending，`Small` | 11.990 ms | 8.688 ms | **1.426x** | 5/5 | 1.283–1.441x |
| typed key descending，`Large` | 12.017 ms | 8.951 ms | **1.343x** | 5/5 | 1.101–1.375x |

八个 workload 中有 39/40 个进程配对改善，elapsed time 约降低 23%–30%。收益不是“所有结果
都不再复制”：full decode 返回 `Vec<u8>` 时仍需拥有 value。主要变化是 clean committed page
可以借用 MDBX 编码，避免旧 raw batch 的临时 key/value materialization；typed fixed-width key
也可直接从借用字节解析。`u64 -> u64` 是新增 after-only workload，没有伪造旧基线，所以这里只
报告其当前绝对数量级，不计算因果比例。

### 完整解码与 projection

这一组不是旧新实现 A/B，而是在同一个 after 二进制、同一个已填充 fixture 上交错执行 Full 与
Projected。两种模式都解析同一个 `u64` key、读取 value 首字节并参与同形 checksum；区别仅是
Full 构造完整 `Vec<u8>`，Projected 直接从 entry 的编码视图读取。每个进程内部仍为 9 个 ABBA
样本，下表汇总五个进程各自的 paired median。

| value / layout | Full median | Projected median | paired Full / Projected | projection wins | 五进程范围 |
|---|---:|---:|---:|---:|---:|
| 64 B，`Small` | 8.828 ms | 7.517 ms | **1.204x** | 5/5 | 1.181–1.222x |
| 64 B，`Large` | 9.233 ms | 7.714 ms | **1.185x** | 5/5 | 1.160–1.214x |
| 8 KiB，`Small` | 7.298 ms | 1.213 ms | **6.307x** | 5/5 | 5.208–6.618x |
| 8 KiB，`Large` | 7.306 ms | 1.268 ms | **5.813x** | 5/5 | 5.637–6.308x |

8 KiB workload 使用 10,000 条 committed records。每项逻辑大小约为 8 B key + 8192 B value，
所以默认 4 MiB byte budget 先于 1024 item limit 生效，有效页约为 511 条；Full 与 Projected 的
admission 完全相同。这里能说的是：在 warm committed scan 中，projection 跳过完整
`StoreValue` decode 和业务对象 materialization。它不是零拷贝承诺：每页仍有私有 `Vec<Cow>`，
dirty page 仍可能由 MDBX 物化为 owned buffer，scan byte limit 也始终按完整编码计费。

### Durable 写入波动

同机完整运行中，10 万条批量写入曾从 `Small/Large ≈ 36/25 ms` 漂移到约 `202/103 ms`；单条
durable overwrite 也出现约 0.8–2.7 ms/事务的整轮变化。这个波动来自当前 WSL2 `/tmp` 的 I/O
历史，远大于 codec 微优化。生产磁盘判断必须在目标机器上独立复测。

## 正确解读这些收益

- 可以说：AppendLog typed batch 明显减少同事务内 metadata/cursor 开销。
- 可以说：cursor prefix GC 在固定单事务 workload 中稳定降低了总延迟。
- 可以说：固定宽度 value 的 committed hot get 可以避免临时 `Vec`，Cell 实测约降低 17% 延迟。
- 可以说：OrderedMap 的 bounded Cow visitor 在同形 64 B full scan A/B 中稳定改善 23%–30%。
- 可以说：warm committed wide-value scan 中，只读少数字段时 projection 可跳过完整业务对象物化。
- 不可以说：Flow、Source 或 Station 吞吐已经提高相同比例；本报告没有测这些上层策略。
- 不可以说：1 KiB/8 KiB durable append 已稳定变快；配对结果不支持。
- 不可以说：OrderedMap scan 是零拷贝；它仍保留有界私有 Cow 页，dirty page 也可能物化。
- 不可以说：所有 `StoreValue` 都会更快；返回 `Vec`/`String` 时仍需拥有结果。

## 重新运行

PR 中的规范 protocol smoke 入口是：

```bash
cargo xtask bench-smoke
```

它使用代码内固定的缩小矩阵实际运行工作区所有 benchmark。以下 Store 单 target 命令用于本地
场景诊断，或在固定 reference 环境建立正式基线：

```bash
cargo bench -p dogpaddle-store --bench cell
cargo bench -p dogpaddle-store --bench ordered_map
cargo bench -p dogpaddle-store --bench append_log
cargo bench -p dogpaddle-store --bench append_log_endurance
```

默认运行是安全的 `smoke` 档。正式基准示例：

```bash
DOGPADDLE_BENCH_PROFILE=reference \
DOGPADDLE_BENCH_ROOT=/absolute/path/on/reference-disk \
cargo bench -p dogpaddle-store --bench append_log

DOGPADDLE_BENCH_PROFILE=reference \
DOGPADDLE_BENCH_ROOT=/absolute/path/on/reference-disk \
cargo bench -p dogpaddle-store --bench append_log_endurance
```

workload 解释见 [`README.md`](./README.md#性能)，typed JSONL 与统一验证矩阵见
[根目录 `TESTING.md`](../../TESTING.md)。
做回归比较时必须固定机器、文件系统、Rust profile、records、record bytes、transaction 数与
codec；至少同时查看 paired ratio、胜出次数、min/median/max，并保留没有收益的结果。
