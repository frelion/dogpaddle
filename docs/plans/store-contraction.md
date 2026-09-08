# Store 收缩性重构计划

状态：S0–S5 已完成实现；本文保留为原设计计划与实施对照。日期：2026-09-08。

## 实施结果、计划差异与验证状态

S0–S5 已全部落地。Store 的唯一后端现为 RocksDB `OptimisticTransactionDB`，对外提供 `Cell`、`OrderedMap`、`OrderedMultiset`、`PartitionedMultiset`、`Queue` 和 `SubscribedLog` 六种类型化持久结构。MDBX、Small/Large placement 和公共 `AppendLog` 已删除；Distinct、Aggregate、PostgreSQL/MySQL CDC spool 与 Flow 持久边已分别迁移到新的直接结构。执行模型仍按原边界保持顺序 Flow 和线性写事务 owner，本轮没有引入并发调度或多写事务 API。把全部已跟踪改动和九个新增文件一起计入后，相对于实施前基线净减少 3,916 行，约为 3,900 行。

实现与原计划的主要差异不是目标或阶段范围变化，而是 S1 已明确选用 `OptimisticTransactionDB`，且 S2–S5 在实际调用方和持久布局证据约束下收定了最终 API。以下阶段正文继续使用当时的未来时态，作为设计动机、取舍和验收标准的历史记录；不能据此推断这些工作仍未开始。

定向验证覆盖了 Store、Operation、Flow、SQL、Change–Store seam 和 benchmark test mode，S3–S5 还完成了独立审判复核。最终 workspace `cargo xtask check` 已完整通过，包括格式、debug/release workspace tests、all-targets Clippy 与 `-D warnings` Rustdoc。

## 目标与边界

以 RocksDB 作为 Store 的唯一实现基础，把 Store 建成牢固、直接的事务型持久数据结构库，降低 Operation 与 Flow 的实现复杂度。本轮实施始终保持顺序 Flow 和单一写事务能力；选择 RocksDB 同时避免在持久布局和上层边界中锁死未来的多 Station 并发，但本轮没有实现、暴露或验证并发写入与并发调度。

评价标准是整体复杂度下降：事实更少、职责更集中、上层更直接、需要同时理解的规则更少。公共 API 或类型数量可以增加，但必须有真实使用场景，并能替代现有复杂性。代码行数和新增功能数量均不作为硬性指标。

### 设计审美

- 概念准确：一个类型、一种结构应当对应一个值得独立命名的职责；名称能帮助读者预测行为。
- 事实唯一：派生信息优先计算或封装，持久化事实由一个结构拥有，避免同步多份事实再增加一致性检查。
- 流程直接：关键执行路径可以顺着阅读，减少纯转发、没有实际变化空间的动态分派和跳转。
- 边界清楚：在外部输入和类型擦除等必要边界认真校验，内部使用已经建立的保证。
- 抽象有用：新结构承担完整而明确的语义，让真实调用更自然；语义不同的结构可以分别存在。
- 删除彻底：替代方案落地时旧概念一起退出，每个里程碑都形成完整、干净的作品。

评审时把代表性的上层调用与 Store 实现一起阅读：上层是否更容易理解，复杂性是否被真正消除或收拢，Store 内部是否依然直接。不能仅通过把原有分支搬进一个大模块就认定收缩成功。

这份计划不单独覆盖当前仓库约束；已实现语义与验证归属以适用的 `AGENTS.md`、产品 README、Rustdoc、`TESTING.md` 和布局证据为准。

### 始终保留的语义

- Store 不依赖 Arrow、Change、Operation 或 Flow；其他产品 crate 不直接使用 RocksDB。
- Operation 的 Definition/binding 保持纯；资源在 build/open 装配；Operation 不接收 Store，也不能自行开始或提交事务。
- 当前 Change 的逐事件顺序、diff、负前缀/overflow 检查与完整 Schema guard。
- Operation state、output 与适用的 input completion 原子提交；失败和背压不留下部分状态。
- commit 成功后才能运行 AfterCommit；外部提交不确定仍有明确的 fail-stop/reopen 边界。
- 同一 Station 同时最多执行一个 turn，已提交的 continuation 能从持久状态恢复。
- 每个事实只有一个持久化 owner；每个阶段完成后只有一条正常读写路径。

### 第一轮不扩展的能力

多写事务、事务冲突与重试、并发 Station 调度、worker/partition/lease、为并发提前拆分 acknowledgement 与 GC、跨层流水线、历史查询、VersionedMap、Merge 型累计结构、Join/TopK/Window 算子本身、新备份平台、动态订阅、rewind、durable pin 语义改变和故障隔离策略调整，均不作为这次基础收缩的附带工作。支撑现有 Aggregate 并可直接服务未来 TopK 的有序状态结构属于本轮；算子执行语义仍由真实需求分别推动。

不建立多后端 trait/registry、双写、fallback、旧格式迁移、通用 ORM 或新的通用执行框架。开发期持久布局变化使用新 state path 并重建；涉及持久 Sink 时遵守现有 ownership 规则，使用新的目标，不能把旧目标直接交给新 Flow 接管。

## 阶段与依赖

| 阶段 | 依赖 | 主要交付 | 本次必须消除或简化的旧复杂性 |
| --- | --- | --- | --- |
| S0 契约和删除清单 | 无 | 一份紧凑的行为/owner/证据地图 | 明确哪些规则属于外部边界、哪些由类型保证，避免重构时重复加检查 |
| S1 RocksDB Store | S0 | 更换唯一后端，完善事务与现有集合，必要的调用方机械迁移 | MDBX 适配、Small/Large、placement 分支和 named-table 管理 |
| S2 Distinct 纵向收缩 | S1 | 完整行 key 与 `OrderedMultiset` | Distinct 的 digest 寻址、桶内查找和桶编解码 |
| S3 Aggregate 索引收缩 | S2 | 直接分组与分区有序多重集合 | 剩余碰撞桶、手工分区 range、极值撤回后的整组重扫、无实际需要的归约间接层 |
| S4 CDC spool 收缩 | S1 | `Queue` 及 PostgreSQL/MySQL bootstrap spool 完整切换 | CDC 的 bounds + scan-one + truncate-one 样板及过宽 spool capability |
| S5 日志订阅归属迁移 | S4 | `SubscribedLog` 及 Flow 完整切换 | 公共 `AppendLog`、Flow 自有 cursor codec、消费者反向引用、frontier/reclaim 协议 |

S2/S3 是关系状态支线，S4/S5 是持久序列支线；它们可以在 S1 后分别推进。序列支线先用小而完整的 CDC Queue 切片收定私有 sequence core，再进行范围更大的 Flow 日志迁移。实际同时修改共享 Store API 时，先合并相关接口，再继续消费方，避免多条分支各自创造近似接口。

每个阶段都是独立、可合并的里程碑：

| 里程碑 | 阶段 | 完成后的作品状态 |
| --- | --- | --- |
| M0 契约地图 | S0 | 当前语义、owner、证据和删除依据明确 |
| M1 Store 基座 | S1 | 完整引擎运行在 RocksDB 上；MDBX 与 placement 全部退出 |
| M2 关系状态 | S2 | Distinct 使用直接 key；`OrderedMultiset` 经过真实调用验证 |
| M3 关系索引 | S3 | Aggregate 使用分区有序索引；最后的碰撞桶、手工 range 与极值重扫退出 |
| M4 私有队列 | S4 | 两个 CDC bootstrap spool 使用窄 FIFO；`AppendLog` 只剩 Flow 一个 owner |
| M5 日志归属 | S5 | Store 拥有固定订阅协议；Flow 不再实现 cursor/frontier/reclaim；公共 `AppendLog` 退出 |

M5 就是本计划终点。并发事务、并发调度以及为并发消除确认写热点的 ack/GC 解耦，都是后续独立项目。

## 数据结构与 API 目录

目标不是把 Store 做成容器陈列室，而是用少量完整结构替代上层重复维护的协议。最终保留 `Cell` 与 `OrderedMap`，新增有序多重集合家族、`Queue` 和 `SubscribedLog`，删除 `AppendLog`。两种多重集合共享私有 multiplicity engine，但有各自准确的持久布局与调用语言；Queue 与订阅日志也共享私有 sequence core，而不共享会产生非法操作的公共 capability。

### 改造前基础结构的去留

| 结构 | 继续承担的职责 | 本轮收缩 |
| --- | --- | --- |
| `Cell<T>` | 至多一个类型化值，参与跨结构原子事务 | 删除 placement；不引入专用 counter、checkpoint 或 allocator Cell |
| `OrderedMap<K, V>` | 点读、写入、删除、有序范围和有界分页 | 删除 Size 泛型与物理布局含义；保持通用索引基础 |
| `AppendLog<T>` | 改造前同时承载 CDC FIFO spool 与 Flow fan-out output 两套语义 | S1 只做机械后端迁移；S4/S5 分别迁入更窄结构后删除公共类型、layout 和 access API；现已删除 |

### 新增的持久数据结构

| 结构 | 状态 | 首个 owner | 独立拥有的不变量 | 替代的旧复杂性 | 阶段 |
| --- | --- | --- | --- | --- | --- |
| `OrderedMultiset<K>` | 引入；S2 用真实代码确定最小表面 | Distinct | key 有总序；不存在即零；持久 multiplicity 恒为正；checked 增减；归零删除 | weight 编解码和重复的 get/check/put/remove；配合直接 key 删除碰撞桶 | S2 |
| `PartitionedMultiset<P, K>` | 引入为 `OrderedMultiset` 的强类型分区形态，共享私有 multiplicity engine | Aggregate admission 与 MIN/MAX index；未来 grouped TopK | 每个 partition 内按 `K` 有序，访问不能越过 partition，计数不变量与全局多重集合相同 | Operation 手写 composite framing、sentinel range 与 `scan(limit=1)` 协议 | S3 |
| `Queue<T>` | 引入 | PostgreSQL/MySQL CDC bootstrap spool | 单 owner FIFO、hard logical capacity、原子 push/pop、可恢复 queued bytes | bounds + scan-one + truncate-one；向算子暴露的任意 offset/range/truncate | S4 |
| `SubscribedLog<T>` | 引入 | Flow edge | 固定订阅者、稳定 offset、每订阅者唯一 position、确认与安全保留 | Flow 的 cursor codec、consumer frontier、反向资源引用和回收协议 | S5 |

这里的“引入”指上层获得一个明确的数据结构，而不强迫每个名字对应一套完全独立的代码。`OrderedMultiset<K>` 可以建立在私有的 ordered key + `NonZeroU64` 核心上；`PartitionedMultiset<P, K>` 复用同一核心，由 Store 负责安全 framing 和分区边界；`Queue<T>` 与 `SubscribedLog<T>` 复用私有 sequence core。公共概念由不变量和合法操作决定，RocksDB column family 数量不决定公共类型数量。

#### `OrderedMultiset<K>`：正 multiplicity 的持久有序集合

它表达一个通用存储事实：key 不存在等价于 count 为零，持久值只能是正 `u64`。事务内的 checked adjustment 接受正负变化，归零删除，负结果或 overflow 失败，并返回本次修改的 before/after。Store 不解释 Arrow、Change、diff 或分组语义。

概念 API：

```text
multiplicity(key) -> u64
adjust(key, delta) -> { before, after }
scan(range, direction, limit)
first(range) / last(range)
```

`delta == 0` 不物化缺失 key；underflow/overflow 不修改目标并毒化整个写事务，避免调用方误吞错误后提交其他部分状态；同一事务连续 adjustment 以及随后 first/last/scan 必须 read-your-writes。访问面不提供任意 `set_count`、put 或 remove。

真实调用方与删除收益：

- Distinct：`canonical row -> count`，删除 digest 寻址、碰撞桶、桶内查找和权重编解码。
- Aggregate admission：`partition(group id)[canonical input row] -> count`，复用同一 checked multiplicity 规则。
- Aggregate indexed argument：`partition(layout, group id)[ordered argument] -> count`，直接提供 MIN/MAX 的首尾候选。

这比一个接受任意闭包的通用 Map Update API 更窄、更容易定义失败语义，并且已经有多个真实消费者。最终返回类型在 S2 以 Distinct 的调用代码为准；即使底层复用 ordered map 核心，也应封闭任意 put/remove，让调用方只能使用正计数语义。

#### `PartitionedMultiset<P, K>`：分区内有序视图

它让调用方先选择一个 typed partition，再在其中调整、首尾定位或有界扫描。Store 负责把 `P` 与 `K` 编成不会串区的内部 key；Operation 分别拥有 `P` 和 `K` 的业务 codec 与排序语义。公共 API 不接收 raw prefix、encoded bound 或手写 successor。

```text
partition(p).multiplicity(key)
partition(p).adjust(key, delta)
partition(p).first() / last()
partition(p).scan(direction, limit)
```

这不是只为未来预埋的结构。Aggregate 当前就用它表达每组 exact admission 和每组 MIN/MAX argument index，并删除手工 `layout | group_id | value` range。未来 grouped TopK 只需让 Operation 定义 `RankKey = sort tuple + deterministic row tie-breaker`，然后在对应 partition 中按方向取前 `K` 个 key 及 multiplicity。Store 不理解 SQL ASC/DESC、NULL、tie、Change 或 TopK 输出更新。

```text
let group = rankings.partition(group_key)
group.adjust(rank_key, diff)
group.scan(direction, limit = k)
```

`rank_key` 的排序 tuple 决定名次，完整 row identity 打破相同排序值的歧义，multiplicity 表达重复行。一次 adjustment 后的首尾和扫描必须读取同一事务已经写入的视图；TopK Operation 以后只需在此之上计算 old/new boundary 与输出差分。

RocksDB 的排序迭代可以支持从边界开始取有界前缀；它不天然提供按序号的 `rank/select`。普通 TopK 不需要为此预建带子树计数的 rank tree。只有未来出现任意第 N 名或大范围 percentile 的真实需求时，才讨论另一个维护累计基数的结构。

#### `Queue<T>`：单 owner 的持久 FIFO

它准确表达两个 CDC bootstrap spool 的工作方式：捕获期从尾部 push，发布期原子 pop 头部，重置期每 turn pop 并丢弃一个头部，容量按 queued logical bytes 硬拒绝。offset、任意 range、任意 truncate target 和 entry forwarding 都不进入 Operation API。

```text
is_empty / queued_bytes
try_push(value, hard_capacity)
pop_front -> optional owned value
```

Publishing 在写事务中 pop、解码并返回 output；若 Schema guard、capacity append 或 commit 失败，pop 随整个事务回滚。Resetting 也调用一次 pop 并丢弃结果，因此天然有界。只有实际证据表明重置时解码大 value 成为问题，才增加 `discard_front`。PG 与 MySQL 切换后，原来的 `bounds + scan(limit=1) + head+1 + truncate_before` 路径一起删除。

#### `SubscribedLog<T>`：带固定订阅者的持久日志

它拥有稳定 offset、构建时固定的 subscriber、每个 subscriber 的唯一持久 position、事务内 acknowledgement、容量和安全保留规则。Store 只认识不透明 subscriber identity 和泛型 payload，不认识 Station、port、Schema、Change 或拓扑。

概念能力：

```text
writer: append / try_append
subscriber: peek / acknowledge expected entry
status: tail / subscriber positions / retention
```

创建时一次性给出非零 subscriber count，并在初始化事务内建立全部 position；open 验证结构配置。writer 和每个 subscription 是从同一持久结构派生的精确 capability，不是额外的 StoreData。运行期不提供 subscribe/unsubscribe、任意设置 position、rewind 或 truncate。

回收是结构内部行为，不在本轮暴露 public maintenance capability。S5 可以继续在确认事务内精确回收；以后若并发证明确有共享写热点，可以在不修改 Flow API 的前提下把内部实现换成滞后、有界的清理。

它的真实调用方是 Flow 的持久边。完成 S5 后，Flow 删除自己的 cursor codec、ConsumerCursor、producer 对 consumer Station state 的反向引用，以及 frontier/reclaim 计算。此时公共 `AppendLog<T>` 已没有产品 owner，并完整删除。

`Queue` 与 `SubscribedLog` 的底层都可以使用同一个私有 sequence core，但它们的公共权限不同：Queue 的 owner 可以消费或丢弃头部；订阅日志的 writer 不能决定 retention，任何 subscription 也不能任意 truncate。用两个准确类型表达合法操作，比给一个日志增加 mode flag 和运行期非法组合更简单。

### 随真实改造加入的小型 API

| API | 引入时机 | 必须产生的简化 |
| --- | --- | --- |
| `first()` / `last()` | S3 Aggregate 极值索引 | 在 typed partition 内直接返回一个 owned 首尾项，删除 `scan(limit=1)` 的 callback、continuation 和重新归约样板 |
| 有界方向扫描 | S3 与 `PartitionedMultiset` 同时加入 | 一个 API 同时服务 MIN/MAX 的一项定位和未来 TopK 的前 K 项；结果数量必须显式有界 |
| Operation-owned order key | S2/S3 状态布局 | Operation 拥有 exact identity、ASC/DESC、NULL 与 tie-break 编码；Store 只可靠保持其总序 |
| 通用 Entry/Update | 只有 `OrderedMultiset` 之外仍出现重复且易错的 get-check-put/remove 时 | 同步执行一次并返回前后值；不缓存 callback，也不在冲突后隐式重跑业务代码 |

### 当前不新增

- `OrderedSet` 使用 `OrderedMap<K, ()>`；需要重复计数与排序的关系索引使用 `OrderedMultiset`。
- 不预建 `PartitionedMap`、`MultiMap`、`GroupedMap`、`IndexedMap`、`CounterMap` 整套笛卡尔积家族；当前只给已经由 Aggregate 证明的有序多重集合增加 typed partition 形态。
- RocksDB Blob、column family、Merge operator、snapshot sequence、compaction filter 和缓存策略都是 Store 实现机制，不是上层数据结构。
- `VersionedMap<K, V, Revision>` 只有出现明确的可恢复历史读取者时才加入。它必须定义应用 revision 与保留下界，不能暴露或持久化 RocksDB 内部 sequence。
- 不用存储内部 MVCC 替代 Change、output log、durable Claim、connector checkpoint 或外部 Sink 的 Prepared/AfterCommit 协议。

## S0：先明确改动依据

工作量限定为检查现有证据和记录决策，不提前设计整套未来 Store。

- 对跨集合原子性、snapshot、read-your-writes、扫描、回滚、poisoning、持久性、capability、日志容量，分别列出当前 owner 和最强证据。
- 对准备删除的检查，指出它由哪个类型、构造边界或更靠近输入的检查保证；没有替代依据的检查保留。
- 区分语义测试与原生 MDBX 布局注入测试；前者作为替换后的验收，后者随实现变更重新编写。
- 确认安全 Rust 绑定能直接实现当前所需的跨集合原子提交、read-your-writes、一致 snapshot、范围读取和同步 durable commit，并确认底层实例未来具有建立多个独立事务的技术路径；选定并锁定唯一实现。不自行实现 MVCC，不设计并发事务公共 API，也不在本阶段决定未来使用 optimistic 或 pessimistic 冲突模型。

出口：可以明确说出 S1 改什么、依赖哪些现有测试、哪些测试必须随物理布局调整。不能以全面补测试或完整架构设计阻塞 S1。

## S1：一次完整的 Store 后端替换

主要修改 `crates/store/`、根依赖及必需的下游类型声明。Flow 继续顺序调度，现有算子仍使用当前状态组织，日志继续使用现有消费和回收语义。

- 用 RocksDB 替换 MDBX；优先使用现成安全事务接口，Store 仍掌握提交边界。
- 在同一阶段删除 Small/Large 与其物理 placement 语义；Operation/Flow 对泛型与声明的修改仅做机械迁移。
- 用 crate-private、稳定的 collection kind 取代 placement：catalog 只记录 `logical name -> namespace id + kind`，`open_data::<D>` 验证 Cell/Map/Log 等结构类别。它不记录 Rust `K/V` 类型或 codec fingerprint；精确 codec 仍由 Operation tag 与 data declaration 负责。
- 保留 Cell、OrderedMap、AppendLog、只读能力和 setup/runtime 分离的职责；清理纯转发、重复分支及暴露原生实现细节的文档。
- 明确 snapshot 视图与事务当前视图，保证同一 Change 内对相同 key 多次读改写的正确性。
- 维持现有范围/方向/分页的可观察语义；如果发现不必要的接口规则，另列变更，不在后端替换中悄悄修改。
- WAL 开启且 commit 满足当前 durable 承诺。RocksDB key 删除与磁盘空间最终释放的区别在 Store 文档中说明。

本阶段保留当前线性写事务能力：`Transactions` 不可克隆，`begin` 继续通过独占借用开始一个写事务，Flow 继续顺序执行。这个限制属于当前执行模型，不成为 collection 的持久语义或 RocksDB 的能力边界。Operation 和数据结构仍只接收 transaction-bound access，因此未来替换事务启动能力时不必重做算子状态 API。

S1 不新增 Backend trait、后端泛型或 registry，也不新增 ConcurrentTransactions、TransactionScope、WorkerTransactions、IsolationLevel、ConflictPolicy、RetryPolicy、资源 lease/read-set/write-set，不能把 RocksDB transaction、snapshot、column family 或 sequence number 暴露给上层。Store 不自动重跑任意 closure 或 `PreparedTurn::apply`。

Small/Large 不提前改造成一套短命的 MDBX 新布局，也不在 RocksDB 后端保留成没有含义的泛型。

出口：全部产品仍可 build/open/advance，事务与集合契约成立；没有 MDBX 依赖、旧后端路径或无效 placement API。

## S2：用 Distinct 做第一条真实纵向切片

- 将 Distinct 的状态改为 `OrderedMultiset<CanonicalRow>`，其逻辑布局是 `canonical row -> positive weight`。
- 保持逐事件 checked update、零点转换、输出顺序、回滚、背压和 reopen 语义。
- 以 Distinct 的实际调用收定 `multiplicity/adjust`、before/after 与错误类型；实现可以复用私有 ordered map 核心，公共调用不重复 get/check/put/remove，也不能绕过不变量写入零值。
- 不为了这一种更新模式增加接受任意 closure 的通用 Entry/Update；以后有第二种真实模式再判断。
- 删除 Distinct 对碰撞桶的使用；Aggregate 仍在使用的共享实现留到 S3 完成后删除。

出口：完整的一个算子更简单，`OrderedMultiset` 的最小表面已经被实际使用；没有碰撞桶和新旧状态路径双读。

## S3：让 Aggregate 直接建立在索引上

按两个完整行为变更实施，避免一次同时更换所有状态和算法：

1. 先把 group 改为直接 `OrderedMap` key，把 exact admission 改为按 group 划分的 `PartitionedMultiset`，保持 checked admission、Fold 结果和逐事件输出。稳定 group ID 初期保留，避免把宽 group bytes 复制进每条 admission/index key；以后只有真实测量证明不值得时才删除。
2. 再把 MIN/MAX argument index 改为按 `(layout, group id)` 分区、按逻辑值排序的 `PartitionedMultiset`，并用 `first()` / `last()` 找到候选。排序 codec 由 Operation 编译和拥有；Store 只处理 typed partition 与有序 key。删除 `scan_layout` 与失去极值后的整组归约路径。

Store 在此阶段补充分区内的 `first`、`last` 与有界方向扫描。它们形成一个足够支持 Aggregate 与未来 grouped TopK 的完整有序访问面；不再让每个 Operation 构造 raw prefix、sentinel 或 successor，也不同时包装为 PartitionedMap/MultiMap/GroupedMap 多套近似结构。

审查 Indexed 的扫描 callback 协议：有序索引替代重扫后，删除没有消费者的 trait 方法和间接层。稳定 function descriptor、函数 tag、binding 和必要的类型/算术语义继续保留。

出口：删去最后的碰撞桶使用和无用实现；极值撤回使用索引定位；NULL、排序、混合 diff、失败原子性和 reopen 证据由 Aggregate 拥有。

## S4：把 CDC bootstrap spool 收成 Queue

新增单 owner 持久 Queue，并先迁移 PostgreSQL 与 MySQL 两个真实消费者。这个阶段不改变 CDC 的 Fresh/Capturing/Publishing/Streaming/Resetting、checkpoint、output 或 AfterCommit 边界。

- Capturing 使用 hard-capacity `try_push`，容量不足时不提交 checkpoint、不 ACK delivery。
- Publishing 在同一事务中 `pop_front`、解码并验证完整 Change，再由 Station 追加 output；背压或失败使 pop 与 output 一起回滚。
- Resetting 每个 turn pop 并丢弃至多一项，清空后再清 checkpoint/phase；reopen 可以继续。只有真实性能证据需要时再增加无解码的 `discard_front`。
- Store 内部以私有 sequence core 保存 head、tail、entry 与 logical bytes；Queue API 不公开 offset、range scan、任意 truncate 或 forwarding。
- 同步更新两个 Operation Definition 的 data class 与稳定布局证据；旧 Flow/CDC state 直接重建，不做兼容读取。

出口：两个 CDC runtime 都只表达 push/pop，原 `bounds + scan-one + truncate-one` 样板删除；`AppendLog` 的产品 owner 只剩 Flow。

## S5：把 Flow 日志协议收进 Store

新增带固定订阅者的日志结构是本计划中明确值得推进的公共结构；最终命名以实现中的职责为准。它拥有 append capability、subscriber capability、持久 position、事务内 acknowledgement 和保留规则，payload 保持泛型。

- Flow build 根据已校验拓扑注册订阅者，Store 不解释 Station、port、Arrow 或 Schema。
- 在发布 Flow Definition 的同一初始化事务中建立全部初始订阅位置。
- Flow 整体切换为 subscription capability；cursor 只有一个持久化位置。
- 保持现有 output soft high-watermark：非空 backlog 的追加不能超过 capacity，空 backlog 仍可容纳一个 oversize Change；Queue 的 hard capacity 不与它强行统一。
- 初始实现可以在 acknowledgement 事务内同步计算安全 frontier、删除已被全部订阅者消费的 entry，并维持精确 logical retained bytes。公共 API 只承诺固定订阅、顺序读取、单调 acknowledgement、容量和可恢复性，不暴露 frontier 的存储方式、`head == min(cursors)` 或 key 清理时机。
- Inbox 保留输入选择、active input、owned Claim 和 Schema guard；Output 保留精确 Schema 和共享输出装配边界。
- 删除 Flow 的 ConsumerCursor、cursor codec、从 producer 反向访问 consumer Station state 的装配，以及 frontier/reclaim 实现。
- producer Output 只持有 Schema 与 writer capability；每个 InputPort 持有自己的 subscription capability。装配不再让 producer 反向保存 consumer state。
- cursor 迁走后重新审计 Station state：零输入和单输入无需持久 active port；多输入只保留一个窄 `Cell<u32>`，不再为所有 Station 固定创建通用字节 Map。
- 若 `ReadOnly<C>` 已无产品消费者，在同一阶段删除该通用 attenuation wrapper；具体 subscription capability 自身表达只读与确认权限。
- Flow、CDC、system host、Change–Store seam、benchmark、README 和 Rustdoc 全部迁移后，删除公共 `AppendLog`、相关 access/entry/scan 类型和旧布局，不保留 alias。

可以在工作分支上先完成 Store 自证再切换 Flow，但本阶段以真实 Flow 完整接入和旧公共日志完整删除为完成条件。只由测试或 benchmark 引用的 API 不构成产品 owner；证据应随新的 Queue/订阅日志边界迁移。

出口：Flow 更薄，订阅协议由 Store 独立拥有，旧 cursor 事实、`AppendLog` 和旧路径已删除；build/open/status、轮转、慢消费者、背压和 AfterCommit 行为仍一致。

## 未来并发护栏，不属于本轮计划

M5 完成后，Flow 仍顺序执行并持有唯一写事务启动能力。未来真正启动多 Station 并发项目时，再依据真实冲突和吞吐证据定义多写事务、冲突、重新准备和提交后 completion 的语义；若固定订阅日志的同步 frontier/reclaim 成为共享热点，再在 Store 内部分离 acknowledgement 与 key GC；最后才改变 Flow 调度。

当前只保持几条窄边界，避免未来重做无关层：

- Operation 不开始、提交或保存事务启动能力，只短期接收 transaction-bound access。
- 同一 Station 不重入；一个 turn 的状态、output 与 input completion 保持一次原子提交。
- AfterCommit 只在对应提交成功后执行；Store 不自动重跑含业务逻辑或外部 completion 的 closure。
- collection handle 不保存活动事务、cursor 或线程局部状态；上层不依赖 RocksDB column family、snapshot sequence 或事务实现。
- RocksDB sequence number 不成为业务 revision、input cursor、checkpoint 或 reopen ABI。
- `SubscribedLog` 的公共契约不暴露同步回收算法，因此未来可以只替换内部实现。

当前各里程碑不预建并发类型、配置、持久状态或测试，也不在 Store facade 中增加另一层全局 Mutex 作为公共正确性模型。

## 新 API 与删除检查的判断规则

### 新结构/API 的准入

每个提案必须回答四件事：

1. 哪个真实调用方现在需要它？
2. 它独立保证什么不变量？
3. 哪段旧逻辑、协议或重复工作会被删除或明显变简单？
4. 新增的概念和配置是否比消除的复杂性更容易理解？

单个调用方也可以足以证明必要性，例如完整的持久订阅协议；不机械要求多个调用方。也不以多出一个结构就否定设计。实现多个相近结构时，先判断能否用同一个明确的能力表达。

| 候选 | 当前处理方式 |
| --- | --- |
| `OrderedMultiset<K>` | S2 的明确交付，由 Distinct 收定 multiplicity 更新语义 |
| `PartitionedMultiset<P, K>` | S3 的明确交付，由 Aggregate 收定 typed partition 与首尾定位；为 grouped TopK 保留有界方向扫描形状 |
| `Queue<T>` | S4 的明确交付，替换两个 CDC bootstrap spool |
| `SubscribedLog<T>` | S5 的明确交付，实际替换 Flow 协议并使 `AppendLog` 可删除 |
| `first/last` 与有界方向扫描 | S3 随真实 Aggregate 索引加入有序多重集合访问面 |
| 原子 Entry/Update | S2/S3 调用形态证明需要时增加 |
| OrderedSet/MultiMap/普通 PartitionedMap | 有现有调用收益后再决定，不提前建立完整系列 |
| VersionedMap/Merge 结构 | 等真实历史或延迟归并语义的消费方 |

### 防御检查的收缩

- 保留外部输入、持久字节、类型擦除装配、Schema 与外部副作用边界上的校验。
- 检查 missing/wrong/unconsumed DataInstances 有实际意义；Definition、binding、DataInstances 的职责不能因为涉及动态类型就一并删除。
- 若某个内层检查只重复同一边界已证明的事实，用类型或更小的作用域表达该保证，并删除重复检查和纯转发错误层。
- 不把 bind 时的 Schema 检查与 runtime output/input guard 自动视为重复；它们面对的信任边界不同。
- 错误层只有在能区分调用方需要采取的动作、指出具体输入或资源时才有价值；纯粹重复包装应收缩。
- 删除测试时指出替代的 owner 证据；修改格式测试不能代替保持语义测试。

## 每次合并的完成条件

- 产品处于完整可运行状态；新能力有真实消费者。
- 旧实现和无用公共出口在本次删除，不留下默认稍后清理的兼容债。
- PR 说明写清：本次收缩什么、Store 增强什么、上层减少什么、改变哪些持久/可观察语义。
- 有失败、回滚、reopen 证据；owner 与 `TESTING.md` 一致，不建立新通用测试框架。
- 定向检查先通过，合并前执行适用的 workspace gate；存储改动使用已有 benchmark smoke 排查明显问题，不以全面 TB 性能研究作为开工前置。
- 完整系统验收集中在后端替换及日志关键边界；不要求每个机械修改都重复完整系统矩阵。

使用工作区要求的 Rust 1.96。主要 gate 为 `cargo xtask check`，并按变更使用 Store/Operation/Flow/SQL correctness、Change–Store seam 和现有 PostgreSQL 系统入口。具体命令和归属以根 `TESTING.md` 为准。

## 原建议的第一项实施任务（历史）

以下内容记录计划启动时的建议顺序；S0–S5 现已完成，不再是待办事项。

从 S0/S1 开始：审计现有 Store 契约的证据，选定 RocksDB 安全事务接口，完成单后端切换，移除 Small/Large 与 placement，并完成必要的下游类型迁移。保留顺序 Flow、现有算子布局、日志消费协议、线性事务 capability 和外部提交边界；不增加并发抽象。

第一项任务的验收不是“能打开 RocksDB”，而是“现有完整引擎在新的、更简洁的 Store 上继续正确运行，并且旧后端和无效 API 已经删除”。达到这一点之后，再以 Distinct 作为最小真实算子切片验证 Store 的新表达方式。
