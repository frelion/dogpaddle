# 关系 Sink 契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## 共享 buffered 协议

SQLite、PostgreSQL、Doris 与 ClickHouse 共用一个私有 buffered 内核。持久资源固定为 `sink.control: Cell<Vec<u8>>` 和 `sink.buffer: OrderedMap<u64, Vec<u8>>`。control 只有 Initialize、Ready；Ready 的开发期 v1 codec 固定 34 bytes：version/phase、head 的 entry start 与 event offset、tail、retained encoded-entry bytes。control 读取和写入上限均为 34 bytes，不保存 mutations、删除 IDs 或目标执行位置。

每个 Change 占据 `sum(abs(diff))` 个连续绝对事件位置。首位置为 1；正负事件都推进位置；`u64::MAX` 只可作排他 tail。buffer key 是 entry 首位置，相邻 key 由完整事件数连接。空 outbox 不重置 tail。head 的行索引和剩余 diff 从 entry 派生；待处理事件数为 `tail - head.event_offset`。

构造只绑定 exact Arrow Schema、唯一 `SchemaBoundChangeCodec`、typed handles 与具体目标，不读状态或执行外部 I/O。公共 `SinkOperation` 只有 `try_enqueue/load/prepare_initialize/persist_initialize/deliver/settle`。四个具体目标直接实现 `RelationTarget` 的事件费用、拒绝已有目标、初始化和 `deliver_prefix`，没有 wrapper、backend registry、receipt、allocator 或另一套运行状态。私有纯 planner 接收完整 Delivery、执行起点、durable tail、原 head 和一次 exact-row lookup；数据库事务由具体 adapter 拥有。

`try_enqueue` 在推进父 frame 的同一个 Store 事务写 outbox 与计费。false 表示尚未初始化或容量暂满，无 Store 写入或运行状态改变。调用方先 `load` 恢复控制。事件位置溢出在编码和任何 put 前报错，不伪装成 backpressure。load 得到的 prefix 不阻止尾部追加。

首次启动在事务外拒绝所有 owned 目标对象；`persist_initialize` 保存 Initialize 后 commit/barrier；`deliver` 创建或验证 owned 空布局；短事务 `settle` 发布 Ready。持久 Initialize 可以重入，但不能覆盖已 Ready 的状态。普通 drain 直接 `load → barrier → deliver → settle`，不增加准备事务或持久执行计划。外部错误或提交不确定使 Flow fail-stop；reopen 保留输入，重新读取目标并规划同一前缀。

settle 只允许当前 head 与加载时 head 相同，tail/retained 不得回退。它删除实际消费的 entry keys，推进到加载后的 head，保留当前 tail，按实际回收 entry bytes 扣当前 retained。不能覆盖合法尾追加，也不能按目标的更长进度跳过尚未加载的本地输入。

## 输入费用与恢复

schema-bound entry 的 v1 布局为 format marker、canonical physical Schema 的 BLAKE3 fingerprint、单个 uncompressed RecordBatch IPC message 和 EOS，不重复完整 Schema或接受 self-contained IPC fallback。encoded entry 加 8-byte key 不超过 8 MiB；编码前无拷贝预检 IPC body。owned decode 在对齐合适时共享 backing，否则局部复制仍受 body 上限约束。

全部 retained buffer 按实际 encoded-entry bytes 加 key 不超过 64 MiB 或 1,048,576 absolute events。完整 encoded delivery 和 target-expanded mutation work 分别不超过 8 MiB，单批最多 1024 mutations。多个小 entry 可合成一批；weighted entry 可跨批切片。这些是逻辑工作界，不是 heap、WAL、磁盘或数据库事务资源硬配额。

reopen 在外部副作用前分页检查完整 buffer 的 span 邻接、schema fingerprint、single-batch framing、Change value、head 范围、事件费用和 retained accounting。首次 load 可读整个最多 64 MiB outbox，每页至多 8 MiB，暂存当前页。此恢复扫描不属于单动作工作界。decoded head 与位置 hint 只是可丢弃缓存，必须与 Store head 精确匹配，rollback 后不匹配就重新派生。

## 关系身份与前缀规划

正事件的 technical ID 等于绝对事件位置；负事件留下空洞。ID 永不复用。ClickHouse 使用 UInt64；SQLite INTEGER PRIMARY KEY、PG/Doris BIGINT 将 `position XOR 2^63` 按 i64 位解释，signed 排序保持事件顺序。事件 ID 域为 `1..u64::MAX`，SQL 域为 `i64::MIN + 1 ..= i64::MAX - 1`；负数和零可以是合法 ID。SQLite 保持 rowid alias，磁盘代价见 [性能对照](../PERFORMANCE.md)。

hash 为 `BLAKE3("dogpaddle.relation-row.v1\0" || canonical_row)[..16]`，只过滤候选。lookup 必须按完整逻辑值比较，返回每行同一 snapshot 的已完成位置 through 与最多 take 个最早 live IDs。所有请求的 take 总量不超过当前剩余负 mutation 数，至多 1024；through 必须小于 durable tail，IDs 必须非零、严格有序、全局唯一且不大于 through。

planner 只执行 `ordinal >= from && ordinal > through` 的单位。既存 IDs 放在 FIFO 前部，新 birth 加到队尾；每次 death 必须消费更早出生的 ID。全部剩余事件逐单位验证非负前缀后才允许业务写，不能只检查净和。纯正批次只预验 canonical 总预算并算术生成 ID。

目标返回的被消费 ID 落在保留范围时，将至多 1024 个 `(ID, delivery row)` 排序：加载首位置之前用完整 original head 的 weighted intervals，之后用完整未裁剪 Delivery，包括因 F/through 跳过的正事件。拒绝负事件空洞、未来出生和不同完整行。本轮新 birth 已由同 canonical 组的真实正事件与 FIFO 顺序证明，不再扫描出生证据。只对命中的目标 ID 出生行检查 bounded canonical size，再直接比较借用 Arrow 单行；浮点按原始位，嵌套 null 按逻辑值，不编码第二份 payload。当前切片 canonical 总量与单个出生行分别不超过 8 MiB；不全量 canonicalize raw entry。跨 entry load 保留最初 head 并计入 decoded-entry 工作。

已经回收的历史 entry 不再有出生证据。契约依赖唯一合法 writer、严格 codec 和 target 完整行 guard，不检测外部共同篡改成另一套语义自洽状态；不增加永久出生表或历史 ID 清单。

## SQLite 与 PostgreSQL

SQLite 只接受绝对 UTF-8 文件路径和新的非保留表名，完整 v1 Arrow 类型无损映射到 STRICT 表。唯一额外 owned 元表保存单例 F，表示下一个尚未提交的事件位置；不保存整行 canonical bytes。PG Definition 只保存 discovery 得到的非敏感 `PostgresTargetSpec`，numeric IP/port/user/password 只存在于构造时注入的 `PostgresSinkConfig`，不接受 DNS 或 TLS。

初始化在同一目标事务建立业务表、索引和单例 F=1。SQLite `BEGIN IMMEDIATE`、PG `SELECT ... FOR UPDATE` 锁内读 F；先检查 `loaded start <= F <= durable tail`。F 已覆盖本批时直接成功；否则 from=F、lookup through=F−1，在同一事务规划和普通 INSERT/DELETE，原子更新 F 到本批排他 end 并 commit。F 可以等于 `u64::MAX`，该值仍不可成为事件 ID。目标已提交但本地未结算、旧长批后重新切短批都由同一个 F 处理；新 ID 冲突是真错误。

SQLite 精确验证业务表与 F 的 owned 布局、对象种类和约束；PG 业务表验证 ownership marker、对象种类及 owned 索引存在性，F 则精确验证列、约束、单例及域。Ready 缺失 F、额外行、错布局或 F 越界都失败，不补 F=1。PG owned 对象含两个 table 与三个 index，额外 rowtype 名称也必须在 discovery 时拒绝；SQLite 禁止额外 index/trigger。目标只属于一个持久 Flow/Sink，禁止外部写入、共享、额外唯一约束、FK、改表或在线 schema evolution。

PG 私有 Tokio session 用同一个五秒绝对 client deadline 覆盖连接/身份/布局、锁、lookup、planner、所有 SQL 分片与 commit，失败丢整个 session。宽 Schema 遵守 65,535 参数上限，同事务切 SQL。SQLite 同轮共享 deadline，busy timeout 使用剩余时间，progress handler 每 1000 VM instructions 检查并取消。真实 PG 验收由 `system-tests/postgres/check_sink.py` 拥有；普通 Cargo gate 不依赖 PG。

## Doris 与 ClickHouse

Doris Definition 只持久化 sink ID、database/table 和唯一 cluster ID；ClickHouse 持久化 sink ID、database/table 和 Atomic database UUID。numeric endpoint 与凭据只存在于各自运行配置。两库 owned 状态表保留完整行/hash/ID/version 与精确 hash 索引，公开 view 只暴露 technical ID/hash 和原逻辑列；live 条件为 version=ID。

birth i 写 `(ID=i, version=i)`；FIFO death d 写 `(ID=i, version=d)`，i<d。Doris 用 merge-on-write Unique Key(ID)、BIGINT version sequence column；ClickHouse 用 `ReplacingMergeTree(UInt64 version)` 和 FINAL。不存在的 ID 也必须写完整 tombstone；迟到的低版本 birth 不能复活它。写前检查现有 ID 的完整行身份与 version 域，已经有更高版本时可跳过。删除终态保留，compaction 只合并同 ID，历史 ID 基数不自动 GC。

负事件行的 lookup 返回含删除历史的 MAX(version) 及最早 live IDs。ClickHouse 同一个 FINAL/request LEFT JOIN 聚合 `maxIf` 与 `groupArraySortedIf(1024)`；Doris 同一 SELECT 的 MAX arm 与有序 LIMIT arm共享 statement snapshot。Doris MAX 的 SQL NULL 才表示无历史 through=0；signed BIGINT 0 是合法位置 2^63，不能混用。分组状态与返回有界不代表 FINAL、JOIN 或历史扫描有界；查询仍受原 64 MiB、五秒 server quota，资源拒绝不得吞掉或抬配额。

Doris 每次新连接设置并读回 strong consistency read、关闭 SQL/query cache、strict insert、禁 missing-version/bad-tablet fallback、group commit off，以及 query/insert timeout 与 64 MiB exec memory。强读新鲜度与单 statement snapshot 是两个独立条件。写入按 SQL bytes/value 数分片，多 statement 在同一显式事务；最终真实 COMMIT 或单 statement INSERT 的 OK info 只接受固定三字段、严格 VISIBLE envelope，PREPARE/COMMITTED/空 info/错误/超时都不能 settle。全部已跳过时可凭已验证强读成功，不制造空 COMMIT 回执。

ClickHouse 所有 HTTP 请求共用一轮五秒绝对 deadline。Doris 同轮共享五秒接受预算，连接使用五秒 socket 空闲 read/write timeout；现同步驱动不能中断正在读取的协议，也不能保证慢速分片或连接 Drop 在五秒内返回。超时或未知结果保留输入，丢连接和验证缓存，重开后重新验证身份/布局/会话并强读。超时不表示远端 abort；停止延迟不能当作即时取消。

两库均禁止 TLS、外部写入、共享及 schema evolution；严格检查 key、version/sequence、distribution、view projection/filter 和 ownership。真实 adapter 验收由 `system-tests/warehouse-sinks/check.sh` 拥有。

## 布局与验证

四库行编码器保留共享 exact Arrow Schema；SQL 类型、nullability 与目录期望从 Field 派生，不另保存列布局。PG 保留 typed NULL、整数范围与 bytea 宽度；CH 保留整数时间和 whole-canonical base64；Doris 保留 UTF-8 明文和其它复杂值 canonical base64。Arrow Null 映射可空物理列。

当前 control、目标 F 布局和 warehouse occurrence-version ownership marker 属于开发期 v1。旧 Flow 和受影响目标必须显式重建；没有 alias、fallback、旧格式读取或迁移，reopen 失败不删除、修复或重新创建已有状态。目标表/索引不逐步骤重验；重新连接必须重验。布局 golden、malformed、reopen、事务回滚、未知提交、重切和尾追加证据归各 owner，性能数字与口径归 [PERFORMANCE.md](../PERFORMANCE.md)。
