# 关系 Sink 契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## SQLite

SqliteSink 只接受绝对 UTF-8 文件路径和新的非保留目标表名。
构造只编译精确 Schema 对应的 `STRICT` 表布局、SQL 和行编码，连接与建表延迟到事务外 prepare/deliver；不得在 SQLite 中增加元数据表或保存整行 canonical bytes。
所有当前 DogPaddle v1 类型都必须无损映射。
运行实例把 SQLite target 与 crate 私有 relation planner 装入下述唯一 buffered Sink 内核，不保留独立 runtime/state 或兼容出口。

## PostgreSQL

PostgresSink 是具体的单输入 exact-relation Sink。
Definition 只保存 discovery 得到的非敏感 `PostgresTargetSpec` canonical JSON；numeric IP、port、user 与 password 只存在于每次 Flow build/open 构造边界显式注入的拥有型 `PostgresSinkConfig`，不接受 DNS endpoint。
私有 Tokio session 必须给完整连接握手、discovery、身份校验和每个数据库工作单元施加 5 秒 client deadline，失败或超时丢弃整个 session。
同一 target spec 只能属于一个持久化 Flow/Sink，不能用于接管或共享已有目标；远端 marker 使用开发期 `dogpaddle.postgres-relation.event-address.v1:` ownership/layout 前缀，精确 logical Schema 由 Flow 构造与运行时 guard 保证。已有目标需随当前 v1 布局重建。
无 TLS 或在线 Schema evolution。
普通 Cargo gate 不依赖 PG，真实本机验收为 system-tests/postgres/check_sink.py。

## Doris 与 ClickHouse

DorisSink Definition 只持久化 sink ID、database/table 和 discovery 得到的唯一 cluster ID；numeric IP、MySQL port、user/password 只属于每次构造注入的 `DorisSinkConfig`。
目标 lookup 的数据库请求或布局复核失败后丢弃缓存连接；重试同一 Loaded batch 时重新连接并复核目标身份与布局。
目标由一个开启 merge-on-write 的 Unique Key 状态表和公开 view 组成，私有 delete marker 同时是 sequence column，公开 technical ID/hash 固定别名为 `$dogpaddle.id`/`$dogpaddle.hash`。
写入按 SQL bytes 与 value 数拆分，多个 statement 必须处于同一显式事务。
ClickHouseSink Definition 只持久化 sink ID、database/table 和 Atomic database UUID；numeric IP、HTTP port、user/password 只属于 `ClickHouseSinkConfig`。
目标由 `ReplacingMergeTree(version)` 状态表和带 `FINAL` 的公开 view 组成，live version 为 0、tombstone version 为 1，旧 live 重放不得复活删除。
两者的状态表都必须包含精确 row-hash 索引，并严格校验 key、version/sequence、distribution、view projection/filter 和 ownership marker；删除终态为阻止不确定旧写复活而保留，compaction 只能合并同一 ID，历史 technical-ID 基数不会自动 GC。
两者均无 TLS、禁止外部写入或在线 Schema evolution，真实容器验收由 `system-tests/warehouse-sinks/check.sh` 拥有。

## 共享 buffered 协议

SQLite、PG、Doris 与 ClickHouse 共用 crate 私有唯一 buffered Sink 内核，持久资源固定为 `sink.control: Cell<Vec<u8>>` 和 `sink.buffer: OrderedMap<u64, Vec<u8>>`。四个具体目标直接实现私有 `RelationTarget`，只提供布局、exact-row lookup、事件大小和幂等写入；`relation` 负责由事件位置构造临时 mutations 与按 logical row 分组。不保留 allocator、relation checkpoint、持久正 ID 清单、target wrapper 或第二套运行状态。
公共 `SinkOperation` 仅暴露 enqueue/load/prepare/persist/deliver/settle 数据协议；不建立可执行回调、backend registry 或 ORM。

每个输入 Change 占据 `sum(abs(diff))` 个连续的绝对事件位置，正负事件都推进位置。首个位置为 1；`u64::MAX` 只可作为排他的 tail，不可成为事件 ID。buffer key 是 entry 的首个事件位置，相邻 key 按前一 entry 的完整事件数推进，不要求整数 key 连续；空 outbox 也不重置 tail。head 保存原 entry key 和下一个事件位置，两者使读取能直接定位 entry；行索引和剩余 diff 由 entry 派生，不持久化。待处理事件数直接为 `tail - head.event_offset`。

control 只有 Initialize、Ready、Prepared。Ready 的 v1 codec 固定 34 bytes：version/phase、head 的两个 u64、tail 与 retained encoded-entry bytes。Prepared 保存 before/after 两组边界，以及按负事件顺序排列的 u64 删除 IDs；长度为 `68 + 8 × negatives` bytes，control 最大读取/写入界为 8260 bytes。它不保存行索引、正 ID 或 target mutation codec。Delivery 只有一个带切片 diff 的 Change 与首个事件位置，正 ID 可由它重建。

构造时从固定 input Schema 创建唯一 `SchemaBoundChangeCodec`，同时拥有运行时 exact-Schema guard 与 buffer codec。`try_enqueue(access, page)` 在同一 Store 事务写入 outbox 和控制计费，Flow 在该事务推进父 frame；false 表示容量暂满且没有任何 Store 写入或运行状态修改。调用方先通过 `load` 恢复控制，再由唯一 writer 写入合法状态。enqueue 只读取最多 34-byte Ready；Prepared 返回 false，不复制删除 ID 清单。永久事件位置溢出在编码和任何 put 前返回错误，不能伪装成可重试的 backpressure。Source/计算节点不通过边日志复制到 Sink。

schema-bound entry 的 v1 布局固定为 format marker、canonical physical Schema 的 BLAKE3 fingerprint、单个 uncompressed RecordBatch IPC message 和 EOS，不重复完整 Schema，也不接受 self-contained IPC fallback。单个 encoded entry 加 8-byte key 不得超过 8 MiB，编码前无拷贝预检 IPC body；owned decode 仅在对齐合适时共享 backing，否则局部复制仍受 body 上限约束。全部 retained buffer 按实际 encoded entry bytes 加 key 不超过 64 MiB 或 1,048,576 absolute events；该口径不是 heap、WAL 或磁盘硬配额。完整 encoded delivery 和 target-expanded mutation work 分别不超过 8 MiB，单批最多 1024 mutations。多个小 entry 的 prefix 合为一次 target batch，weighted entry 可跨批切片。

reopen 在任何外部副作用前分页校验完整 buffer 的 entry span 邻接、schema fingerprint、single-batch framing、Change value、head 范围与 retained accounting。首次 load 逐页最多读取整个 64 MiB outbox，每页至多 8 MiB，暂存当前页而非全部历史；该恢复扫描不属于单动作逻辑工作界。已解码 head 的 span 和位置 hint 仅为可丢弃缓存，hint 必须与 Store head 精确匹配；rollback/retry 不匹配时重新派生位置，不另存前缀索引。

外部 drain：`load(ReadTransactionAccess)` 有界读取 prefix 或原 Prepared；`prepare(pending)` 在事务外 lookup 并构造临时 mutations；`persist_prepared(access, &prepared)` 检查 front 未变并保存边界和删除 IDs；Flow commit/barrier 后 `deliver(&prepared)`；`settle(access, &prepared)` 在独立短事务按实际消费的 entry keys 删除数据并发布 Ready。Prepared reopen 从同一保留 prefix 重建正 ID、负事件行索引和 mutations，不重新 lookup。没有可执行闭包或额外 phase 事实，rollback 后可重读同一 prefix。
首次启动 prepare 在事务外拒绝已有目标，persist 保存 Initialize；barrier 后 deliver 创建或验证兼容空目标，再 settle 发布 Ready。目标已提交、本地未结算时重投同一 Prepared；外部错误、提交不确定要求 fail-stop/reopen。

## 关系身份与重放

正事件的 `$dogpaddle.id` 等于它的绝对事件位置；负事件留下空洞，不创造 ID。每个持久化 Sink 的 ID 按输入顺序递增、永不复用。ClickHouse 使用原生 UInt64；SQLite INTEGER PRIMARY KEY、PG/Doris BIGINT 使用 `position XOR 2^63` 后按 i64 位解释，signed 排序保持事件顺序，合法 SQL 域为 `i64::MIN + 1 ..= i64::MAX - 1`，负数和零均可为 ID。SQLite 继续使用 rowid alias；负 rowid 的 varint 比小正 rowid 更大，磁盘代价见 [性能对照](../PERFORMANCE.md)。
`$dogpaddle.hash` 为 `BLAKE3("dogpaddle.relation-row.v1\0" || canonical_row)[..16]`。hash 只过滤候选，数据库按完整逻辑值精确比较并选择最小 ID。四库 ownership/layout 标记显式区分 event-address v1，旧目标不能因 SQL 类型相同而被接管。

纯正事件批次验证 canonical 总预算，通过算术生成 ID，不做 lookup 或 canonical 分组。混合批次只处理当前有界切片：每个 canonical group 计算最大负前缀缺口，请求本切片负 mutation 数以内的最小 existing IDs，各组返回总量至多 1024；不读取完整 remaining count。返回 IDs 必须合法、唯一、有序且小于切片首个位置。事件顺序优先消费 existing-ID deque，正事件的新 IDs 加到队尾，因此 +3/-3 可使用新 IDs，+3/-4 至少需要一个既存 ID。缺少必要 IDs 时不写当前切片并 fail-stop，早先已结算切片保留。

Prepared 恢复检查负 ID 数、域、唯一性、出生先于删除、当前批次内正出生和完整行身份。保留 original head（包括已消费 prefix）后，将至多 1024 个 prior IDs 排序并一次扫描 diff intervals，拒绝指向已知负事件或其他行的 ID；只对命中的出生行做 bounded canonical-size 检查，再直接比较借用的 Arrow 单行切片；浮点按原始位、嵌套 null 按逻辑值比较，不再编码第二份 payload。当前切片 canonical 总量至多 8 MiB，出生行逐行检查至多 8 MiB，不能全量 canonicalize raw entry。跨 entry 的 load 仍保留最初 head 证据，并计入有界 decoded-entry 工作。
已回收 entry 的历史负位置不再有出生证据；被篡改的 Prepared 若把删除 ID 改成这种空洞，可能按缺失删除的幂等语义通过。这里依赖合法唯一 writer、codec 与 target row guard，不提供历史空洞篡改检测，也不增加出生表、receipt、digest 或远端 allocation frontier。
ClickHouse 使用 `groupArraySortedIf(1024)`，其 [上游实现](https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupArraySorted.cpp) 在累积时保持有限 top-N 状态，禁止 full groupArray 后再截断；每组状态有界不代表 FINAL/filter/扫描总工作有界。lookup 施加 5 秒 max_execution_time、throw overflow 与 64 MiB server memory quota。
SQLite 一次 lookup 或 fixed-ID write 内的全部语句共用一个 deadline，每个工作单元同时使用 5 秒 busy timeout 与 progress handler（每 1000 VM instructions 检查 deadline 并取消）；PG 使用 5 秒服务器 statement/lock timeout 与 client deadline；Doris连接设置 query_timeout=5、exec_mem_limit=64MiB 并保留client read/write timeout。停止延迟受当前有界数据库工作单元限制，不承诺目标不响应时即时 stop。
固定-ID insert 与 delete 是 SQLite/PG 原样 Prepared replay 的目标侧幂等 identity；目标重复 ID 或不存在的删除只有在同一 Prepared 重放语义下才是成功，已存在 ID 必须与对应完整逻辑行一致，其他约束错误不能吞掉。
不另存 receipt、digest 或远端 allocation frontier，因此外部把 Store/目标共同篡改为另一组语义自洽状态不属于恢复契约。
PG 宽 Schema 遵守 65,535 参数上限并在同一事务内切分 SQL；5 秒 work-unit deadline 包含所有分片往返，极宽 Schema 要求低延迟目标。
目标布局只在初始化/重新连接时校验，不逐步骤扫表或查询 MIN/MAX。
输入语义保持事件顺序与非负前缀；目标 SQL 允许整批先插后删，只承诺批次提交后的关系，不承诺目标 WAL 顺序。
目标表、索引、约束由 Sink 独占，不支持外部写入、额外业务唯一约束、trigger/FK、改表或数据库替换恢复。
共享 buffered state、schema-bound Change entry、事件位置身份、row hash 与目标布局取代未发布的旧格式，旧 Flow 和目标必须重建；不提供 alias、fallback、兼容读取或迁移。

这里的 target-expanded mutation work 是 canonical row、技术字段与每列固定 framing 的确定性逻辑计费；8 MiB 限制不代表 driver heap、SQL/wire payload 或数据库事务资源的硬配额。
