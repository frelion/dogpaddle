# 关系 Sink 契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## SQLite

SqliteSink 的 tag 是 10，只接受绝对 UTF-8 文件路径和新的非保留目标表名。
构造只编译精确 Schema 对应的 `STRICT` 表布局、SQL 和行编码，连接与建表延迟到事务外 prepare/deliver；不得在 SQLite 中增加元数据表或保存整行 canonical bytes。
所有当前 DogPaddle v1 类型都必须无损映射。
运行实例把 SQLite target 与 crate 私有 relation planner 装入下述唯一 buffered Sink 内核，不保留独立 runtime/state 或兼容出口。

## PostgreSQL

PostgresSink 的 tag 是 12，是具体的单输入 exact-relation Sink。
Definition 只保存 discovery 得到的非敏感 `PostgresTargetSpec` canonical JSON；numeric IP、port、user 与 password 只存在于每次 Flow build/open 构造边界显式注入的拥有型 `PostgresSinkConfig`，不接受 DNS endpoint。
私有 Tokio session 必须给完整连接握手、discovery、身份校验和每个数据库工作单元施加 5 秒 client deadline，失败或超时丢弃整个 session。
同一 target spec 只能属于一个持久化 Flow/Sink，不能用于接管或共享已有目标；远端 marker 使用开发期 `dogpaddle.postgres-relation.v1:` ownership/layout 前缀，精确 logical Schema 由 Flow 构造与运行时 guard 保证。已有目标需随当前 v1 布局重建。
无 TLS 或在线 Schema evolution。
普通 Cargo gate 不依赖 PG，真实本机验收为 system-tests/postgres/check_sink.py。

## Doris 与 ClickHouse

DorisSink 的 tag 是 18，Definition 只持久化 sink ID、database/table 和 discovery 得到的唯一 cluster ID；numeric IP、MySQL port、user/password 只属于每次构造注入的 `DorisSinkConfig`。
目标 lookup 的数据库请求或布局复核失败后丢弃缓存连接；重试同一 Loaded batch 时重新连接并复核目标身份与布局。
目标由一个开启 merge-on-write 的 Unique Key 状态表和公开 view 组成，私有 delete marker 同时是 sequence column，公开 technical ID/hash 固定别名为 `$dogpaddle.id`/`$dogpaddle.hash`。
写入按 SQL bytes 与 value 数拆分，多个 statement 必须处于同一显式事务。
ClickHouseSink 的 tag 是 19，Definition 只持久化 sink ID、database/table 和 Atomic database UUID；numeric IP、HTTP port、user/password 只属于 `ClickHouseSinkConfig`。
目标由 `ReplacingMergeTree(version)` 状态表和带 `FINAL` 的公开 view 组成，live version 为 0、tombstone version 为 1，旧 live 重放不得复活删除。
两者的状态表都必须包含精确 row-hash 索引，并严格校验 key、version/sequence、distribution、view projection/filter 和 ownership marker；删除终态为阻止不确定旧写复活而保留，compaction 只能合并同一 ID，历史 technical-ID 基数不会自动 GC。
两者均无 TLS、禁止外部写入或在线 Schema evolution，真实容器验收由 `system-tests/warehouse-sinks/check.sh` 拥有。

## 共享 buffered 协议

SQLite、PG、Doris 与 ClickHouse 共用 crate 私有唯一 buffered Sink 内核，持久资源固定为 `sink.control: Cell<Vec<u8>>` 和 `sink.buffer: OrderedMap<u64, Vec<u8>>`。四个具体目标直接实现唯一的私有 `RelationTarget`，只提供目标布局、exact-row lookup、事件大小和幂等固定 ID 写入；共享的 `relation` 代码负责 technical-ID 分配、mutation codec 和按 logical row 的纯 mutation 分组。checkpoint 固定为下一个 technical ID `u64`，Prepared plan 固定为 `Batch`，不保留 target wrapper 或第二套运行状态。
公共 `SinkOperation` 仅暴露具体 enqueue/load/prepare/persist/deliver/settle 数据协议；不建立可执行回调、backend registry 或 ORM。
构造时从固定 input Schema 创建唯一的 `SchemaBoundChangeCodec`，它同时拥有运行时 exact-Schema guard 与 buffer codec。control 状态只有 Initialize、Ready、Prepared；buffer 的每个 value 是一个 schema-bound Change entry，Ready/Prepared 保存连续 `[head, tail)`、当前行剩余 diff、pending event 数和 retained encoded-entry bytes。
`try_enqueue(access, page)` 在同一 Store 事务写入 outbox 和控制计费，Flow 在该事务推进父 frame；false 表示暂不能接受，且没有任何 Store 写入或运行状态修改，调用方可以在同一事务保存已计算页等待重试。调用方必须先通过 `load` 恢复和校验控制；之后由唯一 writer 写入合法状态。enqueue 只准入 Ready 的最大 59-byte 控制，超过此长度的合法状态只能是 Prepared，直接返回 false，不复制固定-ID plan；完整 Prepared 校验仍由恢复和 drain 的 `load` 执行。Source/计算节点不再通过边日志复制到 Sink。多个小 page 的 prefix 继续合并为一次最多 1024 mutations 的 target batch；每个 Change 不必单独开启目标事务。
schema-bound entry 的 v1 持久布局固定为 format marker、canonical physical Schema 的 BLAKE3 fingerprint、单个 uncompressed RecordBatch IPC message 和 EOS，不在每项重复完整 Schema，也不接受 self-contained IPC fallback。单个 encoded entry 加 8-byte key 不得超过 8 MiB，编码前必须无拷贝预检 IPC body；owned decode 仅在对齐合适时共享 backing，否则局部复制仍受 body 上限约束。
全部 retained buffer 按实际 encoded entry bytes 加 key 的逻辑口径不得超过 64 MiB 或 1,048,576 events，该口径不是 heap、WAL 或磁盘硬配额。
完整 encoded delivery 与 target-expanded mutation work 分别受 8 MiB 上限；超限或不能在剩余 technical-ID 区间排空的 input 在 ACK 前失败。有正事件的新 input 以已有 buffered absolute events 加本 input 正事件保守预留 ID 容量，不另存 reservation frontier；纯负 input 不需新 ID，可在 ID frontier 耗尽后继续回收。
reopen 在任何外部副作用前分页校验完整 buffer 的连续 key、schema fingerprint、single-batch framing、Change value、accounting 与 checkpoint 下剩余正事件容量。首次 load 逐页最多读取整个 64 MiB outbox；每页至多 8 MiB，暂存当前页而非全部历史，因此该恢复校验不属于 24 MiB 单动作逻辑工作界。
外部 drain 是具体数据协议：`load(ReadTransactionAccess)` 有界读取 prefix 或原 Prepared；`prepare(pending)` 在事务外执行 lookup 并生成固定 ID；`persist_prepared(access, &prepared)` 检查 front 未改变并保存 Prepared；Flow commit/barrier 后调用 `deliver(&prepared)`；`settle(access, &prepared)` 在独立短事务删除完整消费的 entries 并发布 Ready/frontier。没有可执行闭包或额外 phase 事实，rollback 后可重读同一 prefix；Prepared 重开直接恢复原 fixed-ID plan。
首次启动 prepare 在事务外拒绝已有目标，persist 保存 Initialize；barrier 后 deliver 创建或验证兼容空目标，再 settle 发布 Ready。目标已提交、本地未结算时重投同一 Prepared；不能重新 lookup 或重新分配 IDs。外部错误、提交不确定要求 fail-stop/reopen。
纯正事件批次检查 canonical 总预算和完整 ID 区间，避免 canonical 分组副本；混合批次按完整 canonical row 分组一次 lookup。reopen 的计划校验保留事件顺序、固定 ID 与完整行身份检查。

## 关系身份与重放

`$dogpaddle.id` 在每个持久化 Sink 的全部输入中稳定递增且永不复用，`sink.control` 中的 relation checkpoint 是唯一分配事实；`$dogpaddle.hash` 为 `BLAKE3("dogpaddle.relation-row.v1\0" || canonical_row)[..16]`。
hash 只过滤候选，数据库仍按完整逻辑值精确比较并选择最小 ID。
正事件首次切片仍以 O(1) 算术验证完整 remaining ID 区间。负事件仅验证当前切片：每个 canonical group 计算最大负前缀缺口，请求该组本切片负 mutation 数以内的最小 existing IDs；各组返回总量至多 1024，不读取完整 remaining count。事件顺序从 existing-ID deque 优先消费，正事件的新 IDs 加到队尾，因此 +3/-3 可使用新 IDs，+3/-4 至少需要一个既存 ID。缺少本切片必要 IDs 时不写该片并 fail-stop，早先已结算切片保留；退休非法 raw delta 与外部目标损坏的提前全量失败保证。
ClickHouse 使用 `groupArraySortedIf(1024)`，其 [上游实现](https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupArraySorted.cpp) 在累积时保持有限 top-N 状态，禁止 full groupArray 后再截断；每组状态有界不代表 FINAL/filter/扫描总工作有界。lookup 施加 5 秒 max_execution_time、throw overflow 与 64 MiB server memory quota。
SQLite 一次 lookup 或 fixed-ID write 内的全部语句共用一个 deadline，每个工作单元同时使用 5 秒 busy timeout 与 progress handler（每 1000 VM instructions 检查 deadline 并取消）；PG 使用 5 秒服务器 statement/lock timeout 与 client deadline；Doris连接设置 query_timeout=5、exec_mem_limit=64MiB 并保留client read/write timeout。停止延迟受当前有界数据库工作单元限制，不承诺目标不响应时即时 stop。
固定-ID insert 与 delete 是 SQLite/PG 原样 Prepared replay 的目标侧幂等 identity；目标重复 ID 或不存在的删除只有在同一 Prepared 重放语义下才是成功，已存在 ID 必须与对应完整逻辑行一致，其他约束错误不能吞掉。
不另存 receipt、digest 或远端 allocation frontier，因此外部把 Store/目标共同篡改为另一组语义自洽状态不属于恢复契约。
PG 宽 Schema 遵守 65,535 参数上限并在同一事务内切分 SQL；5 秒 work-unit deadline 包含所有分片往返，极宽 Schema 要求低延迟目标。
目标布局只在初始化/重新连接时校验，不逐步骤扫表或查询 MIN/MAX。
输入语义保持事件顺序与非负前缀；目标 SQL 允许整批先插后删，只承诺批次提交后的关系，不承诺目标 WAL 顺序。
目标表、索引、约束由 Sink 独占，不支持外部写入、额外业务唯一约束、trigger/FK、改表或数据库替换恢复。
共享 buffered state、schema-bound Change entry、relation codec、row hash 与目标布局取代未发布的旧格式，旧 Flow 和目标必须重建；不提供 alias、fallback、兼容读取或迁移。

这里的 target-expanded mutation work 是 canonical row、技术字段与每列固定 framing 的确定性逻辑计费；8 MiB 限制不代表 driver heap、SQL/wire payload 或数据库事务资源的硬配额。
