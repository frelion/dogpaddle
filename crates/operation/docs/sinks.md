# 关系 Sink 契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## SQLite

SqliteSink 的 tag 是 10，只接受绝对 UTF-8 文件路径和新的非保留目标表名。
构造只编译精确 Schema 对应的 `STRICT` 表布局、SQL 和行编码，连接与建表延迟到 turn；不得在 SQLite 中增加元数据表或保存整行 canonical bytes。
所有当前 DogPaddle v1 类型都必须无损映射。
运行实例把 SQLite target 与 crate 私有 relation planner 装入下述唯一 buffered Sink 内核，不保留独立 runtime/state 或兼容出口。

## PostgreSQL

PostgresSink 的 tag 是 12，是具体的单输入 exact-relation Sink。
Definition 只保存 discovery 得到的非敏感 `PostgresTargetSpec` canonical JSON；numeric IP、port、user 与 password 只存在于每次 Flow build/open 构造边界显式注入的拥有型 `PostgresSinkConfig`，不接受 DNS endpoint。
私有 Tokio session 必须给完整连接握手、discovery、身份校验和每个数据库工作单元施加 5 秒 client deadline，失败或超时丢弃整个 session。
同一 target spec 只能属于一个持久化 Flow/Sink，不能用于接管或共享已有目标；远端 marker 只标识 ownership/layout version，精确 logical Schema 由 Flow 构造与运行时 guard 保证。
无 TLS 或在线 Schema evolution。
普通 Cargo gate 不依赖 PG，真实本机验收为 system-tests/postgres/check_sink.py。

## Doris 与 ClickHouse

DorisSink 的 tag 是 18，Definition 只持久化 sink ID、database/table 和 discovery 得到的唯一 cluster ID；numeric IP、MySQL port、user/password 只属于每次构造注入的 `DorisSinkConfig`。
目标由一个开启 merge-on-write 的 Unique Key 状态表和公开 view 组成，私有 delete marker 同时是 sequence column，公开 technical ID/hash 固定别名为 `$dogpaddle.id`/`$dogpaddle.hash`。
写入按 SQL bytes 与 value 数拆分，多个 statement 必须处于同一显式事务。
ClickHouseSink 的 tag 是 19，Definition 只持久化 sink ID、database/table 和 Atomic database UUID；numeric IP、HTTP port、user/password 只属于 `ClickHouseSinkConfig`。
目标由 `ReplacingMergeTree(version)` 状态表和带 `FINAL` 的公开 view 组成，live version 为 0、tombstone version 为 1，旧 live 重放不得复活删除。
两者的状态表都必须包含精确 row-hash 索引，并严格校验 key、version/sequence、distribution、view projection/filter 和 ownership marker；删除终态为阻止不确定旧写复活而保留，compaction 只能合并同一 ID，历史 technical-ID 基数不会自动 GC。
两者均无 TLS、禁止外部写入或在线 Schema evolution，真实容器验收由 `system-tests/warehouse-sinks/check.sh` 拥有。

## 共享 buffered 协议

SQLite、PG、Doris 与 ClickHouse 共用 crate 私有唯一 buffered Sink 内核，持久资源固定为 `sink.control: Cell<Vec<u8>>` 和 `sink.buffer: OrderedMap<u64, Vec<u8>>`；crate 私有 `relation` 只拥有 exact-row lookup、technical-ID 分配、mutation codec、按 logical row 的纯 mutation 分组与 target adapter，不再拥有第二套 runtime/state。
不得建立公共通用 Sink trait、backend enum、registry 或 ORM。
control 状态只有 Initialize、Ready、Prepared；buffer 的每个 value 是一个完整自描述 Change IPC，Ready/Prepared 保存连续 `[head, tail)`、当前行剩余 diff、pending event 数和 retained IPC bytes。
完整 Change admission、control accounting 与 input `Complete` 必须在同一 Store 事务提交；连续小 Claim 可聚合，没有 offered Claim、达到 target event limit 或 8 MiB delivery watermark 时继续无输入内部 drain。
单个 Change 的 canonical uncompressed IPC+8-byte key 不得超过 8 MiB，编码前必须无拷贝预检 IPC body；owned decode 仅在对齐合适时共享 backing，否则局部复制仍受 body 上限约束。
全部 retained buffer 按 IPC+key 的逻辑口径不得超过 64 MiB 或 1,048,576 events，该口径不是 heap、WAL 或磁盘硬配额。
完整 encoded delivery 与 target-expanded mutation work 分别受 8 MiB 上限；超限或不能在剩余 technical-ID 区间排空的 input 在 ACK 前失败。
reopen 在任何外部副作用前分页校验完整 buffer 的连续 key、IPC、精确 Schema、accounting 与 checkpoint 下剩余正事件容量。
首次启动在事务外拒绝已有目标，再持久化 Initialize；AfterCommit 创建或验证同布局的空目标。
批次在 Store 写事务外规划，apply 只持久化 Prepared 的 before/after settlement、target checkpoint 与至多 1024 个具体 mutation；insert 和 delete 都只保存 buffer delivery 中的行索引与固定 ID，不复制完整行或 Station Claim。
AfterCommit 在一个目标事务中先 insert-on-ID-conflict-do-nothing，再核对所有已存在 mutation ID 仍绑定对应完整逻辑行，最后按 ID delete；下一独立 Store turn 删除完整消费的 buffer entries 并发布 Ready。
目标已提交而本地未结算时只从原 buffer 重建并依靠 Prepared plan 的固定 technical ID 重投；从不重投已结算批次。
普通 planning 错误可重试，AfterCommit 错误或提交不确定必须 fail-stop/reopen，全部外部 I/O 不得占用 Store 写事务。

## 关系身份与重放

`$dogpaddle.id` 在每个持久化 Sink 的全部输入中稳定递增且永不复用，`sink.control` 中的 relation checkpoint 是唯一分配事实；`$dogpaddle.hash` 为 `BLAKE3("dogpaddle.relation-row.v1\0" || canonical_row)[..16]`。
hash 只过滤候选，数据库仍按完整逻辑值精确比较并选择最小 ID。
负事件的第一个 delivery slice 在任何部分落地前验证 buffer 中该行完整剩余 multiplicity，后续 slice 不重复全量 admission；正事件的第一个 slice同样验证完整 ID 区间。
查询只返回本批至多 1024 个 ID。
固定-ID insert 与 delete 是 SQLite/PG 原样 Prepared replay 的目标侧幂等 identity；目标重复 ID 或不存在的删除只有在同一 Prepared 重放语义下才是成功，已存在 ID 必须与对应完整逻辑行一致，其他约束错误不能吞掉。
不另存 receipt、digest 或远端 allocation frontier，因此外部把 Store/目标共同篡改为另一组语义自洽状态不属于恢复契约。
PG 宽 Schema 遵守 65,535 参数上限并在同一事务内切分 SQL；5 秒 work-unit deadline 包含所有分片往返，极宽 Schema 要求低延迟目标。
目标布局只在初始化/重新连接时校验，不逐 turn 扫表或查询 MIN/MAX。
输入语义保持事件顺序与非负前缀；目标 SQL 允许整批先插后删，只承诺批次提交后的关系，不承诺目标 WAL 顺序。
目标表、索引、约束由 Sink 独占，不支持外部写入、额外业务唯一约束、trigger/FK、改表或数据库替换恢复。
共享 buffered state、relation codec、row hash 与目标布局取代未发布的旧格式，旧 Flow 和目标必须重建；不提供 alias、兼容读取或迁移。

这里的 target-expanded mutation work 是 canonical row、技术字段与每列固定 framing 的确定性逻辑计费；8 MiB 限制不代表 driver heap、SQL/wire payload 或数据库事务资源的硬配额。
