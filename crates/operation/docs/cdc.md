# CDC Scan 契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## PostgreSQL

PostgresCdcScan 的 tag 是 11，只有一个具体 Scan，不另建公共 IngressScan 或 connector driver trait。
Definition 保存非敏感身份、完整固定列声明、有序 `output_projection` 和必填 `NonZeroU64 bootstrap_spool_bytes`，canonical JSON 是 tag11 的持久 ABI。默认构造器保存 identity projection；投影构造器允许空输出，并要求索引无重复且保持 source 顺序。
它只声明 `postgres_cdc_scan.phase: Cell<u32>`、`postgres_cdc_scan.checkpoint: Cell<Vec<u8>>`、`postgres_cdc_scan.bootstrap_spool: Queue<Vec<u8>>` 和 `postgres_cdc_scan.published: Queue<Vec<u8>>` 四个资源。
首次运行按 `Fresh → Capturing → Publishing → Streaming` 推进：converter 仍以完整列声明校验每个 Debezium envelope 和完整 row image，只为 `output_projection` 构造 Arrow array；空投影显式保留 batch 行数和 diff。runtime 从这份 projected output Schema 构造唯一的 `SchemaBoundChangeCodec`；`initial` 快照期不产生公开 output，而是把 delivery 的可选 Change 编成 schema-bound entry，并与整个 delivery 的 opaque checkpoint、phase 在同一事务提交，之后才 ACK；terminal heartbeat 将快照封口。
schema-bound entry 的 v1 持久布局固定为 format marker、canonical physical Schema 的 BLAKE3 fingerprint、单个 uncompressed RecordBatch IPC message 和 EOS，不在每项重复完整 Schema，也不接受 self-contained IPC fallback。封口后每笔事务从私有 Queue `pop_front` 一条 entry 并追加 Source 自有 published Queue，背压或 commit 失败同时回滚出队和 output。
进入 Streaming 后从封口 checkpoint 以 `no_data` 继续原有 checkpoint/published Queue/真实 Delivery ACK 协议。
Capturing 期 reopen 或普通错误绝不恢复部分快照；它必须先停止 connector、在 Store 事务外删除兼容且非 active 的 source-owned slot，再通过 Resetting 每笔事务 `discard_front(256)` 有界清空 spool/checkpoint 并回到 Fresh，不复制或解码已废弃的 entry。Queue 逐项验证长度与连续性、为每项写 tombstone，但整批只更新一次 metadata；整批删除与最后的 checkpoint/phase 清理一起提交或回滚。Resetting 和 Publishing 直接使用出队返回的空状态推进阶段。
`bootstrap_spool_bytes` 是 Queue 执行的硬逻辑上限，每项计费为 8-byte private sequence 加 projected output 的实际 encoded entry bytes，空队列也不接受超限项；不足时该 delivery 不提交、不 ACK，必须用更大容量重建 Flow。
PG discovery 要求预配置 publication、FULL replica、单张 permanent 非 partition 表，以及首次启动前不存在、之后由该 Scan 独占的 slot 名；spool 必须容纳完整快照和 terminal heartbeat 前的 WAL 重叠。
完整列声明与 `output_projection` 都属于开发期 v1 持久布局；改变投影或读取缺少该字段的旧 Definition 时直接重建状态，不迁移或 fallback。无 TLS、在线 Schema evolution或跨实例 fencing。
TRUNCATE 必须送入 converter 并拒绝；运行中外部修改 slot/publication/schema 不受支持。
所有外部 I/O 在 Store 事务外。`restore(ReadTransactionAccess)` 只读取和验证有界控制；`poll` 返回真实拥有型 Delivery 或具体维护数据；`record(access, &mut delivery)` 在同一事务捕获全部数据、opaque checkpoint 与 phase，容量背压返回 false 时整笔回滚并保留原 Delivery。Flow 提交并完成 WAL barrier 后才调用 `ack(delivery)` 消费原线性句柄；不根据 checkpoint 重造 ACK。`published(ReadTransactionAccess)` 只读返回 Queue front 的 schema-bound 原字节，调用方以 exact output Schema 解码；capture 只 append，因此前项在跨页与下游调用期间保持不变。`consume_published(TransactionAccess)` 用 `discard_front(1)` 无复制删除前项，必须与完成该输入的消费者状态在同一事务提交。Flow root 直接借用 Source front，只在挂起时保存 control；完整计算与路由 Done 后才消费前项。消费不决定捕获 checkpoint，也不推迟真实 ACK。
每个 Source 的 published Queue 硬逻辑容量为 64 MiB，与私有 bootstrap spool 分开；封口前数据只进私有 spool，避免未封口 head 与容量形成死锁。
转换前 preflight 每个 Delivery 至多 2048 data envelopes、4096 physical rows、65536 top-level slots，slots 按完整源列数及更新的双行上界计费。Changes 编码与 checkpoint 合计不超过 8 MiB，超限不提交、不 ACK、不拆 Delivery。capture 的 Store 写入至多 8 MiB 加固定控制；publish 有界读取至多 8 MiB、写入至多 8 MiB 加固定 metadata/tombstone，两者各自低于 24 MiB 逻辑动作界。Queue front 在复制前执行 8 MiB 长度准入。
ACK 不确定或提交不确定要求 fail-stop/reopen；不使用回调、通用 PreparedTurn 或 AfterCommit。
普通 Cargo gate 不依赖 Java/PG，真实本机验收为 system-tests/postgres/check_cdc.py。

## MySQL

MySqlCdcScan 的 tag 是 15，同样是单个具体 Scan，Definition 保存发现的非敏感单表身份、完整固定列、有序 `output_projection` 和必填 `NonZeroU64 bootstrap_spool_bytes`；完整 envelope/row image 校验、projected array 构造和零列行数语义与 PostgreSQL 相同。四个资源为 `mysql_cdc_scan.phase: Cell<u32>`、`mysql_cdc_scan.checkpoint: Cell<Vec<u8>>` 、`mysql_cdc_scan.bootstrap_spool: Queue<Vec<u8>>` 和 `mysql_cdc_scan.published: Queue<Vec<u8>>`。
它也按 `Fresh → Capturing → Publishing → Streaming` 推进，使用 `initial_only` + `snapshot.locking.mode=minimal` 捕获 MySQL 8.4 一致初始快照，以 terminal heartbeat 的 checkpoint 封口，排空私有 spool 后以 `recovery` 继续 binlog。
捕获、封口、发布、背压和 ACK 与 PG 由同一个私有 CDC runtime 实现；数据库连接、记录转换和 checkpoint 身份校验仍由各具体源拥有。
Capturing 期 reopen 通过 Resetting 每笔事务 `discard_front(256)` 至多清理 256 项，逐项写 tombstone 但整批只更新一次 Queue metadata；清空 checkpoint 后重做完整快照，不复制或解码废弃 entry，也不从中间 checkpoint 恢复。
容量是相同的 Queue 硬上限，每项计费为 8-byte private sequence 加 projected output 的实际 encoded entry bytes，空队列也不接受超限项；超限不 ACK 并要求用更大容量重建。
部署角色应具有短时 global read lock 所需权限，但不得授予 `LOCK TABLES`，以便 global lock 失败时在长表锁 fallback 之前失败。真实系统验收入口为 `system-tests/mysql/check_cdc.py`：直接通过公共 Operation/Store 协议在快照封口及 streaming delivery 的 Store commit 后、ACK 前退出，再验证重开与有序后继事件；普通 Cargo 测试不启动 MySQL。
binlog 必须覆盖快照、私有 spool 排空、公开 output 背压与追平全期；过早 `PURGE` 必须 fail closed，不得新选起点。
只支持固定 Schema，无 TLS、在线 DDL 或跨实例 fencing。完整列声明、`output_projection` 和 schema-bound entry 都属于开发期 v1 持久布局；布局变化或旧 Definition 直接重建，不识别、迁移或回退读取旧格式。


## 阅读运行实现

`scan/cdc_runtime.rs` 是两种源唯一的捕获与恢复驱动。它拥有 Connector、phase/checkpoint、私有 spool 与 published Queue。运行步骤不另行持久化；poll 的可丢弃转换进度只在真实 ACK 后发布。恢复完全依赖 phase/checkpoint/queues。
PostgreSQL 在 Capturing 恢复时先事务外清理 source-owned slot；MySQL 无需源 cleanup。之后 Resetting 每笔至多 discard 256 entries，checkpoint/phase 在最后一批一起清除。Capturing 失败后的 reopen 重新建立完整快照，不继续部分捕获。
`postgres_cdc/runtime.rs` 和 `mysql_cdc/runtime.rs` 只实现私有源适配：启动 connector、清理快照资源、转换记录、恢复 checkpoint 及具体错误分类。具体 converter 继续返回 `Captured { change, sealed, progress }`，只有完成通知封口。
`scan/cdc_convert.rs` 统一校验 Connect envelope、heartbeat、snapshot notification 与完整 row image，再按有序投影构造 Arrow Change；未投影列仍验证，空投影保留行数和 diff。
恢复仍保留源差异：PG 只在 Publishing/Streaming 解析可恢复 checkpoint；MySQL 在全部阶段验证 checkpoint，拒绝 Resetting 中有 spool 却没有 checkpoint。新增 published resource 是开发期 v1 layout 变化，缺少它的旧状态直接重建，不迁移或 fallback。
