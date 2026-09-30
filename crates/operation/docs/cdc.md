# CDC Scan 契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## PostgreSQL

PostgresCdcScan 的 tag 是 11，只有一个具体 Scan，不另建公共 IngressScan 或 connector driver trait。
Definition 保存非敏感身份、完整固定列声明、有序 `output_projection` 和必填 `NonZeroU64 bootstrap_spool_bytes`，canonical JSON 是 tag11 的持久 ABI。默认构造器保存 identity projection；投影构造器允许空输出，并要求索引无重复且保持 source 顺序。
它只声明 `postgres_cdc_scan.phase: Cell<u32>`、`postgres_cdc_scan.checkpoint: Cell<Vec<u8>>` 和 `postgres_cdc_scan.bootstrap_spool: Queue<Vec<u8>>` 三个资源。
首次运行按 `Fresh → Capturing → Publishing → Streaming` 推进：converter 仍以完整列声明校验每个 Debezium envelope 和完整 row image，只为 `output_projection` 构造 Arrow array；空投影显式保留 batch 行数和 diff。runtime 从这份 projected output Schema 构造唯一的 `SchemaBoundChangeCodec`；`initial` 快照期不产生公开 output，而是把 delivery 的可选 Change 编成 schema-bound entry，并与整个 delivery 的 opaque checkpoint、phase 在同一事务提交，之后才 ACK；terminal heartbeat 将快照封口。
schema-bound entry 的 v1 持久布局固定为 format marker、canonical physical Schema 的 BLAKE3 fingerprint、单个 uncompressed RecordBatch IPC message 和 EOS，不在每项重复完整 Schema，也不接受 self-contained IPC fallback。封口后每个 turn 在同一事务从私有 Queue `pop_front` 一条 entry 并追加普通 Station output，背压或 commit 失败同时回滚出队和 output。
进入 Streaming 后从封口 checkpoint 以 `no_data` 继续原有 checkpoint/output/AfterCommit ACK 协议。
Capturing 期 reopen 或普通错误绝不恢复部分快照；它必须先停止 connector、在 Store 事务外删除兼容且非 active 的 source-owned slot，再通过 Resetting 每 turn 在一笔事务中 `discard_front(256)` 有界清空 spool/checkpoint 并回到 Fresh，不复制或解码已废弃的 entry。Queue 逐项验证长度与连续性、为每项写 tombstone，但整批只更新一次 metadata；整批删除与最后的 checkpoint/phase 清理一起提交或回滚。Resetting 和 Publishing 直接使用出队返回的空状态推进阶段。
`bootstrap_spool_bytes` 是 Queue 执行的硬逻辑上限，每项计费为 8-byte private sequence 加 projected output 的实际 encoded entry bytes，空队列也不接受超限项；不足时该 delivery 不提交、不 ACK，必须用更大容量重建 Flow。
PG discovery 要求预配置 publication、FULL replica、单张 permanent 非 partition 表，以及首次启动前不存在、之后由该 Scan 独占的 slot 名；spool 必须容纳完整快照和 terminal heartbeat 前的 WAL 重叠。
完整列声明与 `output_projection` 都属于开发期 v1 持久布局；改变投影或读取缺少该字段的旧 Definition 时直接重建状态，不迁移或 fallback。无 TLS、在线 Schema evolution或跨实例 fencing。
TRUNCATE 必须送入 converter 并拒绝；运行中外部修改 slot/publication/schema 不受支持。
所有外部 I/O 在 Store 事务外。真实 delivery ACK 使用 durable AfterCommit，必须先完成 Store durability
barrier；纯 NextStep/resume/progress 切换使用 local AfterCommit 并可共享本轮最终 barrier。ACK 不确定要求
fail-stop/reopen，不用 checkpoint 充当 delivery ID。
普通 Cargo gate 不依赖 Java/PG，真实本机验收为 system-tests/postgres/check_cdc.py。

## MySQL

MySqlCdcScan 的 tag 是 15，同样是单个具体 Scan，Definition 保存发现的非敏感单表身份、完整固定列、有序 `output_projection` 和必填 `NonZeroU64 bootstrap_spool_bytes`；完整 envelope/row image 校验、projected array 构造和零列行数语义与 PostgreSQL 相同。三个资源为 `mysql_cdc_scan.phase: Cell<u32>`、`mysql_cdc_scan.checkpoint: Cell<Vec<u8>>` 和 `mysql_cdc_scan.bootstrap_spool: Queue<Vec<u8>>`。
它也按 `Fresh → Capturing → Publishing → Streaming` 推进，使用 `initial_only` + `snapshot.locking.mode=minimal` 捕获 MySQL 8.4 一致初始快照，以 terminal heartbeat 的 checkpoint 封口，排空私有 spool 后以 `recovery` 继续 binlog。
捕获、封口、发布、背压和 ACK 与 PG 由同一个私有 CDC runtime 实现；数据库连接、记录转换和 checkpoint 身份校验仍由各具体源拥有。
Capturing 期 reopen 通过 Resetting 每 turn 以一笔事务 `discard_front(256)` 至多清理 256 项，逐项写 tombstone 但整批只更新一次 Queue metadata；清空 checkpoint 后重做完整快照，不复制或解码废弃 entry，也不从中间 checkpoint 恢复。
容量是相同的 Queue 硬上限，每项计费为 8-byte private sequence 加 projected output 的实际 encoded entry bytes，空队列也不接受超限项；超限不 ACK 并要求用更大容量重建。
部署角色应具有短时 global read lock 所需权限，但不得授予 `LOCK TABLES`，以便 global lock 失败时在长表锁 fallback 之前失败。真实系统验收入口为 `system-tests/mysql/check_cdc.py`：直接通过公共 Operation/Store 协议在快照封口及 streaming delivery 的 Store commit 后、ACK 前退出，再验证重开与有序后继事件；普通 Cargo 测试不启动 MySQL。
binlog 必须覆盖快照、私有 spool 排空、公开 output 背压与追平全期；过早 `PURGE` 必须 fail closed，不得新选起点。
只支持固定 Schema，无 TLS、在线 DDL 或跨实例 fencing。完整列声明、`output_projection` 和 schema-bound entry 都属于开发期 v1 持久布局；布局变化或旧 Definition 直接重建，不识别、迁移或回退读取旧格式。


## 阅读运行实现

`scan/cdc_runtime.rs` 是两种源唯一的事务与恢复驱动。它拥有具体 Debezium `Connector`、phase/checkpoint/spool handle 和私有 `NextStep`；`turn` 校验输入后只分派一个步骤，持久 `Phase` 仍是 v1 的五个阶段。
`Restore/BeginCapture/Capture/PrepareReset/Reset/Publish/Stream/RestartStream` 取代可相互冲突的恢复、重启和快照失败布尔标记。
PostgreSQL 的 PrepareReset 在写入 Resetting 前停止 connector 并清理 slot；MySQL 恢复时没有 connector，可直接提交 Resetting。
RestartStream 在同一 turn 完成 stop、restart 和 poll。运行步骤不另行持久化；checkpoint、spool 或 output 同事务提交后才消费真实的 `Delivery` 执行 ACK，回滚不推进内存 checkpoint 或捕获进度。快照启动、poll、转换或 schema-bound Change 编码失败均安排完整快照重置，不能继续使用已失败的捕获过程。

`postgres_cdc/runtime.rs` 和 `mysql_cdc/runtime.rs` 只实现私有源适配：启动 snapshot/streaming connector、清理源快照资源、转换记录、恢复 checkpoint 及具体错误分类。它们不访问 Store、不执行 ACK，也不各自维护另一套 Phase/NextStep。私有接口只服务这两种已支持的 Debezium 源，不是公共 connector API、插件 registry 或任意生命周期 hook 框架。
两种源的 converter 直接返回共享的 `Captured { change, sealed, progress }`，保留跨 delivery 的快照进度，且只在完成通知到达时封口（不以最后一行标记代替）。

`scan/cdc_convert.rs` 统一校验两种源完全相同的 Connect envelope、heartbeat、snapshot notification 与完整 row image，再按有序投影构造 Arrow arrays 和 Change。未投影列仍须完成值校验；空投影保留行数和 diff。两种源各自的 `convert.rs` 保留 topic、tombstone、source metadata、snapshot marker 与事件顺序规则，`schema.rs` 仅将各自支持的类型映射到共享 wire kind。错误继续归因到原来源，并保留 PostgreSQL 的 `REPLICA IDENTITY FULL` / `numeric` 与 MySQL 的 `binlog_row_image=FULL` / `decimal` 文案。该合并不改变 phase、checkpoint、spool、ACK 或持久格式。

恢复仍保留源差异：PG 只在 Publishing/Streaming 解析可恢复 checkpoint，未封口 checkpoint 随完整快照丢弃；MySQL 在全部阶段验证已有 checkpoint，并拒绝 Resetting 中有 spool 却没有 checkpoint 的状态。源的 tag、三个资源名称、phase 数字和 checkpoint 原始字节不变。
