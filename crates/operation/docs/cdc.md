# CDC Scan 契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## 运行调优

`PostgresCdcScanConfig::options` 和 `MySqlCdcScanConfig::options` 统一接收 `CdcOptions`。`new()` / `default()` 不设置任何覆盖；具体源在发现和 connector 启动时分别应用自己的默认值，不预先解析或保存第二份调优状态。六个 setter 都在外部 I/O 前校验，失败返回包含 setter 参数名和原因的 `CdcOptionsError`。

| setter | 准入 | connector 属性 |
| --- | --- | --- |
| `connect_timeout(Duration)` | 正整数毫秒，至多 `2147483647` | PG `driver.connectTimeout` 向上取整为秒；MySQL `connect.timeout.ms` 保留毫秒 |
| `query_timeout(Duration)` | 正整数毫秒，至多 `2147483000` | `database.query.timeout.ms` 向上取整至下一个整秒的毫秒数 |
| `retry_limit(u32)` | `0..=2147483647` | `errors.max.retries`，`0` 禁用重试，未设置为无限重试 `-1` |
| `retry_max_delay(Duration)` | 整数毫秒，`301..=2147483647` | `errors.retry.delay.max.ms`，初始延迟固定 `300ms` |
| `heartbeat_interval(Duration)` | 正整数毫秒，至多 `2147483647` | `heartbeat.interval.ms`，只应用于持续捕获 |
| `snapshot_fetch_size(NonZeroU32)` | `1..=2147483647` | `snapshot.fetch.size`，只应用于初始快照 |

PG 发现和 connector 默认连接、查询超时均为 5 秒，快照 fetch size 默认 `10240`。MySQL 发现默认连接、查询超时为 5 秒，connector 分别为 30 秒和 10 分钟；其快照 fetch size 默认省略，保留 Debezium 的流式读取行为。两者持续捕获 heartbeat 默认为 1 秒，bootstrap 固定为 1 毫秒；重试最大延迟默认为 10 秒。

显式连接和查询覆盖同时用于发现与 connector。发现保留精确毫秒；MySQL 发现查询预算用于 socket 读写，connector 用于 JDBC statement。JDBC 的整秒向上取整防止 `1..999ms` 变为无限等待。重试仅作用于启动成功后的 polling 故障，不延长固定 60 秒 readiness 边界，也不控制 PG slot 创建。

这些覆盖只存在于本次进程的 Config，不改变 Definition、Program 身份、资源布局或恢复状态；重开时再次提供。SQL 两个 endpoint 的同名参数通过同一解析路径产生 `CdcOptions`，参数词汇及单位见 [SQL endpoint 合同](../../sql/README.md#endpoint-合同)。

## 固定列声明

两种 Spec 的 `columns` 直接使用 Arrow `Fields`，字段的名称、`DataType` 和 nullability 是唯一列声明；不再维护独立的源 Column/Type 或转换 WireKind。完整 Schema 与投影 Schema 共享这些 `Arc<Field>`。所有字段必须具有非空、无 NUL、唯一的名称和空 metadata；列数仍为 1–1600。列字段反序列化先临时读取 JSON 列值，浅查 data_type 外形只允许标量字符串或 Decimal128/Timestamp，再交原生 Arrow Field 解码；不递归构造不受支持的 Arrow 类型。之后与构造器、纯 binding 复用同一源 Schema 校验。临时 JSON 值增加解码时的分配和遍历，不保留在 Definition 或运行时，也不宣称该改变降低解码峰值内存。这保留旧闭合类型枚举的准入域：即使 raw `serde_json::from_value` 不经过文本解析深度限额，也不能把深层嵌套列交给 Flow 编码。构造器和 binding 的检查早于 Definition 的 1 MiB 编码检查；Flow 的全图 8 MiB 编码检查仍早于 binding。

PG 允许 Boolean、Int16/32/64、Float32/64、Utf8、Binary、Date32、Timestamp(Microsecond, None)、Timestamp(Microsecond, "UTC") 和 Decimal128；MySQL 仅允许 Int16/32/64、Float64、Utf8、Binary 和 Decimal128。两者 Decimal128 都要求 `1 <= precision <= 38`、`0 <= scale <= precision`。其他 timestamp 单位或时区、嵌套类型、Dictionary 与 unsigned 类型均拒绝。源目录发现继续单独拒绝原生不支持的类型、generated/invisible 列等条件，不能以 Arrow 类型准入代替原生检查。PG 的 microseconds 和两源的 precise decimal connector 配置保持不变。

开发期 v1 Definition 直接保存 Arrow Field 的 serde 表示；上游非 Dictionary 字段的 dictionary-only 属性不参与逻辑 Schema 相等或源身份比较。它们不赋予任何字典能力，不另建字段 codec 或规范化副本。原生 Field 编码含这些默认属性及空 metadata，比旧三字段 Column 更大，完整 Definition 仍受 1 MiB 上限约束；旧列表示不能读取，受影响状态直接重建。输出物理 Schema、schema-bound input entry 和捕获/ACK/恢复协议不变。

## PostgreSQL

PostgresCdcScan 只有一个具体 Scan，不另建公共 IngressScan 或 connector driver trait。
Definition 保存非敏感身份、完整固定列声明、有序 `output_projection` 和必填 `NonZeroU64 bootstrap_spool_bytes`，canonical JSON 使用 `postgres_cdc_scan` 稳定名称。默认构造器保存 identity projection；投影构造器允许空输出，并要求索引无重复且保持 source 顺序。
它只声明 `postgres_cdc_scan.phase: Cell<u32>`、`postgres_cdc_scan.checkpoint: Cell<Vec<u8>>`、`postgres_cdc_scan.input: Queue<Vec<u8>>` 三个资源。
首次运行按 `Fresh → Capturing → Sealed → Streaming` 推进：converter 仍以完整列声明校验每个 Debezium envelope 和完整 row image，只为 `output_projection` 构造 Arrow array；空投影显式保留 batch 行数和 diff。runtime 从这份 projected output Schema 构造唯一的 `SchemaBoundChangeCodec`；`initial` 快照期把 delivery 的可选 Change 编成 schema-bound entry，追加唯一 input Queue，并与整个 delivery 的 opaque checkpoint、phase 在同一事务提交，之后才 ACK；snapshot completed notification 将整个 delivery 封口。Fresh、Capturing、Resetting 的 input 不可见，也不得被公开消费者删除；Sealed、Streaming 直接读取和消费原 Queue。可见性只由 Store 中的 phase 决定，不依赖运行时缓存或是否已调用 restore。
schema-bound entry 的 v1 持久布局固定为 format marker、canonical physical Schema 的 BLAKE3 fingerprint、单个 uncompressed RecordBatch IPC message 和 EOS，不在每项重复完整 Schema，也不接受 self-contained IPC fallback。封口额外只改变 phase，不移动、重写或重新编号已有 entry；不保留 published Queue 或 Publish maintenance delivery。封口提交并完成 barrier、ACK 原真实 Delivery 后停止 snapshot connector，随后从封口 checkpoint 开始 streaming。
PG 以 `no_data` 继续捕获。Sealed 允许 input 保留最多 bootstrap 容量；每份 streaming delivery 在任何写入前检查 input 是否已不超过 64 MiB，并以 64 MiB 为追加上限。超过时返回 false、保留原真实 Delivery，连无 Change 的 heartbeat 也不推进 checkpoint 或 phase。首份成功的 streaming record 在同一事务更新 checkpoint 和 Streaming；其后的恢复容量严格为 64 MiB，不能误用可能更小的 bootstrap 容量。
Capturing 期 reopen 或普通错误绝不恢复部分快照；它必须先停止 connector、在 Store 事务外删除兼容且非 active 的 source-owned slot，再通过 Resetting 每笔事务 `discard_front(256)` 有界清空 input/checkpoint 并回到 Fresh，不复制或解码已废弃的 entry。Queue 逐项验证长度与连续性、为每项写 tombstone，但整批只更新一次 metadata；整批删除与最后的 checkpoint/phase 清理一起提交或回滚。Resetting 直接使用 discard 返回的空状态结束重置。
`bootstrap_spool_bytes` 是 bootstrap capture 的 Queue 硬逻辑上限，每项计费为 8-byte private sequence 加 projected output 的实际 encoded entry bytes，空队列也不接受超限项；不足时该 delivery 不提交、不 ACK，必须用更大容量重建 Flow。
PG discovery 要求预配置 publication、FULL replica、单张 permanent 非 partition 表，以及首次启动前不存在、之后由该 Scan 独占的 slot 名；bootstrap 容量必须容纳完整快照和 completed notification 前的 WAL 重叠。
完整列声明与 `output_projection` 都属于开发期 v1 持久布局；改变投影或读取缺少该字段的旧 Definition 时直接重建状态，不迁移或 fallback。无 TLS、在线 Schema evolution或跨实例 fencing。
TRUNCATE 必须送入 converter 并拒绝；运行中外部修改 slot/publication/schema 不受支持。
所有外部 I/O 在 Store 事务外。`restore(ReadTransactionAccess)` 只读取和验证有界控制；`poll` 返回真实拥有型 Delivery 或具体维护数据；`record(access, &mut delivery)` 在同一事务捕获全部数据、opaque checkpoint 与 phase，容量背压返回 false 时整笔回滚并保留原 Delivery。Flow 提交并完成 WAL barrier 后才调用 `ack(delivery)` 消费原线性句柄；不根据 checkpoint 重造 ACK。`published(ReadTransactionAccess)` 只读返回 Queue front 的 schema-bound 原字节，调用方以 exact output Schema 解码；capture 只 append，因此前项在跨页与下游调用期间保持不变。`consume_published(TransactionAccess)` 用 `discard_front(1)` 无复制删除前项，必须与完成该输入的消费者状态在同一事务提交。Flow root 直接借用 Source front，只在挂起时保存 control；完整计算与路由 Done 后才消费前项。消费不决定捕获 checkpoint，也不推迟真实 ACK。
每个 Source 只有一条 input Queue。Capturing、Resetting 与 Sealed 按 `bootstrap_spool_bytes` 校验容量；Streaming 的硬逻辑容量为 64 MiB。配置字段继续表示完整 bootstrap 的容量，不代表第二条物理 spool。快照可大于 64 MiB，封口前隐藏、封口后直接消费，因而不与 streaming 上限形成未封口 head 的死锁。
转换前 preflight 每个 Delivery 至多 2048 data envelopes、4096 physical rows、65536 top-level slots，slots 按完整源列数及更新的双行上界计费。Changes 编码与 checkpoint 合计不超过 8 MiB，超限不提交、不 ACK、不拆 Delivery。capture 的 Store 写入至多 8 MiB 加固定控制，低于 24 MiB 逻辑动作界；封口 Delivery 仍可携带数据，但封口额外只更新 phase，没有逐 entry 的 publication 读取或写入。Queue front 在复制前执行 8 MiB 长度准入。
ACK 不确定或提交不确定要求 fail-stop/reopen；不使用回调、通用 PreparedTurn 或 AfterCommit。
普通 Cargo gate 不依赖 Java/PG，真实本机验收为 system-tests/postgres/check_cdc.py。

## MySQL

MySqlCdcScan 同样是单个具体 Scan，Definition 保存发现的非敏感单表身份、完整固定列、有序 `output_projection` 和必填 `NonZeroU64 bootstrap_spool_bytes`；完整 envelope/row image 校验、projected array 构造和零列行数语义与 PostgreSQL 相同。三个资源为 `mysql_cdc_scan.phase: Cell<u32>`、`mysql_cdc_scan.checkpoint: Cell<Vec<u8>>` 和 `mysql_cdc_scan.input: Queue<Vec<u8>>`。
它也按 `Fresh → Capturing → Sealed → Streaming` 推进，使用 `initial_only` + `snapshot.locking.mode=minimal` 捕获 MySQL 8.4 一致初始快照，以 snapshot completed notification 的 checkpoint 封口，然后以 `recovery` 继续 binlog；沿用上述单 Queue 可见性、容量与真实 ACK 协议。
捕获、封口、发布、背压和 ACK 与 PG 由同一个私有 CDC runtime 实现；数据库连接、记录转换和 checkpoint 身份校验仍由各具体源拥有。
Capturing 期 reopen 通过 Resetting 每笔事务 `discard_front(256)` 至多清理 256 个 input entries，逐项写 tombstone 但整批只更新一次 Queue metadata；清空 checkpoint 后重做完整快照，不复制或解码废弃 entry，也不从中间 checkpoint 恢复。
容量是相同的 Queue 硬上限，每项计费为 8-byte private sequence 加 projected output 的实际 encoded entry bytes，空队列也不接受超限项；超限不 ACK 并要求用更大容量重建。
部署角色应具有短时 global read lock 所需权限，但不得授予 `LOCK TABLES`，以便 global lock 失败时在长表锁 fallback 之前失败。真实系统验收入口为 `system-tests/mysql/check_cdc.py`：直接通过公共 Operation/Store 协议在快照封口及 streaming delivery 的 Store commit 后、ACK 前退出，再验证重开与有序后继事件；普通 Cargo 测试不启动 MySQL。
binlog 必须覆盖快照、sealed input 消费、下游背压与追平全期；过早 `PURGE` 必须 fail closed，不得新选起点。
只支持固定 Schema，无 TLS、在线 DDL 或跨实例 fencing。完整列声明、`output_projection` 和 schema-bound entry 都属于开发期 v1 持久布局；布局变化或旧 Definition 直接重建，不识别、迁移或回退读取旧格式。


## 阅读运行实现

`scan/cdc_runtime.rs` 是两种源唯一的捕获与恢复驱动。它拥有 Connector、phase/checkpoint 与唯一 input Queue。运行步骤不另行持久化；poll 的可丢弃转换进度只在真实 ACK 后发布。恢复完全依赖 phase/checkpoint/input。
PostgreSQL 在 Capturing 恢复时先事务外清理 source-owned slot；MySQL 无需源 cleanup。之后 Resetting 每笔至多 discard 256 entries，checkpoint/phase 在最后一批一起清除。Capturing 失败后的 reopen 重新建立完整快照，不继续部分捕获。
`postgres_cdc/runtime.rs` 和 `mysql_cdc/runtime.rs` 只实现私有源适配：启动 connector、清理快照资源、转换记录、恢复 checkpoint 及具体错误分类。具体 converter 继续返回 `Captured { change, sealed, progress }`，只有完成通知封口。
`scan/cdc_convert.rs` 统一校验 Connect envelope、heartbeat、snapshot notification 与完整 row image，再按有序投影构造 Arrow Change；未投影列仍验证，空投影保留行数和 diff。
恢复仍保留源差异：PG 只在 Sealed/Streaming 解析可恢复 checkpoint；MySQL 在全部阶段验证 checkpoint，拒绝 Resetting 中有 input 却没有 checkpoint。唯一 input resource 取代旧 spool/published，是开发期 v1 layout 变化；旧状态直接重建，不迁移或 fallback。

封口后提前 poll streaming 可能比旧搬运协议更早遇到连接、转换或 admission 错误，进而在更多 bootstrap 输入尚未计算时 fail-stop；不保证先完成整个快照计算才报告后继捕获错误。错误不能修改已封口 input；重开仍从同一持久 checkpoint 恢复。停驻的未 ACK Delivery 和 connector 缓冲沿用既有上限，不增加另一个 payload 副本。性能对照见 [Operation 性能](../PERFORMANCE.md)。
