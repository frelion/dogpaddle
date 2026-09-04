# ADR-0001：在 Rust 宿主中嵌入单例 HotSpot JVM 与 stock Debezium Engine

- **状态**：Accepted
- **日期**：2026-09-04
- **范围**：DogPaddle 外部 CDC Source 的进程、JVM、Engine 与 JNI bridge 模型
- **路线**：[`DEBEZIUM_ROADMAP.md`](../../DEBEZIUM_ROADMAP.md)
- **决策来源**：[GitHub #2](https://github.com/frelion/dogpaddle/issues/2)

## 背景

DogPaddle 是嵌入 Rust 应用进程的持久 Dataflow 引擎。现有运行模型有几个对 CDC 至关
重要的约束：

- `Flow` 唯一持有 MDBX 写事务启动能力；
- Station 只在一次调用期间借用读写能力；
- `Operation::turn` 可因 `Idle`、错误、output 背压或外层 commit 失败而重放；
- Source 与其他 Operation 使用同一 `turn`，只以 `None` 表示零输入；
- build/open 需要从 canonical Definition 纯粹地重建 Schema binding 和运行资源；
- 工作区禁止 DogPaddle 代码使用 `unsafe`。

第一个生产 Source 需要 PostgreSQL CDC。直接在 Rust 中重写 `pgoutput`、snapshot、Schema、
offset 与故障恢复，会同时承担 connector 协议和 Dataflow 事务两类复杂度。Debezium 已经
提供成熟、开源且覆盖多数据源的 connector 实现，并通过 Debezium Engine API 支持嵌入。

用户明确不希望运行 sidecar 进程。因此需要在下列目标之间做决策：

- 保留 DogPaddle 的 Rust 嵌入式产品模型；
- 复用上游 Debezium connector，不自维护 CDC 引擎 fork；
- 保持 MDBX 事务、背压和 reopen 语义；
- 不把 PostgreSQL 特例写进 Flow/Station 核心；
- 让 JNI 边界可测、有界，且不需要 Java 反向持有 Rust 地址。

## 决策

DogPaddle 采用以下组合：

> **Rust host + 每进程单例内嵌 HotSpot JVM + stock Debezium Engine +
> Rust pull/ack 薄 Java bridge。PostgreSQL 只是第一个试点。**

### 1. Rust 是宿主与最终协调者

Rust 进程创建 JVM，拥有 Flow、Store、SourceDriver 和 connector handle 的生命周期。
Java 只负责运行 Debezium Engine、缓冲一个有界 delivery，并在 Rust 明确 ACK 后通知
Debezium `RecordCommitter`。

Rust 保留以下权威：

- 何时 poll；
- 何时将 delivery 持久到 MDBX；
- 何时允许 ACK；
- 何时运行 `Flow::advance`；
- 何时 stop connector；
- 哪份 offset 是恢复真相。

Java 线程不直接调用 Flow、不开始 MDBX transaction，也不改变 Station 调度。

### 2. 一个 OS 进程至多创建一个 HotSpot JVM

JVM host 是进程级单例。多个 Flow 或 connector 可在同一 JVM 中拥有独立 Debezium Engine
handle，但不为每个 Source 创建 JVM。

第一次成功初始化会固定：

- JVM options；
- class path 和 bridge/JAR 版本；
- JDK/HotSpot 运行时边界。

后续初始化请求必须兼容，否则返回明确错误，不启动第二 JVM。不把 HotSpot
destroy/recreate 当成正常恢复机制；所有 Engine 在进程退出前应尽力 stop，JVM 由进程
终止回收。

D1 的可重复基线为 Java 17 bytecode、JDK 21 runtime 和 `jni-rs` `0.22.x`
Invocation API。DogPaddle 自有 Rust 代码不写 `unsafe`；JNI 底层的安全责任留在经审查的依赖中。运行时不长期保存
thread-local `JNIEnv`，线程按 JNI 库的安全 API attach/detach。

### 3. 使用 stock Debezium Engine

“stock”在本 ADR 中指：

- 使用 Debezium 上游发布的 Maven artifacts；
- 使用公开 Debezium Engine API 和受支持的 SPI；
- 不 fork Debezium connector；
- 不复制、替换或按同名 class 覆盖 Debezium 内部实现；
- 不让 DogPaddle Rust 代码依赖 Debezium 内部 package 名。

DogPaddle 可以实现 Debezium 公开 SPI，例如 offset backing store，前提是该实现只用受支持
契约。薄 Java bridge 是 DogPaddle 自有代码，但它不改写 connector 语义。

D1 锁定 Debezium `3.6.2.Final`。任何升级都是显式工作，需重跑 bridge、offset、
SourceRecord envelope、connector 与故障恢复验收，不使用浮动 latest 版本。

### 4. Java bridge 只提供 Rust pull/ack API

bridge 是 connector-neutral 静态 API：

```text
create(config_bytes) -> handle
start(handle)
poll(handle, timeout, max_bytes) -> owned_bytes | timeout | status
ack(handle, token)
status(handle)
stop(handle, deadline)
```

D1 对应的精确 JNI 形状是 `create([B)J`、`start(J)V`、`poll(JJI)[B`、
`ack(JJ)V`、`stop(JJ)V` 与 `status(J)[B`。生产版本可以对 envelope 做版本化演进，
但不能改变 pull/ack 方向或单 outstanding 契约。

该 API 的语义是：

- `create` 只创建独立 handle，不把 Java object reference 暴露给连接器代码；
- `start` 启动该 handle 的 Engine 工作线程；
- Engine callback 只把完整批次封装到有界 Java 队列；
- `poll` 由 Rust 主动调用，timeout 表示当前无数据，返回值是 Rust 拥有的字节；
- 每个 handle 同时至多有一个 outstanding delivery；
- 未收到正确 token 的 `ack` 前，bridge 不交付下一批；
- `ack` 才允许 bridge 对本批调用 `RecordCommitter.markProcessed` 和
  `markBatchFinished`；JNI `ack` 只发信号，committer 调用仍在原 Engine callback 线程
  按顺序完成；
- `status` 可观察 created/starting/running/failed/stopping/stopped 及当前 outstanding；
- `stop` 有 deadline，超时返回错误而不无界阻塞 Rust 宿主。

不存在 Java→Rust callback、Rust function pointer、跨 JNI 边界的借用 buffer，也不在 Java 线程上
重入 `Flow::advance` 或 `Flow::ingest`。这是安全和可控性决策，不是一个可随 connector
改变的优化选项。

D1 诊断 envelope 需不丢字段地保留 PostgreSQL `SourceRecord` 的 source partition、source
offset、key、value 和 schema，用来验证 bridge 可控性；其 JSON 表达不承诺区分任意 connector
offset 中所有 Java 数值运行类型。产品的 type-injective opaque wire encoding 在 D3 固化，
并必须继续满足版本化、owned bytes、有界大小与 connector-neutral。

### 5. MDBX 是 durable offset 唯一真相

D1 缺省使用 `MemoryOffsetBackingStore`，并允许试验显式透传
`FileOffsetBackingStore`；它们都不构成 DogPaddle 的重启恢复声明。D1 使用
`OffsetCommitPolicy.always()`，PostgreSQL fixture 设置 `lsn.flush.mode=connector`，以保持
ACK 和 connector LSN flush 的可观察控制。D2 先建立 generic durable ingress，D3 才将
Debezium opaque partition/offset 与它集成。

D3 及以后的提交规则是：

1. Rust poll 到一个 delivery；
2. `Flow::ingest` 把 accepted checkpoint、delivery identity 和 pending payload 原子写入 MDBX；
3. 只有 commit 成功，或 MDBX 已经记录相同 delivery，Rust 才 ACK Java bridge；
4. Java Engine 的 offset store 可在进程内前进，但新进程必须由 MDBX accepted checkpoint 重建；
5. IngressSource 在一个后续 MDBX transaction 中同时清除 pending 并写 Station output。

不使用 Java 本地 offset 文件作为 fallback、加速缓存或双写副本，因为崩溃后无法
可靠判定它与 MDBX 哪个更新。Rust/Flow 把 offset 当作 opaque bytes，不从 PG LSN 或其他
connector-specific 字段推导通用顺序。

### 6. PostgreSQL 只是第一个试点

PostgreSQL connector 在 D4 中验证 bridge、opaque offset、durable ingress 和 `Change` 转换。
以下内容可以是 PostgreSQL-specific：

- connection/publication/slot/table 配置；
- database system identity 与 fencing；
- PostgreSQL 类型映射；
- before-image/replica-identity 要求；
- PostgreSQL 错误分类与运维诊断。

以下内容不得是 PostgreSQL-specific：

- JVM 创建与共享；
- bridge handle 与 start/poll/ack/status/stop；
- owned delivery envelope 的外层协议；
- durable ingress 的 accepted/pending/duplicate/backpressure 语义；
- Flow/Station 的事务和调度边界。

D7 必须用第二个 connector 证明这一分界，然后才能宣称存在稳定的多 Source 架构。

### 7. Snapshot 不与持续 CDC 首版绑定

D1 使用 `snapshot.mode=no_data`，D4/D5 只验证固定 Schema 的持续 WAL CDC。初始
snapshot 是 D6 的独立 generation 语义，必须单独证明：

- snapshot 一致性位置；
- snapshot/WAL 无缝交接；
- generation 完成事实；
- 长时间背压下的内存与 WAL retention；
- 每个阶段的 crash/reopen。

默认优先评估 stock Debezium snapshot。若它不能提供足够的可观察与恢复契约，改用
Rust snapshot reader 属于新架构决策，需要后续 ADR，不由 D6 实现者隐式决定。

## 结果

### 积极结果

- 复用 Debezium 的 connector 生态和上游维护；
- 无 sidecar 部署和独立控制面，保留 DogPaddle 的嵌入式产品形态；
- Rust pull 使 MDBX transaction 与 JNI/Java 线程之间没有重入调用；
- 延迟 ACK 与 durable ingress 可用少量状态机覆盖崩溃窗口；
- connector-neutral bridge 为 D7 第二 Source 留出真实复用路径；
- 使用 stock Engine 避免长期跟随 Debezium 内部类变化。

### 负面结果与代价

- 宿主需要 JDK/HotSpot，发布体积、启动时间和常驻内存显著上升；
- JNI 引入跨语言错误、线程 attach、reference 和 class-loader 生命周期；
- HotSpot fatal error 可使整个 Rust 进程退出，没有 sidecar 故障隔离；
- JVM options/class path 是进程级配置，不能由不同 Flow 独立更改；
- Java bridge、JAR 依赖树、license、SBOM 和 CVE 升级成为 DogPaddle 的发布责任；
- 单 outstanding delivery 优先正确性而非最大吞吐，后续优化必须以等价故障证据为前提；
- PostgreSQL 试点仍需要自行实现 Connect Schema/Value 到 Arrow `Change` 的精确映射。

## 被拒绝的备选方案

### Debezium Server 或 Kafka Connect sidecar

它们提供成熟的独立运行模型，但会引入额外进程、部署、IPC、监控和恢复真相，
不符合本产品的嵌入式要求和用户对 sidecar 的明确拒绝。

### 在 Rust 中重写 PostgreSQL CDC

可以避免 JVM，但需要 DogPaddle 长期维护 logical replication、snapshot、类型映射、
schema change、offset 和 PostgreSQL 版本差异，且无法自然扩展为多数据源方案。

### 每个 connector 创建一个 JVM

HotSpot 不是按 Flow 隔离的轻量 runtime。多 JVM 会放大资源占用、native 库与销毁问题，
并使 JVM options/class path 冲突更难裁决。

### Java callback 直接推送给 Rust

该模式需要 Java 保存 Rust callback/native pointer，容易出现悬垂引用、重入 MDBX transaction、
跨线程 panic/exception 和 stop 竞态，也与工作区禁止 `unsafe` 的方向冲突。

### Fork Debezium 或覆盖其内部类

这可以为特定版本提供更短路径，但会把 Debezium 内部 ABI 变成 DogPaddle 的升级负担。
只使用公开 Engine API/SPI 是本决策的核心价值。

### GraalVM native image 或其他 AOT 形式

这可能减少传统 JVM 部署感，但 connector、反射、动态加载与上游测试覆盖存在额外
不确定性，且不再是本路线要验证的 stock HotSpot 运行边界。

### 让 Debezium 直接写 Station output

这会绕过 `Operation::turn`、Schema guard、capacity-aware append 和 consumer frontier，并要求
Java/driver 获得 Flow 的写事务能力。外部 delivery 必须先通过 D2 durable ingress，再由普通
Source Operation 输出。

## 后续决策

本 ADR 不冻结以下细节，它们由对应阶段的证据决定：

- D3 产品 delivery envelope 的具体 binary encoding；
- SourceDriver 最终是由 `Flow` 直接持有，还是由同进程 runner 在 Flow 外协调；
- PostgreSQL 多表是每表 Engine、单 Engine 路由，还是更高层组合；
- D6 使用 stock Debezium snapshot 还是需要独立 Rust snapshot reader；
- D7 的第二 connector 选择；
- 在真实性能证据前，是否从单 outstanding delivery 扩展为有序多 delivery pipeline。

这些选择不得改变本 ADR 已冻结的四个核心事实：Rust host、单 HotSpot JVM、
stock Debezium Engine、Rust pull/ack bridge。如果后续证据要求改变它们，应以新 ADR
取代本决策，而不回写历史。
