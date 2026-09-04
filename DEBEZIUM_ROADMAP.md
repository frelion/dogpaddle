# DogPaddle Debezium Source 路线图

本文固化 DogPaddle 引入 Debezium Engine 的 D0–D7 实施顺序、阶段边界和通过门槛。
它是 [GitHub #2](https://github.com/frelion/dogpaddle/issues/2) 的仓库内路线基线，不表示尚未通过验收的能力已经交付。
总体架构决策见
[`ADR-0001`](docs/adr/0001-embed-debezium-engine.md)，通用算子路线见
[`OPERATOR_ROADMAP.md`](OPERATOR_ROADMAP.md)。

截至 2026-09-04，D0 与 D1 已完成，D1 结论为 **GREEN**；可重复证据见
[`experiments/debezium-d1/D1_REPORT.md`](experiments/debezium-d1/D1_REPORT.md)。项目按约定停在
D1 等待 owner review，D2 尚未开始。

## 目标与成功定义

目标是在 Rust 应用进程内嵌入成熟的开源 Debezium Engine，先实现 PostgreSQL CDC，
同时不把 PostgreSQL 特例写进 Flow、Station 或通用驱动协议。“完成”不只是能读到
WAL，而是同时满足：

- 不运行 Kafka Connect、Debezium Server 或其他 sidecar 进程；
- Rust 宿主仍是 Flow 调度、MDBX 事务和恢复语义的唯一所有者；
- 一次外部 delivery 要么可从 MDBX 重放，要么没有被 Debezium ACK；
- 输出仍是精确 Schema、保留行序与 diff 的 DogPaddle `Change`；
- 背压、进程崩溃、PostgreSQL 重启、Flow reopen 和版本升级均有可重复的验收证据；
- 第二个 Debezium connector 能重用同一套 JVM、bridge、driver 和 ingress 边界。

## 已冻结的架构选择

| 主题 | 决策 |
| --- | --- |
| 宿主 | Rust 是主进程与生命周期协调者；Java 不反向调用 Flow |
| JVM | 每个 OS 进程至多一个内嵌 HotSpot JVM，多个 connector engine 共享 |
| Debezium | 使用上游 stock Debezium Engine 和公开 SPI，不 fork、不替换内部类 |
| 边界 | Rust 通过 JNI 主动 `start/poll/ack/status/stop`；不使用 Java→Rust callback 或 native pointer |
| 持久真相 | D3 起 MDBX 保存 opaque connector partition/offset；Java 文件不是第二份 durable offset |
| 试点 | PostgreSQL 是第一个 connector 试点，不是通用 API 的特例 |
| Snapshot | 初始 snapshot/generation 单独放在 D6；D1–D5 先证明持续流与恢复 |

D1 的固定试验基线是 Debezium `3.6.2.Final`、JDK 21 和 `jni-rs` `0.22.x`
Invocation API。这是可重复基线，不是“自动跟随 latest”策略；产品升级规则在 D5 验证。

## 术语与责任

- **JVM host**：Rust 中创建并保持进程级 `JavaVM` 的部分。
- **Java bridge**：极薄的 connector-neutral Java 封装；在 Java 线程上运行 stock Engine，
  并向 Rust 提供有界、拥有型字节交付。
- **SourceDriver**：Rust 侧 connector 生命周期对象；它可以启动和轮询 Engine，但不能自行开始
  DogPaddle 写事务。
- **durable ingress**：外部世界与 Flow 事务之间的持久交接点。
- **connector**：PostgreSQL、MySQL 等 Debezium 数据源实现；不等于 JVM 或 bridge。
- **delivery**：可被单独 ACK 的一个批次，含稳定 ID、完整 source partition/offset 和
  payload；每个 connector 同时至多一个 outstanding delivery。

## 不可破坏的跨阶段边界

1. `Operation::turn` 不执行 poll、ACK、连网、JVM 启动或其他无法随 MDBX 回滚的副作用。
2. 外部 Source 仍经普通 Source Operation 的 `turn(None, ...)` 和 `Action::Commit` 输出；
   不新建 Source 专用 Station 调度协议。
3. Flow 长期唯一持有 `Transactions`；Station、Operation、bridge 和 Java 线程都不得保存
   事务启动能力。
4. `FlowFactory::build/open` 继续先 canonical decode、全图 Schema bind，再创建或打开资源；
   它们不连接数据库、不启动 JVM、不解析 secret。
5. 外部 delivery 只能在 durable ingress commit 后 ACK。Ingress 中待输出的 payload 与
   accepted checkpoint 必须原子持久。
6. IngressSource 清除 pending 与 Station output append 在同一写事务中；输出 Schema
   失配、容量拒绝或 commit 失败都保留 pending。
7. MDBX 是 accepted connector offset 的唯一 durable 真相。Java 侧 offset store 只是 Engine 运行适配，
   必须能从 MDBX 的 opaque bytes 重建。
8. Definition 只持久精确 Schema、非敏感 source identity 和行为配置；密码、token
   和完整 secret DSN 只属于运行期 driver。
9. Java bridge 不向 Rust 借用 `SourceRecord`、`ByteBuffer` 或 JNI local reference；`poll` 返回
   版本化的 owned bytes。
10. 不为第一个 PostgreSQL connector 引入 Flow/Station 中的 connector enum、PG 分支或动态
    Store catalog 旁路。
11. 生产 runtime 中每个 driver 与 delivery 都必须绑定 durable source generation；旧 generation
    的 ingest/ACK 一律 fail closed。D1 只是隔离试验，generation fencing 在 D5/D6 落地并验收。

## 版本、构建与许可证策略

- D1 精确锁定 Debezium `3.6.2.Final`、`jni-rs 0.22.4`、Java 17 bytecode、Temurin
  `21.0.9+10`/Maven `3.9.11` linux/amd64 镜像 digest，以及 PostgreSQL 16 fixture digest；
  完整值记录在 D1 README 与 lockfile 中。
- JAR 与 JDK 不提交进 Git。构建从 Maven/Cargo lock 与 digest-pinned 容器重现；stock
  Debezium 源码审计同时锁定 tag 和 commit。
- 不自动跟随 Debezium、Kafka Connect、JDK、JNI 或 connector 版本。任一升级都必须单独 PR，
  重跑 source audit、bridge/JNI、offset/ACK、真实 connector 与 crash/reopen gate。
- D1 只记录依赖物料规模与主许可证，不构成发布包。D3 在任何 Java artifact 进入产品分发前，
  必须生成完整 transitive SBOM、保存 artifact checksum、审查许可证并随包提供所需 NOTICE；
  D5 再把 CVE 与升级 rehearsal 作为发布门。

## 阶段总览

| 阶段 | GitHub | 主问题 | 交付后的可信结论 |
| --- | --- | --- | --- |
| D0 | [#4](https://github.com/frelion/dogpaddle/issues/4) | 契约是什么 | 关键决策、非目标、门槛和风险已冻结 |
| D1 | [#3](https://github.com/frelion/dogpaddle/issues/3) | stock Engine 是否可控 | Rust 能在同进程稳定 start/poll/ack/stop 原版 Engine |
| D2 | [#5](https://github.com/frelion/dogpaddle/issues/5) | 外部事件如何安全进入 Flow | 不依赖 JNI/PG 的 generic durable ingress 关闭事务窗口 |
| D3 | [#6](https://github.com/frelion/dogpaddle/issues/6) | 原型如何成为可恢复 runtime | 生产 bridge/runtime 使 MDBX 的 opaque offset 成为唯一真相 |
| D4 | [#7](https://github.com/frelion/dogpaddle/issues/7) | PostgreSQL 行如何变成 Change | 固定 Schema 单表 WAL 试点正确表达 insert/update/delete |
| D5 | [#8](https://github.com/frelion/dogpaddle/issues/8) | 是否能发布 | crash、fencing、背压、升级、安全和长稳证据齐备 |
| D6 | [#9](https://github.com/frelion/dogpaddle/issues/9) | 初始全量如何接入 | snapshot/generation 以独立可恢复阶段与 WAL 无缝交接 |
| D7 | [#10](https://github.com/frelion/dogpaddle/issues/10) | 架构是否真的通用 | 第二个 connector 重用同一套边界，再提取被证明的共性 |

D1 与 D2 可在 D0 后并行；D3 必须同时通过 D1 和 D2。D4 依赖 D3，D5 依赖
D4。D6 故意晚于持续 CDC 的发布加固；D7 不允许因为“未来也许复用”而提前抽象。

## D0：Contract

### 边界

D0 只冻结架构契约、阶段依赖和停止条件，不修改产品代码，也不将可行性假设
写成已验证事实。

### 交付

- 本路线图；
- 冻结进程/JVM/Engine/bridge 模型的 ADR-0001；
- D1–D7 的通过门槛、非目标和主要风险；
- 与根 README 和通用算子路线的双向链接。

### 验收

- 文档能唯一回答“谁启动 JVM、谁 poll、谁 ACK、谁保存 offset、何时允许 ACK”；
- D1 与 D3 的恢复声明明确分开；
- Snapshot 不会被暗含为 D4/D5 的完成条件；
- 非目标不会被误读为已承诺能力。

### 退出条件

ADR 被接受，GitHub #2 与仓库文档使用同一 D0–D7 切分，D1 黑盒验收无需再选
进程模型或 bridge 方向。

### 主要风险

最大风险是在试验前冻结过多细节。D0 因此只冻结责任与正确性边界；可以不改变
这些边界的 wire encoding、批大小和具体 public API 在后续阶段确定。

## D1：Stock controllability spike

### 边界

D1 是隔离在 `experiments/debezium-d1/` 中的可行性试验，不依赖 DogPaddle Change、Store、
Operation 或 Flow，不进入产品 crate。它使用 `snapshot.mode=no_data`；缺省使用
`MemoryOffsetBackingStore`，也可为黑盒试验显式透传 `FileOffsetBackingStore` 配置。两者
都只用来证明 stock Engine 可控，**不声明 DogPaddle 进程重启恢复**。

### 交付

- `experiments/debezium-d1/bridge/`：Maven 管理的薄 Java bridge，使用 stock Debezium
  `3.6.2.Final`，生成 Java 17 bytecode，以 JDK 21 作为固定运行基线；
- `experiments/debezium-d1/host/`：独立 Cargo host，使用 `jni-rs` `0.22.x`
  Invocation API 创建 JVM；
- connector-neutral 静态 API：

  ```text
  create(config_bytes) -> handle
  start(handle)
  poll(handle, timeout, max_bytes) -> owned_bytes | timeout | status
  ack(handle, token)
  status(handle)
  stop(handle, deadline)
  ```

  D1 的精确 JNI 形状为 `create([B)J`、`start(J)V`、`poll(JJI)[B`、
  `ack(JJ)V`、`stop(JJ)V` 和 `status(J)[B`；

- 版本化的 D1 诊断 envelope，不丢字段地携带 PostgreSQL `SourceRecord` 的 source
  partition、source offset、key、value 和 schema；其中 partition/offset 的 JSON 值只用于
  可行性观测，并不是 D3 所需的 type-injective opaque codec；
- 可重复的 PostgreSQL fixture 与黑盒命令。

Java bridge 在 Engine callback 内只写入有界队列；Rust 主动 poll。每个 connector 至多一个
outstanding delivery，只有 Rust `ack` 后 bridge 才对该批执行
`RecordCommitter.markProcessed` 与 `markBatchFinished`：JNI `ack` 只发信号，实际 committer
调用仍在原 Engine callback 线程上按顺序完成。D1 使用 `OffsetCommitPolicy.always()`；
PostgreSQL fixture 只允许 connector 自身控制 LSN flush，因此设置 `lsn.flush.mode=connector`。

### 验收

- 黑盒证明 Rust host 和 JVM 处于同一 OS 进程，未启动 Java sidecar；
- 一个 JVM 可顺序创建、启动和停止 Engine handle，状态可观察；
- `poll` timeout 是正常无数据结果，不是失败；返回字节在 JNI 调用后仍完全属于 Rust；
- 未 ACK 的 delivery 保持稳定 token，且 bridge 不交付后续 batch；
- ACK 后才可观察下一批，重复/错误 token 返回结构化错误；
- ACK 前 PostgreSQL `confirmed_flush_lsn` 与标准 file offset bytes 都不变，ACK 后二者均推进；
  fresh Engine + 同一 persistent slot 的组合 restart witness 跳过已 ACK 记录，但不宣称 file
  store 被单独隔离证明，更不宣称捕获的 JSON offset 能注入新 Engine；
- `max_bytes`、单槽有界交付和 `stop` deadline 都可黑盒触发，失败不变成 hang；
- Java exception、危险配置和 connector class 加载失败可转换为稳定状态/错误，不越过 JNI 边界崩溃；
- 同一 OS 进程 ID 与 JVM identity 可观察，并在 fresh Engine handle 间保持不变。

### 退出条件

在一个文档化的支持平台上，从干净 checkout 可一条命令构建并重复全部黑盒验收；
已记录准确 JDK、Debezium、JNI、Maven 与 PostgreSQL 版本。若只能通过 fork Debezium、
覆盖内部类或 Java→Rust callback 才实现可控 ACK，D1 失败并重开 ADR，不进入 D3。

### 主要风险

- HotSpot 创建、线程 attach/detach、class path 与 native library 差异；
- Engine callback 与 Rust poll/stop 之间的死锁；
- Debezium 批次语义无法支持一个 outstanding 和延迟 ACK；
- delivery 大于 `max_bytes` 时的前进规则不明；
- D1 JSON 会合并某些 Java 数值运行类型，不能直接升级为跨 connector 的 opaque offset codec；
- 试验误把 `MemoryOffsetBackingStore` 或显式 `FileOffsetBackingStore` 的行为当成
  DogPaddle/MDBX 恢复保证。

## D2：Generic durable ingress

### 边界

D2 只改造 DogPaddle 的持久输入边界，使用纯 Rust fake driver 验证。产品 crate 不引入
JNI、JDK、Debezium 或 PostgreSQL 依赖，也不在 Flow 中实现 Engine 生命周期。

### 交付

- 一个内建、固定 exact output Schema 的 `IngressSourceDefinition`；
- `Flow::ingest(station_id, delivery)` 和 resume-state 读取能力；
- 版本化 ingress state codec，保存 accepted opaque checkpoint、last delivery identity
  与至多一个 durable pending delivery；
- `Accepted` / `Duplicate` / `Backpressured` 结果以及 Schema、identity、checkpoint、
  codec 和 Store 错误；
- `OperationBinding::materialize` 到 Flow 的窄 ingress capability，不向连接器暴露 Store
  handle 或 transaction starter。

一个 delivery 可包含一个非空 `Change`，也可以仅推进 checkpoint，用于 heartbeat 或
被过滤的 source record。后者不伪造空 `Change`。v1 使用单个 durable pending slot 和
显式最大 delivery bytes，不提前引入第二条可预取日志。

### 验收

- exact Schema mismatch、非 canonical Change、超大 delivery 在开始写事务前失败；
- accepted checkpoint、last delivery ID 和 pending payload 在同一写事务中提交；
- 相同 delivery ID 在 pending 存在或已输出后都是幂等 `Duplicate`；
- 不同 delivery 遇到 pending 返回 `Backpressured`，不改写 checkpoint；
- checkpoint gap/fork 结构化拒绝，不将 connector 乱序解释为重试；
- IngressSource 通过 `turn(None)` 输出，pending clear 与 Station output append 同事务；
- output 背压、Schema guard、Operation 错误、Store poison 和 commit 失败都保留 pending；
- ingest commit 后、Operation 输出前 reopen 仍会输出一次；输出 commit 后的相同
  delivery 不会再输出；
- build/open 仍先完成纯校验，失败无目录副作用，资源布局和 definition 有 golden。

### 退出条件

上述行为由 `dogpaddle-flow` 公共 correctness 测试证明，测试只使用 fake driver 且覆盖
每个 commit/crash 窗口。Store 和 Change 不为 ingress 引入 connector 知识；Station 的
claim/cursor/output-retention 契约未被改变。

### 主要风险

- 为通用性过早暴露过大的公共 driver API；
- 在 shared `Cell<Vec<u8>>` 中保存过大 batch；
- 把 connector offset 当成可比较整数，而不是 opaque 字节；
- 为了“少一次写入”而让 `Flow::ingest` 直接写 Station output，从而绕开 Operation 与
  retention 协议。

## D3：Production runtime/bridge 与 durable opaque offset

### 边界

D3 将 D1 的可行性试验重做为可发布的 connector-neutral runtime，并与 D2 ingress 集成。
它仍不定义 PostgreSQL 行到 Arrow 的产品映射，也不承诺 snapshot。

### 交付

- 进程级单例 JVM host：首次初始化后固定 JVM options 与 class path；
- 可版本化、有界、无 Java→Rust callback 的 bridge 协议和错误模型；
- Rust `SourceDriver`/runner 生命周期：`create/start/poll/ack/status/stop`；
- 对多个 Engine handle 的隔离、正确 stop、显式 dispose/reclaim 和 JVM 进程级状态管理；
- 使用 Debezium 公开 offset-store SPI 的 adapter，从 D2 accepted checkpoint 注入完整、
  opaque partition/offset；
- 可重现的 Java artifacts、checksum、license/SBOM 输入和打包方式。

Java 侧可使用进程内 offset store 满足 Engine 协议，但恢复必须从 MDBX 注入；不允许
一份 Java offset 文件与 MDBX 竞争“哪个更新”。Rust 只在 `Flow::ingest` 返回
`Accepted` 或同 delivery 的 `Duplicate` 后 ACK。

### 验收

- 两个 connector handle 共享同一 JVM，又拥有独立 config、queue、status、offset 和 stop；
- 无 active MDBX transaction 时才进入 JNI `poll/ack/stop`；
- 从空 offset 启动、从 MDBX opaque offset 重启和含 pending delivery 重开均可重复；
- 在 poll、ingest commit、ACK、pending output 前后分别中断，无已 ACK 但不可恢复的
  payload，也无重复 Change；
- ACK 失败保留 outstanding delivery 并可幂等重试，不提前 poll 下一批；
- Java exception、connector failure、JVM 初始化冲突和 stop timeout 都有结构化 Rust 错误；
- 不依赖 Debezium 内部 package/class，上游 JAR 可由已记录 checksum 验证。

### 退出条件

runtime/bridge 有自身公共协议测试和与 D2 的端到端故障测试；干净进程中无需
Java sidecar 或外部 offset 文件就能重启。只有满足这一点后，D4 才能把行转换错误
置于可恢复的 ACK 边界中。

### 主要风险

- HotSpot 通常不适合在同一进程中 destroy/recreate，一次失败初始化可能污染整个进程；
- JVM 崩溃会终止 Rust 宿主，不具备 sidecar 的故障隔离；
- Debezium 的部分启动 phase 可能暂时拒绝 `close`；生产 stop 需要可重试、phase-aware 的状态机，
  不能把 D1 对已运行 Engine 的 one-shot shutdown worker 直接当成最终实现；
- class-loader、日志系统、TLS/native library 与宿主应用发生冲突；
- 发布包大小、JDK 可用性与多平台支持成为产品负担；
- 在 Rust 解析 connector offset 字段会把本应 opaque 的 Debezium 版本边界扩散到引擎。

## D4：PostgreSQL connector pilot 与 fixed-Schema conversion

### 边界

D4 只承诺 PostgreSQL 单库单表、固定 Schema 的持续 WAL CDC。默认
`snapshot.mode=no_data`；初始全量数据属于 D6。不在一条 Station output 中混合多个不同
Arrow Schema，不处理在线 DDL/schema evolution。

### 交付

- PostgreSQL Source 的非敏感持久 definition：database/source identity、publication/slot/table
  identity、精确 Arrow Schema 和转换选项；
- 在 `FlowFactory` 之前运行的显式 discovery/planning API，把 PostgreSQL catalog 结果固化为
  Definition；build/open/bind 不查询 PostgreSQL；
- `SourceRecord` 到 `Change` 的固定 Schema 转换；insert `+1`、delete `-1`、update 按
  before `-1` 然后 after `+1` 的顺序输出；
- 明确的 PostgreSQL 类型/nullability/decimal/temporal 支持矩阵，未承诺类型 fail closed；
- connector runner 与运行期 secret resolver，密码不进入 Flow definition；
- publication、replication slot 的归属、创建、重用和删除策略。

v1 要求可获得完整 before row，默认要求 `REPLICA IDENTITY FULL`。若只有 key-only old row，
DogPaddle 不能伪造完整 `-1` 记录；在有独立状态重建设计之前必须拒绝该表。

### 验收

- 真实 PostgreSQL `pgoutput` 下 insert、delete、update 映射为预期的完整、有序 diff；
- null、主键、文本/二进制、整数、布尔、已声明的 decimal/temporal 路径有端到端证据；
- 同一 transaction 中多个行事件的源顺序保持，不使用 Change 物理批界伪造事务语义；
- heartbeat、不相关表和无行输出记录可仅推进 durable checkpoint；
- runtime 校验 database system identity、publication、slot 和 table identity，错配时不 ACK；
- Schema drift、DDL、丢失 before image、超范围数值和不支持类型使 connector 停在明确失败状态；
- Flow drop/reopen、PostgreSQL 重启和短时网络中断后从 accepted opaque offset 继续。

### 退出条件

一个不使用 `SequenceSource` 的真实 Flow 能执行

```text
PostgreSQL CDC → IngressSource → Transform → SqliteSink → drop/reopen
```

并由 PostgreSQL 源表变更与 SQLite 最终关系共同校验 insert/update/delete。所有支持类型、
replica identity、slot/publication 归属与 DDL 行为都已写入公共文档。

### 主要风险

- PostgreSQL/Debezium 的旧值可用性与 DogPaddle 完整撤回语义不匹配；
- Debezium Connect Schema 与 Arrow 在 decimal、timestamp/timezone、array/json 上语义不同；
- 外部更改 publication、slot 或表结构使持久 Definition 失效；
- 单表模型为简单而状态数据库重复启动太多 Engine/slot；只有真实负载证明后才设计
  multi-table routing。

## D5：Release hardening

### 边界

D5 不扩大 connector 功能面，专门将 D4 的固定 Schema 持续 CDC 变成可发布能力。
Snapshot、在线 DDL 和第二 connector 仍非目标。

### 交付

- poll、ingest、ACK、output、reconnect 各边界的确定性 fault-injection 矩阵；
- database/slot/Flow/connector-instance fencing，防止两个活动驱动者使用同一 durable identity；
- 有界 Java queue、单 outstanding delivery、DogPaddle output capacity 和 PostgreSQL WAL 保留的
  端到端背压观测；
- start/reconnect/stop deadline、graceful shutdown、不可恢复 failure 和运行状态 API；
- 指标与诊断：JVM/Engine 状态、poll/ACK latency、outstanding bytes、accepted/emitted
  checkpoint、backpressure 源头、slot/WAL lag；
- secret redaction、TLS 边界、JDK/JAR provenance、license/SBOM/CVE 流程；
- 精确版本升级流程：旧 opaque offset fixture、bridge envelope、Definition/state golden、
  上游 Debezium connector 兼容性评审。

### 验收

- 在每个持久或外部副作用前后强制 kill/restart，不丢失已 ACK payload，最终关系不重复；
- 第二个驱动者不能抢占已活动 identity，过期 generation/lease 的 ACK 被拒绝；
- 长时间下游背压时内存受控，WAL lag 可观察，解压后按原顺序追平；
- PostgreSQL 重启、断网、认证失效、slot 丢失、JVM exception 和 stop timeout 有稳定分类；
- 并发多 Flow/connector 长稳测试没有 deadlock、JNI local/global reference 泄漏或无界队列增长；
- 对锁定版本的升级 rehearsal 明确得出“可直接 reopen”或“必须重建”，不猜测迁移。

### 退出条件

持续 CDC 的 correctness、fault-injection、长稳、性能基线、运维手册、支持平台与依赖物料表
同时完成。在此之前不宣称 PostgreSQL Source 可用于生产；在 D6 之前明确标注
“不包含初始全量”。

### 主要风险

- 嵌入 JVM 与 Rust 同生共死，HotSpot fatal error 没有 sidecar 隔离；
- 背压时 PostgreSQL slot 持有 WAL，可导致磁盘耗尽；
- 凭据、Debezium config 或 SourceRecord 日志泄露数据；
- Debezium/JDK 安全升级与 offset/schema 兼容性冲突；
- 单 JVM 中一个 connector 的失控线程影响其他 Flow。

## D6：Snapshot generation staging

### 边界

D6 才引入初始全量，并将它建模为显式、可持久的 generation 阶段。它不依赖
`Idle` 暗示 snapshot 完成，不把 snapshot/WAL 交接隐藏在 Java 队列或物理 `Change` 批界。

### 交付

- 持久 generation identity 和至少 `NotStarted/Snapshot/Streaming/Failed` 的恢复状态；
- 一个可证明的 snapshot read position 与 WAL start/resume position 交接协议；
- snapshot row 到 `+1` Change 的有界分批，与 D2 ingress 使用同一背压和 ACK 原则；
- generation 完成事实的持久表达及 reopen 行为；
- 对 stock Debezium snapshot 的可观察性和恢复能力评估。

首选是继续使用 stock Debezium 支持的 snapshot 机制。若它不能暴露足够信息以证明
snapshot/WAL 交接和 DogPaddle 恢复语义，才另立 ADR 评估“Rust 读 snapshot，Debezium 读 WAL”。
不得在 D6 实现中默默偏离 ADR-0001。

### 验收

- 静态源表的初始行每行恰好以 `+1` 出现一次；
- snapshot 进行期并发 insert/update/delete，展平结果无缺口且最终关系与 PostgreSQL 一致；
- 在获取 read position、读取中、最后一批、generation-complete commit 和切换 WAL 前后
  逐点 crash/reopen；
- 大表和单行超大值的内存、delivery bytes、背压和 WAL retention 有明确上界/观测；
- snapshot 失败不会被误报为 streaming，重试不会叠加旧 generation 的已提交行；
- 流式 D4/D5 用户可以继续显式选择 `no_data`，不被强制重做 snapshot。

### 退出条件

大表、并发写、全部阶段崩溃和下游长时背压的模型/端到端验收通过；文档能明确
说明 generation 从哪个 PostgreSQL 一致性位置产生、哪个 durable 事实将 Source 切换为
Streaming。

### 主要风险

- PostgreSQL exported snapshot、replication slot 和 WAL LSN 的一致性交接很容易出现隐蔽缺口；
- 长 snapshot 使数据库保留过多 MVCC tuple/WAL；
- Debezium snapshot offset 的版本内部细节泄漏到 Rust 语义；
- 为提高速度过早引入并行 chunk，导致顺序、重启和清理状态急剧复杂化。

## D7：Second connector proof

### 边界

D7 用第二个真实 Debezium connector 检验架构，而不是预先设计一个“支持所有 Source”的
抽象。具体 connector 在 D5 后根据用户价值与 fixture 成本选择；MySQL 是自然候选，
但 D0 不冻结它。

### 交付

- 第二 connector 的固定 Schema streaming pilot 和端到端 fixture；
- 对 D1/D3 bridge、D2 ingress、driver lifecycle、opaque offset 与错误模型的原样复用证据；
- PostgreSQL 与第二 connector 的 capability matrix，不用最小公分母隐藏差异；
- 只对两个实现语义完全相同的小组件做重构；
- 如必须改变 ADR-0001，增加新 ADR 而不修改历史事实。

### 验收

- Flow、Station 和 Store 不出现 PostgreSQL/MySQL connector 枚举、类型分支或特殊事务路径；
- 单 JVM 同时运行两种 connector engine，其 handle、queue、offset、status 和 stop 互相隔离；
- 第二 connector 经过同一 `poll → durable ingest → ACK → Operation output` 故障矩阵；
- connector-specific Schema/type/snapshot/fencing 规则保留在各自模块中；
- 新抽取的公共组件由 PostgreSQL 和第二 connector 的公共测试共同所有。

### 退出条件

第二 connector 在不更改 Flow/Station 核心契约、不 fork Debezium、不增加 sidecar 的前提下
达到与 PostgreSQL streaming pilot 同级的 correctness/reopen 证据。到此才能宣称 Debezium
路径是多 Source 架构，而不是“恰好能跑 PostgreSQL”。

### 主要风险

- 不同 Debezium connector 的 offset、snapshot、transaction metadata 和 schema-change 模型差异很大；
- 为了共用代码而丢失 connector-specific 的安全校验；
- 第二 connector 触发 bridge wire-format 或 offset-store SPI 的不兼容变更；
- 把只有两个实现的结构过早公开为稳定插件 API。

## 统一停止条件

任一阶段出现下列情况都不应用“后面再补”跨过门槛：

- stock Engine 无法通过公开 API 延迟并精确 ACK；
- 只能使用 Java→Rust callback、Rust raw pointer 或 Debezium 内部类覆盖才能完成；
- accepted offset 可在 payload durable 之前前进；
- Java offset 文件与 MDBX 存在无法裁决的双真相；
- Flow build/open 必须连接外部系统才能重建 binding；
- Schema drift 或丢失 before image 被静默转换为错误的 `Change`；
- 背压可导致无界 Java/Rust 内存增长；
- 升级时无法证明旧 offset/envelope 可读，又不愿明确要求重建。

## 明确非目标

- Kafka Connect 集群、Debezium Server、Kafka 中间层或任意 sidecar 编排；
- 在 Rust 中重写 PostgreSQL logical replication 协议以取代 Debezium；
- D1 就承诺 durable recovery，D4 就承诺 initial snapshot；
- 动态 Schema evolution、DDL migration 或一条 Station 中的多 Schema 数据流；
- 跨 PostgreSQL 与 MDBX 的分布式事务或 exactly-once 上游提交宣称；
- 在第二 connector 之前发布稳定的通用 connector 插件 ABI；
- 通过 fork Debezium、复制 RisingWave 的内部 class override 或自维护 Java CDC 引擎换取短期便利。
