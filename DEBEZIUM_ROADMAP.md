# DogPaddle Debezium Source 路线图

本文固化 DogPaddle 引入 Debezium Engine 的 D0–D7 实施顺序、阶段边界和通过门槛。
它是 [GitHub #2](https://github.com/frelion/dogpaddle/issues/2) 的仓库内路线基线，不表示尚未通过验收的能力已经交付。
总体架构决策见
[`ADR-0001`](docs/adr/0001-embed-debezium-engine.md)，通用算子路线见
[`OPERATOR_ROADMAP.md`](OPERATOR_ROADMAP.md)。

截至 2026-09-04，D0 与 D1 已完成，D1 结论为 **GREEN**；可重复证据见
[`experiments/debezium-d1/D1_REPORT.md`](experiments/debezium-d1/D1_REPORT.md)。D1 已由 owner
合并。多 agent 对抗审查随后将原 D2/D3 重排：先把 Debezium 做成独立、窄小的产品组件，
再围绕已经稳定的 delivery/checkpoint API 建 MDBX durable ingress。D2 的实现与验收记录见
[PR #12](https://github.com/frelion/dogpaddle/pull/12)；合并后由 GitHub #5 自动关闭。

## 目标与成功定义

目标是在 Rust 应用进程内嵌入成熟的开源 Debezium Engine，先实现 PostgreSQL CDC，
同时不把 PostgreSQL 特例写进 Flow、Station 或通用驱动协议。“完成”不只是能读到
WAL，而是同时满足：

- 不运行 Kafka Connect、Debezium Server 或其他 sidecar 进程；
- Rust 宿主仍是 Flow 调度、MDBX 事务和恢复语义的唯一所有者；
- 一次外部 delivery 要么可从 MDBX 重放，要么没有被 Debezium ACK；
- 输出仍是精确 Schema、保留行序与 diff 的 DogPaddle `Change`；
- 背压、进程崩溃、PostgreSQL 重启、Flow reopen 和版本升级均有可重复的验收证据；
- 第二个 Debezium connector 能重用同一套 JVM、bridge、runtime API 和 ingress 边界。

## 已冻结的架构选择

| 主题 | 决策 |
| --- | --- |
| 宿主 | Rust 是主进程与生命周期协调者；Java 不反向调用 Flow |
| JVM | 每个 OS 进程至多一个内嵌 HotSpot JVM，多个 connector engine 共享 |
| Debezium | 使用上游 stock Debezium Engine 和公开 SPI，不 fork、不替换内部类 |
| 边界 | Rust 通过窄公共 API 主动 `start/poll/ack/stop`；JNI handle、token 与 status 仅属私有实现 |
| 持久真相 | D3 起 MDBX 保存 opaque connector partition/offset；Java 文件不是第二份 durable offset |
| 试点 | PostgreSQL 是第一个 connector 试点，不是通用 API 的特例 |
| Snapshot | 初始 snapshot/generation 单独放在 D6；D1–D5 先证明持续流与恢复 |

D1 的固定试验基线是 Debezium `3.6.2.Final`、JDK 21 和 `jni-rs` `0.22.x`
Invocation API。这是可重复基线，不是“自动跟随 latest”策略；产品升级规则在 D5 验证。

## 术语与责任

- **JVM host**：Rust 中创建并保持进程级 `JavaVM` 的部分。
- **Java bridge**：极薄的 connector-neutral Java 封装；在 Java 线程上运行 stock Engine，
  并向 Rust 提供有界、拥有型字节交付。
- **Connector**：`dogpaddle-debezium` 暴露的线性 Rust 生命周期对象；它可以启动和轮询
  Engine，但不知道 Flow 或 MDBX。
- **durable ingress**：外部世界与 Flow 事务之间的持久交接点。
- **connector**：PostgreSQL、MySQL 等 Debezium 数据源实现；不等于 JVM 或 bridge。
- **delivery**：可被单独 ACK 的一个批次，含 records 与 ACK 前候选 checkpoint；Java token
  只是 run-local 私有能力，不是 durable ID。每个 connector 同时至多一个 outstanding delivery。

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
   和完整 secret DSN 只属于运行期 coordinator。
9. Java bridge 不向 Rust 借用 `SourceRecord`、`ByteBuffer` 或 JNI local reference；`poll` 返回
   版本化的 owned bytes。
10. 不为第一个 PostgreSQL connector 引入 Flow/Station 中的 connector enum、PG 分支或动态
    Store catalog 旁路。
11. D2 不提前发明 generation、lease 或通用 SourceDriver；并发 connector fencing 在 D5、snapshot
    generation 在 D6 分别以真实需求落地。

## 版本、构建与许可证策略

- D1 精确锁定 Debezium `3.6.2.Final`、`jni-rs 0.22.4`、Java 17 bytecode、Temurin
  `21.0.9+10`/Maven `3.9.11` linux/amd64 镜像 digest，以及 PostgreSQL 16 fixture digest；
  完整值记录在 D1 README 与 lockfile 中。
- JAR 与 JDK 不提交进 Git。Rust 依赖由 Cargo.lock 固定；Java 依赖由 POM/BOM 中的精确版本和
  digest-pinned 构建镜像解析，成品 bundle 再生成完整 checksum 与 SBOM。stock Debezium 源码
  审计同时锁定 tag 和 commit；这里不虚构仓库里并不存在的 Maven lockfile。
- 不自动跟随 Debezium、Kafka Connect、JDK、JNI 或 connector 版本。任一升级都必须单独 PR，
  重跑 source audit、bridge/JNI、offset/ACK、真实 connector 与 crash/reopen gate。
- D1 只记录依赖物料规模与主许可证，不构成发布包。D2 的开发/验收 bundle 必须生成完整
  transitive SBOM、保存 artifact checksum，并原样保留每个 JAR 内的 license/NOTICE；D2 bundle
  不对外发布。正式许可证义务审查、顶层 notices、CVE 与升级 rehearsal 统一作为 D5 发布门，
  不能把“生成了 SBOM”写成已经完成法律或安全审查。

## 阶段总览

| 阶段 | GitHub | 主问题 | 交付后的可信结论 |
| --- | --- | --- | --- |
| D0 | [#4](https://github.com/frelion/dogpaddle/issues/4) | 契约是什么 | 关键决策、非目标、门槛和风险已冻结 |
| D1 | [#3](https://github.com/frelion/dogpaddle/issues/3) | stock Engine 是否可控 | Rust 能在同进程稳定 start/poll/ack/stop 原版 Engine |
| D2 | [#5](https://github.com/frelion/dogpaddle/issues/5) | 原型如何成为简单可靠的产品 runtime | 独立 crate 用 pre-ACK 完整 checkpoint 封住 Debezium/JNI |
| D3 | [#6](https://github.com/frelion/dogpaddle/issues/6) | 外部 delivery 如何安全进入 Flow | 最小 generic durable ingress 关闭 MDBX/ACK 事务窗口 |
| D4 | [#7](https://github.com/frelion/dogpaddle/issues/7) | PostgreSQL 行如何变成 Change | 固定 Schema 单表 WAL 试点正确表达 insert/update/delete |
| D5 | [#8](https://github.com/frelion/dogpaddle/issues/8) | 是否能发布 | crash、fencing、背压、升级、安全和长稳证据齐备 |
| D6 | [#9](https://github.com/frelion/dogpaddle/issues/9) | 初始全量如何接入 | snapshot/generation 以独立可恢复阶段与 WAL 无缝交接 |
| D7 | [#10](https://github.com/frelion/dogpaddle/issues/10) | 架构是否真的通用 | 第二个 connector 重用同一套边界，再提取被证明的共性 |

D2 依赖 D1 已证明的控制边界；D3 只围绕 D2 已稳定的 public API 建持久交接。D4 依赖
D2 与 D3，D5 依赖 D4。D6 故意晚于持续 CDC 的发布加固；D7 不允许因为“未来也许复用”
而提前抽象。

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
- D1 的可控性证据与 D2 的独立 checkpoint 恢复声明明确分开；
- Snapshot 不会被暗含为 D4/D5 的完成条件；
- 非目标不会被误读为已承诺能力。

### 退出条件

ADR 被接受，GitHub #2 与仓库文档使用同一 D0–D7 切分，D1 黑盒验收无需再选
进程模型或 bridge 方向。

### 主要风险

最大风险是在试验前冻结过多细节。D0 因此只冻结责任与正确性边界；可以不改变
这些边界的 wire encoding、批大小和具体 public API 在后续阶段确定。

## D1：Stock controllability spike

本节记录 D1 完成时的历史实现与验收。D2 开始后，仍有价值的 PostgreSQL 黑盒被迁移为只调用
`dogpaddle-debezium` 公共 API 的 fixture；原 D1 Java bridge 与 JNI runtime 已删除，历史证据保留在
D1 report 和 Git 中，不再形成第二套产品实现。

### 边界

D1 被接受时是隔离在 `experiments/debezium-d1/` 中的可行性试验，不依赖 DogPaddle Change、Store、
Operation 或 Flow，不进入产品 crate。它使用 `snapshot.mode=no_data`；缺省使用
`MemoryOffsetBackingStore`，也可为黑盒试验显式透传 `FileOffsetBackingStore` 配置。两者
都只用来证明 stock Engine 可控，**不声明 DogPaddle 进程重启恢复**。

### 历史交付

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
  可行性观测，并不是 D2 所需的完整 opaque checkpoint；
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
- ACK 前 PostgreSQL `confirmed_flush_lsn` 与标准 file offset bytes 都不变；ACK 允许 offset
  commit，随后一次 poll/stop 才使 PostgreSQL connector 执行已安排的 LSN flush，黑盒最终观察到二者推进；
  fresh Engine + 同一 persistent slot 的组合 restart witness 跳过已 ACK 记录，但不宣称 file
  store 被单独隔离证明，更不宣称捕获的 JSON offset 能注入新 Engine；
- `max_bytes`、单槽有界交付和 `stop` deadline 都可黑盒触发，失败不变成 hang；
- Java exception、危险配置和 connector class 加载失败可转换为稳定状态/错误，不越过 JNI 边界崩溃；
- 同一 OS 进程 ID 与 JVM identity 可观察，并在 fresh Engine handle 间保持不变。

### 退出条件

在一个文档化的支持平台上，从干净 checkout 可一条命令构建并重复全部黑盒验收；
已记录准确 JDK、Debezium、JNI、Maven 与 PostgreSQL 版本。若只能通过 fork Debezium、
覆盖内部类或 Java→Rust callback 才实现可控 ACK，D1 失败并重开 ADR，不进入 D2。

### 主要风险

- HotSpot 创建、线程 attach/detach、class path 与 native library 差异；
- Engine callback 与 Rust poll/stop 之间的死锁；
- Debezium 批次语义无法支持一个 outstanding 和延迟 ACK；
- delivery 大于 `max_bytes` 时的前进规则不明；
- D1 JSON 会合并某些 Java 数值运行类型，不能直接升级为跨 connector 的 opaque offset codec；
- 试验误把 `MemoryOffsetBackingStore` 或显式 `FileOffsetBackingStore` 的行为当成
  DogPaddle/MDBX 恢复保证。

## D2：Product Debezium runtime 与 pre-ACK checkpoint

### 边界

D2 将 D1 的控制原型重做成独立 `dogpaddle-debezium` 产品 crate。它的 Rust API 与 Java
bridge 只依赖通用 Engine/Kafka Connect 契约，不依赖 Change、Store、Operation、Flow，也没有
PostgreSQL 代码分支；D2 的参考发行包会包含 PostgreSQL connector，作为第一个真实试点。
它不创建通用 `SourceDriver` trait，也不定义行到 Arrow 的映射或 snapshot。

D2 的目标公开调用面为：

```rust,ignore
let runtime = DebeziumRuntime::open(distribution)?;
let mut connector = runtime.start(config, resume_checkpoint.as_ref())?;
loop {
    if should_stop() {
        break;
    }
    let Some(delivery) = connector.poll(timeout)? else {
        continue;
    };
    persist_atomically(delivery.records(), delivery.checkpoint().as_bytes())?;
    delivery.ack()?;
}
connector.stop(deadline)?;
```

`Delivery` 是借用 `&mut Connector` 控制权的线性 guard，但 records 与 checkpoint 都是 JNI
返回后完全属于 Rust 的 owned allocations。Drop 绝不自动 ACK；`ack(self)` 消耗 capability。
handle、token、status、JNI、Java object 和 classpath 列表全部是私有实现。

### 交付

- 进程级单例 JVM：相同 canonical path 与已验证内容指纹才复用，路径或内容变化显式冲突；
- connector-neutral Java bridge，只有一个 outstanding batch，无 Java→Rust callback；
- `ConnectorConfig` 只做 secret-safe properties 容器，runtime 强制单 task、ordered、always
  commit、自有 offset store，并拒绝 SMT/predicate 与调用方 offset store；
- 自有进程内 `OffsetBackingStore`，能从完整 checkpoint 初始化，不读写 Java offset 文件；
- ACK 前用 Kafka Connect 4.3.0 的 public `OffsetStorageWriter` 对本批每条
  `SourceRecord.sourcePartition/sourceOffset` 做无副作用 preview；
- 将 preview raw delta 与完整 accepted raw map 合并，生成绑定 Engine name 与 connector class、
  key-sorted、带 framing/checksum 的 versioned checkpoint；
- ACK 后才在原 handler thread 调真实 `markProcessed` 与 `markBatchFinished`，并要求 actual raw
  store image 与 preview checkpoint 完全相同；
- ACK success 精确表示 handler 已 settle 且 backing-store image 已匹配，不冒充 connector-specific
  外部 progress 已同步发布；健康的 PostgreSQL task 预期在后续 poll/stop 执行已安排的 flush，
  真实 fixture 只把 eventual observation 当运维证据，绝不把它纳入 ACK success；
- versioned、bounded binary delivery wire，记录原始 SourceRecord 顺序，并以 schemas-enabled
  Kafka Connect JSON bytes 表达 key/value/header data；
- start failure 清理、stop deadline、outstanding abort、显式 dispose，以及私有诊断；
- Maven/JDK 独立构建出的 `distribution/lib/*.jar`；普通 Cargo gate 不调用 Maven、不联网、
  编译时不要求 JDK；
- D1 PostgreSQL fixture 改成只通过本 crate public API 驱动真实 connector。

Checkpoint 是“完整 connector offset-store image”，不是 delivery identity。一个 connector 可能
合法地让多个事件共享 position，durable ingress 不得拿 checkpoint 冒充事件 ID。MySQL 等
connector 还需要 schema history；D2 不把 offset checkpoint 夸成所有 connector 的完整状态，
第二 connector 接入时必须单独解决和证明附加 durable state。

### 验收

- Rust public API 不出现 numeric handle/token、status JSON、JNI/JVM 或 raw classpath；
- checkpoint 在 ACK 前可取，覆盖多个 source partitions，corrupt/truncated/wrong binding fail closed；
- preview delta 与真实 backing-store `set` delta 以及完整结果逐字节一致；
- checkpoint 单独初始化 fresh Engine，无 `FileOffsetBackingStore`；PostgreSQL 物理 slot 仍负责
  保留 WAL，但客户端 resume position 来自 checkpoint，而不是从 slot 状态猜测；
- Delivery Drop 后重 poll 得到相同 records/checkpoint；stop outstanding 不 ACK；
- 两个 handle 共享 JVM，但 config、queue、offset、failure、stop 与 dispose 相互隔离；
- startup failure/timeout、running stop timeout、重复 stop 与 reclaim 有组件证据；
- D1 的真实 PostgreSQL 顺序、ACK 前 LSN 不推进、ACK 后 eventual LSN、unacked replay 和
  restart gate 改走 product API；
- `cargo xtask check` 在没有 Java artifact 的普通 Rust 环境保持通过；Java/PG gate 显式运行。

### 退出条件

从干净 checkout 可以分别运行普通 Rust gate 与 pinned Java/真实 PostgreSQL gate。fresh Engine
只靠调用方保存的 pre-ACK checkpoint 恢复，不存在 Java offset 文件；D1 不再拥有第二份 JVM/JNI
host runtime、bridge、delivery codec 或生命周期实现，它只保留调用公共 API 的黑盒 CLI。未达到
checkpoint 独立恢复前，只能称 D2 foundation，不能进入 D3。

### 主要风险

- `OffsetStorageWriter` 是 Kafka Connect public class，但属于精确版本绑定的 runtime API；升级
  必须重审 preview/actual 等价性，不能承诺跨任意 Connect 版本恢复；
- HotSpot 通常不能在同一进程 destroy/recreate，首次初始化必须在启动 JVM 前完成 distribution 校验；
- JVM fatal error 会终止 Rust 宿主，没有 sidecar 隔离；
- class-loader、日志、TLS/native library、发布体积、JDK 与多平台成为产品负担；
- Debezium startup phase 的 shutdown 行为复杂，不能把 D1 的一次性 worker 原样升级；
- schemas-enabled JSON 仍是 Connect record 表达的版本边界，但不承担 checkpoint 语义。
- Debezium 的 generic task commit failure 可能被 Engine 内部转成非抛出结果；backing-store
  checkpoint 仍可精确验证，但 connector-specific external progress 必须独立监控和验收。

## D3：Generic durable ingress

### 边界

D3 只改造 DogPaddle 的持久输入边界，使用纯 Rust scripted delivery producer 先验证，再连接 D2。JNI、JDK、
Debezium、connector config 和外部 I/O 都不进入 Operation turn，也不进入 Flow build/open。

### 交付

- 一个固定 exact output Schema、走普通 `turn(None)` 的 `IngressSourceDefinition`；
- 窄 `Flow::ingest` 与 resume-state 读取 API；
- 一个 versioned state cell，原子保存 accepted opaque checkpoint、最小重复接纳凭据以及至多
  一个 canonical pending Change；
- `Accepted`、精确 `Duplicate`、`Backpressured` 与明确 conflict/error；
- pending clear 与 Station output append 的同事务组合；
- Flow 外的薄 coordinator，按 `poll → convert → ingest commit → Delivery::ack` 执行，且 JNI
  调用期间没有 active MDBX transaction。

一个 delivery 可以产生一个非空 `Change`，也可只推进 checkpoint；后者用于 heartbeat 或被
Rust adapter 忽略的 source record，不伪造空 Change。v1 只允许一个 durable pending slot。

在 D2 的线性 Delivery 与“ACK 不确定即从已持久 checkpoint 重启”语义下，D3 必须重新证明
到底需要保存多少 delivery receipt；不能沿用 JVM token，也不能假定 checkpoint 对每批唯一。

### 验收

- exact Schema mismatch、超限与 codec 错误在写事务前失败，无 durable side effect；
- accepted checkpoint、最小 receipt 与 optional pending payload 同一写事务提交；
- 只有 `Accepted` 或经精确验证的 `Duplicate` 才允许 ACK；Backpressured/error 不 ACK；
- IngressSource 通过普通 `turn(None)` 输出；pending clear 与 output append 同事务；
- output capacity、Schema guard、Operation error 和 commit failure 都保留 pending；
- ingest commit 后、output 前 reopen 仍输出一次；output commit 后 reopen 不再重复；
- checkpoint-only delivery 只推进 resume state；
- build/open 保持纯 binding、失败无目录副作用，definition/state/resource layout 有 golden；
- scripted delivery producer 覆盖每个 crash/commit 窗口，随后 D2 runtime 运行相同端到端故障矩阵。

### 退出条件

`dogpaddle-flow` 的单一 public correctness target 证明全部行为。Store 和 Change 不含 connector
知识，Station claim/cursor/output-retention 契约不变；bridge/connector 永远看不到 Store handle
或 transaction starter。

### 主要风险

- 为尚不存在的第二 connector adapter 提前创建 trait、registry、generation 或 lease；
- 用不唯一的 checkpoint 代替 delivery receipt；
- 在 shared `Cell<Vec<u8>>` 中无上限保存 batch；
- 为“少一次写”而直接写 Station output，绕开 Operation 与 retention；
- 在 MDBX transaction 内跨 JNI ACK，制造不可恢复的外部副作用。

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
- snapshot row 到 `+1` Change 的有界分批，与 D3 ingress 使用同一背压和 ACK 原则；
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
- 对 D1/D2 bridge/runtime、D3 ingress、connector lifecycle、opaque offset 与错误模型的原样复用证据；
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
