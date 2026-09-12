# dogpaddle-debezium

`dogpaddle-debezium` 把标准 Debezium Engine 嵌进当前 Rust 进程，并把它缩成一个很小的拉取接口：

```text
start → poll → 持久化 records + checkpoint → ack → poll → … → stop
```

这个 crate 只负责 Debezium、Kafka Connect records 和 offset。它不知道 Arrow、`Change`、Store、Operation
或 Flow，也不把 PostgreSQL LSN、MySQL binlog position 等 connector 私有位置暴露给上层。

## 一批数据如何经过它

```text
数据库变化
    ↓
Debezium connector（嵌入 JVM）
    ↓ poll
Delivery { records, checkpoint }
    ↓ 调用方先把两者一起持久化
ack
```

最小公共用法如下：

```rust,no_run
use std::time::Duration;

use dogpaddle_debezium::{ConnectorConfig, DebeziumRuntime};

# fn persist_atomically(_: &[dogpaddle_debezium::Record], _: &[u8]) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
# fn should_stop() -> bool { true }
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let runtime = DebeziumRuntime::open("/opt/dogpaddle-debezium")?;
// 这里只演示控制流；真实 PostgreSQL connector 还需要数据库、表、slot 等属性。
let config = ConnectorConfig::new(
    "orders",
    "io.debezium.connector.postgresql.PostgresConnector",
)?
.property("database.hostname", "127.0.0.1")?
.property("database.user", "cdc")?;

let mut connector = runtime.start(config, None)?;
loop {
    if should_stop() {
        break;
    }
    let Some(delivery) = connector.poll(Duration::from_secs(1))? else {
        continue;
    };
    persist_atomically(delivery.records(), delivery.checkpoint().as_bytes())?;
    delivery.ack()?;
}
connector.stop(Duration::from_secs(30))?;
# Ok(())
# }
```

这里最重要的顺序是：**先持久化，再 ACK。** 如果进程在两者之间退出，重启时把已经持久化的
`Checkpoint` 传回 `start`，即可从这批数据之后继续。

恢复 API 本身只有四步：

```rust,no_run
use dogpaddle_debezium::{Checkpoint, ConnectorConfig, DebeziumRuntime};

# fn resume(runtime: &DebeziumRuntime, saved: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
let checkpoint = Checkpoint::from_bytes(saved)?;
let config = ConnectorConfig::new(
    "orders",
    "io.debezium.connector.postgresql.PostgresConnector",
)?;
// config 还要补回与首次启动一致的 connector properties。
let _connector = runtime.start(config, Some(&checkpoint))?;
# Ok(())
# }
```

## 五个核心控制类型

| 类型 | 白话含义 |
| --- | --- |
| `DebeziumRuntime` | 已验证 runtime bundle 和进程内 JVM 的共享入口 |
| `ConnectorConfig` | engine 名、connector class 和 Debezium properties |
| `Connector` | 一个正在运行、顺序使用的 connector |
| `Delivery` | 当前唯一一批尚未确认的数据 |
| `Checkpoint` | 接受当前批次后完整 offset store 的不透明快照 |

`Record` 是 owned Rust 数据，保留 topic、Kafka partition、时间戳、key、value 和有序 headers。key、value
与 `Header` value 使用 Kafka Connect schemas-enabled JSON bytes；Rust 层不解释 connector payload。
另外两个公共类型 `Error` / `ErrorKind` 提供不会回显 property value 的稳定错误分类。

### 为什么 Delivery 借用 Connector

`Delivery<'_>` 活着时独占借用 `Connector`，所以 Rust 类型系统直接阻止以下情况：

- 同时 poll 第二批；
- 一边持有当前批次一边 stop；
- 通过另一个 token 重复 ACK。

`ack(self)` 消费 Delivery。直接丢弃 Delivery 不会 ACK；同一个 connector 的下一次 poll 会返回相同的
outstanding bytes。这里没有额外 delivery ID 或 delivery token，因为 `Delivery` 本身就是唯一确认权。

ACK 结果不确定时，Connector 会被标记为不可继续使用。调用方应 stop，并从 ACK 前已经持久化的 checkpoint
重新 start，而不是猜这次 ACK 是否成功。

## Checkpoint 为什么在 ACK 前产生

Debezium 通常在处理完成时才写 offset，但 DogPaddle 必须先把“数据”和“读到哪里”放进自己的事务。
Java bridge 因此新建一个 `OffsetStorageWriter`、只捕获写入的内存 store 和同规则的 Kafka Connect
`JsonConverter`，预演当前批次会产生的原始 offset delta：

1. 从上一次已接受的完整 offset map 开始。
2. 合并当前 records 的 partition/offset。
3. 返回候选完整 map，编码成 `Checkpoint`。
4. 调用方持久化 records 与 checkpoint。
5. `ack()` 先把预演 delta 设为本次期望值，再运行真正的 Debezium committer；实际 backing store 收到写入后，
   按原始字节核对 delta 和最终完整 checkpoint 都完全一致。

Checkpoint 绑定稳定的 engine name 和 connector class，可能包含多个 source partition。它不是 delivery ID，
也不是某一种数据库位置。这个绑定不检查其余 connector properties 是否兼容；具体 source identity、固定 Schema
和 schema-history 责任仍由上层 Operation 保证。恢复时只读取显式传入的 checkpoint，不使用 Java offset 文件。

ACK 成功表示 Engine handler 和 offset-store image 已经结算，不保证每个 connector 的外部进度标记立刻可见。
例如 Debezium PostgreSQL 的 `confirmed_flush_lsn` 可能在后续 poll 或 stop 才推进；这是 WAL 保留和监控问题，
不改变“records + checkpoint 先持久化”的正确性边界。

## Runtime bundle 与 JVM

`DebeziumRuntime::open` 只接受 DogPaddle 构建的、平台对应的 runtime payload：

```text
dogpaddle-debezium-runtime-<target>/
├── MANIFEST
├── runtime-sbom.json
├── TEMURIN-NOTICE.md
├── runtime/              # 固定版本的 Eclipse Temurin JRE
└── debezium/             # bridge、connector 和依赖 JAR
```

`open` 校验 target、Temurin release、必要运行文件、JAR 清单与 hash，并通过 bundle 内的绝对路径加载
`libjvm`。它不会搜索 `PATH`、`JAVA_HOME`、`JDK_HOME` 或系统 Java。

一个进程最多只有一个 HotSpot JVM。再次打开同一个 canonical bundle path 会复用它；尝试打开另一个 bundle
会明确失败。`DebeziumRuntime` 被丢弃不会卸载 JVM：进程级 `OnceLock` 会让它存活到进程结束，也不能重新配置。
DogPaddle 必须是进程内第一个且唯一的 JVM initializer；JVM 启动后的 bridge/runtime 校验失败通常也需要重启进程。
bundle 必须在整个进程生命周期内保持不可修改，并安装在不受非信任用户写入的位置。

支持的 payload target：

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

Linux 目标要求 GNU/glibc，不支持 musl 或 Alpine。macOS archive 当前是未签名的开发产物；发布签名、
notarization 和完整 native dependency closure 不属于这个 crate 当前的交付承诺。

payload 只包含可复用的 Java runtime 与 Debezium distribution，不包含 DogPaddle executable 或测试 host；
最终发布包由上层打包流程组合两者。

## 运行边界

每个 `Connector` 有意保持线性：一个 task、一个 outstanding Delivery、有序 batch、没有 SMT。同一个
`DebeziumRuntime` 可以启动多个 engine name 不同的 Connector，但同名实例不能并存。

| 调用 | deadline |
| --- | --- |
| `start` | 固定 30 秒 |
| `poll` | 调用方传入 |
| `Delivery::ack` | 固定 30 秒 |
| `stop` | 调用方传入 |

这些都是同步 API。`poll` 超时返回 `Ok(None)`，表示当前没有一批可交付数据，不代表 connector 已结束。

`ConnectorConfig::max_delivery_bytes` 限制一次完成后跨 JNI 复制的编码 frame，默认 16 MiB。它不限制 JVM heap、
connector 内部队列或数据库日志占用。单个 delivery 超限是终止性 connector 错误，需要调整配置并重启。

Connector properties 中与 task、offset、commit、converter、SMT 和 DogPaddle 协议相关的 key 由 runtime 保留，
调用方不能覆盖。错误只包含 runtime 控制的上下文，不回显 property value，避免泄露密码。

应显式调用 `Connector::stop` 做确定性关闭。Drop 只请求 best-effort 后台清理，不等待 connector shutdown 和
engine name 注销完成；需要立即复用同一名字时必须先成功 stop。

## Connector 相关恢复责任

这个 crate 只保存 offset，并不声称 offset 足以恢复所有 Debezium connector。需要 schema history 或额外启动协议的
细节由具体 Operation 拥有：

- PostgreSQL 试点不需要 schema history。
- MySQL Scan 在公开数据前生成固定 seed checkpoint，普通运行以 `recovery` 模式从 seed 或更新的 checkpoint
  重建临时 `MemorySchemaHistory`。

固定 Schema、binlog/WAL 保留、初始快照、通知记录和重置策略都属于
[`dogpaddle-operation`](../operation/README.md)，不进入这个 connector-neutral API。

## 读代码的顺序

1. [`src/connector.rs`](src/connector.rs)：`Connector`、`Delivery` 和 ACK 所有权。
2. [`src/checkpoint.rs`](src/checkpoint.rs)：checkpoint framing、绑定和校验。
3. [`src/config.rs`](src/config.rs)：配置及 runtime 保留项。
4. [`src/jvm.rs`](src/jvm.rs)：进程 JVM、JNI 生命周期和 deadline。
5. [`bridge/src/main/java/dev/dogpaddle/debezium/`](bridge/src/main/java/dev/dogpaddle/debezium/)：offset 预演和 Java Engine bridge。
6. [`src/bundle.rs`](src/bundle.rs)：runtime payload 校验。

## 构建与验证

普通 Cargo gate 不调用 Maven、不下载 Java artifact，也不要求系统安装 JDK：

```bash
cargo test -p dogpaddle-debezium --locked
```

构建 Java distribution 需要本地 JDK 与 Maven；构建 runtime payload 还需要 `curl`、`tar`、Python 3，以及
`sha256sum` 或 `shasum`：

```bash
crates/debezium/scripts/build-distribution.sh
crates/debezium/scripts/build-runtime-bundle.sh x86_64-unknown-linux-gnu
```

runtime bundle workflow 会在四个平台上验证完整生命周期：open、start、poll、丢弃后重投、ACK、stop、
checkpoint-only restart 和下一批恢复。确定性 probe 位于 `system-tests/debezium-runtime/`；真实 PostgreSQL
恢复矩阵由独立的 `system-tests/debezium-postgres/scripts/run.sh` 拥有。完整入口见
[`TESTING.md`](../../TESTING.md)。

## 版本边界

当前固定 Debezium `3.6.2.Final`、Kafka Connect `4.3.0`、Java 17 bytecode、Eclipse Temurin JRE
`21.0.12.1+1` 和 `jni-rs` `0.22.4`。checkpoint framing、delivery wire、JNI commands、bundle layout、
offset converter 和 ACK 语义都是开发期 v1 持久或运行协议。

升级必须重新通过 preview-versus-actual、checkpoint restore、四平台 bundle 和真实 connector gate。当前不提供旧
runtime、旧 wire 或旧 checkpoint 的迁移和兼容分支；开发期 fixture 直接重建。
