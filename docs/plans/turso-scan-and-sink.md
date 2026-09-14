# Turso Scan、Sink 内核与后续实现交接

状态：WIP；当前停在通用 durable buffered Sink 内核骨架

日期：2026-09-14

## 回家后的最短续接路径

本次交接分支是 `codex/turso-sink-kernel-wip`。交接前的 `main` 基线是
`08076b2dc049f51946f541411d8bd39c600c49bc`。包含本文的提交就是唯一需要续接的 WIP 快照。

已有仓库时：

```bash
git fetch origin
git switch --track origin/codex/turso-sink-kernel-wip
```

如果本地已经存在同名分支：

```bash
git switch codex/turso-sink-kernel-wip
git pull --ff-only
```

然后先执行：

```bash
git status --short
git log -1 --oneline
git diff origin/main...HEAD --stat
cargo test -p dogpaddle-flow input_station_without_a_claim_can_commit_internal_work --locked
cargo test -p dogpaddle-flow complete_without_an_offered_claim_is_a_rollback_error --locked
cargo test -p dogpaddle-operation --test correctness discard:: --locked
cargo test -p dogpaddle-operation --test correctness equi_join::inner_runtime::runtime_idles_without_input --locked
```

恢复工作时可以直接给 Codex 下达：

```text
继续 docs/plans/turso-scan-and-sink.md 中暂停的工作。先核对交接分支与当前 diff，
完成通用 buffered Sink runtime 和 owner correctness；再迁移 SQLiteSink、PostgresSink；
现有 Sink 全部通过后才实现 Turso adapter 和 Turso Scan。不要重新设计第二套 Sink 协议。
```

## 目标演化与当前范围

最初问题是调研 Turso Scan 和 Sink。实现讨论随后收敛为：先把现有 SQLite/Postgres 关系 Sink 中
可复用的持久缓冲、批次、prepare/deliver/settle 和 reopen 语义收进 operation crate 内的通用 Sink
内核；用两个已有 Sink 证明抽象完整，再增加 Turso-specific adapter。这样 Turso 不会引入第三套
外部副作用协议。

当前 WIP 只覆盖两项：

1. 允许有输入的 `TurnOperation` 在暂时没有 Claim 时继续驱动持久内部工作；
2. 建立通用 durable buffered Sink 的 batch、control state 和 target adapter 骨架。

当前尚未开始 Turso-specific Sink adapter，也没有 Turso Scan 代码。不要把文件名或原任务标题误读为
这些部分已经实现。

## 必须保持的仓库边界

- Sink 仍是 `OperationKind::Sink(NonZeroU32)`，必须独占 Station，且没有 output。
- Station 不保存事务启动能力；Operation 只在调用期间获得 transaction-bound access。
- 外部副作用不能在 Store transaction 内假装原子提交。持久 prepare、外部 deliver、持久 settle 与
  reopen 必须形成明确、可重放的协议。
- input completion、Sink durable state 和本轮 Store 写入必须在同一事务提交；外部提交不确定时继续
  使用 fail-stop/reopen 边界，不能静默重试成可能重复写入。
- Definition/binding/materialize 保持单向：Definition 纯绑定并声明全部 Store data；runtime 不接收
  Store，不保存 Definition，也不能自行创建资源。
- Store 只提供通用 `Cell`、`OrderedMap` 等结构，不理解 Change、批次或目标数据库语义。
- 公共 API、持久资源名、codec 和 operation tag 都属于开发期 v1 ABI；改动时要同步 literal/layout/
  reopen evidence，不增加旧格式兼容层。
- 先迁移 SQLite/Postgres 证明共享内核，再实现 Turso；不要建立 Turso 专用 Station、runner、队列或
  第二套 lifecycle。

## 已完成：无 Claim 时的内部推进

此前 Station 对任何“有输入但当前没有 Claim”的 Operation 直接返回 `Idle`。这会阻止 buffered
Sink 在输入暂时追平后继续排空已经持久化的 buffer。

当前改动把协议收定为：

- `None` 表示本轮没有 offered Claim；Scan 始终收到 `None`，输入 Operation 在上游暂无数据时也可能
  收到 `None`；
- 有持久内部工作时，Operation 可以返回 prepared turn，并最终用 `Action::Commit` 提交内部进度；
- 没有内部工作时返回 `Turn::Idle`，Station 不开启写事务；
- `Action::Complete` 只在本轮确实提供 Claim 时合法；没有 Claim 的 `Complete` 是 rollback error；
- Atomic 和 exclusive-atomic adapter 在没有输入时统一 `Idle`；现有 Discard 和 EquiJoin 也改为同样
  行为。

相关文件：

- `crates/flow/src/station/runtime.rs`：删除无 Claim 的提前 `Idle`，按实际 Claim 校验 `Complete`；
- `crates/flow/src/station/input.rs`：删除只用于旧分支的 `is_input_free`；
- `crates/flow/src/station/protocol.rs`：错误语义改为“没有 offered input 却 Complete”；
- `crates/flow/src/station/tests/transaction.rs`：增加无 Claim 内部 commit 和非法 Complete 回滚证据；
- `crates/operation/src/operation/mod.rs`：更新 `TurnOperation`/`Action` 合同，Atomic adapters 无输入时
  `Idle`；
- `crates/operation/src/operation/sink/discard.rs` 与 correctness：无输入时 `Idle`；
- `crates/operation/src/operation/transform/equi_join/` 与 correctness：无输入时 `Idle`；
- `crates/flow/README.md`、`crates/operation/README.md`：同步公共协议说明。

这里没有改变 Claim 的 durable identity：Subscription position 仍是唯一持久 input identity；内存
Claim 仍只是可丢弃副本。无 Claim 内部推进也不能 acknowledge、拆分或伪造输入。

## 已落盘但未接入：通用 buffered Sink 骨架

新目录：`crates/operation/src/operation/sink/buffered/`。

### `mod.rs`

草拟了 crate-private `SinkTarget` adapter：

- `require_absent` / `initialize` / `initial_checkpoint`：目标所有权与首次初始化；
- `prepare`：用完整 Change、旧 checkpoint 和稳定 batch ID 产生新 checkpoint 与可持久化 plan；
- `deliver`：执行目标侧副作用；
- checkpoint/plan 的稳定 encode/decode：让 prepared work 可在 reopen 后恢复；
- `MAX_BATCH_EVENTS`：目标后端的安全批次上限。

共享数据声明暂定为：

```text
sink.control: Cell<Vec<u8>>
sink.buffer: OrderedMap<u64, Vec<u8>>
```

名称是否能直接替换 SQLite/Postgres 当前资源布局，必须在迁移时结合它们的 v1 golden/layout tests
确认；不要仅因骨架已有常量就跳过 ABI 审查。

### `state.rs`

当前 control codec 版本为 `1`，状态机有三相：

```text
Initialize
Ready { buffer, next_batch_id, checkpoint }
Prepared { before, after, batch_id, checkpoint, plan }
```

`BufferState` 保存：

- `head: Option<Position>`；
- `tail`；
- `pending_events`；
- `retained_bytes`。

`Position` 保存 sequence、row index 和当前 diff 尚未交付的绝对 event 数。`Prepared` 同时保存
settlement 前后的 buffer 状态，codec decode 会检查版本、截断、trailing bytes、batch ID exhaustion
和 settlement 单调性。

### `batch.rs`

当前草稿按 diff 的绝对值计算 event 数，不把一行 `diff = 100` 错当成一个 event。`load` 可以在
row/diff 中间切分批次，保持正负符号；读取时校验完整 Change Schema、buffer position、缺失 entry、
计数 overflow/underflow 和 retained-byte 状态，并用 Arrow batch concat 构造一个有界交付 Change。

当前 byte accounting 把 `u64` map key bytes 与编码 Change bytes 计入 retained bytes。这个口径必须与
runtime admission、测试和 README 完全一致。

## 当前明确未完成的部分

1. `buffered/mod.rs` 已声明 `mod runtime;`，但 `runtime.rs` 尚不存在。
2. `crates/operation/src/operation/sink/mod.rs` 尚未声明 `mod buffered;`，因此三个新文件目前不会进入
   正常编译；普通 `cargo check` 即使通过，也不能证明这份骨架能编译。
3. 尚未定义 `BufferedSink<T>` 的构造、materialize 输入、Store handles、内存 target 生命周期和
   `TurnOperation` 实现。
4. 尚未实现完整状态转换：初始化、input admission、无 Claim drain、prepare 持久化、AfterCommit
   deliver、成功 settle、失败/reopen，以及 target commit 不确定的 fail-stop 处理。
5. 尚未决定用户 batch preference、retained-byte capacity 和 pacing 参数由哪个 Definition payload
   持久化，以及怎样与 `MAX_BATCH_EVENTS` 组合。
6. 尚未把已交付的 buffer entries 从 `OrderedMap` 删除；settlement 后的删除、head/tail 回收和
   retained-byte accounting 必须在同一事务中证明。
7. `batch.rs` 仍有临时的 `Arc`/`_schema_clone` dead-code 占位，应在模块接入并格式化时清理，不能作为
   最终实现保留。
8. 尚无 buffered owner correctness：codec golden、corrupt state、batch slicing、mixed diff、容量、
   backpressure、commit rollback、deliver failure、reopen、prepared recovery、重复交付边界都未覆盖。
9. SQLiteSink 和 PostgresSink 仍运行在旧的 `sink/relation` 共享实现上，尚未迁移。
10. Turso connection/config、Definition tag/payload、runtime resource、目标 ownership、SQL endpoint、
    identity 和 system/correctness tests 均未实现。
11. Turso Scan 尚未形成代码或已批准的持久 checkpoint/分页/一致性合同，应在 Sink 内核迁移稳定后
    单独收定，不能把 Sink buffer 状态复用于 Scan。

## 建议实施顺序

### S1：先完成并封闭通用 runtime

1. 阅读现有 `sink/relation/runtime.rs`、`state.rs`、`plan.rs`，逐项映射当前已经证明的 prepare、
   AfterCommit、settle 与 reopen 行为；只提取两个现有 Sink 都需要的机制。
2. 实现 `buffered/runtime.rs`，接入 `sink/mod.rs`，立即清理 dead code 并运行 rustfmt。
3. 给 buffered 模块写 crate-owner 单元/correctness evidence；先用一个内存 fake target 覆盖状态机，
   不用真实数据库阻塞核心协议。
4. 明确每个 turn 的动作：
   - 有 Claim：把完整输入编码并 admission 到 durable buffer，与 `Complete(None)` 同事务提交；
   - 无 Claim 且 Ready buffer 非空：加载有界 batch，持久化 Prepared，并用 `Commit(None)`；
   - Prepared：根据既定协议恢复或执行 deliver，只有确认目标结果后才能 settle；
   - 无 Claim 且 Ready buffer 空：`Idle`。
5. 对 external deliver 的 exactly-once/at-least-once 边界必须沿用现有关系 Sink 已有的 target-side
   batch identity/transaction 证据，不能只靠内存 flag。

### S2：迁移现有 Sink，删除旧共享内核

1. 先迁移 SQLiteSink，保留现有 row identity、16-byte `row_hash` ABI、目标所有权和 reopen evidence。
2. 再迁移 PostgresSink，保留目标 schema/ownership、事务和错误边界。
3. 两者通过后删除被完全替代的 `sink/relation` runtime/state/plan；不要保留兼容 wrapper 或两条正常
   路径。
4. 同步各自 Definition data declarations、materialize、literal/resource-layout golden、README 和
   benchmark/correctness owner。

### S3：实现 Turso Sink adapter

1. 先根据 Turso/libSQL Rust 客户端的当前 API 明确本地/远端连接方式、事务、批量参数、错误类别和
   retry/idempotency 能力；依赖版本必须精确 pin 并记录 feature/TLS 选择。
2. 给 Turso 分配唯一稳定 operation tag；Definition 只持久化会改变语义或持久布局的值，凭据和临时
   runtime 参数不得进入 payload。
3. adapter 只实现 target-specific ownership、checkpoint/plan codec 和 delivery；buffering、批次与
   settlement 必须复用通用内核。
4. 先补 operation correctness，再决定是否向 SQL v1 增加 endpoint；SQL identity、env snapshot、
   canonical state path 和 secret 排除规则必须与现有数据库 endpoint 一致。

### S4：单独设计 Turso Scan

Turso Scan 不属于 buffered Sink 内核。开始前必须明确它是一次性 snapshot scan、可恢复分页 scan，
还是某种持续 change source；三者的 checkpoint、Schema discovery、外部一致性和 reopen 语义不同。
在这些问题有明确答案前，不要创建空泛的通用 Scan adapter。

至少要收定：

- exact logical Arrow Schema 从何而来、何时绑定、open 时怎样稳定重建；
- 分页顺序和稳定 tie-breaker，是否要求用户提供主键；
- checkpoint 是 row key、page token、数据库 snapshot identity 还是其他值；
- snapshot 期间源数据变化时的一致性承诺；
- 外部请求、Store checkpoint、output append 与 AfterCommit 的重放边界；
- connection secret 与稳定 endpoint identity 的分离；
- cancellation、timeout、retry 和错误是否要求 reopen。

## 验证清单

交接时已经执行 `git diff --check`，通过。随后启动了
`cargo test -p dogpaddle-flow input_station_without_a_claim_can_commit_internal_work --locked`；这是当前
机器的冷构建，命令仍在编译 DataFusion 等依赖时为尽快完成交接而中止，退出码为 `130`，中止前没有
出现项目源码编译错误或测试失败。当前不能据此声称测试通过，恢复后必须重新运行以下命令。另需注意，
`buffered/` 尚未接入模块树，现阶段即使普通 workspace check 通过也不会验证三个草稿文件。

恢复实现后按风险从窄到宽执行：

```bash
cargo fmt --all -- --check
cargo test -p dogpaddle-flow --locked
cargo test -p dogpaddle-operation --test correctness --locked
cargo test -p dogpaddle-sql --test correctness --locked
cargo clippy -p dogpaddle-flow -p dogpaddle-operation --all-targets -- -D warnings
cargo test --workspace --locked
cargo xtask check
```

新 `buffered/` 文件在接入模块树之前还应显式执行一次 rustfmt/check，避免未参与 Cargo module
discovery 的草稿被漏掉。最终 gate 必须使用根 `Cargo.toml` 指定的 Rust 1.96。

最低新增证据：

- 无 Claim：Idle、内部 Commit、非法 Complete rollback；
- input admission 与 Subscription acknowledgement 原子提交；
- mixed positive/negative diff 的 event-budget slicing；
- 单个绝对 diff 跨多个 batch；
- retained-byte/event overflow、ItemTooLarge/容量和 corrupt entry 不产生半页；
- prepare transaction rollback 和 backpressure 不遗留 Prepared；
- deliver 失败、成功、结果不确定、settle commit failure、reopen Prepared；
- batch ID exhaustion、control codec golden/truncation/trailing bytes；
- SQLite/Postgres 迁移前后结果、target ownership、reopen 和 16-byte row hash 完全一致；
- Turso-specific adapter 的 target-side idempotency/transaction 证据；
- 若增加 Turso Scan，再单独覆盖分页 checkpoint 与源侧一致性。

## 已知风险与恢复时先检查的问题

- 当前骨架先写了状态形状，runtime 尚未反向验证它是否是最小充分状态；不要因为已写 codec 就拒绝
  必要的 v1 破坏性调整。
- `Prepared` 中保存 before/after、checkpoint 和 plan，但 external delivery 与 settle 的准确顺序必须
  从现有 relation Sink 证据迁移，不能凭名字猜测。
- `pending_events` 使用 diff magnitude，而 Arrow row count 是物理行数；所有批次和容量测试必须明确
  区分二者。
- `OrderedMap<u64, Vec<u8>>` 的逻辑 byte 口径必须与 Store admission 口径一致；不要把 RocksDB 物理
  压缩大小或内部 key bytes暴露成公共合同。
- 如果目标限制按 SQL parameter、encoded bytes 或 rows 而非 relation events 计数，
  `MAX_BATCH_EVENTS` 可能不够，应由真实 SQLite/Postgres/Turso adapter 证据推动调整。
- 本交接没有声称 Turso Rust SDK 的具体 API、事务或 CDC 能力；恢复时必须用当前官方资料和精确依赖
  版本重新确认，不能依赖记忆。

## 完成定义

这一轮只有在以下条件全部满足时才算完成：

- 通用 buffered Sink runtime、state、batch 与 tests 完整接入并通过 owner checks；
- SQLiteSink、PostgresSink 都迁移到唯一共享内核，旧 relation 内核完全删除；
- Operation/Flow/SQL 文档与持久 golden/layout 同步；
- workspace gate 通过；
- Turso Sink 使用相同内核并有自己的稳定 Definition、adapter 和 evidence；
- Turso Scan 的产品语义明确后独立实现，不借用或复制 Sink 状态机。
