# DogPaddle 测试协议

测试的目标不是覆盖实现行，而是用最少的独立证据锁住公共语义、持久格式、事务边界和性能口径。
同一行为只保留一个最强所有者；测试辅助代码不得发展成第二套产品模型。

## 四条证据轨道

| 轨道 | 回答的问题 | 主要手段 |
| --- | --- | --- |
| 公共契约 | 调用者能否观察到承诺的行为与错误？ | `tests/correctness`、真实公共 API、失败后状态断言 |
| 模型与变形 | 大量组合是否符合一个独立、可读的 oracle？ | 固定 seed、小模型、稳定重批、穷举小图 |
| 格式与损坏 | 稳定字节能否互操作、重开并安全拒绝损坏？ | golden/raw layout、标准 reader、truncation、no-panic |
| 事务与恢复 | commit、rollback、poison、进程死亡后是否只出现全旧或全新？ | 真实 MDBX、reopen、Store 层 SIGKILL、Flow 阶段恢复 |

性能测试是第五条独立轨道：它只测已经由 correctness 证明有效的 workload，不替代任何行为断言。

## 所有权

- `src/**/tests.rs` 只保存必须访问私有状态的不变量、错误注入和独立算法 oracle。能用公共 API
  证明的行为必须放到公共 target。
- 每个产品 crate 只有一个显式公共 target：`tests/correctness.rs`，领域文件位于
  `tests/correctness/`。
- Operation + Store 的组合由 Operation 拥有；Flow + Operation + Change + Store 的组合由 Flow
  拥有。
- Change 与 Store 是刻意独立的 sibling；它们唯一的外部接缝位于不可发布的
  `integration-tests/change-store/`。
- benchmark fixture、oracle 与计时边界归 workload 所有者。共享
  `test-support/bench-protocol/` 只拥有 profile、运行目录、主机指纹、typed JSONL、统计和完成记录。

产品 manifest 关闭自动 test/bench 发现，显式声明 target；产品 library 设置 `bench = false`。
测试不得为了注入 fixture 扩张产品 API，也不得引入万能 Store trait、persona DSL 或跨 crate test-utils。

## 当前矩阵

| 所有者 | 公共正确性重点 | benchmark target |
| --- | --- | --- |
| Change | Schema、Change、Projection、IPC golden/interop/malformed | `change_core`、`change_codec` |
| Store | capability、事务、布局、集合、分页、容量、SIGKILL | `cell`、`ordered_map`、`append_log`、`append_log_endurance` |
| Operation | Definition codec/materialize、状态 commit/rollback/reopen、重批 | `operation_core` |
| Flow | build/open、拓扑、Claim 重放、Commit/Complete、背压、reclaim、腐败状态 | `flow_lifecycle`、`flow_runtime` |
| Change + Store | full/projected owned entry decode、decode poison 后 forwarding/cursor 回滚 | `change_append_log` |

“不适用”不通过空 target 表示：没有独立长稳状态的 crate 不建立 endurance target。

## Change + Store 的最小接缝

组合包只保留两个不能由 Change 与 Store 各自的证明组合推出的 witness：

1. nested、variable-width、非零 Arrow slice offset 的 full/projected decode 在 entry transaction
   结束后仍然 owned；
2. corrupt Change decode poison 同一 Store transaction，并回滚已经发生的 forwarding/cursor 写入。

稳定重批属于 Change/Operation；AppendLog paging、计费、truncate、reopen、crash 和 endurance
属于 Store。组合包不得重建这些矩阵。

## 持久化变更的最低证据

每个稳定协议至少需要：

1. 写侧 golden 或独立 raw layout 断言；
2. 读侧 decode/open/reopen；
3. 一个不复用生产算法的语义或互操作 oracle；
4. malformed/corruption 拒绝且无 panic、无部分写入；
5. 精确资源名、类型、size/codec 与失败后状态。

Store 层用一组多对象 transaction、drop/poison、snapshot、read-your-writes 与 SIGKILL 测试证明物理
原子性。上层只重复自己的协议阶段、对象组合和 reopen 义务，不为每个 crate 复制 SIGKILL harness。

## Benchmark 协议

全部 10 个 target 是独立 release 进程，只有两个统一设置：

- `DOGPADDLE_BENCH_PROFILE=smoke|reference`：选择 owner 内固定、不可拼出非法组合的规模。
- `DOGPADDLE_BENCH_ROOT=/absolute/path`：reference 的固定文件系统根；smoke 默认使用临时目录。

不接受逐维环境变量。fixture、seed、预热、结果 oracle 和文件清理必须位于计时外。普通测试禁止
wall-clock 断言。持久化 reference 必须报告实际文件系统路径；所有 target 报告 rustc、OS/kernel、
CPU、Cargo profile、git revision/dirty state 和实际配置。

stdout 的 machine records 使用 typed JSONL。每个 target 必须依次产生唯一 environment、唯一
configuration、带稳定 `series` 的原始 `sample | observation`，并以唯一 `completion` 结束；completion
后不得再有 machine record。configuration 用 `expected_samples` 声明精确的 duration sample 总数，
并可用 `required_observations: {series: count}` 声明每条非 duration observation series 的精确非零数量。
通用 gate 分别验证 sample 总数与每条 sample/observation series 从零连续且唯一的 index，拒绝未声明、
缺失、重复或额外 observation；它不解析 owner payload，也不接受第三种 data discriminator。
`cargo xtask bench-smoke` 从 Cargo metadata 自动发现全部 target，逐进程执行，拒绝无输出、畸形 JSON、
错误 benchmark identity、退役的派生 summary record、缺失或非末尾 completion；新增 benchmark 不需要
维护第二份 target 清单。

常规 benchmark 的 machine stream 只保留原始样本，进程内人类表格报告派生统计；配对样本以
`pair + side + sample index` 无损表达一一对应关系。Operation 的 per-operation 耗时由 `elapsed_ns / operations`
派生；Flow runtime 的总耗时、速率和 p50/p95 由 sample `elapsed_ns`、work counts 与
`round_latencies_ns` 派生，不重复写回 machine fields。AppendLog 长稳的 append/truncate 事务也使用标准
SampleRecord，并按 record width 和动作拆分稳定 series；checkpoint 与 terminal 使用同一个固定
ObservationRecord，其中 checkpoint series 保留状态与文件大小，terminal series 只补充 raw samples
无法导出的 wall elapsed 和最终 reopen checksum。两者都由 `required_observations` 提供精确数量证据。
p50/p95/p99/max、吞吐、peak 和 tail spread 均从这些 raw facts 派生并显示在人类 summary。正式前后
对比只能使用相同代码协议版本、rustc、机器、profile、文件系统和 workload；不设置机器相关的 CI
wall-clock 阈值。

## Canonical gates

本地和 pinned MSRV CI：

```bash
cargo xtask check
cargo xtask bench-smoke
```

`cargo xtask check` 依次运行：

```bash
cargo fmt --all -- --check
cargo test --workspace --locked
cargo test --workspace --release --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

CI 另用最新 stable 运行一次 debug workspace tests，证明前向兼容。常用定向命令：

```bash
cargo test -p dogpaddle-store --test correctness transaction::
cargo test -p dogpaddle-flow --test correctness runtime_corruption::
cargo test -p dogpaddle-change-store-integration
cargo bench -p dogpaddle-flow --bench flow_runtime
```

## 新增或删除测试

新增测试前依次问：

1. 它锁住了哪个尚无证据的公共承诺或故障边界？
2. 最强所有者是谁？能否扩展现有 fixture/table/property，而不是新建 framework？
3. expected 是否独立于被测实现？失败时能否定位到一个契约？
4. 如果它只是更弱测试的重复，是否应替换旧测试而不是叠加？

代码重构后，若公共 correctness 已经更强地蕴含某个白盒 witness，应删除白盒版本。golden、独立
model、malformed/no-panic、真实 reopen/crash 和 compile-fail capability 证据不能仅为减少数量而删除。
