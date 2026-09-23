# CDC 运行协议收敛：结构与验证记录

基线为 `f397c45104474238beb53c82f39d252253ab0bbb`。本次把 PostgreSQL/MySQL
重复的快照捕获、封口、发布、重置及 streaming 提交协议收敛到 Operation 私有
`CdcRuntime<Source>`。目标是减少需要同时维护的协议实现，不承诺吞吐提升。

## 实际减少的维护职责

| 项目 | 基线 | 本次 |
| --- | --- | --- |
| 完整 CDC 运行状态机实现 | 2 | 1 |
| 五阶段持久状态的推进主体 | 2 | 1 |
| 八步骤 turn 分派主体 | 2 | 1 |
| spool/checkpoint 与提交后 ACK 的编排主体 | 2 | 1 |
| runtime 生产文件物理行数 | 928 | 686 |

行数含空行和注释，不含两个源文件末尾的测试；新共享文件的测试模块声明计入。
旧文件分别为 488/440 行，新共享文件 425 行，两个源适配分别为 130/131 行。
约 26.1% 的生产行数减少不等于开发时间或全项目复杂度减少 26.1%。原有测试保留，
另增加了源清理失败、清理后本地回滚和两种源恢复差异的证据。

共享实现持有真实 Debezium Connector 和借用它的线性 Delivery。具体源保留连接、
记录转换、源资源清理、checkpoint 校验和错误分类，不再各自执行 Store 访问或 ACK。
没有公共 connector 框架、注册表、新的持久状态或整图事务。

## 行为与持久性边界

- 每个 reset turn 至多清理一条 spool entry，每个 publish turn 至多发布一条 Change。
- PostgreSQL 清理 slot 成功后才能持久进入 Resetting；MySQL Capturing reopen
  仍可直接在 restore 事务中进入 Resetting。
- 两个源的 checkpoint 验证差异、资源名、tag、phase 数字和 opaque checkpoint 字节保留。
- 回滚不 ACK，不提前推进 capture progress 或 streaming resume。
- 一个明确的错误恢复修正：PG 快照 IPC 编码失败现在也安排 PrepareReset，
  与捕获失败后重做完整快照的契约一致。此前该分支直接返回错误而保留 Capture。

## 本地推进 smoke

新增 owner benchmark `cdc_bootstrap` 使用同一份 harness 对照基线与本次代码。
smoke 每个样本预置 8 条各 64 行的 spool entry，计时一次 restore 加 8 次逐条处理，
共 9 次同步提交。fixture、编码与 seed、结果 oracle、teardown 不计时。
输出逐条比较完整 RecordBatch/diff，结尾校验 spool、phase 和精确 checkpoint。

本轮在 WSL/Linux、Rust 1.96 release 下运行；每 case 10 samples，20 ms warmup，
100 ms 目标采样时间，Criterion 按 fixture 的实际耗时延长采样。
初轮复核发现两个 worktree 共用 Cargo target 时实际复用了基线产物（原新版可执行文件与重编译基线 SHA256 相同），因此初轮
结果作废，不作为性能对照。后续强制分别重编译 Operation 与同一份 harness，
确认构建日志中的源码路径，再分别保存可执行文件并校验不同的 SHA256。
在无后台构建时按“旧、新、新、旧”顺序各运行两次；下面列出每轮 Criterion
时间点估计的范围，单位 ms。完整区间和原始产物保存在独立 evidence 目录。

| 场景 | 基线两轮 | 本次两轮 |
| --- | ---: | ---: |
| PG publish | 10.15–10.75 | 10.41–11.73 |
| PG reset | 9.38–10.88 | 10.38–10.83 |
| MySQL publish | 9.18–10.36 | 10.19–10.36 |
| MySQL reset | 10.21–11.59 | 9.79–10.20 |

样本间波动明显，PG publish 的点估计两轮偏高，其余场景没有同向稳定变化。
不能据此断言性能提升或严格无回归。没有新增持久状态、额外同步提交或
常驻缓存；吞吐风险仍需真实负载长期监测。

这组基准不启动外部 connector，不测 capture、poll、ACK、slot 清理或完整 Flow
输出日志，不能代表端到端 CDC 性能。

复现入口：

```sh
CARGO_BUILD_JOBS=1 DOGPADDLE_PERF_PROFILE=smoke \
  cargo bench --locked -p dogpaddle-operation --bench cdc_bootstrap
```

## 恢复证据

真实 PostgreSQL 16.15 的 `system-tests/postgres/check_cdc.py` 已通过：

- 初始快照、terminal checkpoint 提交后 ACK 前崩溃；
- 未完成私有快照被杀进程后重置 slot/spool 并完整重做；
- spool 发布回滚、streaming 背压重放；
- 2050 行来源事务跨 delivery，首个 delivery 提交后 ACK 前崩溃；
- CDC 到 PostgreSQL Sink 的 Prepared/远端已提交恢复，以及后续更新和删除；
- 运行凭据不进入持久 Store。

真实 MySQL 8.4.6 到 SQLite 的产品 SQL/CLI 场景也已通过：初始两行快照、在线
UPDATE/DELETE/INSERT、SIGINT 正常停止、停机期间 UPDATE/INSERT、使用同一 state
重开并精确追平，逐阶段核对完整结果且无重复。使用仓库锁定的 MySQL 镜像，以及
官方脚本重新构建并校验的完整 PG/MySQL runtime bundle；Java bridge 的 51 项测试通过。
此场景不注入 ACK 窗口崩溃，不将 PG 的崩溃覆盖冒充为 MySQL 的同等覆盖。

抽象与职责、正确性与事务恢复、性能与资源分别由三个未参与产品实现的 Agent
独立复审，无确认 finding。主 Agent 核实了具体结论；最后的格式修正又经正确性
审查确认不改变事务和恢复行为。

验证状态：当前代码的 `cargo test --workspace --locked` 完整通过；CDC 定向
correctness 19 项、unit 43 项通过；`cargo clippy --workspace --all-targets --locked
-- -D warnings` 在 Clippy 修正后通过。真实 PG 系统门禁、MySQL 产品重开场景、
Java bridge 51 项测试及隔离的 release 全目标构建也已通过。此前版本的
debug/release 工作区测试均通过；最终版本的 release 工作区测试在 WSL
重启后重新运行时被用户要求停止，未完成。最终 `cargo xtask check` 与严格
Rustdoc 也未完整通过，不把中断的运行记作成功。
