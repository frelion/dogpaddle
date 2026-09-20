# 仓库指南

DogPaddle 是 Rust 2024 工作区。使用根 `Cargo.toml` 指定的 **Rust 1.96**；工作区禁止 unsafe 代码。

## 文档所有权

修改一个领域前，先读其 owner README 及链接的维护契约。**下表中的 owner 文档是该领域唯一的当前设计约束来源**；根 README 只介绍产品定位、已有能力和边界。本文件只规定跨工作区协作规则，不重复算子、布局或状态机细节。

| 领域 | 当前约束入口 |
| --- | --- |
| Arrow Schema、Change、事件顺序、IPC 与投影 | [Change README](crates/change/README.md) |
| 事务能力、catalog、六种类型化集合、分页与容量 | [Store README](crates/store/README.md) |
| JVM payload、Delivery/checkpoint、start/poll/ack/stop | [Debezium README](crates/debezium/README.md) |
| Operation 构造、表达式、关系算子、CDC 和 Sink | [Operation README](crates/operation/README.md) 及其 `docs/` 契约 |
| 算子图、自动 Station 划分、构建/恢复、事务与调度 | [Flow README](crates/flow/README.md) 及 [运行契约](crates/flow/docs/runtime.md) |
| SQL subset、endpoint、identity、start 与 lowering | [SQL README](crates/sql/README.md) |
| 唯一产品命令、状态路径与停止行为 | [CLI README](crates/dogpaddle/README.md) |
| 测试所有权、系统 host、数据规格和性能口径 | [TESTING.md](TESTING.md) |

产品 crate README 同时作为 Rustdoc 首页；不可发布的测试 package 维护自己的测试说明即可。
`docs/plans/` 只保存历史提案和当时的取舍，不是当前规范。修改行为、接口或验证方式时直接更新 owner 文档及 API 文档；不要在根文件重新复制一份规则。

## 依赖与职责

- Change 的库和正常依赖不依赖 Flow、Operation 或 Store；Store 不依赖 Arrow 或其他引擎层。
- Debezium 是独立进程内 runtime，不依赖 Change、Store、Operation 或 Flow。
- Operation 只拥有计算、类型化状态和具体外部适配，不依赖 Flow；Station 只在 Flow 内部存在。
- SQL 是依赖 Flow 与 Operation 的最上层编译入口；底层不反向依赖 SQL。`dogpaddle` 保持薄 binary，不增设 library、runner 或第二套生命周期/执行引擎。
- 产品不得依赖 `integration-tests/`、system-test host 或 test-support。没有产品组合根的 sibling seam 才进入不可发布的 `integration-tests/<seam>`；当前只有 Change–Store 接缝。

## 构建与验证

- `cargo build --workspace`：使用工作区锁定的依赖构建七个产品 crate、不可发布的 Change–Store 接缝包、两个系统验收 host 包、性能上下文与 xtask。
- `cargo test --workspace`：运行单元测试、集成测试和文档测试。
- `cargo test -p dogpaddle-change-store-integration`：只运行 Change 与 SubscribedLog 的外部组合测试。
- `cargo test -p dogpaddle-store --test correctness transaction::`：运行指定公共测试区域；所有 crate 的公共测试 target 都统一命名为 `correctness`。
- `cargo fmt --all -- --check`：检查格式，不修改文件。
- `cargo clippy --workspace --all-targets -- -D warnings`：执行已配置的 `all` 和 `pedantic` Clippy 规则。若命令不可用，请先安装 Clippy rustup 组件。
- `cargo xtask check`：运行格式、debug/release correctness、Clippy 与 Rustdoc 的统一工作区 gate。
- `cargo test --workspace --benches --locked`：以 test mode 执行全部 Criterion target，验证 benchmark 可以进入普通工作区 gate。
- `DOGPADDLE_PERF_PROFILE=smoke cargo bench -p dogpaddle-store --bench ordered_map`：运行一个 owner-specific smoke；profile 必填，只接受 `smoke` 或 `reference`，reference 还必须设置绝对 `DOGPADDLE_PERF_ROOT`。完整 target 表见 `TESTING.md`。

## 交付审查

每次功能实现完成、准备交付前，必须使用多个独立 Agent 对当前完整 diff 做复审，至少分别覆盖：是否存在冗余抽象或职责重复、正确性与恢复/事务边界、性能与内存放大或资源耗尽风险。主 Agent 必须逐项核实带文件和行号的 finding，修复确认的问题并重新运行相称的 correctness、benchmark、Clippy 与完整 gate；即使没有 finding，也要在交付时明确报告已审查的风险面。不能用同一 Agent 的实现结论代替独立 review。

## 编码与持久化

遵循标准 `rustfmt` 输出，使用四空格缩进。模块、函数、变量和测试使用 `snake_case`；类型和 trait 使用 `UpperCamelCase`；常量使用 `SCREAMING_SNAKE_CASE`。工作区禁止 unsafe 代码。公共 API 必须提供文档；可失败的方法应包含 `# Errors` 小节。不要为旧 API 保留兼容层。

需要显式事务作用域时，在同一个 `{ ... }` 内完成 `begin()`、全部访问和 `commit()`，不要为 access 对象另设内层块。所有权和事务边界必须通过现有能力类型表达，不增加隐式事务或第二套状态事实。

当前持久格式是开发期 v1。稳定名称、tag/payload、schema、codec、资源布局、identity 与目标布局变化必须同步更新对应 golden/layout/reopen 证据。旧数据库及受影响目标直接重建；不增加 alias、fallback、旧格式识别、迁移或兼容层。恢复失败不得自动删除、重建或改写已有状态。

## 测试组织

测试名称描述行为；公共行为统一由显式 `correctness` target 验证，私有测试按行为域组织。manifest 关闭自动 test/bench 发现，产品 library 设置 `bench = false`。具体分卷、准入、系统验收及 benchmark 规则统一遵循 [TESTING.md](TESTING.md)。

不建立通用 experiment、benchmark plan、registry、validator 或跨 owner 结果协议；可合并实验归入 correctness、system test 或 owner benchmark。用 `tempfile` 隔离存储，不提交本地数据库或 `target/`。

## 提交与 Pull Request

遵循仓库已有的 Conventional Commits 格式，例如 `feat(flow): ...`、`perf(store): ...` 和 `refactor(store): ...`。标题应简洁、使用祈使语气，并限定到具体 crate。Pull Request 应说明行为及持久性影响、关联相关 issue、列出已运行的检查命令；涉及存储性能时，还应提供基准对比。产品定位或能力边界变化应更新根 README；crate 接口、语义或验证方式变化应更新对应 crate README 和 API 文档。不要提交 `target/` 或本地数据库文件。
