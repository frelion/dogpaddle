# 仓库指南

## 项目结构与模块组织

DogPaddle 是一个 Rust 2024 工作区。根目录 `README.md` 只介绍产品定位、核心能力和当前边界。`crates/store/` 实现基于 MDBX 的事务存储、编解码器和类型化集合，其测试分布在 `tests/architecture/` 与 `tests/collections/`，基准测试位于 `benches/store.rs`。`crates/flow/` 围绕 `Flow`、`Stage` 和 `Operation` 实现持久化 DAG 执行，其集成测试位于 `tests/execution.rs` 与 `tests/topology.rs`。每个 crate 的概念、用法和验证方式写入自身的 `README.md`，该文件同时作为 Rustdoc 首页；具体 API 文档保留在源码的 `///` 注释中。

## 构建、测试与开发命令

- `cargo build --workspace`：使用工作区锁定的依赖构建两个 crate。
- `cargo test --workspace`：运行单元测试、集成测试和文档测试。
- `cargo test -p dogpaddle-store --test architecture transaction::`：运行指定测试区域；可按需替换包、测试目标或过滤条件。
- `cargo fmt --all -- --check`：检查格式，不修改文件。
- `cargo clippy --workspace --all-targets -- -D warnings`：执行已配置的 `all` 和 `pedantic` Clippy 规则。若命令不可用，请先安装 Clippy rustup 组件。
- `cargo bench -p dogpaddle-store --bench store`：运行 release 模式的存储基准测试；工作负载环境变量见 `README.md`。

请使用根目录 `Cargo.toml` 指定的 Rust 1.96 或更高版本。

## 编码风格与命名约定

遵循标准 `rustfmt` 输出，使用四空格缩进。模块、函数、变量和测试使用 `snake_case`；类型和 trait 使用 `UpperCamelCase`；常量使用 `SCREAMING_SNAKE_CASE`。工作区禁止 unsafe 代码。保持事务边界明确，并维持通用存储原语、类型化集合、流调度和操作语义之间的职责分离。公共 API 必须提供文档；可失败的方法应包含 `# Errors` 小节。

## 测试规范

测试名称应清楚描述行为，例如 `pending_rolls_back_operation_state`。私有实现测试放在对应模块旁，公共行为和持久性测试放在所属 crate 的 `tests/` 目录。使用 `tempfile` 创建隔离的临时存储。目前没有硬性覆盖率指标；相关改动应覆盖成功、错误、回滚以及重新打开或崩溃恢复等场景。

## 提交与 Pull Request 规范

遵循仓库已有的 Conventional Commits 格式，例如 `feat(flow): ...`、`perf(store): ...` 和 `refactor(store): ...`。标题应简洁、使用祈使语气，并限定到具体 crate。Pull Request 应说明行为及持久性影响、关联相关 issue、列出已运行的检查命令；涉及存储性能时，还应提供基准对比。产品定位或能力边界变化应更新根 README；crate 接口、语义或验证方式变化应更新对应 crate README 和 API 文档。不要提交 `target/` 或本地数据库文件。
