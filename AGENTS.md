# 仓库指南

## 项目结构与模块组织

DogPaddle 是一个 Rust 2024 工作区。根目录 `README.md` 只介绍产品定位、设计目标和当前边界。`crates/store/` 实现 MDBX 事务存储、编解码器和类型化集合；`crates/operation/` 承载具体 Operation 的纯 Definition 与后续语义；`crates/flow/` 拥有 Flow 拓扑、生命周期以及内部 Stage 运行时。Flow 的私有拓扑根据具体 Definition 的精确输入数量验证连接；在闭合 Definition 联合落地前，不公开通用 Definition trait。每个 crate 的概念、用法和验证方式写入自身的 `README.md`，并作为 Rustdoc 首页。当前 Flow 工作不得修改 `dogpaddle-store` 的 API、语义或文档，除非任务明确解除该边界。

## 构建、测试与开发命令

- `cargo build --workspace`：使用工作区锁定的依赖构建三个 crate。
- `cargo test --workspace`：运行单元测试、集成测试和文档测试。
- `cargo test -p dogpaddle-store --test architecture transaction::`：运行指定测试区域；可按需替换包、测试目标或过滤条件。
- `cargo fmt --all -- --check`：检查格式，不修改文件。
- `cargo clippy --workspace --all-targets -- -D warnings`：执行已配置的 `all` 和 `pedantic` Clippy 规则。若命令不可用，请先安装 Clippy rustup 组件。
- `cargo bench -p dogpaddle-store --bench store`：运行 release 模式的存储基准测试；工作负载环境变量见 `crates/store/README.md`。

请使用根目录 `Cargo.toml` 指定的 Rust 1.96 或更高版本。

## 编码风格与命名约定

遵循标准 `rustfmt` 输出，使用四空格缩进。模块、函数、变量和测试使用 `snake_case`；类型和 trait 使用 `UpperCamelCase`；常量使用 `SCREAMING_SNAKE_CASE`。工作区禁止 unsafe 代码。保持事务边界明确：需要显式作用域时，在同一个 `{ ... }` 中完成 `begin()`、所有访问和 `commit()`，不要为访问句柄另设内层块。维持单向职责分离：Operation 不依赖 Flow，Stage 只作为 Flow 内部运行单元存在，Store 不依赖其他引擎层。公共 API 必须提供文档；可失败的方法应包含 `# Errors` 小节。不要为旧 Flow 保留兼容接口。

## 测试规范

测试名称应清楚描述行为，例如 `finish_rejects_a_multi_stage_cycle`。私有实现测试放在对应模块旁，公共行为和持久性测试放在所属 crate 的 `tests/` 目录。使用 `tempfile` 创建隔离的临时存储。目前没有硬性覆盖率指标；实现持久化后应覆盖成功、错误、回滚、重新打开和崩溃恢复场景。

## 提交与 Pull Request 规范

遵循仓库已有的 Conventional Commits 格式，例如 `feat(flow): ...`、`perf(store): ...` 和 `refactor(store): ...`。标题应简洁、使用祈使语气，并限定到具体 crate。Pull Request 应说明行为及持久性影响、关联相关 issue、列出已运行的检查命令；涉及存储性能时，还应提供基准对比。产品定位或能力边界变化应更新根 README；crate 接口、语义或验证方式变化应更新对应 crate README 和 API 文档。不要提交 `target/` 或本地数据库文件。
