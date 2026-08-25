# 仓库指南

## 项目结构与模块组织

DogPaddle 是一个 Rust 2024 工作区。根目录 `README.md` 只介绍产品定位、已有能力和当前边界。`crates/store/` 实现 MDBX 事务存储、编解码器和类型化集合；每个持久化集合以最后一个 `Small` 或 `Large` 泛型参数显式声明物理规模，Store 直接创建或打开具体集合实例，裸句柄与 placement 不对外开放。`crates/operation/` 承载具体 Operation 的纯 Definition、稳定编码、持久化 Data 声明和执行语义，运行 trait 与具体算子统一放在 `operation/`，并在其下按 `source/`、`transform/`、`sink/` 分类，当前包含 SequenceSource 与 Count；`crates/flow/` 提供公共 Builder、拓扑校验、持久化 `build/open`，并拥有内部 Stage。Flow 内部按生命周期拆分：`build/` 统一拥有 `FlowBuilder`、`StageRef`、Flow/Stage Definition、纯校验和稳定编码，并在构建时创建全部资源；`flow/` 处理已构建 Flow 的打开与生命周期，并在打开时解析全部资源；`stage/` 只保存运行期 Stage 及其已注入的类型化数据对象。Stage 不接收 Store，不创建或打开资源，也不知道物理 placement 或稳定资源名。拓扑是 Flow Definition 的连接关系，不再拥有独立 Builder。每个 crate 的语义、用法和验证方式写入自身 `README.md`，并作为 Rustdoc 首页。

## 构建、测试与开发命令

- `cargo build --workspace`：使用工作区锁定的依赖构建三个 crate。
- `cargo test --workspace`：运行单元测试、集成测试和文档测试。
- `cargo test -p dogpaddle-store --test architecture transaction::`：运行指定测试区域；可按需替换包、测试目标或过滤条件。
- `cargo fmt --all -- --check`：检查格式，不修改文件。
- `cargo clippy --workspace --all-targets -- -D warnings`：执行已配置的 `all` 和 `pedantic` Clippy 规则。若命令不可用，请先安装 Clippy rustup 组件。
- `cargo bench -p dogpaddle-store --bench store`：运行 release 模式的存储基准测试；工作负载环境变量见 `crates/store/README.md`。

请使用根目录 `Cargo.toml` 指定的 Rust 1.96 或更高版本。

## 编码风格与命名约定

遵循标准 `rustfmt` 输出，使用四空格缩进。模块、函数、变量和测试使用 `snake_case`；类型和 trait 使用 `UpperCamelCase`；常量使用 `SCREAMING_SNAKE_CASE`。工作区禁止 unsafe 代码。保持事务边界明确：需要显式作用域时，在同一个 `{ ... }` 中完成 `begin()`、所有访问和 `commit()`，不要为访问对象另设内层块。维持单向职责分离：Operation 不依赖 Flow，Stage 只作为 Flow 内部运行单元存在，Store 不依赖其他引擎层。Operation 可以接收不能提交的 `TransactionAccess`，并用自己持有的具体 `Cell` 或 `OrderedMap` 创建事务级 Access；它不能接收、开始、提交或保存 Transaction。公共 API 必须提供文档；可失败的方法应包含 `# Errors` 小节。Flow Definition 的 magic、版本、校验算法、Operation tag，以及 `flow/definition`、`stage/{index:08x}/...` 资源名都是持久化兼容性边界，修改时必须提供迁移设计和黄金字节或布局测试。每个 Stage 在 build 时必须获得一个显式为 `Small` 的 state map；未来运行状态只能使用已声明资源。`OperationDefinition` 是 operation crate 内的 sealed trait；具体 Definition 返回稳定的“逻辑名称 → 类型化 Store data class”声明，Flow 负责完整资源名并通用 create/open 具体实例，再把具名实例表交给 materialize 直接装配 Operation。materialize 必须按名称和精确类型取出实例，不能依赖声明顺序，具体 Definition 与运行 Operation 都不能接收 Store。Flow 不能枚举具体算子。Store 的 `Small`/`Large`、collection 类型和 codec 都是持久化 schema；Store 只验证 Size，Operation tag 对应的代码 schema 负责 collection 与 codec 一致性。`source`、`transform`、`sink` 分类模块必须能容纳任意多个算子，不得拥有或重导出分类级的单一 tag 或 decoder；稳定 tag 和 decoder 永远属于具体算子模块，公共 decoder 表按具体模块路径逐项注册。新增内建 Operation 必须注册唯一稳定 decoder，并覆盖 tag 唯一性、黄金字节、资源布局和 reopen。不要为旧 API 保留兼容层，当前也不要提前公开 `run` 空壳。

## 测试规范

测试名称应清楚描述行为，例如 `finish_rejects_a_multi_stage_cycle`。每个源码模块目录只维护一个 `tests.rs`，其中统一覆盖该目录内模块及子模块的私有实现；不要为单个源码文件再创建同名测试子目录。公共行为和持久性测试放在所属 crate 的 `tests/` 目录。使用 `tempfile` 创建隔离的临时存储。目前没有硬性覆盖率指标；持久化变更必须覆盖成功构建、纯校验失败无文件副作用、不完整构建、资源布局、稳定编码和重新打开。

## 提交与 Pull Request 规范

遵循仓库已有的 Conventional Commits 格式，例如 `feat(flow): ...`、`perf(store): ...` 和 `refactor(store): ...`。标题应简洁、使用祈使语气，并限定到具体 crate。Pull Request 应说明行为及持久性影响、关联相关 issue、列出已运行的检查命令；涉及存储性能时，还应提供基准对比。产品定位或能力边界变化应更新根 README；crate 接口、语义或验证方式变化应更新对应 crate README 和 API 文档。不要提交 `target/` 或本地数据库文件。
