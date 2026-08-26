# 仓库指南

## 项目结构与模块组织

DogPaddle 是一个 Rust 2024 工作区。根目录 `README.md` 只介绍产品定位、已有能力和当前边界。`crates/change/` 定义共享 Arrow Schema、`Change`、Schema 校验、Schema 绑定的顶层投影，以及每个 Change 独立、自描述、恰好一个 RecordBatch 的完整 Arrow IPC Stream 编码与选择性读取；它的库代码与正常依赖不依赖 Flow、Operation 或 Store。`crates/store/` 实现 MDBX 事务存储、编解码器和类型化集合；每个持久化集合以最后一个 `Small` 或 `Large` 泛型参数显式声明物理规模，Store 直接创建或打开具体集合实例，裸句柄与 placement 不对外开放，且不依赖 Arrow。`crates/operation/` 承载具体 Operation 的纯 Definition、稳定编码、持久化 Data 声明和执行语义，运行 trait 与具体算子统一放在 `operation/`，并在其下按 `source/`、`transform/`、`sink/` 分类，当前包含 SequenceSource 与 Count；共享记录模型不属于 Operation。`crates/flow/` 提供公共 Builder、拓扑校验、持久化 `build/open`，并拥有内部 Stage。Flow 内部按生命周期拆分：`build/` 统一拥有 `FlowBuilder`、`StageRef`、Flow/Stage Definition、纯校验和稳定编码，并在构建时创建全部资源；`flow/` 处理已构建 Flow 的打开与生命周期，并在打开时解析全部资源；`stage/` 只保存运行期 Stage 及其已注入的类型化数据对象。Stage 不接收 Store，不创建或打开资源，也不知道物理 placement 或稳定资源名。拓扑是 Flow Definition 的连接关系，不再拥有独立 Builder。`integration-tests/change-store/` 是不可发布的下游测试包，只通过公共 API 验证完整 Change Stream 与 `AppendLog<Vec<u8>>` 的接缝；产品 crate 不得依赖它。每个产品 crate 的语义、用法和验证方式写入自身 `README.md`，并作为 Rustdoc 首页；不可发布的测试 package 只需维护自身测试说明。全工作区测试所有权、数据规格和性能口径统一写在根目录 `TESTING.md`。

## 构建、测试与开发命令

- `cargo build --workspace`：使用工作区锁定的依赖构建四个产品 crate、不可发布的集成测试包与内部测试工具。
- `cargo test --workspace`：运行单元测试、集成测试和文档测试。
- `cargo test -p dogpaddle-change-store-integration`：只运行 Change 与 AppendLog 的外部组合测试。
- `cargo test -p dogpaddle-store --test correctness transaction::`：运行指定公共测试区域；所有 crate 的公共测试 target 都统一命名为 `correctness`。
- `cargo fmt --all -- --check`：检查格式，不修改文件。
- `cargo clippy --workspace --all-targets -- -D warnings`：执行已配置的 `all` 和 `pedantic` Clippy 规则。若命令不可用，请先安装 Clippy rustup 组件。
- `cargo xtask check`：运行格式、debug/release correctness、Clippy 与 Rustdoc 的统一工作区 gate。
- `cargo xtask bench-smoke`：使用仓库固定的缩小参数实际执行全部 10 个 release benchmark target。
- `cargo bench -p dogpaddle-store --bench cell`、`--bench ordered_map`、`--bench append_log`、`--bench append_log_endurance`：分别运行 Cell、OrderedMap、通用 AppendLog 和 AppendLog 长稳 release 基准测试；Change 使用 `change_core`/`change_codec`，Operation 使用 `operation_core`，Flow 冷路径使用 `flow_lifecycle`，Change + Store 使用 `change_append_log`/`change_append_log_endurance`。工作负载与口径见根目录 `TESTING.md` 及各自的测试或性能说明。

请使用根目录 `Cargo.toml` 指定的 Rust 1.96 或更高版本。

## 编码风格与命名约定

遵循标准 `rustfmt` 输出，使用四空格缩进。模块、函数、变量和测试使用 `snake_case`；类型和 trait 使用 `UpperCamelCase`；常量使用 `SCREAMING_SNAKE_CASE`。工作区禁止 unsafe 代码。保持事务边界明确：需要显式作用域时，在同一个 `{ ... }` 中完成 `begin()`、所有访问和 `commit()`，不要为访问对象另设内层块。维持单向职责分离：Operation 不依赖 Flow，Stage 只作为 Flow 内部运行单元存在，Store 不依赖其他引擎层。Operation 可以接收不能提交的 `TransactionAccess`，并用自己持有的具体 `Cell` 或 `OrderedMap` 创建事务级 Access；它不能接收、开始、提交或保存 Transaction。公共 API 必须提供文档；可失败的方法应包含 `# Errors` 小节。Flow Definition 的 magic、版本、校验算法、Operation tag，以及 `flow/definition`、`stage/{index:08x}/...` 资源名都是持久化兼容性边界，修改时必须提供迁移设计和黄金字节或布局测试。每个 Stage 在 build 时必须获得一个显式为 `Small` 的 state map；未来运行状态只能使用已声明资源。`OperationDefinition` 是 operation crate 内的 sealed trait；具体 Definition 返回稳定的“逻辑名称 → 类型化 Store data class”声明，Flow 负责完整资源名并通用 create/open 具体实例，再把具名实例表交给 materialize 直接装配 Operation。materialize 必须按名称和精确类型取出实例，不能依赖声明顺序，具体 Definition 与运行 Operation 都不能接收 Store。Flow 不能枚举具体算子。Store 的 `Small`/`Large`、collection 类型和 codec 都是持久化 schema；Store 只验证 Size，Operation tag 对应的代码 schema 负责 collection 与 codec 一致性。`source`、`transform`、`sink` 分类模块必须能容纳任意多个算子，不得拥有或重导出分类级的单一 tag 或 decoder；稳定 tag 和 decoder 永远属于具体算子模块，公共 decoder 表按具体模块路径逐项注册。新增内建 Operation 必须注册唯一稳定 decoder，并覆盖 tag 唯一性、黄金字节、资源布局和 reopen。不要为旧 API 保留兼容层，当前也不要提前公开 `run` 空壳。

Change crate 的 `Change` 是无事件时间、有稳定事件顺序、允许重复、未 consolidation 的 Arrow 批量差分，也是有序变化流的一个非空连续物理片段；行位置属于语义，`Change`、其 codec 和运行层不得排序或隐式抵消事件。`Change` 不携带应用前状态，因而不验证撤回是否存在；事件生产者必须遵守有效流契约，维护或物化相应关系的组件负责按记录等价关系保证任意记录的“应用前权重 + 已处理前缀累计 diff”非负，并在验证到负权重前缀时报错、回滚。`(AppendLog offset, Change row_index)` 是当前持久化分批下的单边遍历坐标而非稳定 event ID；物理批次可以稳定合并或切分，但变换前后展平的事件序列必须逐项不变。Operation 的可观察结果必须对这种稳定重批保持不变，除非以后由独立的窗口、barrier 或 flush 信号明确引入语义边界。每个持久化 Change 必须编码成一个标准、完整、自描述的 Arrow IPC Stream。Stream 内嵌 physical Schema，第零字段固定为非 null Int64 `$dogpaddle.diff`，后续字段构成 logical record Schema；Stream 必须恰好包含一个非空 RecordBatch，并以 canonical EOS 完整结束。不得增加 DogPaddle 自定义 envelope、独立 Schema resource、Schema fingerprint 或 segment。运行层以 `AppendLog<Vec<u8>>` 保存编码结果，每个日志 entry 恰好保存一个完整 Stream，解码不得依赖 entry 外部 Schema。`ChangeProjection` 必须绑定精确 logical Schema，顶层字段索引严格递增，只能删列；diff 始终隐式保留，空投影合法，List/Struct 只按完整子树选择。内存投影必须共享原 Arrow buffer；选择性 IPC 解码必须先校验内嵌 Schema、完整 framing 和全部无需读取 payload 即可判断的 batch metadata，只复制并解码 diff 与所选字段，返回不借用 entry 或事务的普通 owned Change。未选字段的 UTF-8、List offsets 等值级约束不验证，需要完整审计时使用全量解码。投影是读取能力，不得改变写入字节、entry 大小或持久化格式。`$dogpaddle.` 字段名和 `dogpaddle.` Schema/Field metadata key 是保留命名空间。Arrow IPC metadata version、writer options、Schema marker、physical diff 布局、行序和允许的 Arrow 类型都是持久化兼容性边界；修改时必须更新完整 Stream 黄金字节、标准 Arrow reader 互操作、顺序保持、零/多 RecordBatch 拒绝、截断、尾随字节和 reopen 测试。Change crate 不实现 Store collection，Store 不依赖 Arrow。

## 测试规范

测试名称应清楚描述行为，例如 `finish_rejects_a_multi_stage_cycle`。每个源码模块目录只维护一个 `tests.rs`，其中统一覆盖该目录内模块及子模块的私有实现；不要为单个源码文件再创建同名测试子目录。每个 crate 的公共行为和持久性测试只能有一个显式 Cargo target：`tests/correctness.rs` 作为入口、`tests/correctness/*.rs` 按领域拆分；manifest 必须关闭自动 test/bench 发现，产品 library 设置 `[lib] bench = false`，并逐项声明 target，防止重新碎片化。能经公共 API 证明的行为不得留在白盒测试中。

正常产品依赖的跨 crate 契约由组合根拥有：Operation + Store 归 Operation，Flow + Operation + Store 归 Flow。只有没有产品组合根的 sibling seam 才建立 `publish = false` 的 `integration-tests/<seam>` package；当前 `crates/change/tests/` 不得依赖 Store，`crates/store/tests/` 不得依赖 Change，必须同时依赖二者的行为、性能或长稳验证统一放在 `integration-tests/change-store/`。使用 `tempfile` 创建隔离的临时存储。普通测试不得使用 wall-clock 断言；benchmark 的 fixture、seed、预热和结果校验必须位于计时外，输出原始样本及 rustc/CPU/profile/git 信息，持久化 reference 运行还必须使用显式固定文件系统。目前没有硬性覆盖率指标；持久化变更必须覆盖成功构建、纯校验失败无文件副作用、不完整构建、资源布局、稳定编码和重新打开。完整规范见根目录 `TESTING.md`。

## 提交与 Pull Request 规范

遵循仓库已有的 Conventional Commits 格式，例如 `feat(flow): ...`、`perf(store): ...` 和 `refactor(store): ...`。标题应简洁、使用祈使语气，并限定到具体 crate。Pull Request 应说明行为及持久性影响、关联相关 issue、列出已运行的检查命令；涉及存储性能时，还应提供基准对比。产品定位或能力边界变化应更新根 README；crate 接口、语义或验证方式变化应更新对应 crate README 和 API 文档。不要提交 `target/` 或本地数据库文件。
