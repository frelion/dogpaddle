# DogPaddle 算子简化重构执行记录

> 历史提案：记录当时方案与取舍，不作为当前实现约束。当前设计以根 [AGENTS.md](../../AGENTS.md) 指向的 owner 文档为准；不要据此恢复已删除的 API 或抽象。

> 主设计：`operator-simplification-roadmap.md`
> 执行开始基线：`fbaa5dc`，`main...origin/main [ahead 1]`；开始时除两份已批准计划文档外无产品代码变更。
> 当前阶段：P0–P3 实现、全量迁移、本地 Cargo gate与最终独立复审均已完成。

## 阶段 P0

- 状态：完成。
- 输入 revision：`fbaa5dc29297783cdbb81a629154e2df9c592203`
- 工具链：`rustc 1.96.0 (ac68faa20 2026-05-25)`
- 产品代码基线：未修改；实施前只有两份未跟踪的已批准计划文档。
- 基线 gate：`rustc --version && cargo xtask check` 使用工作区 Rust 1.96 启动并完成 correctness、bench test mode、Clippy/Rustdoc/doctest 等各段；后台 job 生命周期结束后句柄被回收，终端输出未保留单一最终 exit marker，因此最终交付仍会重跑完整 gate，P0 不把该次运行作为最终证据。
- 只读盘点完成：
  - 装配迁移清单 agent `6b3f6a76-13fc-4d5d-9bf7-41dde04a8f56`
  - ABI/测试矩阵 agent `50f76a83-37d0-4adb-86e7-73ebcfc79d1f`
- 盘点范围：全部 19 个 tag、固定/条件资源布局、RuntimeResource 类型、Flow/测试/bench/system-host 直接消费者、可见性风险与精确回归命令。
- 发现并纳入执行：旧 `OPERATOR_ROADMAP.md` 的 17 算子计数不可作为 oracle；Doris/ClickHouse 缺专门 durable reopen witness；Aggregate 缺清晰命名的 Exclusive→Turn setup witness；公开 Store catalog 不支持任意 extra 或同 kind codec 身份审计。
- 架构约定：`AGENTS.md` 已改为 operation-owned typed setup，删除旧声明/erased materialization 责任描述。
- 下一阶段放行：是。P1 必须用私有具体 bound dispatch 编译证明全部布局；失败则按主计划停止。

## 阶段 P1

- 状态：实现与当前验证完成。
- 实现 agent：`238be90b-dd8b-4e19-87d3-59adf4b387da`，独占修改 `crates/operation/`。
- 门槛结果：全部 19 个 built-in 通过不透明 `OperationBinding` 和唯一 setup dispatch 进入具体 typed `create/open`；Flow 只传稳定前缀、Store setup/open 借用和首 Operation 的不透明资源，不枚举算子。
- 删除的层：`DataDeclaration`、`DataInstances`、erased data slot/materializer 以及按声明装配的旧路径已从生产实现移除。资源逻辑名、collection 类型和 codec 归具体算子；`RuntimeResource` 只保留拥有型 `Any` 擦除和精确类型校验/取回。
- binding 结果：`BoundBody` 封装具体编译结果与布局选择；Aggregate 的 Atomic/Exclusive 差异在统一 setup 后 normalization；EquiJoin 的条件状态布局由 bind 一次决定，再由 typed create/open 使用。
- 编译证据：`cargo check -p dogpaddle-operation -p dogpaddle-flow --lib`，exit 0；`dogpaddle-operation` 与 `dogpaddle-flow` production libraries 均通过。
- Flow 证据：`cargo test -p dogpaddle-flow --test correctness`，exit 0；47 passed、0 failed、0 ignored。
- 后续 P2/P3 与最终 gate 证据见下节。

## 阶段 P2/P3 与最终本地验证

- 全部 Operation correctness、bench 与 PostgreSQL gate host 已迁移到 `Store::setup` + `create_operation` / `open_operation`；生产、测试、bench 和 system-host Rust 源码中已无 `DataDeclaration`、`DataInstances`、`MaterializeError` 或 `binding.materialize`。
- Aggregate 每个 `BoundLayout` 缓存 `min_slot: Option<usize>` / `max_slot: Option<usize>` 两个全局 slot 索引，不假设 slot 连续；保留 `indexed_slot` 编号/去重、`GroupState.extremes` codec、partition 编号和输出次序。correctness 以 `MIN(text), MIN(bytes), MAX(text), MAX(bytes)` 覆盖非连续全局 slot 与 reopen 撤回；benchmark 保留原 `repeated_min_max` 同-layout 纵向口径，并另增不同 value columns 的 `distinct_layout_min_max`。
- `cargo test -p dogpaddle-operation --test correctness --locked`：exit 0，208 passed。
- `cargo test -p dogpaddle-flow --test correctness --locked`：exit 0，47 passed。
- `cargo check -p dogpaddle-postgres-system-hosts --bins --locked`：exit 0。
- `cargo test --workspace --locked`：exit 0，包含全部单元、correctness 和 doctest。
- `cargo test --workspace --benches --locked`：exit 0；首次运行暴露 resource bench 在 setup transaction 尚持有 RocksDB 时重复 open 的问题，修正为 drop setup transactions 后从同一 reopened Store 同时获得观察 handle、runtime Operation 和 transactions，随后全量重跑通过。
- `cargo clippy --workspace --all-targets -- -D warnings`：exit 0。
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked`：exit 0。
- `cargo xtask check`：exit 0。
- `cargo fmt --all -- --check` 与 `git diff --check`：exit 0。
- 未运行真实 PostgreSQL/JVM/warehouse 外部系统测试；普通 Cargo gate 保持离线，系统 host 仅完成编译验证。

## 第二轮代码收敛

- Astra 复审纠正了首轮的代码量口径：不能把删除 448 行私有测试、同时遗漏未跟踪的 `setup.rs`，表述为“核心生产代码净减少 367 行”。该数字作废。
- 第二轮开始时的临时未提交工作树记录值为：产品 crate 的 `src/**/*.rs` 物理行数（仅排除独立 `tests.rs` 与 `tests/`，仍包含生产文件中的 `#[cfg(test)]` 模块）43,906 行，其中 operation 为 28,550 行。该快照没有独立 commit/tree，不能从当前 Git 历史重建，只作为本轮现场对比而非仓库基线。
- 新增两个 crate-private typed state helper，仅封装稳定物理名拼接、现有 `StoreData` create/open 和原错误映射；九个有状态实现继续显式写出具体 collection 类型与逻辑名，但删除重复的局部 name 与 `map_err` 样板。
- 第二轮收敛后同口径分别为 43,806 与 28,450 行，相对现场记录净减少 100 个物理源码行；独立测试文件与文档不计入该差值，但这不是严格剔除所有测试代码后的生产 LOC 指标。
- 修正 `ExclusiveTransform` capability 矩阵，使 Atomic 与 Turn body 都合法；Atomic 仍在 setup 时适配成 Turn。恢复 decoder tag 唯一/完整集合、MissingOutput/UnexpectedOutput、Exclusive Atomic/Turn 与 Atomic→Turn 拒绝的最小白盒测试，不恢复旧 DataInstances 测试框架。

## 稳定约束清单

实施中不得改变：Operation Definition tag/payload、资源逻辑名与 ordinal 物理名前缀、collection codec、Change IPC、Atomic/Turn/AfterCommit 协议、build/open 顺序、owner-before-bind、一次 setup commit 发布、RuntimeResource 凭据边界。

## 阶段记录模板

每阶段补充：完整 diff、删掉/新增机制、命令与退出码、独立审查 finding 及处理、未运行系统测试和原因、是否允许下一阶段。
