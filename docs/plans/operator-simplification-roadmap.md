# DogPaddle 算子简化重构路线图

> 历史提案：记录当时方案与取舍，不作为当前实现约束。当前设计以根 [AGENTS.md](../../AGENTS.md) 指向的 owner 文档为准；不要据此恢复已删除的 API 或抽象。

> 状态：已按本路线图实施并通过本地完整 gate。基线：`fbaa5dc`（main，工作区原本干净）。
> 目标读者：负责实施、审查和接手维护的 Agent，以及只掌握 Rust 基础语法的开发者。
> 本文保留实施前的决策与阶段设计；配套 `operator-simplification-agent-brief.md` 给出交接与审查提示，实际命令和验证证据见 `operator-simplification-execution.md`。

## 0. 决策摘要

我们选择**缩短启动装配链，保留事务执行协议，局部简化算子内的数据关系**。

当前最值得删除的不是 `Commit/Complete` 的区别，而是“声明类型 → 函数指针创建 → Any 装箱 → 按名称取出 → downcast → 闭包构造运行对象”这条通用装配链。

目标路径是：

```text
纯 Definition
  → 全图 Schema 检查，得到有具体类型的临时绑定结果
  → operation 内部装配代码直接创建/打开状态表
  → 直接构造运行算子
  → Flow/Station 按现有协议运行
```

主要设计决策：

- 保留 sealed `OperationDefinition`、稳定 decoder/tag、统一 `bind` 校验入口。
- 保留现有 `Operation::{Atomic, Turn}` 和执行 trait，不引入新的运行总枚举。
- 用 operation 内部的一个绑定结果枚举取代 materializer 闭包工厂；该枚举不持久化、不暴露具体变体到 Flow。
- 用专用装配入口直接创建/打开具体类型的 collection，删除 `DataInstances` 和 data 类型擦除机制。
- 保留 `RuntimeResource` 的小型类型擦除边界：它解决一次性的外部运行配置注入，不是本轮性能问题。
- 不增加通用 Setup trait、Engine、Coordinator、ClaimEngine、公共 CDC driver、公共 Sink 框架。
- 不改变持久资源名、codec、Definition 字节、Change 语义、输出容量契约。

**重要限制：这是经过源码对照的设计，不是编译证明。** 无人能诚实承诺重构不会遇到未知问题。本计划通过 P1 前置验证门槛，把可能推翻方向的风险放在全量迁移之前。P1 不通过，不得进入 P2；不得靠保留双轨兼容机制掩盖失败。

## 1. 为什么做，以及不做什么

### 1.1 主要问题

1. 新增或理解一个普通算子，需要掌握的启动机制超过其业务复杂度。
2. 同一资源先被声明、再擦除类型、再装入字典，最后还原类型。运行算子本身其实已经持有具体 collection。
3. `OperationBinding` 同时承担 Schema 结果、闭包工厂、运行配置类型检查和执行形式适配；关系不直观。
4. Aggregate 用 layout/slot/call 索引表达共享关系，更新时又遍历 slot 反查归属。
5. Join/Sink 必须处理恢复，但局部业务处理和阶段推进交织；不能仅靠拆文件解决。

### 1.2 本项目明确不包含

- 不改变 Atomic 整批原子语义；Aggregate 跨轮清理另立设计。
- 不删除 Probe 或改变 Join 首次输出前必须完成的验证。
- 不增加 output 单条硬上限；当前空 backlog 接受 oversized entry 是有意的活性契约。
- 不引入 row-id/dictionary、共享 blob、自定义 Change envelope、第二套 Schema 存储。
- 不合并 EquiJoin/AsOfJoin，不合并 PG/MySQL CDC 驱动，不重写 buffered sink 的持久协议。
- 不做并发调度、执行引擎替换、依赖升级、tag/version 升级、旧格式迁移。
- 不承诺减少逻辑 Store 调用就按比例减少 fsync：一次事务中的多次操作并不各自同步提交。

### 1.3 对前序审查的校正

- Station 的本地 fail-stop 检查顺序可加固，但 `Flow::advance` 已预检全部 Station；不能把它当公共路径已确认的 P1 故障。全局预检不可删。
- CDC 队列发布时 pop 与 output append 在同一事务，重复写入不等于同时保留两份完整快照。
- Aggregate 极值缓存是空间换读取；不能只因重复就删除。
- `Any`、`Box`、文件长度不单独构成吞吐问题的证据。
- AfterCommit 在成功 Commit 和 Complete 后都可能运行；失败恢复可重放外部动作，不能宣称任意外部调用只执行一次。
- Aggregate 清空组时残留 extrema 清理是必要行为：分组/参数级撤回校验允许组权重归零时分区仍有条目。不能删除循环，也不能悄悄改为跨轮处理。

## 2. 已核对的源码证据

行号仅对应基线；执行时以符号定位。

| 证据 | 基线位置 | 设计约束 |
|---|---|---|
| 擦除数据、函数指针、materializer | `crates/operation/src/definition.rs:33–107` | 优先删除的机制 |
| 资源类型预检与消费 | `definition.rs:327–370`; `resource.rs:26–41` | build 创建路径前完成 presence/type 预检 |
| 隐式 Exclusive 适配 | `definition.rs:387–410` | 保留不可融合语义，消除闭包套闭包 |
| canonical build、预绑定、原子发布 | `crates/flow/src/build/mod.rs:195–229` | 必须保持顺序 |
| 通用 DataInstances 创建 | `build/mod.rs:294–343` | 将其替换成不枚举具体算子的装配调用 |
| owner 检查、打开资源、短 snapshot | `crates/flow/src/build/open.rs:30–85` | owner mismatch 在 bind 前失败 |
| missing resource 错误映射 | `build/open.rs:164–175` | 不能降级成模糊 operation error |
| setup 可变借用与 consuming commit | `crates/store/src/store/transaction.rs:36–80` | 顺序创建 owned handle，不能借用 setup 到运行期 |
| Distinct 简单状态与执行 | `operation/transform/distinct.rs:18–39,83–118,121–166` | 单状态样板；不要改语义 |
| Aggregate 三个具体状态 | `operation/transform/aggregate/runtime.rs:27–37` | 运行时已是具体类型，无须再造 state 对象层 |
| Join 条件资源集合 | `operation/transform/equi_join/definition.rs:22–43` | Inner、counted、residual counted 必须精确区分 |
| prepared borrow 与回调 | `crates/operation/src/operation/mod.rs:110–237` | 不能把 borrowed work 强行改 owned 状态机 |
| 公共测试直接依赖 data API | `crates/operation/tests/correctness/support.rs` 及各算子测试 | API 迁移必须包含测试/bench/system hosts |

## 3. 目标设计：少一套资源框架，不多一套运行框架

### 3.1 职责边界

| 所在处 | 允许 | 禁止 |
|---|---|---|
| Definition / schema bind | 表达式编译、精确 Schema、纯参数与资源需求判断 | Store、时间、随机、外部 I/O |
| Flow build/open | 路径、canonical Definition、拓扑、owner、ordinal 命名、setup 生命周期与发布 | 枚举具体算子；让 Station 接触 Store |
| operation 专用 setup 装配模块 | 接受短借用的 StoreSetup/Store；创建/打开具体 collection；消费绑定结果 | 开连接、读网络、开始运行期事务、保留 Store |
| 运行算子 | 具体 collection、编译参数、可从 durable state 重建的临时对象 | Definition/Binding/Store/事务启动能力 |
| Station | 现有事务、输出、确认与 AfterCommit 协议 | 资源创建、tag/具体算子分支 |

新装配模块是**替代原资源执行框架的单一位置**，不是再加在旧框架外的包装。它只在启动时存在。

### 3.2 必须显式修订的架构约定

当前 `AGENTS.md` 要求 Flow 通用创建声明资源、binding closure 不接收 Store、materialize 按名消费 `DataInstances`。新方案不满足原装配段落，不能声称完全不变。

实施开始时，P0 必须单独更新该段，限定为：

> Definition 与 Schema binding 保持纯函数式边界。Flow 拥有 Store setup 生命周期与带 ordinal 的资源命名；operation 内独立的 setup 模块在调用期间借用 setup/store，为已验证绑定创建或打开具体状态并构造 runtime。具体 Definition、绑定过程、运行实例和 Station 均不接收或保存 Store。Flow 不枚举具体算子。运行能力在 setup 发布或 reopen 校验完成后才移交给 Flow。

此处是实现期需要批准并记录的约定变更，不是本规划轮偷偷修改仓库规则。后续执行本路线图时必须先完成 P0，不能一边违背旧规则一边假装遵守。

### 3.3 API 草案（待 P1 编译冻结，不是现有 API）

沿用 `OperationBinding` 名称，避免再创造 BoundPlan/Assembly 等公共对象。其内部改为具体绑定结果，而不是闭包。

```rust
// operation crate 内部；variant/type names 在 P1 统一冻结。
enum BoundBody {
    AtomicReady(Box<dyn AtomicOperation>), // 无状态表达式等可直接构造
    TurnReady(Box<dyn TurnOperation>),     // 仅适用无需资源的 ready runtime
    Distinct { input_schema: SchemaRef },
    Aggregate(aggregate::BoundAggregate),
    EquiJoin(equi_join::BoundEquiJoin),
    AsOfJoin(asof_join::BoundAsOfJoin),
    // 其余有状态 Scan、count、具体 Sink/CDC，各有 owned bound data。
}

pub struct OperationBinding {
    output_schema: Option<SchemaRef>,
    kind: OperationKind,
    body: BoundBody,
}

// 跨 crate 必须 public，但可以 doc(hidden)；具体枚举和数据类型不公开。
// 在独立 operation::setup 模块中的自由函数，不是 bind 方法。
pub fn create(
    binding: OperationBinding,
    setup: &mut StoreSetup,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError>;

pub fn open(
    binding: OperationBinding,
    store: &Store,
    prefix: &str,
    resource: RuntimeResource,
) -> Result<Operation, OperationSetupError>;
```

这段只定义能力边界。完整变体清单和字段必须在 P1 由真实算子推导，不能让后续执行者凭空补齐。

- `kind` 来自显式 Definition kind，通过统一 bind 校验后写入，不从 tag/数据个数反推。private constructors 不能绕过统一入口导出未经验证的 binding。
- 保留 `output_schema()` 与 `validate_resource()`。后一方法只做 presence/exact Rust type 预检，不做外部 I/O；装配消费 resource 时再次防御性校验。
- AtomicReady 是现有执行对象，不再额外包一个“无状态 binding struct”。是否接受 trait object ready 变体由 P1 实测代码清晰度核对；不新增第二个 runtime enum。
- 有状态算子只保存真正需要延后的 owned 编译参数；不要把完整 Definition Clone 放进去。
- `BoundAggregate` 当前只含 calls/layouts/slots，不够承担完整装配。P1 必须将其变为包含 input/output Schema、group expressions 等所需参数的完整绑定结果，或与现有类型整合，不能偷偷丢失字段。
- 有较大变体时可 Box 单个变体，避免 enum size 放大；不得为统一大小把所有字段再包一层工厂。
- 具体 codec 如 GroupState、EntryPartition、continuation 不导出为公共 API；仅对 setup 所需范围开放 crate 内可见性，优先把具体构造放在算子自己的模块。
- `OperationSetupError` 定义具体缺失资源（完整物理名）、Store 错误、运行配置/构造错误；Flow 映射到原有可观察错误分类。既有 `MaterializeError` 中属于运行配置的分类可保留；已删除 DataInstances 的 missing/extra 实例错误不可用虚假兼容分支保留。

### 3.4 创建/打开路径如何写

优先使用两个直接函数，不引入含 generic 方法的 dyn Setup trait（不可对象安全），不引入创建模式 registry。

示意：

```rust
// 各调用短借用 setup；返回 owned handle，不借用 setup。
let weights = setup.create_data::<OrderedMultiset<Vec<u8>>>(
    &format!("{prefix}/distinct.weights"),
)?;
let runtime = DistinctOperation::from_bound(input_schema, weights);
```

open 用 `store.open_data::<D>(...)`，绝不能缺失时 create。缺失映射为携带完整名称的错误。

- Flow 只生成 `station/{station:08x}/operation/{operation:08x}` 前缀；operator 添加固定逻辑名称。
- 使用现有命名函数推导唯一 prefix 实现；禁止散落两份 formatting 规则，ordinal 0 不省略。
- create/open 允许短而直白的重复资源调用。共享物理名 helper 可以是普通函数，不为去掉几行代码重新造资源 provider trait。
- 条件 Join 资源选择必须来自绑定中的一个配置事实，不能 create/open 各重写一套会分歧的条件。可共享无 I/O 的具体模式判断；不增加一个泛化 collection-kind 枚举。
- 固定名称改为算子内字符串常量；类型出现在实际 `create_data::<D>` / `open_data::<D>` 调用。布局测试是独立 oracle，不能从被测代码的同一列表生成全部期望。
- Store catalog 只检查 collection kind，不能证明不同 `OrderedMap<K,V>` 的 codec 相同。codec 与 tag 的对应继续由实现和 golden/reopen 测试守护，不声称新方案增强了该能力。
- 当前 DataInstances::finish 只拒绝传入集合中未消费的实例，不审计整个磁盘 catalog。新 API 没有可注入任意实例集合，消除了这类调用方注入错误，但直接装配代码仍可能错误创建未使用资源，类型系统不能阻止它。测试必须独立验证所有预期名称、已知禁止名称不存在（尤其 Join 三种布局交叉禁止的 counts），并审查每个 create/open 调用的句柄确实被 runtime 使用。因公共 Store 没有 catalog 枚举，本轮不宣称检测任意未知额外名称的全集合审计；该剩余风险靠完整源码审查。不要为了测试新建产品 catalog API，也不改变无关磁盘条目的既有容忍策略。

### 3.5 执行形式与资源配置

- 保留 Atomic / Turn 执行 trait；普通算子只实现 apply。
- 在统一 bind 校验 kind/body 的能力一致性，保留 Scan/Transform/Sink output 规则及 arity 检查。
- Exclusive 实例仍不可融合；**所有 atomic body，包括有状态 Aggregate，而非仅 AtomicReady**，都必须在构造后通过唯一 kind-based 最终适配步骤。具体 body 先构造 `Operation::Atomic` 或 `Operation::Turn`，统一校验/规范化：AtomicTransform + Atomic 原样保留，ExclusiveTransform + Atomic 使用现有 atomic-to-exclusive 适配器，合法 Turn kinds + Turn 原样保留，其他不匹配拒绝。纯 bind 预先根据 body 的执行能力做同一合法性校验；构造后的步骤是防御验证，不将可静态拒绝的问题拖到 setup。不得用“无状态 ready 变体”替代能力判断。
- `RuntimeResource` 的 Any/TypeId 保留为唯一必要的配置注入边界。具体 type 预检与消费需由同一具体绑定分支配对，并有 wrong/missing/unexpected 测试。
- 只有 Station 首项取得 resource，尾项收到 none；未知 Station ID 在 setup 前拒绝。
- 构造 target 只保存已验证参数，连接、建表、ACK 均保持现有运行期边界。类型匹配不等于配置值有效；保留各 concrete config 的既有校验和错误阶段。

## 4. 不可破坏的 build/open 时序

### 4.1 Build

1. 纯拓扑检查、稳定 Definition encode。
2. decode 为 canonical Definition，不用未解码对象执行。
3. 全图按拓扑传播 Schema；每个 Station 按 operation 顺序完成纯 bind。
4. 完成可纯验证的绑定结构、配置类型与未知资源 ID 检查。明确变更一项内部缺陷的发现阶段：删除声明表后，不再承诺在运行时、创建路径之前检查固定资源常量是否重复；这改由独立布局测试、源码审查和 setup 重名拒绝守护。若代码缺陷造成固定名称重复，允许留下未发布的 incomplete build，绝不能发布 Definition。任何用户输入派生的名称/布局选择错误仍须在纯阶段预检。不要把测试时检查写成运行时前置保证。
5. `Store::setup` 后顺序创建 definition Cell、Station 资源和具体算子资源。
6. `setup.commit` 唯一一次原子发布 catalog、Station active/subscriptions 初始值、Definition。
7. consuming split，组装已有 Station，交给运行期 Flow。

`StoreSetup::create_data` 要求 `&mut self`；不能同时保存多个捕获同一可变借用的 creator 闭包。资源创建不放到 commit 回调里；回调只能初始化和写入。

### 4.2 Open

1. 拒绝携带新拓扑的 open 请求。
2. `Store::open`，短只读 snapshot 读取 Definition owned bytes，立即结束该 snapshot。
3. decode；比较 owner identity，包括 Some/None，失败在 bind 和资源消费前退出。
4. 全图纯 bind 和运行配置预检。
5. 通过 `&Store` 顺序打开必需 collection，不创建、不修复、不重建。
6. 单独短 snapshot 校验 Station active/subscriptions 等既有状态。
7. snapshot 完全释放，再 consume Store 成为运行事务能力。

不让绑定或 runtime 保存 snapshot。open 的“同一 setup 生命周期”不是让一份 snapshot 活到所有初始化结束。

## 5. 前置验证矩阵：P1 必须全部通过

P1 用真实产品算子，不添加 test-only Operation。验证可在临时开发分支中进行，但不合并实验目录、不保留长期双轨 API。基线数据只写 tempfile/隔离测试路径，不触碰用户数据库。

| Witness | 风险 | 必须证明 |
|---|---|---|
| Filter/Select/SchemaAlign | 无状态、空投影、Exclusive 资格 | 输入精确 Schema；零列保持行数/diff；binding kind 与 runtime 一致 |
| RunningEventCount | Cell 与观测语义 | 原资源名和 codec；重开计数；忽略 diff 的事件计数不变 |
| Distinct | 单状态 | typed create/open，负前缀、overflow、回滚、reopen |
| Aggregate | 私有 codec、多状态、编译字段、实例资格 | 三资源精确类型与名称，完整 bound 字段迁移；加入因表达式资格而必须 Exclusive 的真实 Aggregate witness，构造后必须为 Turn；原极值/分组字节不变 |
| EquiJoin | 条件资源 | Inner 三资源；非 Inner 无 residual 多 key_counts；有 residual 多 match_counts；不得串用 |
| AsOfJoin | 宽索引/continuation | 三资源与恢复页顺序不变 |
| SQLite buffered Sink | 无 RuntimeResource、嵌套 target 泛型 | 已编译 target 构造不连接、不建表；Prepared 重放保持 |
| Postgres Sink + PG/MySQL CDC | 外部配置与 linear Delivery | 缺/错/多资源前置拒绝，凭据不进入 Definition/debug；构造无外部 I/O；旧 turn 协议不改 |
| Flow head+tail | 多个 operation ordinal | 精确资源名、尾项无 resource、尾项错误整体回滚 |
| SQL / system hosts / benches | 公共装配 API 下游 | P1 盘点全部消费者，代表性外部 correctness fixture 与 Flow build/open 对新签名编译通过，冻结全量迁移映射；全部消费者编译与无旧 API 残留是 P2 门槛，不提前要求 P1 完成 P2 |

P1 验证输出必须包括：完整 BoundBody 变体清单；新增/删除类型与机制清单；create/open 签名编译证据；上述 witness 用例映射；复杂度与风险评审结论。

**失败条件**：需要把 Store 塞给 Definition/runtime；需要 Flow 匹配具体算子；出现第二套资源字典或工厂；改动 tag/codec 才能装配；无法保留错误阶段；必须通过新宏/HRTB framework 才能实现直接构造。遇到任一项停在 P1，提交具体设计修订，不继续半迁移。

## 6. 分阶段执行工作包

每个阶段在自己的 diff 中可审查；运行期重构不得与存储格式变化合并。以下内容保留实施前工作包定义；完成证据见执行记录。

### P0：锁定基线与批准装配边界

**输入**：本计划、AGENTS.md、operation/flow README、TESTING.md、当前 git 状态。

**修改**：架构约定与测试证据清单；不改业务算法。

**任务**：
- 确认基线是否漂移，不覆盖用户未提交改动。
- 记录完整 gate 基线，若已失败必须分清既有故障，不能宣称重构引起/解决。
- 按 §3.2 精确更新装配责任约定，保留其他所有事务/持久/安全规则。
- 盘点所有 data/materialize 调用点：operation/flow/sql、benches、system hosts、README/doc tests。记录 stable tag、资源名、kind、codec、conditional layout 的独立期望。
- 装配的 before/after 示例写在 operation README 草案中，不能靠新增大篇术语说明来解释新框架。

**退出门槛**：无未解决架构指令冲突；基线测试记录可追溯；独立 reviewer 确认边界变更范围。

### P1：前置技术验证和设计冻结

**依赖**：P0。

**修改范围**：临时开发分支中的 operation 装配代码、现有 correctness 域。不要创建仓库级 experiments/。

**任务**：
- 实现 §3 草案的最小真实原型并完成 §5 所有风险类别。简单 Distinct 编译通过不能替代其他 witness。
- 独立完成 build/open signature 的 Rust 1.96 编译；验证 owned collection 可跨 setup 生命周期、短 snapshot 不逃逸。
- 将未知变体/参数字段补齐；冻结错误映射和 runtime-resource 校验表。
- 比较删除的 DataName/DataDeclaration/DataInstance/DataInstances、函数指针、工厂闭包与新增枚举/参数结构；明确保留的 RuntimeResource 擦除。
- 类型数不是唯一指标：检查新人从 bind 到具体构造是否只需一个直接分派，是否仍需跨字典/回调追踪。

**退出门槛**：所有 witness 与 borrowed protocol 编译测试通过，三类独立审查通过。将临时原型整合为 P2 工作起点；失败时不能交付为新默认路径。

**禁止**：只画设计图后说 P1 通过；把测试删除当作 API 迁移完成。

**P1 的边界**：允许在不交付的临时分支中为代表性 witness 保留旧调用点，以便验证新 API；这不是允许 P2 结束时保留兼容层。必须冻结的内容是所有变体的字段/能力/资源表与代表性可运行路径，而不是提前完成所有下游重写。

**明确的失败退路**：若 P1 证明枚举/直接装配不值得，撤下未发布原型，恢复基线装配机制。低风险替代只做现有 definition.rs 的职责整理、普通循环、具体 typed constructor 和源码导读，保留原 DataInstances/materializer；不宣称满足真实减层或提升性能。将失败证据及这一替代的较小收益提交用户确认，不能自行宣布原目标达成。不得中途自动切换到 scoped facade，把另一套架构无审查地塞进同一路线。

### P2：一次完整的装配 API 切换

**依赖**：P1 完整通过。

**修改文件域**：`crates/operation/src/{definition.rs,lib.rs,resource.rs}`、新增 setup 模块、各 concrete binding；`crates/flow/src/build/{mod.rs,open.rs,schema.rs,codec.rs}`；所有下游测试/bench/host 调用点与文档。

**任务**：
- 全量接入已冻结的直接装配路径。迁移可以在工作分支逐个完成，最终提交不能含 Legacy variant/兼容 alias/双轨 dispatch。
- 删除 data 擦除机制及 materializer closure；保留统一 bind 的 Schema、arity、kind/output/能力校验。
- Flow build/open 改为清晰普通循环，减少嵌套 zip/map；仍不枚举具体算子。
- error mapping 保留 Station 上下文及完整物理名；不泄漏 runtime config。
- 测试改为真实 setup/open 路径。布局错配、缺少磁盘资源、错误类型/配置、负前缀/事务失败测试不能丢。
- `tests/correctness/support.rs` 当前接受任意 physical_names 映射；改为单个 prefix + 实际稳定逻辑名，不保留重映射兼容层。fixture 从 Store::create 后 create/open 改为 Store::setup → 新 create → setup.commit。StoreSetup 没有 open/read API；需要 probe/corruption handle 的 fixture 必须完成 staging 后释放运行能力，再 Store::open 并在 into_transactions 前取得 owned typed handle，或者使用创建时明确返回给测试的本地 handle，不能扩大产品装配返回值为测试服务。CDC/Sink fixture 尤其要检查独占 Store 生命周期。
- 公共 doc(hidden) API 也是下游编译依赖；`lib.rs` exports、Rustdoc 示例、benchmark support 和 system host 都必须迁移，不能只让库本身编译。
- 原 MissingData/WrongDataClass/UnexpectedData 注入容器测试按新不可表达性删除或改成相应 typed open/layout 测试，并在变更说明中逐项列出替代证据。
- 旧 format/tag/golden/layout 不得变化。临时目录生成基线数据库，用新实现 reopen 相同 Definition 与场景；基线 fixture 只在测试隔离路径保留，不提交数据库。

**退出门槛**：完整 workspace gate 与 bench test mode 通过；运行配置相关系统 witness 按环境执行；没有旧装配引用（历史 plans 文档引用除外）；产品持久字节未变。

**提交建议**：`refactor(operation): construct typed state without erased data assembly`。允许多个可构建提交，但不允许发布部分迁移为完成。

### P3：Aggregate 关系直化与不改语义的局部优化

**依赖**：P2；不与装配改动同时进行。

**修改域**：aggregate definition/runtime/tests/bench。

**具体设计**：
- 每个 BoundLayout 预计算自己对应的 min_slot/max_slot（或等效固定索引）。保持现有全局 slot 编号、去重与 codec 顺序。
- `caches_key/promote_cached_extreme/refresh_cached_extreme` 不再每次扫描所有 slots 寻找 layout。重复 `MIN(x)` 仍共享 slot，`MIN(x)/MAX(x)` 共享 layout。
- 主 apply 保留按输入事件顺序的主循环；提取少量具体私有函数，不能用 visitor/pipeline/strategy trait 藏主流程。
- 不增加 AggregateState 封装把现有具体 groups/entries/control 再包装一遍。
- 保留 group death 残留分区清理、group ID 不复用、缓存更新与 entries 同事务。
- 保留每事件旧行 -1/新行 +1；不把一批事件净额合并后替代输出。

**测试/性能**：新增不同表达式形成多个真实 layout 的 case；当前重复 `col(value)` 的 MIN/MAX 会去重，不能作为多 layout 基准。覆盖宽 Utf8/Binary、NULL、重复 call、撤回缓存极值、组消失、回滚/reopen。比较同机相同 fixture 的 Criterion raw results，不能只验证 Action shape。

**退出门槛**：输出序列与持久状态等价；无 codec/layout 改动；性能未出现未解释的回退；主循环比原来更可顺读。

### P4：表达式重复与复杂算子可读性

**依赖**：P2；为减少 shared-file 冲突建议 P3 后执行。

**任务**：
- Select/SchemaAlign 只共享表达式求值和 RecordBatch/Change 构造的短函数。保留每个算子的 exact Schema guard（空表达式时不能靠 evaluate 校验）、错误上下文、metadata/nullability 差异。
- 复用已有 codec 的相同长度前缀字符串逻辑，先验证 framing 完全一致。不新建 codec registry。
- EquiJoin、AsOfJoin 各自保留主阶段推进 match；重排或提取已有职责的普通函数。若要拆文件，入口必须能看见 phase、下一 phase、Commit/Complete 条件；不把两者共用成 PagedTurn。
- Buffered sink 区分 durable Initialize/Ready/Prepared 和 transient load/plan/deliver 状态，去掉重复转换、让阶段跳转集中；不得合并为一个状态枚举，不新增 Coordinator/Ledger 对象。
- 保留 Atomic-head 唯一适配，消除重复的小段准备逻辑；不改 borrowed work/AfterCommit 生命周期。

**执行方式**：表达式、EquiJoin、AsOfJoin、buffered 分开 diff；各自只在能够明确删除间接调用/重复判断时才提取。无需为了清单完成而强行移动每个文件。

**退出门槛**：独立审查能沿一个入口指出所有状态变更与恢复点，现有 correctness/resource benchmarks 无回退。不能以文件行数下降代替可读性证据。

### P5：测量剩余成本；只实施证据充分的性能改进

**依赖**：P3/P4 稳定。此阶段不是持久布局重写许可。

**任务**：
- 在 owner benchmark 内补 Distinct 重复/唯一/宽行、正负 diff workload；按现有 manifest 手动注册 bench，更新 TESTING.md。
- Aggregate 扩展热点组、多个真实 layout、宽 key；Join/Sink 复用现有资源 runner，不造全局 metrics framework。
- 区分 logical state bytes、编码字节、Rust heap、RocksDB native heap、WAL、磁盘文件大小；未测量项标为 unavailable，不相互替代。
- Store 操作计数只在有现成局部观测点时增加 test/bench instrumentation；不能为了指标污染稳定 Store API。
- 若批内重复读写确为热点，先单独设计一个有明确字节/条目上限的 Distinct transaction-local cache。输出仍每事件决定；每次事件都 checked arithmetic，负前缀先失败；flush 的 net delta 不能假定总能装进 i64（u64 状态差可能超界）。超预算必须有同事务、确定性的 flush/退回原路径，不能跨事务保留缓存。
- 缓存优化必须与无缓存基线做序列、状态、回滚和切批 metamorphic 对照；收益不明确或实现反而更绕则明确不合入。

**不自动实施**：Aggregate group cache、row dictionary、共享 blob、frontier cache、清理 continuation、output hard cap。这些只产出独立建议及量化证据。

**退出门槛**：测量报告完整；任何已合入优化都有稳定语义证据。没有优化收益也可以如实结束 P5，不为达到“优化”名义强改。

### P6：最终接手验收

- 更新 operation/flow README 与相关 Rustdoc；旧 AGENTS 装配描述全部一致。
- 用 Distinct 给出完整“定义 → bind → create/open → apply”的源码阅读路径；用一个 Sink 解释外部副作用为何需要额外协议。
- 新人验收：不阅读通用字典/闭包工厂即可指出状态字段、创建位置、执行入口、事务是谁提交。复杂算子要求理解主阶段，不要求新手立即证明全部关系算法。
- 审计已删除符号、兼容层、遗留分支；核对稳定 tag/names/golden 和编译依赖未意外变化。
- 三个独立 reviewer 审查完整 diff：抽象/职责、正确性/恢复、资源/性能。主 Agent 逐项核实并处理 findings，不能只收集报告。
- 完整 gate、bench test mode 与适用真实系统测试完成后才宣布实施完成。环境缺失标 blocker/未验证，不能等同通过。

## 7. 验收命令与证据规则

### 7.1 每个可交付阶段的基础命令

使用仓库指定 Rust 1.96，先 `rustc --version` 核对。以下命令需在仓库根目录执行，执行者检查每个退出码。

```bash
cargo fmt --all -- --check
cargo test -p dogpaddle-operation --test correctness
cargo test -p dogpaddle-flow --lib
cargo test -p dogpaddle-flow --test correctness
cargo test -p dogpaddle-sql --test correctness
cargo test -p dogpaddle-operation --doc
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask check
cargo test --workspace --benches --locked
```

测试名以当前 manifest 为准；若基线之后重命名，先查 manifest，更新记录，不静默跳过。

### 7.2 相关性能 smoke

```bash
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench aggregate_extrema
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench equi_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench equi_join_resources
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench asof_join
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench asof_join_resources
DOGPADDLE_PERF_PROFILE=smoke cargo bench --locked -p dogpaddle-operation --bench buffered_sink
```

smoke 证明可运行，不等于可靠性能结论。reference 使用显式绝对 `DOGPADDLE_PERF_ROOT`、相同主机/工具链/固定文件系统，记录基线与改后 revision，fixture/预热/oracle 在计时外。性能回退要复跑排噪并解释，不设置无依据的统一收益百分比。

真实 connector gates 按 `TESTING.md` 和对应 system-tests README 的参数/环境执行：`system-tests/postgres/check_cdc.py`、`check_sink.py`、`check_sql.py`，`system-tests/warehouse-sinks/check.sh`，以及现有 MySQL/独立 Debezium workspace 适用 gate。不得为跑通测试更换依赖版本或硬编码凭据。

### 7.3 行为与恢复检查清单

- build 的纯校验失败无路径副作用；canonical decode 使用正确对象。
- owner identity mismatch（包括 Some/None）不进入 bind/装配。
- 资源名包含 station/operation ordinal；正确 collection kind/codec；缺资源 open 不自愈。
- 多输入 durable pin、重复边 subscription、fan-out retention 不变。
- Atomic tail 失败、output schema 错误、背压、commit failure 全部回滚；None 只停止尾链。
- Commit 不确认输入，Complete 原子确认；AfterCommit 在提交后运行，error/panic 全局 fail-stop。
- 空投影保留行数/diff；精确 Schema metadata/nullability 保持。
- 输入事件负前缀、overflow、切批与分页等价性；Aggregate 例外仍按分组/参数校验。
- EquiJoin 所有 conditional layouts、Probe/ClearShadow/Emit 恢复；AsOf RHS rematch。
- buffered Prepared 固定 ID 重放、reopen 全 buffer 校验、外部 I/O 不占 Store 事务。
- CDC 私有快照发布和 ACK 时序不变；不建立共享 CDC driver。

## 8. 并行与交付控制

- 一个集成人员负责 definition/setup/Flow 公共接缝，禁止多个实现 Agent 同时改这些文件。
- P1 的设计与反方可以并行只读；P2 concrete 算子迁移只能在公共签名冻结后按独立文件分工，最后统一集成。
- 不并行运行会争抢 benchmark 资源的性能任务；不要混合不同编译参数导致文件锁竞争却报告为性能回退。
- 每阶段记录：输入 revision、改动文件、删掉的机制、保留约束、测试命令/退出码、审查 finding 与处理、未验证项、下一阶段是否放行。
- 失败只撤销本阶段由 Agent 制造的变更；不得 reset/clean 用户工作、不删除现存数据库。未发布的工作分支允许重整，但必须保留证据记录。
- 执行许可来自用户后续明确的实施授权；本次“落地 roadmap 文件”不是产品代码修改许可。

## 9. 对抗审查与裁决记录

本设计由主 Agent 核对源码，另有两个独立只读 Agent 从直接装配设计、事务/类型/恢复反方检视。此处记录设计论证，不冒充已编译验证。

| 争议 | 裁决 |
|---|---|
| 是否立刻 enum 替代全部动态分派 | 否。只替代启动 materializer，保留 Definition/运行 trait 和 RuntimeResource。 |
| direct setup 是否符合当前 AGENTS | 不符合原装配条款；P0 必须显式修订责任边界。 |
| 是否只拆 definition.rs 就算完成 | 否。可作为阅读整理，但不满足真正减层目标。 |
| 是否采用 scoped typed facade 替代字典 | 未选。该方案确实可删除 erased data transport，但保留 materializer FnOnce、纯 DataSpec/TypeId 清单、used-name 完整消费检查，并新增 scoped capability/lifetime。它是合理备选，不是无效设计；本计划为更大幅度减少概念选择直接装配，并明确接受固定布局内部错误可能延后至 setup 发现的取舍。 |
| 最终文档审查发现哪些问题 | 已修正 P1/P2 全量迁移的循环依赖、stateful Aggregate Exclusive 适配遗漏、固定布局重复名检查阶段歧义、无 catalog 枚举时的资源全集合审计过度承诺，并补充 P1 失败退路。 |
| 枚举是否可能扩大复杂度 | 是。P1 必须覆盖所有复杂 witness 和新增/删除机制盘点；失败停，不靠双轨补丁。 |
| 是否能省掉 create/open 区分 | 否。build 是可变 staging，open 是只读打开；保留直白分支比新 Setup trait 更简单。 |
| 是否删 AfterCommit/PreparedTurn | 否。borrowed linear work 与事务权限不是重复概念。 |
| 是否为统一 data 资源引入 collection-kind enum | 否。kind 不能表达具体私有 codec，会再造一次类型擦除。 |
| 资源字典 extra 实例检查如何保留 | 新 API 不再接受 arbitrary injected data，静态具体构造替代该注入风险；不谎称原系统做过全磁盘 catalog 审计。 |
| 性能/空间收益是否已测量 | 未测量。本文件不保证吞吐倍数或磁盘节省比例。 |

## 10. 完成定义

完成不是“所有文件都重命名过”，而是：

- [x] 旧 data 擦除/字典/materializer 链已真实删除，无兼容路径。
- [x] 具体算子的状态创建与执行依赖可直接追踪。
- [x] build/open 时序和执行恢复协议保持。
- [x] Aggregate layout/slot 的反查关系被直接表达，持久布局不变。
- [x] 表达式/Join/Sink 的改动有实际阅读收益，没有通用框架增殖。
- [x] 数据与性能结论有独立验证，未知项未被粉饰为通过。
- [x] 文档、测试、bench、system hosts 与 API 一致。
- [x] Rust 1.96 全 gate 与独立审查闭环（findings 已修复并复核）。

实际实施与验证记录见 `operator-simplification-execution.md`；真实 PostgreSQL/JVM/warehouse 外部系统 gate 未在本地 Cargo gate 中运行，未作虚假声明。
