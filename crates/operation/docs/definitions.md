# Operation 定义与表达式契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## 构造入口

具体 `XxxOperation` 运行类型及其构造入口只在 operation crate 内可见；公共调用方通过具体 Definition 的统一 checked `construct` 得到 `Operation`，不另设手工装配入口。

`OperationDefinition` 是 operation crate 内的 sealed trait；每个具体 Definition 实例必须手动声明完整的 `OperationKind::Scan`、`AtomicTransform(NonZeroU32)`、`TurnTransform(NonZeroU32)`、`ExclusiveTransform(NonZeroU32)` 或 `Sink(NonZeroU32)`，role、融合资格与非零 input arity 不能从拓扑位置、空 data 或具体 tag 反向推断。
每个 Station 包含非空、有序的普通 Operation 列表：首项可以是 Scan、任意 arity 的 Atomic 或 TurnTransform，之后只能是单输入 Atomic；Exclusive 与 Sink 必须独占。
TurnTransform 使用完整 turn/continuation 协议，但其未提交 turn 必须能从未变化的 durable state 重放，因此允许吸收 Atomic 尾链；Exclusive 表示必须先形成独立持久化输出边界。
首项决定 Station 的输入角色与 arity，末项决定 output 属性，Flow 只校验 Station，不能枚举具体算子。
表达式 Operation 的实例级资格只决定能否融合；不合格实例仍走原有独占绑定与执行路径，本次不扩大或收紧表达式支持。
Filter、Extend、Select、SchemaAlign 检查自身全部表达式，Aggregate 同时检查所有 group expression 与 call argument。
具体 Definition 通过 trait object 上不可覆盖的统一 `construct` 入口，把有序、精确的 input logical `SchemaRef`、已限定资源名范围的短期 `DataScope` 和首 Operation runtime resource 一次性构造成最终运行 `Operation` 与精确 output Schema。
入口统一校验 input arity、全部 input/output DogPaddle Schema、runtime resource 类型、kind/output 与执行能力一致性；private sealed `construct_unchecked` 只实现具体算子规则、表达式编译、类型化状态句柄取得和最终 runtime 构造，外部调用方不能绕过公共校验。
资源前缀由调用方通过 `DataScope::scoped` 限定；具体 Definition 只向 `DataScope::data` 传固定逻辑名，不拼接全局资源名。Store 声明/查找错误透明传递，保留完整资源名。
Scan 接收空 inputs，Scan/Transform 必须给出完整 output Schema，Sink 必须没有 output。
构造不得读取业务状态、开始事务、访问外部系统、时间或随机性；相同持久化 tag、payload 与有序 input Schemas 必须维持相同 Schema、状态资源集合和执行语义。

## 分类与注册

`scan`、`transform`、`sink` 分类模块必须能容纳任意多个算子，不得拥有或重导出分类级的单一 tag 或 decoder；稳定 tag 和 decoder 永远属于具体算子模块，公共 decoder 表按具体模块路径逐项注册。
新增内建 Operation 必须注册唯一稳定 decoder，并在算子自己的 correctness 文件覆盖 tag 唯一性、literal golden、资源布局、construct、turn 与适用的 reopen。
Flow 只按机制保留代表性 witness；只有新增 arity 或 Schema propagation、runtime-resource 方向、外部副作用边界、持久 data/continuation 或 recovery 阶段时才增加 Flow case，不逐算子复制同一 build/open/reopen 矩阵，也不得用 test-only Operation 代替真实产品语义。

## 表达式

Filter、Extend、Select 与 SchemaAlign 的公共入口直接接收 DataFusion `Expr`；fallible `try_new` 使用 `datafusion-proto` 编码表达式，Definition 直接持久化这份 protobuf，并在 decode/open 时用同一 DataFusion 版本还原。
DogPaddle 不再维护另一套表达式 AST、operator/type/nullability 规则或递归深度限制。
Schema bind 必须通过 DataFusion `create_physical_expr` 建立 exact-input-Schema-bound `PhysicalExpr`，表达式类型、nullability、cast 与运行期 `evaluate` 语义均以 DataFusion 为唯一实现；该 API 假定 logical coercion 已完成，Operation 层不运行 logical/SQL planner，也不额外插入隐式 cast，直接调用 Operation API 时需要显式 `cast`。
DogPaddle 只负责 Definition/Flow 边界、完整 Schema guard 及 Change 语义。

DataFusion Expr protobuf 是 Expression payload 的版本绑定格式，不承诺跨 DataFusion 版本兼容。
工作区全部 DataFusion direct/transitive crate 必须精确 pin 到 `82335b426d8851db6a7b965f3d43053c585cabfd`，并保持唯一 Arrow 59.3.0 与 sqlparser 0.62.0 类型族；升级时必须审查 ASOF logical lowering、proto roundtrip、physical planning 和执行语义。
若新版本不能读取或保持旧 payload 语义，必须 bump 外层 Operation Definition tag/version，并要求重建 Flow，不在同一版本内猜测或迁移旧表达式。
Filter 的 tag 是 5，output Schema 精确等于 input，只保留 non-null true；全删返回 `None`，部分筛选必须用同一 predicate 保持 records/diffs 对齐。
Extend 的 tag 是 6，每个实例只追加一个由 `name + Expr` 唯一推导类型和 nullability 的字段，保留 input FieldRef 与 Schema metadata，不接受调用者重复声明 Field/type。
二者都不声明 Operation data；公共证据必须覆盖 proto golden/roundtrip、build 静态拒绝无目录副作用、decoded Definition 的 construct/turn 语义、open 重新构造、成功 build/open/reopen、Filter 空/全量/部分选择与携带混合 diff 的重批、Extend Schema metadata/nullability 和 Array/diff 共享。

Select 与 SchemaAlign 的运行实例共用私有 `BoundProjection`：同组表达式共享 `DFSchema`，执行时整组检查一次 exact input Schema（空投影也检查），保留逐字段错误上下文和各 Definition 的独立 Schema/metadata/codec 规则。

## 简单 Transform

RunningEventCount 的 tag 是 2，只声明 `running_event_count.count: Cell<u64>`，固定输出 non-null `UInt64` 字段 `count`。
它按输入行序观察每一行并将 durable count 加一，忽略 diff 数值，输出每个更新后的 count 且 diff 固定为 `+1`；它是事件观测算子，不是关系 cardinality Aggregate。
公共 API 与资源路径的破坏性重命名不提供 alias、fallback 或迁移；旧数据库直接删除并重建，不为旧版本增加识别或兼容协议。

Select 的 tag 是 7，以有序 `name + Expr` 列表一次性计算完整 output，所有表达式都绑定到同一个原始 input Schema，不能引用同一 Select 新建的别名；空 Select 合法并保留输入行数与 diff。
UnionAll 的 tag 是 8，Definition 只保存非零 input arity，要求所有输入具有完全相同的 logical Schema，按端口原样转发 Change。
二者都不声明 Operation data，也不引入 planner、额外表达式层或专用 Flow 抽象。

SchemaAlign 的 tag 是 9，以有序 `name + Expr + target nullability + Field metadata` 和独立 Schema metadata 显式产生完整 output Schema；字段类型只从绑定表达式推导，cast/try_cast 必须写在 Expr 中。
它允许 non-null 到 nullable 的放宽，拒绝 nullable 到 non-null 的收窄；所有表达式绑定同一个原始 input Schema，空字段定义合法并保留输入行数与 diff。
metadata 按 key canonical 排序，重复 key 必须在构造期拒绝，不能静默覆盖。
SchemaAlign 不声明 Operation data，不提供隐式 coercion，也不为 SQL 或其他上层接口引入专用 Flow 抽象。
