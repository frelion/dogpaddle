# Operation 定义与表达式契约

本文件是该领域的维护契约；用法和阅读入口见 [crate README](../README.md)。
修改实现时同步更新本文件及其所属测试。

## 构造入口

具体 `XxxOperation` 运行类型及其构造入口只在 operation crate 内可见；公共调用方通过 `OperationDefinition::construct` 的统一 checked 入口得到 `Operation`，不另设手工装配入口。

`OperationDefinition` 是内建算子的封闭 enum，只集中给出纯 `input_count()`，供无句柄 Schema 编译与构造前 arity 检查。具体 Definition 只保存自身计划数据。构造得到的 `Operation::{Atomic,Paged,Source,Sink}` 是角色和融合资格的唯一权威，不另维护平行 role enum；Flow 绑定后据此推导融合和调用深度。
head 可接非空或空的单输入 Atomic 尾链，融合索引只在内存中存在，不拥有独立 ID、持久资源或生命周期。
PagedTransform 使用借用 Resume 的 step；未提交页从未变化的真实状态和帧位置重算。
Source/Sink 使用具体 capture/delivery 数据协议，不参与通用 prepared/callback 执行接口。
普通关系表达式统一要求 immutable、逐行可执行；Definition 构造与 decode 都执行相同准入规则，不能借恢复绕过。
Filter、Select、Aggregate 固定构造成 Atomic；Aggregate 同时检查 group expression 与 call argument。
`OperationDefinition::construct` 把有序、精确的 input logical `SchemaRef`、已限定资源名范围的短期 `DataScope` 和首 Operation runtime resource 一次性构造成最终运行 `Operation` 与精确 output Schema。
入口统一校验 input arity、全部 input/output DogPaddle Schema、runtime resource 类型，以及最终 runtime 能力与 output 存在性；各具体 Definition 的私有 `construct_unchecked` 只实现自身规则、表达式编译、类型化状态句柄取得和最终 runtime 构造，外部调用方不能绕过公共校验。
资源前缀由调用方通过 `DataScope::scoped` 限定；具体 Definition 只向 `DataScope::data` 传固定逻辑名，不拼接全局资源名。Store 声明/查找错误透明传递，保留完整资源名。
Source 接收空 inputs；checked construct 根据最终 runtime variant 要求 Source/Transform 有完整 output Schema、Sink 没有 output。纯 output_schema 只验证存在的 Schema，不构造 runtime 或维护平行 role 分类。
构造不得读取业务状态、开始事务、访问外部系统、时间或随机性；相同持久化算子名、计划数据 与有序 input Schemas 必须维持相同 Schema、状态资源集合和执行语义。

## 计划表示与持久化

`OperationDefinition` 是唯一的封闭类型分发和公开 Serde JSON 计划表示，以稳定 snake_case 算子名标记，例如 `{"filter":{"predicate":"..."}}`。Operation 不再拥有独立持久 envelope、encode/decode API 或 codec 错误类型；Flow 直接嵌入这份计划，统一拥有版本、完整消费、canonical 字节比较、总长和 checksum。没有数字 tag 目录、逐算子解码分发或第二份 Payload 类型；新增算子只扩充已有 enum 与能力分发，不增加注册表。

原生反序列化拒绝未知字段和非法结构；`UnionAll` 的非零 arity、CDC 非零容量和 Aggregate 六变体调用由类型表达。CDC 的 Arrow `Fields` 在反序列化时复用源 Schema 的浅类型、名称与空 metadata 校验，保持源支持列域；不引入另一套字段表示或 codec。`StoredExpression` 保留完整 protobuf roundtrip、canonical 与 immutable/row-local 证明；表达式不是未经验证的计划字节。独立调用 Serde 不承诺 JSON 字节唯一化或错误脱敏；持久 Flow 的 JSON 错误只保留静态类别和位置，Display、Debug 和 source 都不保留原始输入。

具体 Definition 是待绑定的计划数据。除上述 CDC 列域外，公开 JSON 反序列化与持久解码不证明非空 join keys、group list、目标身份/路径或 Schema 业务规则；这些约束在同一个纯验证/编译路径执行，`output_schema` 与 `construct` 都必须经过，且早于取得 Store 数据句柄或任何外部 I/O。便利构造器也复用该定义验证；不存在受信任 Definition 包装层。各 owner 的大小上限（CDC/远端 Sink 1 MiB、ASOF 字段上限）保留于该路径。单独调用 Serde 反序列化可以暂时持有超 owner 上限的待绑定计划；它不再提供每种 owner 的提前字节准入。Flow 在解析前仍限制整个持久 Definition 不超过 8 MiB。

修改计划格式直接更新当前 v1 golden 和 reopen 证据并重建旧状态，不提供旧数字 tag 识别、alias、fallback 或迁移。算子 correctness 覆盖稳定名称、literal golden、资源布局、construct、step 和适用的 reopen。
Flow 只按机制保留代表性 witness；只有新增 arity 或 Schema propagation、runtime-resource 方向、外部副作用边界、持久 data/Resume 或 recovery 阶段时才增加 Flow case，不逐算子复制同一 build/open/reopen 矩阵，也不得用 test-only Operation 代替真实产品语义。

## 表达式

Filter 与 Select 的公共入口直接接收 DataFusion `Expr`；fallible `try_new` 使用 `datafusion-proto` 编码表达式，Definition 在 JSON payload 中以 base64 保存这份 protobuf，并在 decode/open 时用同一 DataFusion 版本还原。
DogPaddle 不再维护另一套表达式 AST、operator/type/nullability 规则或递归深度限制。
Schema bind 必须通过 DataFusion `create_physical_expr` 建立 exact-input-Schema-bound `PhysicalExpr`，表达式类型、nullability、cast 与运行期 `evaluate` 语义均以 DataFusion 为唯一实现；该 API 假定 logical coercion 已完成，Operation 层不运行 logical/SQL planner，也不额外插入隐式 cast，直接调用 Operation API 时需要显式 `cast`。
DogPaddle 只负责 Definition/Flow 边界、完整 Schema guard 及 Change 语义。

DataFusion Expr protobuf 是 Expression payload 的版本绑定格式，不承诺跨 DataFusion 版本兼容。
工作区全部 DataFusion direct/transitive crate 必须精确 pin 到 `b631f2c7d92d0a38637a8d8ae980e2474b891f34`，并保持唯一 Arrow 60.0.0 与 sqlparser 0.63.0 类型族；升级时必须审查 ASOF logical lowering、proto roundtrip、physical planning 和执行语义。
开发期持久格式始终按 v1 处理；DataFusion 升级若改变 payload 或执行语义，应更新 v1 golden 与 reopen 证据，并重建受影响的 Flow，不增加旧表达式识别、迁移或兼容分支。
Filter output Schema 精确等于 input，只保留 non-null true；全删返回 `None`，部分筛选必须用同一 predicate 保持 records/diffs 对齐。
Filter 与投影不声明 Operation data；公共证据覆盖 proto golden/roundtrip、静态拒绝无目录副作用、decoded Definition 的 construct/apply、open 重新构造、Filter 空/全量/部分选择与混合 diff 重批，以及投影 Schema metadata/nullability 和 Array/diff 共享。

Project/Extend 的独立 Definition 和运行实例已删除。
选列与改名直接使用 `SelectDefinition::try_new([(name, Expr), ...])`；追加列使用
`SelectDefinition::try_extend(&input_schema, fields)`，它立即展开为普通 Select 字段列表，不保存 mode、输入 Schema 或单独的持久格式。

Select 直接构造私有 `BoundProjection`，由它实现 `AtomicOperation`，不另设算子运行包装类型：同组表达式共享 `DFSchema`，执行时整组检查一次 exact input Schema（空投影也检查），运行错误使用公共 `ProjectionError`，保留 port、逐字段错误上下文及底层错误链。

## 简单 Transform

RunningEventCount 只声明 `running_event_count.count: Cell<u64>`，固定输出 non-null `UInt64` 字段 `count`。
它按输入行序观察每一行并将 durable count 加一，忽略 diff 数值，输出每个更新后的 count 且 diff 固定为 `+1`；它是事件观测算子，不是关系 cardinality Aggregate。
公共 API 与资源路径的破坏性重命名不提供 alias、fallback 或迁移；旧数据库直接删除并重建，不为旧版本增加识别或兼容协议。

Select 以有序 `SelectField<E = Expr>` 列表一次性计算完整 output；字段直接保存 name、expression 与可选 nullable/metadata 覆盖，普通 `(name, Expr)` 通过标准 `From` 转为无覆盖字段。`try_new` 接收可转为字段的集合，只把每个 Expr 转成一次 `StoredExpression`；Definition 复用同一字段 ADT 保存 canonical 表达式，不另设 Payload、mode 或 SchemaAlign 算子。
所有表达式都绑定到同一个原始 input Schema，不能引用同一 Select 新建的别名；空 Select 合法并保留输入行数与 diff。字段类型只从绑定表达式推导，cast/try_cast 必须写在 Expr 中；nullable 的 None 继承表达式推导，Some 只允许 non-null 到 nullable 的放宽，拒绝收窄。
字段 metadata 的 None 使直接列引用（包括改名）继承源字段 metadata，计算列为空；Some 精确覆盖，Some(empty) 清空。Schema metadata 默认继承输入，可用消费式 `with_metadata` 精确覆盖或清空。覆盖使用 Arrow 的 typed Metadata，按 key canonical 排序；条目数量、文本总量与保留 namespace 统一归完整 output 的 `validate_schema` 检查。typed map 接收已唯一化的键，不另维护 iterator 重复键诊断；Flow 持久 codec 的 canonical 比较仍拒绝 JSON 重复键和其它非 canonical 字节。
纯列引用共享输入 Arrow arrays，全部投影共享 diff buffer；保留完整子树，字段重排无需复制数据。
绑定后的严格递增纯列引用若输出 Schema 精确等于对应输入投影，自动复用 ChangeProjection 快路径，避免重扫已验证的 diff 和 Decimal 值；改名、重排、覆盖与计算按输出 Schema 使用共享执行实现。
Select 开发期 v1 payload 省略 None 覆盖，显式保留 Some(false) 与 Some(empty)；普通 Select 的 JSON payload 字节保持不变。旧 SchemaAlign variant 不识别或迁移；使用该 variant 的 Flow 与受影响目标直接重建。
UnionAll Definition 只保存非零 input arity，要求所有输入具有完全相同的 logical Schema，按端口原样转发 Change。
Select 与 UnionAll 都不声明 Operation data，也不引入 planner、额外表达式层或专用 Flow 抽象。

## 不可重放表达式的准入与能力审计

当前锁定的 DataFusion revision 上，`Expr::from_bytes` 使用 `TaskContext::default()`：scalar function registry 为空，默认 logical extension codec 也不能重建 ScalarUDF。`random`、`uuid`、`input_file_name`、`file_row_index` 是 volatile，`now/current_timestamp`、`current_date`、`current_time` 是 stable；它们与外部 UDF 一样，不能通过当前 Rust Definition 的完整 round-trip。不能从 SQL 对这些函数的拒绝推断 Rust API 行为；这里的结论来自锁定源码的 decoder 路径及三种 volatility 的 codec 拒绝证据。

此次没有发现能够经过当前默认 codec、绑定并执行的 stable/volatile ScalarUDF。已有 UDF 仍先报告编码/解码错误；immutable 标记也不能替代函数注册。未绑定 Placeholder 等部分 Expr 过去可以持久化并被分为 Exclusive，随后在绑定时失败；现在 Definition 构造和 decode 直接拒绝非逐行可重放节点。嵌套节点同样检查。已准入的表达式继续由 DataFusion 负责类型、nullability 和求值，不引入第二套 AST。

`ExpressionDefinitionError::NonReplayable` 表示这项新准入失败，各 owner 包装错误仍保留字段、key 或 argument 位置。现有状态不做 alias、迁移或自动修复；旧 tag、continuation 或精确布局受影响时使用新的 state 与目标。
