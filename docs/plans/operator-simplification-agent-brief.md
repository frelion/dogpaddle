# 算子重构 Agent 执行与交接说明

主计划：[operator-simplification-roadmap.md](operator-simplification-roadmap.md)。本文件不能单独替代主计划；发生冲突以主计划的安全边界、P1 gate 和当前用户授权为准。

## 1. 直接交给实施 Agent 的任务

```text
你负责 DogPaddle 算子简化重构。先读取 AGENTS.md、TESTING.md、
docs/plans/operator-simplification-roadmap.md 和本文件。

先检查当前用户是否已授权产品代码实施；仅有“生成 roadmap”时只做文档。
已授权实施时，从 P0 开始，不跳过 P1 前置验证。

目标：删除持久 data 的类型擦除、字典运输和 materializer 闭包工厂；
采用 operation 内部具体绑定结果和直接 typed create/open。
保留纯 Definition/全图 bind、RuntimeResource 注入、Atomic/Turn 事务协议。
不引入 Engine/Coordinator/Setup trait，不修改稳定数据格式。

一个阶段只有测试、独立审查和证据记录齐全才能标为通过。
P1 必须覆盖全部风险类别，不能因 Distinct 能编译就启动全量迁移。
当前规划没有编译证明；你必须用真实 Rust 1.96 编译和行为测试补齐。

遇到方案不可行：停止进入后续阶段，报告具体签名/借用/语义矛盾与修订方案。
禁止偷偷保留 Legacy API 或用新的泛化框架绕过设计失败。
```

## 2. 人员与文件所有权

- 集成 Agent：definition/setup/Flow build-open 的唯一写入者；负责阶段状态和冲突处理。
- 算子 Agent：公共签名冻结后才按不同算子目录迁移；不得自行扩大公共 API。
- 审查 Agent A：抽象/职责/可读性；必须独立于被审实现作者。
- 审查 Agent B：事务、恢复、Schema、资源注入、持久 ABI。
- 审查 Agent C：内存上限、CPU/Store 操作放大、启动资源、基准口径。
- 审查意见由集成 Agent 逐项查代码核实；不能机械接受或只转述。

本轮设计用了两个独立审查者覆盖设计/反方，不能代替未来实施的三面完整 diff 审查。

## 3. 每阶段交接模板

```markdown
## 阶段 Pn
- 状态：未开始 / 执行中 / 验收失败 / 通过
- 输入 revision：
- 输出 revision 或完整 diff：
- 允许修改的文件：
- 实际新增/删除的机制：
- 持久字节变化：应为无；若有则暂停
- 已运行命令、退出码、证据路径：
- 未运行命令、原因：
- 审查 Agent 与风险面：
- Findings：文件/行号、严重度、复现条件、处理、复验
- 既有故障与本阶段新增故障分别列出：
- 是否允许下一阶段：是/否；依据
```

实施时将记录放在 `docs/plans/operator-simplification-execution.md`，只有事实证据才标通过。不写入 runtime memory 代替文件交接。

## 4. 审查提示词

### A：真正减层了吗？

```text
只读审查完整 diff，不替实现者辩护。
检查是否真实删除 DataInstances/erased factory/FnOnce materializer，还是换名保留。
BoundBody 是否仅承载启动参数，是否又引入 runtime/Definition/resource 多重总枚举？
新增泛型/宏/trait 是否必要？Flow 是否开始识别具体算子？
一个普通算子的创建路径是否仍要跨字典和闭包追踪？
给出具体文件/行号和可执行替代，区分个人偏好与阻塞问题。
```

### B：是否保持安全与恢复？

```text
只读审查完整 diff，重点检查：
canonical encode/decode → 全图 bind/resources → Store setup；
owner-before-bind；短 snapshot 生命周期；一次 setup commit 发布；
资源 ordinal 名称、条件 Join layout、kind 只检查 collection 而非 codec；
Exclusive Atomic 适配、尾项资源限制、None/Commit/Complete/AfterCommit 顺序；
错误路径不修复/重建数据库、输入负前缀/输出序列不变。
检查修改/删除测试是否真实被新证据替代。
每条 finding 要有失效路径、影响和定位，不凭名称推断。
```

### C：资源与性能是否被误报？

```text
只读审查完整 diff：检查 BoundBody 最大变体、owned 参数重复 clone、
setup/snapshot/连接生命周期、缓存额外内存与失败回滚、benchmark 计时边界。
不能把 get/put 数等同 fsync 数，不能把 logical bytes 等同磁盘/WAL/heap。
检查 Aggregate 多 layout benchmark 是否真的使用不同表达式，
以及优化是否保留每事件输出、负前缀、NULL/极值撤回和 group cleanup。
报告实证、推断和未测量项；不制造虚构性能倍数。
```

## 5. 停止条件

以下任一发生，先停止当前批次而非扩大设计：

- P0 架构约定仍相互冲突。
- P1 需要新通用工厂/字典才能通过复杂算子。
- 运行实例开始保存 Store、binding、Definition 或事务启动能力。
- 只有更改稳定资源名/codec 才能让新装配工作。
- 为测试方便保留旧注入 API、任意物理名重映射层。
- 既有错误或测试被删而没有新契约对应的替代证据。
- 把 Aggregate 清理分页、output hard cap 或存储字典混进无行为变更重构。
- 缺外部测试环境却宣称真实系统恢复验证通过。

不要 destructive reset，不覆盖用户变更，不删除用户数据库。需要清理的仅是本轮明确创建的隔离 tempfile/测试产物。

## 6. 交付给人的最后检查

让读者直接找到 Distinct 的：输入 Schema、weights 状态类型、创建/打开代码、apply、事务提交方。
再让读者找到 Aggregate 的：layout 与 min/max slot 对应、逐事件更新、清理发生点。
如果仍须先解释一套新框架才能回答，不能仅凭绿色测试宣称“新手友好”完成。
