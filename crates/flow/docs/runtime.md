# Flow 运行契约

本文件是 Flow 的执行、持久化和恢复约束。Operation 算法与外部源/目标的具体语义分别归其 owner。
持久格式处于开发期 v1；布局或融合规则变化必须重建受影响状态，不提供兼容识别、迁移或自动修复。

## 图与资源

`flow/definition` 是唯一持久 DAG。它保存 owner identity、每个 Operation 的稳定 ID、具体 Definition 和有序输入 ordinal；
格式以 `dogpaddle.flow\0`、big-endian u16 v1 起始，随后直接保存唯一 typed plan 的 canonical JSON，末尾 big-endian u32 CRC32 覆盖此前全部字节。每个节点直接嵌入 Operation 的 Serde 计划，不再包第二个 Operation envelope。
Definition 最多 8 MiB，Operation 最多 1024，每个 Operation 最多 1024 个输入端口（允许重复生产者），ID 非空、无 NUL、最多 1024 UTF-8 字节且全图唯一。
解码在解析前检查全图长度和 checksum；nodes/inputs 数组在解码第 1025 个元素之前拒绝，不先分配完整数组再计数。不信任数组 size hint。JSON 直接借用输入解析，保留默认递归限制，不经过整图 Value 暂存；嵌套表达式与类型在 canonical protobuf 字符串中，不增加 JSON 深度。逐字重编码比较拒绝额外空白、字段重排、重复 metadata key、冗余 null 和其它非 canonical 表示；None 覆盖与 Some(empty) 不混同。
JSON 错误只报告静态类别和行列，不保留原始 serde 错误或表达式细节，不再附加持久解码节点 ID；拓扑和 binding 的逻辑 ID 诊断不变。编码仍先产生完整字节再检查总长，这不是 RSS 硬上限。JSON 字段名、owner identity 数组和 ordinal 表示改变最终字节数，接近 8 MiB 的旧计划可能不再准入；旧二进制节点格式直接重建，不识别或迁移。


根必须是 Source，叶必须是 Sink，输入数量与角色相符，所有输入有输出。构建引用必须来自同一 factory 的较早声明；持久解码同样要求每条输入 ordinal 严格小于当前节点 ordinal，因此图按声明序天然无环。向前引用、自引用和越界输入统一拒绝，不另维护拓扑排序或运行 schedule。任何 DAG 都可按拓扑声明表达。
新建只解析一次这份原始计划的连接、融合和深度，编码仅用于持久保存，不进行整图 encode→decode 往返；open 才完整解码和重新验证。所有 Schema 与资源先绑定，再由一笔 StoreSetup 事务发布 catalog 和 Definition。
Operation 状态前缀为 `operation/{ordinal:08x}`，逻辑 ordinal 与持久 Definition 一致。
装配按声明序消费每个 Definition，复用其 ID 与 inputs 进入一个 RuntimeNode；该节点直接拥有已构造的 Operation、必要的 output codec 和线性的 pending Delivery。运行期不另保留完整纯计划或 Operation/codec/pending 平行数组；Source/Sink 角色直接由已构造的 Operation 表达。持久 Definition 和调用栈格式不变，open 重新解析并绑定后同样释放纯计划。Flow 不保留表达式逻辑 AST 与 protobuf 根；已编译表达式可能共享其必要值，调用者另存的 Definition 也可延长原计划生命周期，因此这不是进程 RSS 上限。

Flow codec 只绑定 Source 的原始输出与每个融合段末端的输出。前者解码已发布队首，后者编码/解码跨段传递的页；Source 有改变 Schema 的尾链时，两种 Schema 分别绑定。装配时由既有 heads/tails 临时标记末端，不新增运行期索引。每个逻辑节点仍构造并校验精确 Schema，供下游绑定；段内只传 Arrow Change，不构造未使用的物理 Schema 和 fingerprint。恢复先验证实际 head、端口和父子调用边，再选择 codec；伪造融合尾节点为 head 时只读拒绝。此调整不改变持久字节或融合规则，也不保证整个进程 RSS 的下降幅度。

一个单输入 Atomic 只有在其上游恰有一条消费边时才吸收到上游 head 的尾链。Paged 与多输入 Atomic 可以做 head，
Source 的 head 执行已捕获输入的 identity 切片。Sink 是终点。融合尾链只保存在内存；不存在第二张持久图。
最长 head 路径最多 64 层，Sink 不占 frame。消费者顺序由逻辑声明顺序和输入端口顺序唯一确定。

## 唯一执行位置

两个 OrderedMap 以 u32 深度为 key：`flow/frames` 保存 Frame control，`flow/outputs` 保存当前待发送页。
root 的 input 直接借用其 Source 的已发布队首；child 的 input 借用深度减一的父 pending 页。
root 完成前队首不动；Source 捕获只在队尾追加。没有 root input Cell 或 child input 副本。
最大 control key 是栈顶，没有独立 top Cell。深度从零连续递增。

Frame 保存 head ordinal、可选 input port 和两种 phase：

- `Run(Resume)`：调用 head 后依次执行 Atomic 尾链。
- `Send { next_consumer, after: More(Resume) | Done }`：先把当前页发完，之后继续或返回。

Resume 由 Operation 提供严格 codec；Flow 不解释算法 cursor。它只在 Frame 内有一个持久副本。
control 独立于大 payload，推进 cursor 不重写祖先输入或完整栈。Frame codec 固定版本、字段顺序与 phase tag，
末尾 Resume 使用其自身严格 codec；拒绝截断、未知 tag、非规范编码和超过 64 KiB + 32 字节的 control。

## 原子转移

1. 空栈从 Source `published` 只读取得队首，直接执行临时 root 的第一页；正常完成不写激活帧。
   首次合法输入的计算失败会回滚本页，再用小事务只保存初始 Run control，使重开仍重试同一队首。
2. 执行页：head 与所有 Atomic tail 在同一事务做出一页，随后直接调用下游；More 必须推进位置。
   无输出 Done 直接弹帧，无输出 More 只更新 control。
3. 调用计算 child：保存父 pending 页和 Send control，压入 child control，同时推进父 next_consumer。
   child 借用父页，不另存 input；child 的计算页在下一笔事务执行。
4. 调用 Sink：在共享预算内按顺序 `try_enqueue`，与父 next_consumer 同事务推进，不创建 Sink frame。
   容量拒绝保证零写入，因此可以提交已计算页和此前接受的消费者；Send 只保留尚未完成的消费者位置。
5. 返回：本层全部消费者完成时，More 删除 pending 并转 Run，Done 删除本层 control/output；root 同事务 `consume_published` 删除 Source 队首。
   直接完成的页不先持久化 Send 或 pending；父层返回仍由下一次栈动作处理。

计算接口借用 `&self`、TransactionAccess 与 StepBudget；不得保留状态相关缓存或第二份进度。
Flow 从只读事务加载当前不可变输入后才打开写事务。一个活跃栈独占关系计算；Source 捕获与 Sink drain 不写关系索引。
DAG 保证当前 Join 不会在自己的后代中出现，因此分页期间对侧关系不变。

计算的原子边界是本页 head 加完整 Atomic 尾链。晚事件非法可保留早页；整个 Change/Delivery 不保证回滚。
Aggregate 或 ASOF 产生的一对 `-old,+new` 在其本页和尾链中原子，跨图边不附加 atom marker。

## 预算与调度

捕获 Delivery 的 envelope/row/slot/编码限制由 Source 执行；完整转换 Change 与 checkpoint 合计最多 8 MiB，
不拆分一个真实 Delivery。捕获受 24 MiB 逻辑工作界约束；bootstrap 封口只改变控制，不搬运输入 payload。
后代 input 和所有 pending output 最多 1 MiB、256 物理行、16,384 顶层标量槽。

每个计算 attempt 使用共享 4 MiB StepBudget，head work items 初始 256，尾链只扣逻辑字节而不重复扣 head items。
预算或页形状不足时丢弃本笔事务，从同一输入和 Resume 将 head items 减半，最小为一，最多九次。
语义错误不缩页；最小工作项仍超限则失败。不会拆开融合尾链绕过回滚边界。

输入加载另外计费：root 最多读 8 MiB，所以一次无重试 root attempt 最多涉及 8 + 4 MiB；九次 attempt
最坏额外涉及 36 MiB。运行时按实际已扣逻辑预算累计失败 attempt；为整个有限重试序列预留调度额度。
首次 root 失败另预留一份 control 写入，以固定失败输入；一轮最多 32 个栈动作，栈工作预留 80 MiB；Source 捕获另有 24 MiB 界。Sink 首次恢复校验可读取整个最多 64 MiB outbox，
普通 drain 读取有界前缀并持久化 Prepared；此额外工作按 Sink owner 契约计，不冒称整轮 128 MiB 硬界。
Source、root 选择与 Sink 各自按 Operation 声明顺序跨轮轮转；空栈在本轮剩余额度内检查 Source 的已发布队列，空队列不阻止检查后续 Source。

每个 Source 的 input Queue 按捕获/封口与 Streaming 阶段计容量，未确认 Delivery 另计；每个 Sink 的 outbox 单独有界。
栈层保留槽不与祖先或 outbox 共用容量，因此 child 不会等待祖先释放自己必须依赖的同一池。
编码栈 pending payload 最多 `D` MiB，D ≤ 64；root 输入仍计入 Source 的阶段容量（详见 CDC owner 契约）。
关系历史和外部日志保留不在此界内。

`advance()` 轮转服务一个 Source、有限栈工作、一个 Sink。长 root 允许后续 root 计算头部阻塞。
较早声明的深层 Sink 先于较晚声明的浅层 Sink 获得交付轮次，不按图层数排序；reopen 从各自首个声明重新轮转。容量拒绝不阻止本轮 drain；Idle 只表示本轮无进展。外部语句 deadline 属于具体 adapter，不能把轮转机会解释为即时取消。

## Durability 与重开

计算和路由的提交共用 DurabilityBatch；真实 CDC Delivery ACK、目标 deliver 前执行 barrier，返回前也必须完成 barrier。
目标 Prepared 先提交并持久化，再交付，再结算。commit/barrier/外部效果不确定时整个 Flow fail-stop，后续 advance 拒绝执行。运行中的 panic 同样先锁住运行时，再继续展开；不会把部分改变的内存对象复用于下一轮。
语义失败回滚本页，先完成已有提交的 barrier 后返回错误。重开不能修改、删除、修复或重新初始化已有状态。

open 逐层检查：depth 连续；只有零层 Source；head 是实际 head 且不是 Sink；port 合法；Resume variant、ordinal
和算法位置与 input 相符；Run 无 pending 且在栈顶；Send 有合法 pending，consumer 未越界。有 child 的父必须为 Send，
child head/port 等于父 consumers[next_consumer - 1]，child input 唯一来自父 pending 页。
拒绝 orphan payload、缺失资源、错 schema 和错误 control。只保留有界 control 与相邻 payload，不一次解码整栈。
源与目标恢复通过各自只读接口验证必要持久事实，Flow 不复制其状态机。
