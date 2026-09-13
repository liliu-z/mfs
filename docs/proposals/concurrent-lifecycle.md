# 文件目标合并、并行执行与配置替换方案

状态：2026-09-13 方案已进入实现；用户明确保留 active_run_id、stage 和 attempt 信息。MFS 当前合同以 [设计正文](../design.md) 为准，验收见 [backlog](../backlog.md)。本文保留设计取舍与接入要求；StashBase 旧 daemon 尚未迁移，不能把库侧实现视为应用完成。

## 1. 当前实现与建议的差别

- MFS 已有公开 `cancel(DocumentId)`、`retry(DocumentId)`、`reprocess(DocumentId)`、`reprocess_namespace(..., processors=...)`，以及接受新 chunker/embedder 的 `reindex(...)`。不能说目前没有换模型入口。
- 当前取消持久记录用户意图并通知正在执行的调用；方法返回不等于实际调用已经退出。当前 `wait(report)` 跟随文件/范围的最新目标，不是历史操作回执。
- 实施前只有一个文件 Worker；现在已有持久 active_runs、多 Worker 与配置双代切换。
- StashBase 的 PDF、图片、DOCX 准备状态栏和音视频转录区域已有 Cancel。它们调用 `POST /api/files/cancel-preparation`，按源文件取消准备工作并等待对应执行退休。当前没有接入 MFS。MFS 的 DocumentId 按 namespace 隔离；StashBase 的同一物理路径可能对应多个 DocumentId，接入时需要显式映射。

三种 interface 方案分别是：

1. **三个通用入口**：reconcile、configure、control。所有行为收进命令类型，方法少，但调用者仍须学习相同数量的命令与返回语义，收益有限。
2. **可扩展执行平台**：资源准入、Adapter 版本绑定、调度策略都可注入。能覆盖播放与转录共享模型等场景，但常见调用者需要了解太多执行配置。
3. **保留文件操作，集中配置变化**：继续提供 sync/upsert/remove/cancel/retry/reprocess；新增一个配置变更入口，合并、执行和恢复全部藏在 Lifecycle 内。

推荐方案 3，内部采用方案 2 的资源准入 seam。独立 MFS 使用本地容量管理，StashBase 使用宿主共享容量，两种 Adapter 有真实用途；不开放任务 DAG，也不让宿主管理阶段或释放文件占用。生命周期变化集中在 Lifecycle，索引版本变化集中在 Index，应用只声明目标并读取状态。

## 2. 文件保存最新目标与当前执行

每个 `(namespace incarnation, DocumentId)` 保存以下信息。名称为概念字段，最终表结构可调整。

| 记录 | 内容 | 作用 |
| --- | --- | --- |
| desired | target sequence、input version / deleted、取消门 | 最新已接收目标，可覆盖合并 |
| active | 已有 target version、捕获的 input/config version、stage/state、checkpoint、重试信息 | 当前文件处理链的逻辑占用，保留独立 active_run_id |
| active 的调用身份 | execution token、实际执行租约 | 区分同一阶段的不同次调用，拦住迟到结果；不要求另建 invocation 表 |
| publication | 按配置代保存的文字与索引版本、input version | 当前哪些产物可以用于读取 |
| cleanup debt | 明确的产物路径、collection/run/publication 版本 | 后台副作用的清理责任，不能随目标合并丢失 |

用户明确不要求简化字段。active_run_id 标识处理链，stage/state 表示阶段与执行状态，attempts 计数重试，attempt_token 标识本次调用。不得只用 current_stage 区分不同版本或迟到调用；真实执行租约一直保留到调用退出。

区分输入版本与处理配置版本。源内容未变但换模型时，旧配置的产物仍可以有效；不能因为处理配置变了，就把它误判为旧源内容。现有公开 SourceRevision 与配置的关系需要迁移说明，不能直接按字符串比较替代这两个内部概念。

### 合并与交接

| 时序 | 持久状态与执行 |
| --- | --- |
| add A1，尚未执行，update A2 | desired 直接成为 A2，只执行 A2 |
| add A1，尚未执行，delete | desired 成为删除；没有旧产物、读者或清理责任时可直接回收 |
| A1 Processor 运行，update A2、A3 | active 继续指向 A1；desired 只保留 A3，通知 A1 已过时 |
| A1 Processor 退出，此时 desired 是 A3 | 丢弃 A1 的发布资格，跳过 A1 后续切片/向量；退休 A1，执行 A3 |
| A1 运行时 delete | 接收事务立即撤销可见性；等实际调用退出，回收 A1 的副作用 |
| A1 失败或退避，此时 update A2 | 若没有实际调用在运行，直接退休 A1，执行 A2，不等 A1 成功或退避到期 |

“后继等待前序”指等待前序的实际调用退出，不要求过时处理链全部执行成功。同一文件至多一个实际阶段调用；不同文件可以同时执行。同一物理文件出现在不同 namespace 时，逻辑文件占用各自独立，物理操作仍需覆盖所有引用。

用户请求只使用短 SQLite 事务保存目标、取消资格与清理责任。Processor/Embedder 期间没有长 SQLite 写锁。扫描、内容读取本身仍需时间；异步只保证不等待后续处理与索引。

取消是独立的用户意图，不由 cleanup 完成、sync 或配置变化自动解除；显式 retry/reprocess 才恢复。取消不删除源、不撤销已经生效的文件修改，也不授权清理仍有效的既有 publication。delete/drop 的必要清理继续执行。

取消后的保存与清理：Internal 原件、仍有效的已准备文字，以及可恢复的 checkpoint/chunks/向量文件保持引用；未引用的临时/废弃产物在真实执行退出后由 GC 回收。取消不承诺立即清空该文档的磁盘或后端行。失效、部分写入的后端行不得可见，清理责任不能因取消丢失。取消门是文件级的，普通 sync 即使观察到新输入也保持取消；这与失败不同，旧输入的失败不应污染新输入。cleanup 恢复必须以当前取消门为准；对应已复现漏洞已纳入独立回归。

### External 输入与旧调用失败

active 捕获的是一次输入观察，不保证外部原路径仍保存 V2 bytes。MFS 不为此复制或保留 External 原件。处理期间可能读到后来写入的内容，当前 SourceGuard 会在可检测时拒绝不匹配的结果；这不构成外部文件快照保证。

允许直接处理最新外部内容也不能把它记进 V2 内容 hash 的缓存，否则后续恢复/去重可能给真正的 V2 返回 V4 的结果。发现变动时应丢弃旧结果、重新观察接收最新输入；若采用“处理时观察”的变体，也须按实际验证的内容身份记录，不能只沿用旧 fingerprint。

V2 的 Processor 抛异常后，在 finally 中退休实际调用。若最新目标已为 V4，V2 的错误只属于旧调用，不更新 V4 的失败状态，不等待旧重试预算耗尽，也不要求把旧链跑成功。V4 独立从所需阶段开始。暂时无法清理的旧副作用按精确版本记录，不应仅因旧任务失败阻止新目标；整体存储不可用仍须停止发布。没有返回的挂死调用需要可取消协议或受监督进程，不能把普通线程超时当成真实退出。

### Internal 接收的提交顺序

当前实现已经先复制、后提交目标：copy 到唯一 work 路径并计算 hash → 文件 fsync → rename 到唯一 originals 版本路径并 fsync 目标目录 → SQLite 事务登记新目标、输入引用、旧结果失效 → 返回。不会覆盖旧版本原件；当前 accept_original 位于目标事务之前。

它不是文件系统与 SQLite 的跨系统原子事务，而是以 SQLite 提交为逻辑接收点：文件尚未完成时没有新目标；文件完成而事务失败时保留旧目标，最多产生可回收的无引用新文件；事务提交后目标引用已经落盘的完整文件。响应丢失需要重读持久状态或用已有幂等键重取，不能直接删掉可能已经被引用的新文件。

并行重构必须补齐/验证三项：正在复制和待提交的 staging 有明确 GC 使用租约；原件 rename/fsync 在长生命周期锁之外完成，随后短事务复核目标；新建目录的父目录项也按平台能力持久化。当前代码只明确 fsync 目标 originals 目录，不能据此宣称所有新建父目录及所有平台都已通过断电恢复验证。旧输入只在 active、checkpoint 和读者不再引用后才可回收。

## 3. 多线程、阶段与资源

建议初始工作池容量 4，可配置；这不是保证同时跑 4 个模型。保留独立查询容量，GC 使用低优先级执行。调度器可以是一个短临界区中的协调循环，不需要“一种状态一个线程”。

工作单位为一个文件的一次阶段调用：Processor → Chunker → Embedder → 索引写入/发布。必要时索引阶段内部可分批，但调用方不管理这些细节。

1. 调度器找到可执行阶段。
2. 一次申请该阶段全部容量：工作槽、heavy/light 或模型额度、Adapter 并发额度。
3. 短事务复核目标、run、取消门及 attempt，再提交实际执行。
4. 阶段结束，私有产物落盘；短事务记录结果、后继阶段或错误。
5. 归还线程和资源，保留该文件 active run，直到链完成或安全退休。

等资源不能占着工作线程排队，也不能持 SQLite/Lifecycle/后端锁。Processor/Chunker 默认串行使用，明确声明线程安全和容量后才并行；本地处理的内部线程、native 子进程也必须计入预算。Embedder 不使用对象或资源额度，后台与查询各受自己的执行池限制，具体实现负责线程安全和服务约束。

StashBase 当前 light=2、heavy=1，另外 classifier=4；这些是任务容量，不等于固定 OS 线程数。接入后播放、转录和 MFS Processor 的本地处理由同一个容量所有者仲裁，不能 Node 和 Python 各自认为还有一个空闲 heavy。

推荐先沿用宿主的共享资源所有权：MFS 的本地 Admission Adapter 可替换为宿主 RPC Adapter。宿主可以请求转录暂时让出 heavy；这属于调度暂停，不设置用户取消门。实际调用退出或子进程确认终止后才归还额度，再执行播放。RPC 断开或等待超时不证明远端执行已停止，不能立即重发同一额度。

普通阶段完成自然让出。长 Processor 的 checkpoint 仍有价值：持久保存分页 OCR/转录进度，降低取消和抢占延迟；它成为可选恢复能力，不再是一个 worker 给所有文件轮流执行的唯一办法。不能强制中断任意 Python/native 调用；需要取消协议或受监督子进程。

## 4. 发布与故障恢复

SQLite 与 Milvus 无法共用一个事务。文件逻辑占用本身不能解决两边原子提交，必须采用以下发布协议：

1. 先持久登记 run 的产物/后端写入责任。
2. 在 run 私有位置产生完整文件并持久落盘；使用稳定行 ID 向指定 collection generation 写入后端结果。
3. 后端达到约定的可查询完成条件后，短 SQLite 事务核验 namespace incarnation、input version、config generation、run 和 attempt。
4. 仍有资格，才更新 publication 指针并记录阶段完成；已经过时，就只保留清理责任。
5. 搜索依 SQLite 中的资格过滤候选。只有物理行、不在 publication 中的结果不可见。

后端行身份包含 namespace incarnation、collection generation、DocumentId、publication generation、chunk ordinal。重试同一写入使用相同行 ID；清理只删明确版本。当前“删同文件所有非我 snapshot”及宽范围 `delete_document` 必须收紧后才能扩大并发。

| 故障点 | 结果及恢复 |
| --- | --- |
| 目标接收事务失败 | 不返回 accepted；调用方明确得知失败 |
| 产物已写、阶段事务未提交 | 重开后按登记责任验证/复用或清理；不能直接显示成功 |
| 后端已写、publication 未提交 | 行仍不可见；按稳定 ID 重放，再尝试发布 |
| publication 已提交、回执丢失 | 重读持久状态确认，不重复生成新版本 |
| 旧调用迟到返回 | run/attempt 不匹配，禁止发布，仅处理其清理责任 |
| 单个阶段可重试失败 | 持久记录次数、错误、next retry；退避时不占线程 |
| 阶段永久失败/额度耗尽 | 文件 failed，保留有效既有产物，其他文件继续 |
| Adapter/模型不可用 | blocked，显示缺失条件，不热循环重试 |
| 用户取消 | cancelled / cancelling；无自动重试，真实执行退出前保留占用 |
| SQLite 持续故障 | 停止新准入和发布；所有等待共用健康检查，立即报告 StorageFailed |

可以沿用当前每阶段最多 5 次尝试的初始策略，并增加适合远端调用的退避和抖动。任务让出不刷新预算；完成一个阶段后，下一阶段有独立预算。新源目标不继承过时源的失败次数。

重启时持有 state dir 排他所有权，先确认旧 native 执行已退休，再恢复已持久接收的目标。每次实际重试用新 attempt token。SQLite 保存 Adapter 兼容清单，不保存 Python 对象；宿主重新绑定实现。close 停止准入、通知执行退出，等实际读写退休后再关闭存储。

## 5. 接收、完成、取消与回执

必须分别表达三件事：目标已经持久接收；需要的结果已经可用；实际执行与清理已经退出。单个 `done` 无法同时表达它们。

- 文件 UI 显示最新目标：queued、processing、indexing、retrying、blocked、failed、cancelling、cancelled、ready；附阶段、可重试性和具体原因。
- 接收方法先返回目标版本及接收标识，后续通过状态/事件读取结果。异步失败不能事后修改已经返回的 HTTP 成功，也不能只记日志；应用必须展示失败及 Retry。
- 用户点击取消时，先持久接收取消，再显示 Cancelling；实际调用退出后显示 Cancelled。用于物理文件操作的停止屏障仍由 quiesce 提供。
- 删除接收后搜索已经失效，`cleanup_pending` 可继续为真；这不等于文件仍可搜索。

以下历史回执为未确认备选，不纳入已确认待修复范围；现有 RECEIPT-002 的当前状态等待继续有效。如果以后产品需要回答“这一次 Sync 完了吗”，可以另议可持久查询的操作回执：每个操作关联去重后的目标版本，记录 pending / succeeded / superseded / cancelled / failed。A1 未完成便被 A2 取代，A1 回执为 superseded，不能说 A1 已执行成功；A1 先成功再收到 A2，历史 A1 仍为 succeeded。

回执是观察记录，不是任务 FIFO；多个回执可引用同一目标，实际执行仍只有 active 和最新 desired。只保留有限时间的终态历史，过期要返回明确 expired/unknown，不能重新解释为当前文件状态。RPC 可用 client request key 幂等重取同一次接收结果，避免请求已提交但响应丢失时重复发起操作。

为避免悄悄改变现有 `wait(report)` 的合同，新增回执使用单独的 `operation_status/await_operation`（名称暂定）；现有 `wait(file/scope/report)` 继续跟随最新目标并包括现有清理完成语义。`await_operation` 必须区分 superseded 与成功；失败和取消返回明确终态，超时只结束等待。扫描不完整也必须保留 observation errors，不伪装完整接收。

## 6. 配置变更与批量替换

统一配置入口：

```python
accepted = mfs.configure_namespace(
    "folder-id",
    processors=[pdf_processor_v2, audio_processor],
    chunker=chunker,
    embedder=embedder_v2,
    indexing="hybrid",
)
```

一次原子提交所有配置变化，避免先换 Processor 全库跑一次，再换 Embedder 又跑一次。比较稳定兼容描述，不比较对象地址；仅凭向量维度相同不能判定 embedding space 相同。

| 配置变化 | 必需重做 |
| --- | --- |
| 仅同兼容实现的凭据/连接绑定改变 | 重新绑定，无重建 |
| Embedder 的 embedding space 改变 | 复用有效文字/兼容 chunks，重算向量与索引 |
| Chunker 改变 | 复用文字，重新切片、向量和索引 |
| Processor 兼容输出发生变化 | 受影响格式重新处理，并重建后续索引 |
| 同时改 Processor 和 Embedder | 一次目标配置，一次必要处理链 |
| 关闭索引或删除/排除成员 | 立即撤销相应查询资格，清理异步进行 |

`open_namespace` 只绑定持久配置对应的实现，不因创建了新 Python 对象自动改配置。保留 retry 和同配置 reprocess，处理坏产物、强制重做等情况；保留管理用途的同配置 reindex 修复。可以逐步移除 reindex/reprocess_namespace 上“顺便修改配置”的职责，统一到 configure_namespace。

### 活动代与候选代

G0 保存正在服务的配置、文字路由、collection 和对应查询 Embedder。G1 在私有位置构建新配置的产物。Processor 改变时新文字也属于 G1，不能先把全库 grep 切到新文字、语义搜索仍混用旧配置。

单纯配置变化允许 G0 继续服务。源更新、删除、规则排除则立即撤销两代中的旧输入资格。构建期间，为继续服务最新源，可以在同一文件的顺序执行中维护 G0 和 G1，复用兼容文字/chunks；不同模型有必要计算两套向量。这里需要公平调度，防止一直维护 G0 而饿死 G1。

推荐严格完整切换：

1. G1 的成员是当前已接收、仍应参与的文件，成员和期望 input version 随接收事务更新。
2. 删除立即移出要求，不等物理清理；输入变化使旧完成标志失效。
3. 某个必需成员失败或被取消，构建明确 failed/blocked，G0 继续可用。不能静默漏掉仍在 G0 可搜索的文件后宣告切换成功。
4. 短事务验证配置目标仍为 G1，所有必需成员的 G1 publication 匹配当前输入，随后切换活动配置与 generation。
5. 新查询捕获 G1 及其 Embedder；已进入的查询持 G0 租约完成。G0 的真实读写全部退出后再清理。

成员策略需要明确：从未有有效产物且原已取消的文件，可按明确的“不参与构建”策略记录；已有有效 G0 结果的文件不能因取消被静默移除。默认保守地阻止会丢现有成员的切换，用户可 retry、显式排除/删除，或将目标配置改回 G0。

持续写入可能让 G1 无法追上当前目标；不能同时保证无限制接收、切换覆盖全部最新文件、固定时间完成。状态应报告未完成数量及原因，超时不改为成功。另一种“固定基线后切换”能较快切换，但可能暂时遗漏基线之后新增/修改的文件，本方案不默认采用。

构建 G1 时再换 G2/G3，只保留最新目标，停止 G1 新工作，等在途调用退休，再构建最新代；G0 持续服务。限制同时存在一个 active、一个 building，及有真实租约的 retiring 集合；有资源上限，不能无限积累废弃模型和 collection。

重启必须允许按持久配置版本重新绑定 G0 和 G1 两套 Adapter。提供按 configuration revision 绑定的扩展，以及读取 active/desired 配置清单的入口；普通单代调用保持简单。缺 G1 实现只阻塞构建，G0 仍可用。绝不能用 G1 Embedder 查询 G0 collection。

## 7. 扫描与轻量路径索引

现有平方级问题来自“每个旧文件都遍历所有 protected 路径”。无变化 sync 也会支付这笔代价。

近期修复可用规范化路径集合与有限祖先查询，将这种判断从 O(N²) 降到 O(N × 路径深度)。规模扩大时，在 SQLite 文件目录中加入轻量目录记录及 parent/path 索引，不额外维护另一份完整内存文件树。

概念记录为 `(namespace incarnation, path_key, parent_key, name, kind, observed_stat)`。扫描一个目录后，将完整观察到的直接孩子集合与已知孩子比较：新增/变化接受新目标，明确消失的接受删除，未变文件不创建后台任务。目录被文件替代时，利用路径索引找旧子树。

全量 sync 仍需枚举文件；只看父目录 mtime 不能知道孩子内容有没有变化。stat 快路径与 content 验证保留现有区别。无需为了 last_seen 而在每轮扫描重写所有无变化文件。

失败必须按覆盖范围处理：某个子目录读取失败，不能把其中未看到的文件当成删除；整个 root 不可访问，也不能当作“完整扫描得到空目录”。只有成功观察能证明的消失才接受删除。

同 namespace 扫描串行，不同 namespace 可并行。遍历/哈希期间不持实例全局 mutation 锁；提交时校验捕获的 namespace incarnation、root、规则版本及文件期望版本，防止旧扫描覆盖新接收或把新目标扫掉。规则/root 已变时拒绝过时提交并重新观察，不能沿用旧覆盖范围做删除。

## 8. 派生文件、父子 namespace 与一致性

当前默认位置：

```text
MFS state/
  namespaces/<incarnation>/
    originals/   # Internal 原件
    derived/     # 两种 namespace 的受管理派生物
    work/        # 工作目录
```

External 原件留在原目录，默认受管理派生物也集中在 MFS state。例外是 Processor 明确返回工作目录之外的 text_path/grep_path，此时可能是借用路径；直接 grep 外部原件也是依赖外部源。这些路径不属于 MFS 管理的持久产物，丢失就报告不可用，不要求自动复制或跨 namespace 引用计数。

Internal 原件与默认派生物都集中存储；自定义 Processor 返回外部借用路径仍是例外。迁移建议把正式派生输出交给 MFS 管理，通过 artifact handle/读取 interface 提供给 Viewer；读者持有使用租约时 GC 不得删除对应文件。

父子目录各自是 namespace，但 namespace 不形成生命周期继承树：

- drop 父 namespace：删它自己的元数据、索引与自有产物；不删 External 原目录，不顺带 drop 子 namespace。
- 从磁盘删除父目录：子 namespace 的原件也可能消失。直接依赖原件的 grep 可能报错；已有集中派生文字可能仍可读，既有索引也可能仍可用，直到 MFS 观察并接收失效。
- 子 namespace 的成功 sync 确认删除后，立即撤销相关资格。若整个子 root 已不可访问，sync 报不完整/不可用，不能假装成功观察到全部删除。应用若确定要移除成员，可显式 drop。

删除立即生效使用当前已经存在的“SQLite 资格过滤”方向，扩展到每代 publication。查询捕获候选后，在返回前复核 document 仍存在、输入版本匹配且 publication 有效。查询可能需要继续补候选，避免被已失效的行挤满 top-k。无法撤回已经返回的结果；保证按最终资格检查的先后顺序定义。

| 场景 | eventual | strong |
| --- | --- | --- |
| 新增 | 尚未发布时可能缺失 | 等所需文字或索引阶段；失败明确报错 |
| 源更新 | 旧输入立即无资格，新输入未就绪时有空档 | 等所查询阶段达到当前输入 |
| 删除已接收 | 立即过滤，允许后台还有物理行 | 同样立即过滤，不等被删文件清理 |
| 配置构建 | 用仍有效的 G0 | 等目标配置可用；构建失败/阻塞明确报错 |

strong 针对所查询能力：grep 不因无关 Embedder 故障等待；语义查询需要相应向量。配置替换期间，strong grep 可以读取已完成、匹配目标文字配置的 G1 文字，而不提前切换 eventual 的活动路由；strong 语义查询等待索引切换。因此表中的“目标配置可用”也按所查询能力判断。strong 表达当前有效结果的就绪，不承诺 External 文件脱离 MFS 观察后永远不变，也不保证物理清理完成。

## 9. StashBase 文件事务仍需单独处理

SQLite 中冻结 active 只冻结元数据，不冻结 External bytes。MFS 不复制/hardlink/reflink External 原件；源前后检查能识别常见变化，但不能替代外部不可变快照。Viewer、另一个 namespace 或其他程序也不遵守单 DocumentId 的逻辑占用。

宿主可控的 rename/delete 继续使用：

```text
宿主路径事务锁
→ quiesce 所有受影响 namespace/path
→ 磁盘 rename/delete
→ sync 新旧范围，确认完整且已持久接收
→ 释放 quiesce
→ 后台准备/索引；单独报告失败或滞后
```

不要在禁止相关执行的 quiesce 内 wait 索引。后续 embedding 失败不自动回滚已经提交的磁盘改名。磁盘或接收失败需要补偿时，在租约内补偿并重新 sync；如果租约已释放，先重新取得相关范围的租约。

宿主持久记录文件事务步骤；daemon 重启后先恢复这些事务与禁止执行的范围，再绑定并恢复工作。进程内 ScopeLease 不是磁盘事务日志，不能靠超时自动释放来假装故障恢复完成。RPC 取消、状态、接收必须保持可响应，不能被同一个串行 dispatcher 中的长 wait/reindex 阻塞。

## 10. 实施与验收

1. 先修现有“cleanup 后恢复覆盖用户取消”、reindex 不报告致命存储故障、无变化 sync 平方级遍历三个已复现问题。
2. 引入 desired/active/publication/cleanup 的不变量和迁移，单 worker 下先验证交接与恢复，再开启并行。
3. 收紧后端行版本与删除范围，加入资源准入、每 Adapter 容量、按 run 保存的暂存文字及精确读写租约。
4. 接入统一配置变化与双代构建，明确状态/回执/strong 等待合同。
5. 最后迁移 StashBase 取消、共享 heavy、Viewer 派生物与文件事务；不能仅替换 Python 调用名称。

必要的确定性交错验收：运行前合并；运行中 update/delete/cancel；失败退避时新目标；cleanup 中取消后重启；后端写完前后崩溃；旧 attempt 迟到；同文件不重叠而不同文件并行；阶段间释放容量但保留 run；共享 heavy 断连不重复发放；查询超时仍保护旧 collection；构建中源变更/删除/失败/取消/连续换模型；缺新模型重启时旧查询可用；局部扫描失败不误删及无变化工作量；删除立即过滤且清理滞后；rename 持租约时 daemon 退出与恢复。

实施状态以 [backlog](../backlog.md) 为准。MFS 库侧实现包括目标合并、持久 active、资源准入、候选代切换、精确清理、扫描优化、文字/索引分离等待及启动恢复门。StashBase 的旧 mfs-cli daemon、真实转换器、共享 RPC grant 和宿主事务日志仍属于应用迁移工作，不由库测试证明。
