# MFS 设计

本文是唯一设计正文，统一本轮已确认的行为。实现与验收结果见 [backlog](backlog.md)，StashBase 的具体接入见 [对接说明](stashbase-integration.md)。

## 1. 实例、namespace 与存储

MFS 是嵌入式 Python 库。保留现有实例：一个实例持有状态目录、数据库连接、内存待办和后台处理线程。业务配置属于 namespace，没有全局 Processor、Chunker、Embedder、ignore 或索引策略继承。

一个 namespace 有稳定身份、Internal/External 所有权类型及独立配置。DocumentId 是 (namespace, doc_id)，不同路径即使内容相同也是不同文档。计算结果可以复用，文档身份不合并。

- SQLite：每实例一个 catalog，共享元数据表。文档和任务按 namespace/doc_id 区分；每文件保存当前任务，namespace 控制任务单独调度。
- Milvus：每实例一个数据库文件；每 namespace 一个活动 collection 和至多一个构建代，支持不同向量维度。collection 由不可复用的 namespace incarnation 与配置代命名；被查询/执行租约保护的退休代暂时保留。
- 文件：MFS 拥有的原件、派生文字、附属产物和临时文件按 namespace 整理。目录独立不表示能脱离共享数据库单独迁移。
- External root 与 MFS 状态目录分开，不能重叠。不同 namespace 的 External 真实根可以相同或嵌套；源文件引用可以相同，文档身份、观察、规则和索引独立。删除 namespace 不删除外部原件。

```text
mfs-state/
  catalog.sqlite
  milvus.db
  namespaces/
    <namespace-id>/
      originals/       仅 Internal 原件
      derived/         必要转换生成的文字和附属文件
      work/            临时工作文件
```

打开已有实例不自动创建外部模型。每个 namespace 成功绑定执行器后，其需要执行器的任务才能继续；状态读取、删除和读取有效已保存文字不需要模型。一个 namespace 绑定失败不改变其他 namespace。

## 2. 外部实现与兼容性

创建和重开 namespace 时，由调用方传入 Processor、Chunker、Embedder 对象。库可以提供默认 Chunker 实现，但没有实例级配置回退。

持久化的是兼容清单，不是 Python 对象、连接、函数或密钥：

| 实现 | 必须核对 |
| --- | --- |
| Processor | id、version、影响输出的 options、media types、后缀路由 |
| Chunker | id、version、影响片段的 options |
| Embedder | embedding_space、dimension、索引 metric 和向量表示 |

相同 dimension 不等于相同向量空间。声明与已保存清单或实际 collection schema 不一致时，绑定立即报错并指出差异；不能自动覆盖配置或暗中重建。实际向量输出继续检查数量、维度、有限数值。

调用方可以显式向两个 namespace 传同一个对象；持久身份、规则和索引仍独立。模型调用不持有 MFS 状态锁。Embedder 允许后台与查询并发调用，不使用额外的对象并发门或资源准入；实现自行保证线程安全及具体模型/服务的限制。同一 Processor/Chunker 对象默认串行，只有具体实现类显式声明 concurrency 才允许并发，子类不继承此承诺。Processor 的单文件可变状态放在局部变量或每次独立的 ProcessingContext/work_dir 中；共享模型和缓存自行保证线程安全。sniff 是快速、无状态的头部识别，不占 process 的资源/并发额度，可能与同对象的 process 同时调用。

有意更换 Processor/Chunker/Embedder 使用 configure_namespace 一次提交。reprocess_namespace 返回 ConfigurationReport，是强制处理的兼容入口；reindex 是阻塞等待的索引修复入口，两者走相同的候选代发布协议。configure_index 保留暂停和索引模式控制。源内容未变的索引重建保留有效文字；源已替换则不能恢复旧文字。迁移要有持久状态，失败可重试，不能把半完成配置作为活动配置。

## 3. 源所有权与 Processor

External 只支持 sync，不提供 add/upsert/remove。MFS 记录原路径、观察到的 hash/stat 和处理状态，绝不复制外部原始数据；临时 staging、稳定快照、镜像、hardlink/reflink 备份和正文 JSON 缓存也不例外。外部文件随后改变、丢失或损坏由用户负责，读取可以失败。

Internal 接收 upsert/remove，MFS 保存原件并负责其生命周期。

Processor 路由决定哪些格式可以处理：显式 media type、注册的源后缀及 sniff 选择一个实现。没有对应 Processor 时不索引任意二进制文件。源从受支持变成不支持时仍撤销旧结果。

| 输入 | Grep 的文字 | 排名索引的文字 |
| --- | --- | --- |
| External TXT/Markdown | 直接读取外部原文件 | 原文件文字 |
| Internal TXT/Markdown | MFS 保存的原件 | 原件文字 |
| PDF/DOCX/图片/音视频 | 必要提取生成的文字，或应用已有文字文件的引用 | 提取文字 |
| StashBase HTML 路径 | 可以继续 grep 原 HTML | Processor 在内存提取的文字 |

不是所有文件都生成 Markdown。Processor 可以直接返回已有文字文件的引用，也可以产生新的提取文字。应用已有派生文件只借用指针。一个文件同一格式只选一个 Processor，不重复运行库转换和应用转换。

MFS 保留 UTF-8/PDF 处理，并补齐旧库提供的基础 DOCX。StashBase 已有的增强 PDF、HTML、OCR、转录和播放转换继续由应用提供，通过 Adapter 接入。播放格式转换和“搜原视频还是生成视频”由 StashBase 决定。

Processor 返回文本或文字引用，以及 SourceMap；Chunker 独立切片。索引不再按生成文件后缀设置第二份 whitelist。

## 4. 文字、SourceMap 与去重

正文留在一个合适的文件位置：External 原件、Internal 原件、MFS 必要派生文件，或者应用已有派生文件。SQLite 保存引用、SourceMap、hash、配置和任务状态，不保存正文。Milvus 为 BM25 保存片段文字，并保存向量、文档身份、版本和片段定位。

文字引用读取保留 CRLF，UTF-8 BOM 仅在解码时移除；校验、read、grep、切片统一使用解码后文字的 UTF-8 偏移。SourceMap 描述处理文字的 UTF-8 字节范围对应源文件的页/行/时间，不是源二进制偏移。Milvus 一行对应一个片段。同一文档可有多行，重叠片段保留各自位置。

切片计划只保存范围/hash 等恢复信息。向量复用使用独立的完整计算缓存：key 包含 namespace incarnation、index_epoch、完整 dense 配置和片段 hash；只有整批向量通过数量/维度/有限值验证后才写入。SQLite 保存带校验和的二进制向量，不保存片段正文；缓存逻辑大小上限为每实例 32 MiB，按 LRU 淘汰，SQLite 页及 WAL 开销另计。缓存命中不恢复旧文档的搜索资格，旧索引行删除不再导致相同片段必然重新 embedding。缓存写入仍核验当前执行，旧 revision/epoch 的迟到结果不能重新填入已清理的缓存。

显式 reindex 推进 index_epoch，旧缓存不再命中并由有界 LRU 回收；drop 清理旧 incarnation 的缓存，同名重建使用新 incarnation。缓存损坏或淘汰后重新计算，不影响现有搜索结果。升级后的缓存开始为空，不保证首次重命名就命中；它是有界计算优化，不是无限期保存旧向量的合同。

直接文本的 grep 使用实际读取的行号和偏移。借用的提取文字仅在内容 hash 仍匹配时沿用保存的来源映射，已变化则退回当前文字行号。两次 sync 之间，grep 与索引不保证来自相同 bytes。

## 5. 接收、处理与恢复

```text
sync / internal upsert → 观察和去重 → SQLite 接收最新目标 → 返回
                                             ↓
                  多个 Worker：process → chunk → embed → publish
                                             ↓
                          短事务校验并记录每阶段结果
独立维护：精确索引清理、配置代切换/退休、受管理文件 GC
```

每文件保存 latest target 和 durable active run。保留 active_run_id、stage/state、attempts 及每次调用的 attempt_token；这些信息区分文件版本、阶段和实际调用。active_runs 捕获输入、配置、恢复进度及引用，不能通过清掉 token 假装实际调用已经退出。同一文件至多一个实际调用，不同文件按容量并行。

未执行的 add/update 合并成最新版本；delete 直接替换目标并撤销可见性。运行中的 V2 不阻塞接收 V3/V4，目标只保留 V4。V2 实际退出后跳过过时后续阶段，转向 V4；V2 算子失败不记到 V4，也不要求旧链成功。阶段成功只释放线程和资源，active run 在链完成、安全替换或终止前仍保留。

Internal 在短事务前完成 copy/hash、文件 fsync、唯一 originals 路径 rename 和目录 fsync。复制和待提交原件有 GC pin；提交时复核 namespace incarnation 与接收顺序。SQLite 提交是逻辑接收点：它不与文件系统组成跨系统事务。提交失败保留旧目标，最多留下无引用新文件；确认丢失先核对持久结果，不删除可能已接收原件。reprocess 可以引用已有不可变原件，但创建新的目标版本。

External 不保存历史 bytes。处理可能碰到更新后的原路径；SourceGuard 在处理/checkpoint/索引读取时检查观察身份和 hash，不允许按 V2 hash 缓存 V4 结果。外部任意写入不受 MFS 锁控制，不承诺快照隔离。

取消门独立保存用户意图。cancel(DocumentId) 返回表示取消已接收，不表示实际调用已退出；不删源、不撤销已完成且仍有效的 publication。普通 sync、清理和配置变更不会解除取消，包括源再次变化。retry/reprocess 才恢复。仍有引用的原件、文字与 checkpoint 保留；无引用临时物由 GC 回收，失效索引由持久 cleanup debt 清理。

每个阶段完成都更新 SQLite。可重试故障按阶段指数退避，最多 5 次；永久失败直接 failed，缺能力为 blocked。 ExecutionPolicy.stage_timeout 默认 300 秒（有限正数），从阶段领取开始计时，覆盖 process/chunk/embed 及 Worker 的后端阶段。独立期限线程将超时目标持久标记为 failed / ExecutionTimeout，撤销 attempt 的提交资格；协作检查点和 run_process 同时检查期限。超时不自动重试，显式 retry 仍须等待旧调用真实退出；崩溃恢复保留已持久失败。准备、embedding、写入及完成边界再次检查，丢弃迟到结果。相同内容的 sync 不重试失败；新内容是新目标，有自己的失败预算。状态包含当前阶段、attempts、实际 executing、active_run_id/attempt_token、错误及 cleanup_pending。当前状态可轮询，不要求用户确认回执，也不保存每次调用的历史队列。

wait(DocumentId)、wait(namespace, path=...) 和 wait(report) 跟随最新文件/范围，包含配置构建和物理清理。ConfigurationReport 同样可等待；它不是历史完成凭证。failed/blocked/cancelled 抛 OperationFailed，不完整扫描报告观察失败，存储无法核对立即抛 StorageFailed，超时只终止等待。显式 idempotency_key 仍去重接收，不覆盖后来文件状态。

## 6. 替换、删除与可见性

源替换、删除或规则排除在接收事务中撤销旧文字及索引资格。新源处理失败不回退旧 PDF。后台先写特定 collection/snapshot 的完整行，再由 SQLite 校验输入版本、namespace incarnation、配置代、active run 和 attempt 后发布。后端写完但发布未提交的行不可见，恢复可按稳定行 ID 重放。

删除责任独立于最新文件目标，按 incarnation、collection generation、DocumentId、snapshot 精确记录。旧清理失败不会把新源任务标成失败，也不能宽范围删除新版本。清理失败有独立退避/错误；retry(DocumentId) 可重启关联清理。namespace 退休清理失败另记在 namespace_configuration，文件处理继续；资源上限可能暂缓再建下一代。

查询捕获活动 collection、相应 Embedder 和 publication/input_version。返回前核对输入仍是当前成员，删除/规则排除立即过滤；同输入的配置切换不让已经开始的旧查询混用新模型。真实查询退出前保留原 collection，调用方超时不是退出。过滤失效结果后在候选预算内补取。

strong search 等目标配置及当前索引就绪，strong grep 只等当前文字；已删除文件的后台清理不会阻塞 strong。wait(report) 则包括物理清理，二者完成条件不同。派生文件、执行中的临时文件和读句柄有精确引用/pin，GC 可与不相关文件的处理并行，不删除 External 原件或借用文件。

sync 只在成功观察的范围内确认缺失。root 不可访问不是完整空目录，不能据此批量删除。close 开始后，扫描在目录项、文件及哈希分块之间协作退出，返回 complete=False 和 Closed 失败；保留已接收变化，跳过剩余缺失推断，未观察文件留待下次完整 sync。已经阻塞的单次系统 I/O 仍须等待返回。父子 namespace 独立；drop 父 namespace 不删外部目录，也不删除子 namespace。

## 7. Namespace 规则与索引控制

Ignore 是 namespace 自己的一份有序规则，无全局继承，也不隐式读取 .gitignore。每条规则有稳定 rule_id、include/exclude 动作和模式，支持文件、目录、后缀及 glob；后匹配规则覆盖前规则。支持原子增删改和排序。

- 无斜杠模式匹配任意层级名称；带斜杠模式相对 namespace root，前导 / 表示根锚定。
- 目录规则覆盖后代；支持 *、?、字符集合和 **。
- 父目录排除后仍允许显式包含后代，扫描不能直接剪掉可能被重新包含的目录。
- expected_revision 只用于规则更新的冲突检查；不匹配立即报错，不阻塞等待。
- 规则提交后读路径和任务提交使用新规则；新增排除立即失效并清理，重新包含通过 sync/reconcile 重新接收。
- StashBase 特有的 sibling/派生文件关系由应用转成明确规则，不把应用业务回调塞进基础 glob。

namespace_configuration(namespace) 返回只读配置副本：namespace/kind/root、indexing、paused、max_file_bytes、完整 manifest 和 pending_manifest、active_revision/pending_revision、退休清理状态。indexing 表示当前服务模式；切换到 off 立即禁止排名查询。它不包含运行对象或凭据；未绑定 Adapter 也可读取。规则继续通过 rules(namespace) 获取。

索引策略为 off/bm25/hybrid，并独立提供 paused：

| 策略 | 行为 |
| --- | --- |
| off | 保留文件观察、处理和 grep，清除排名索引，不调用 Embedder |
| bm25 | 只构建 BM25，不调用 Embedder |
| hybrid | 构建 BM25 和 dense，必须绑定相容 Embedder |
| paused | 暂停新增索引工作；源观察、必要文字处理、逻辑失效和旧代清理继续 |

paused 按任务实际使用的索引模式生效，包括从 off 开启的候选代；off 的完成与清理不受索引暂停阻塞。search(namespace, text, mode="bm25") 只选择这次查询通道，不会关闭后台 embedding。cancel(DocumentId) 用于文件当前任务，不取消 namespace 配置；更换 Embedder 由 configure_namespace 自动使旧候选失效，无需先 cancel。取消单文件也不等于 namespace 暂停。

## 8. 搜索与读取

删除公开 query 名称，不保留别名。非排序文字、文件名和路径匹配统一使用 grep；已知文档读取使用 read。排名检索使用 search(namespace, text, mode="bm25" | "vector" | "hybrid")。search 和 grep 的第一个参数必须是一个已存在的 namespace；不从 Filter 推断 namespace，也没有省略后搜索全实例的默认值。ByNamespace 已删除，ByDocumentId/UnderPath/AnyOf 的身份必须属于这个 namespace，越界报 InvalidFilter。

grep 逐文档读取，具有文档数、匹配数和读取量预算，超过预算返回 truncated；不能先加载全库全文再应用 limit。路径筛选无需读取正文。源后缀、namespace 和路径筛选在排名后端 top-k 之前应用。

grep 的 timeout 同样是总等待预算，包含排队、文字就绪、读取、匹配、来源定位和 Chunker；默认 5 秒，None 不设期限，0 立即超时。来源定位利用有序 SourceSpan 的二分查找，匹配偏移仅计算命中边界。某个文件 SourceUnavailable 或暂时 CapabilityUnavailable 时，保留其余文件的结果，设置 truncated=True，并在 failures 中返回 GrepFailure(id, TaskError)。损坏的持久状态、存储故障和无效查询仍使调用失败。read 已知文件仍直接报读取错误。文字打开逐级拒绝新出现的 symlink/reparse 和非普通文件，不重新 resolve 已存引用；显式借用的根外派生文件仍合法。

search 默认 consistency="eventual"、timeout=5.0 秒。eventual 查询当前有效索引，允许结果尚未补齐，但不能返回已失效旧源。显式 strong 目前等待指定的这一个 namespace 的索引工作。timeout 是调用方搜索的总等待预算，包含查询执行容量等待、一致性等待、query embedding、Milvus 调用、候选扩充及合并。到期抛 WaitTimeout；None 不设期限，0 立即超时。grepping 文字不需要等待 embedding。目录级 strong 尚未实现，讨论方案见下文。

SearchExecution 使用单调时钟建立一个 deadline；等待和阶段边界复用它，取得串行后端锁后重新核对并向 Milvus 传递剩余时间。排名搜索和 grep 各使用独立的有界执行池，容量分别为 ExecutionPolicy.queries（默认各 4），并与后台文件 Worker 池分开；向量查询占满槽位不占用 grep 的执行槽。超时调用返回后，尚未退出的 Adapter 继续占用容量及资源租约，返回后丢弃结果，不执行后续阶段，避免连续超时无限增加线程或任务。close(timeout=30) 立即关闭请求准入，由唯一清理线程取消执行、等待实际调用/读句柄退出，再关闭存储。调用方等待超时抛 WaitTimeout，清理继续，实例锁和资源在真实退出前不释放；再次 close 等同等待同一次清理，timeout=None 可无限等待。不能强杀不响应取消的 Python Adapter；需要硬退出时由宿主终止 MFS 进程。

hybrid 在指定 namespace 的同一 collection 内对 BM25/dense 使用 RRF。Reader 只打开该 namespace 的检索路由，一次查询只计算一次 query embedding；不提供跨 namespace 路由、分数比较或结果合并。宿主若有多个 Folder 的产品入口，由宿主明确组织各自范围和结果，MFS 不赋予它们统一排名。

使用 document_status、list_document_statuses、scope_status 读取当前状态。StashBase 自行判断文字是否可用并选择自己的 grep fallback；不新增 ready 订阅或额外范围就绪系统。

## 9. 内部职责与迁移

保留 MFS 入口和三个外部 Adapter interface。内部按职责拆分，不按职责启动线程：

| 内部职责 | 内容 |
| --- | --- |
| Lifecycle | 接收、失效、目标、执行许可、阶段提交、取消、等待和文件引用事务 |
| Preparation | Processor 调用、进度、checkpoint 和协作退出 |
| Indexing | 切片、embedding 复用、Milvus 发布/删除/重建 |
| Reader | 单 namespace 有界 grep、read、BM25/dense 融合及有效版本检查 |
| ArtifactStore | 受管理文件持久化、引用、读句柄和 GC |

慢调用在状态锁外，提交在短事务内统一核验。执行模块不各自维护另一套目标或互相归并队列。

处理缓存在生命周期锁内定位并 pin 元数据文件，文件读取、校验和 JSON 解析在锁外完成。返回前重新核对缓存条目及其引用文件的存续状态，再为引用取得覆盖本次处理的 pin；并发替换或 GC 使缓存失效时重新计算，旧读取失败不能删除后来替换的缓存条目。

SQLite 使用最多 8 个连接的实例内池，查询/事务结束即归还，不绑定调用方线程寿命。嵌套事务复用当前租约，最外层提交或回滚；事务中不得调用适配器、访问外部文件或反向申请生命周期锁。文档枚举按 128 行键集分页，每页释放连接，读取方逐条复核版本与资格；不持数据库游标跨越 grep/Chunker 等用户代码。

SQLite catalog schema 8 保存上述状态；元数据表结构可升级，但旧 namespace 需要调用方使用 migrate_namespace 显式提供适配器和索引模式。迁移撤销旧处理文字和索引，再从 Internal 原件或 External 原路径处理，不回放旧正文缓存。缺少可用原件或处理实现时明确报错。旧布局中的受管理文件在引用释放后由 GC 回收。

## 10. 验收

必须验证实际用户行为：External 未产生原件副本；原文变动后 grep 读当前文件；替换/删除/排除后旧结果立即失效；新处理失败、迟到结果、重启与重建均不能复活旧源；删除后重建不误删新数据；GC 不删外部引用。

namespace 验证包含不同 dim/model 共存、重开不匹配报错、规则原子更新与 include 后代、独立暂停/关闭索引、同根/嵌套根的独立生命周期及状态目录保护。后台验证包含反复 sync 合并、单文件顺序、多文件并发、崩溃恢复与当前任务等待。搜索验证包含显式单 namespace、拒绝跨 namespace Filter、grep 预算、过滤下推、总等待超时及无公开 query。

MFS 测试不等于 StashBase 已迁移。对接实际转换器、检索效果和应用事件链路需要在 StashBase 单独验收。

## 11. 内部职责与状态迁移

Lifecycle 独占接收、领取、阶段提交、取消和资格状态；多个 Worker 共享这一份状态，执行模块不维护第二套队列。Preparation 负责算子和产物；Indexing 负责索引阶段；Configuration 负责候选代成员、切换和退休；IndexCleanup 负责版本精确的清理债务。NamespaceRuntime 管理 Adapter 绑定、Processor/Chunker 资源准入与 collection 路由。Reader 通过 ReadView 读取资格；ArtifactStore 管理引用与 GC。

ExecutionPermit 包含文件/namespace 身份、输入版本、配置代、active_run_id、attempt_token、取消信号和资源租约。Prepared/Chunked/Embedded/Published 等结果只交回 Lifecycle；统一的 finish_execution 在真实调用退出后提交或丢弃结果，最后退休租约。

SQLite 自动升级至 schema 8，增加 active_runs、build_targets、build_documents、index_cleanup，保留 targets、取消门、引用、已准备结果和有界 vector_cache。旧 schema 7 无 active 的目标仍可恢复；旧运行状态重新排队。没有恢复已移除的 runs/wait_operations/wait_target_sets/run_dependencies 历史等待表，旧 operation ID 仍不能作为等待凭证。

## 12. 处理调度与后续扩展

### 目录级 strong

当前 search(namespace, text, consistency="strong") 只等待显式指定的一个 namespace。StashBase 一个 Folder 一个 namespace 已隔离其他 Folder，默认 eventual 适合交互搜索，本轮不增加目录级 strong。

未来若要“只等 notes/，不等 recordings/”，可按 UnderPath/ByDocumentId/AnyOf 选择当前目标，同时计入 namespace 重建；继续沿用同一次搜索总 deadline，避免目录外工作阻塞。现有 wait(namespace, path=...) 已能单独等待路径当前工作，但不提供搜索快照隔离，也不隐含改变 strong 的范围。

### 故障确认、输入校验与宿主生命周期

状态命令在 SQLite 提交前或提交后抛错时，必须先从持久 namespace/target/document 重建内存资格，再释放 Lifecycle 锁。删除、规则、取消、重建和配置使用同一个事务核对入口；执行完成仍使用原有有条件重试/退休协议。核对失败就停止调度、撤销查询资格并报告 StorageFailed，旧许可不能继续提交。显式提供的新 Adapter 仅在持久 manifest 相符时绑定，包含重建已经提交但调用报错的情形。

取消目标遇到索引重建时，用户取消门保持；已准备文字对应的索引阶段重置到 chunk，并清空旧计划及批次进度，不能在新的空 collection 上继续旧 publish。strong、wait_ready 和当前范围等待共享失败终态判断，failed/blocked/cancelled 立即报告 OperationFailed。

External 准备以源 hash 和文件身份/变动时间建立校验。checkpoint 文件复制后、处理返回及缓存发布前重新检查；未被观察确认的改变，包括修改后恢复字节和 mtime，不能发布恢复数据或完整结果。兼容的同内容 sync 允许刷新 stat，并重新验证 hash。输入校验与外部写入并非原子快照：若其他写入者在处理期间改变、恢复内容并通过同内容观察更新 stat，MFS 无法证明 Processor 的全部读取属于同一瞬间。宿主自己的写入必须使用 Scope Lease 和路径互斥；需要更强读取语义的 Adapter 还须自行保证稳定输入。处理缓存使用新的格式键，避免继续复用旧版本未经此校验产生的条目。

索引阶段读取文字时核对处理快照的 text_hash；不将后来变化的借用文字配上旧 SourceMap。text_path 与 grep_path 指向本次 work_dir 时都复制到受管理目录并建立引用；read 与 grep 为不同于索引文字的 grep 视图统一重建行定位，外部借用文件仍由宿主保证存续。sync 记录成功观察范围和失败前缀，按文件的路径祖先查询覆盖集合；无关子目录失败不再阻止已确认缺失的撤销。

POSIX run_process 由独立监督进程启动命令进程组，用管道 EOF 检测宿主死亡。监督进程继承 PROCESS_LOCK 的 flock 描述符；清理并确认命令组没有仍运行的成员后才释放，防止新实例恢复与旧 native 执行/文件使用重叠。监督进程保留未 reap 的组长 PID，避免退休时将复用 PID 当旧进程；僵尸不再持有源句柄，不阻碍退休。若不能确认退休，保留锁并重试。它依赖 POSIX waitid 和 /bin/ps；命令不能自行脱离受管理 session。Windows 使用 Job Object 的原生父死清理。冻结应用通过公开 run_process_supervisor() 在初始化前分派私有监督调用；普通 Python 直接启动随包提供的脚本。

### 宿主源操作与迁移准入

`quiesce(scopes: Sequence[UnderPath], timeout=None) -> ScopeLease` 要求至少一个显式范围。接收租约时捕获 namespace incarnation，禁止匹配的新文件执行、namespace 控制工作及源文字读取，协作停止正在运行的调用，并等待实际执行/读取退出。超时或 close 会解除这次尚未取得的租约；重叠租约独立计数，释放旧 namespace 的租约不限制后来同名 namespace。租约可由另一线程关闭；实例关闭后释放也安全。租约不持有实例的前台调用许可，不阻塞 close。

租约内 read 报 CapabilityUnavailable，需要读取文字的 grep 对相应文件返回部分失败；metadata grep、已有排名搜索及受管理 artifact 句柄不需要打开源文件，仍可使用。读取注册与当前 revision 校验在同一锁内进行，旧文档记录不能在 namespace 重建后迟到打开源。用户取消状态不受临时停止影响，释放后有效目标从持久阶段/checkpoint 继续。租约不能控制外部编辑器，也不替代应用的文件事务锁：sync 在租约内仍可观察源，扫描读取与磁盘操作由宿主互斥；应用把同一源对应的全部 namespace 范围一起退休，并在租约释放后再 wait。播放转换及 Viewer 句柄仍由宿主退休。

`create_namespace(..., processing_paused=True)` 持久禁止准备/新增索引的领取；`configure_processing(namespace, paused=...)` 修改此门并请求活动文件执行协作退出。删除/旧代清理和 namespace 控制仍可执行；是否已经实际退出需另用 quiesce。namespace_configuration 返回 processing_paused；旧记录缺此字段解释为 False，无需改 catalog schema。

迁移先创建处理暂停的 namespace、观察源，再调用 `restore_document_state(id, expected_revision=..., state="failed" | "cancelled", error=...)`。它只接受匹配 revision、尚未执行的 upsert；failed 必须有 TaskError，cancelled 建立持久用户取消门。目标保存一次导入声明，相同声明重放不改变后来显式 retry/cancel 的结果，不同声明或旧 revision 拒绝。宿主持久保存迁移步骤，恢复所有规则/用户意图后才解除处理暂停。此接口不读取 StashBase 数据库，也不提供另一套运行队列。

### 多 Worker 与资源准入

ExecutionPolicy 默认 workers=4、queries=4、resources={"heavy":1,"light":2}；另有一个索引/配置维护线程和一个可关闭的 GC 线程。容量不是固定 OS 线程总数：Milvus/native 库还可能有自己的线程。调度先非阻塞申请完整资源，再持久领取阶段；等资源不占文件执行或 Worker。

Processor/Chunker 可声明 workload="heavy"/"light"，或 resources 映射；未声明时默认 heavy=1。内置文本、DOCX、Chunker 使用 light，PDF 使用 heavy。远程处理可以显式 resources={}；同一对象的 concurrency 限制仍生效，默认 1。内置无状态文本/Chunker 允许并发，子类需重新声明。grep Chunker 与后台使用相同对象/资源额度。

Embedder 的后台调用受 workers 限制，前台调用受排名搜索池 queries 限制；两者不共享额外额度，MFS 不读取 Embedder 的 concurrency、workload 或 resources 声明。共享 Embedder 对象或其他处理占满 heavy 资源不会因此阻塞查询 embedding。具体模型的串行要求、线程安全和服务限流由实现或宿主负责；查询超时后，实际调用仍保留查询槽和 collection 租约直到返回。

LocalAdmission 可被同进程宿主共享；Admission.try_acquire(resources) 可由宿主替换，但必须非阻塞、一次全部获得。跨进程实现应通过预取/异步通知更新本地 grant，不能持 Lifecycle 锁做 RPC。归还发生在实际调用退出后，超时、断连、取消接收都不允许宿主重复发放仍在使用的额度。

checkpoint 仍持久保存恢复数据，并可在当前目录更紧急的任务到达时协作让出。set_active_scopes 和等待老化决定优先级，后台老化最多提升到 active-folder，不能超过显式交互任务；阶段结束归还资源。长任务应在有界单元后 checkpoint。普通 Python/native 调用不能强杀；需要协作取消或 run_process 的进程监督。

### 配置一次接收、候选代完整切换

configure_namespace(namespace, processors=..., chunker=..., embedder=..., indexing=...) 原子接收目标清单，返回 ConfigurationReport。比较兼容描述与模式，相同配置只换绑定。仅 Embedder 变动复用文字和兼容切片；Chunker 变动重切片；Processor 变化只重做受影响格式。reprocess_namespace 强制重处理，reindex 强制重建索引，两者复用同一协议。

声明接收不逐个写入所有成员；维护线程每轮最多补齐 32 个缺失/过期候选目标，逐个释放生命周期锁，故障和重启均从持久声明恢复。成员目标和准备文字在同一事务保存；提交确认丢失时核对并采用精确的持久值。候选维护失败最多自动重试 5 次，以 pending_error/pending_failures/pending_retry_at 公开；重新提交相同清单重置故障预算，沿用原候选 revision。候选缺成员时 strong 文字就绪也必须等待补齐。重绑时在采用运行对象前原子复核 incarnation 和配置代，验证期间晋升/替换则拒绝过时绑定。

维护重试计数覆盖晋升事务，只有完整一轮成功后才清除故障。成员尚未生成时，wait 等待候选补齐，不把活动代的旧失败误报为候选失败。晋升和 namespace 删除会移除不再使用的运行绑定；在途执行/查询保留自身对象引用到实际退出，闲置 Worker 不保留上一张执行许可。库不主动关闭宿主共享的 Adapter。

grep_path 与内存索引文字并用时，已发布后的临时文字可释放。重开或配置变更需要该文字时，先退休 chunk/embed 阶段，再以 process 阶段重新申请 Processor/concurrency/资源额度；不能在切片或 embedding 的租约内直接运行 Processor。临时文字释放与同文件下一次执行领取受同一生命周期锁保护。

G0 继续服务，G1 保存私有文字/索引。源变动更新两代的期望成员；同文件仍顺序执行。所有当前成员在 G1 成功才切换，失败或用户取消阻止切换而保留 G0。strong grep 可读取 G1 已完成的文字，不依赖其向量成功；eventual grep 留在 G0。

configure_index 比较最新候选（无候选时比较 active），只改 paused 不覆盖候选模式；关闭后再开启同样以最后请求为准。开启自动追赶已接收输入，不触发 External sync；关闭保留准备文字，排名索引异步清理。再次改配置时只保留最新候选；过时调用在阶段边界和协作检查点提前退出；旧在途调用真实退出后才继续同文件。最多一个 active、一个 building，以及有界的 retiring generations；两代尚不能退休时暂停新 collection 创建。切换事务同时更新配置、collection 和 publication；提交后确认丢失也必须采用已持久配置对应的绑定。

namespace_configuration 返回 active_revision/pending_revision 和两份清单。重启通过 open_namespace(..., configuration_revision=...) 分别绑定 G0/G1，缺新模型只阻塞构建；不允许 G1 Embedder 查询 G0 collection。

### 并行扫描

同 namespace 扫描串行，不同 namespace 可并行；遍历和 hash 不持实例全局 mutation 锁；root 的 resolve/stat 也在生命周期锁外完成，随后锁内复核 namespace 身份与配置。protected/nonmember 用集合及有限祖先查询，消除旧 O(N²) 路径检查。现有 SQLite namespace/path 主键和目标索引承担目录查询；不维护第二份完整文件树，也不凭目录 mtime 跳过子文件检查。

扫描捕获 namespace incarnation、绑定、root 和规则版本，提交时复核；删除只针对扫描开始时已知且版本未变的目标，旧扫描不能删除后来接收的文件。无变化不创建后台任务，失败前缀保守保留。

未知后缀 sniff 在生命周期锁外执行，并复用已安全打开的源 descriptor 的 head；接收时只核对所选媒体/Processor 声明，不重新打开 External 路径或再次调用 sniff。Internal sniff 同样在锁外；从 Path 复制输入通过 no-follow/nonblocking 的普通文件打开，避免 stat 与打开间替换为 FIFO 导致接收/关闭无法退出。任意外部写入仍没有快照隔离，处理阶段继续核对已接收源 hash。

### 宿主启动恢复

MFS.open(..., start_paused=True) 在任何 Worker、索引维护和 GC 启动前安装恢复门。允许配置、状态与 sync，读取暂不可用。宿主读取自己的持久磁盘事务日志，绑定实现、quiesce 所有关联范围、恢复磁盘操作并确认 sync 完整接收，释放租约后调用 resume_background()。该门本身不是事务日志；重启时宿主必须根据未完成日志再次选择 start_paused。

StashBase 现有旧 mfs-cli daemon 和 Node Preparation 队列尚未迁移。本轮提供 MFS 的执行、共享资源和恢复边界；应用侧的原生转换器、RPC grant、路径日志与 Viewer 句柄适配见对接文档，不把库测试称作应用验收。
