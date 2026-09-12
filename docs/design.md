# MFS 设计

本文是唯一设计正文，统一本轮已确认的行为。实现与验收结果见 [backlog](backlog.md)，StashBase 的具体接入见 [对接说明](stashbase-integration.md)。

## 1. 实例、namespace 与存储

MFS 是嵌入式 Python 库。保留现有实例：一个实例持有状态目录、数据库连接、内存待办和后台处理线程。业务配置属于 namespace，没有全局 Processor、Chunker、Embedder、ignore 或索引策略继承。

一个 namespace 有稳定身份、Internal/External 所有权类型及独立配置。DocumentId 是 (namespace, doc_id)，不同路径即使内容相同也是不同文档。计算结果可以复用，文档身份不合并。

- SQLite：每实例一个 catalog，共享元数据表。文档和任务按 namespace/doc_id 区分；每文件保存当前任务，namespace 控制任务单独调度。
- Milvus：每实例一个数据库文件；每 namespace 一个活动 collection，支持不同向量维度。collection 使用不可复用的 namespace 创建 ID 命名。
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

调用方可以显式向两个 namespace 传同一个对象；持久身份、规则和索引仍独立。模型调用不持有 MFS 状态锁；Adapter 需要支持查询和后台调用可能重叠，或自行串行化。

有意更换 Processor/Chunker/Embedder 分别使用 reprocess_namespace / reindex；索引模式和暂停使用 configure_index。源内容未变的索引重建保留有效文字；源已替换则不能恢复旧文字。迁移要有持久状态，失败可重试，不能把半完成配置作为活动配置。

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

显式 reindex 推进 index_epoch 并清理该 namespace 的旧缓存；drop 清理旧 incarnation 的缓存，同名重建使用新 incarnation。缓存损坏或淘汰后重新计算，不影响现有搜索结果。升级后的缓存开始为空，不保证首次重命名就命中；它是有界计算优化，不是无限期保存旧向量的合同。

直接文本的 grep 使用实际读取的行号和偏移。借用的提取文字仅在内容 hash 仍匹配时沿用保存的来源映射，已变化则退回当前文字行号。两次 sync 之间，grep 与索引不保证来自相同 bytes。

## 5. 接收、处理与恢复

```text
调用方 sync / internal upsert
  → 观察源、计算 hash、判断是否变化
  → SQLite 事务保存最新目标、撤销旧结果、登记清理责任
  → 更新内存待办，返回接收结果

一个后台 worker
  → 清理旧代
  → Processor 读取输入/产生必要文字
  → Chunker 产生范围和 hash
  → 复用已有向量，计算缺失向量
  → 发布完整的新索引代
  → 记录完成，释放临时文件

独立 GC
  → 回收不再被有效文档、任务或读句柄引用的 MFS 文件
```

每实例一份内存待办，键为 (namespace, doc_id)。每文件只有一个最新目标，整个实例只有一个后台文件处理链路；不建立多个阶段队列。SQLite 是可恢复状态，内存只用于调度；没有任务时等待通知，不循环扫描 SQLite。

重复 sync 未改变内容/配置时沿用任务；若已验证内容 hash 相同但源 stat 改变，事务更新当前目标的观察元数据，保留 revision、阶段、attempt token 和取消状态。正在执行的旧任务副本提交阶段结果时，合并到最新目标，不能覆盖新的观察元数据。R1→R2→R3 尚未开始则只处理 R3；R1 正在运行时，允许该调用退出，旧结果不能提交成 R3。阶段提交前核对目标 revision、namespace 创建代和执行身份。已经承诺的旧代清理责任不能被最新目标覆盖。

sync 不等待 Processor/embedding；调用方可以继续 sync/grep。慢 Adapter 会推迟其他文件的后台处理和物理删除，所以对外失效必须在接收事务中完成。

wait(DocumentId) 或 wait(namespace, path=...) 核对当前目标，期间接受新源就继续等新源，namespace 的重建/删除也计入。传入 MutationReport、SyncReport、DropReport 只是对应身份/范围的简写。当前 failed/blocked/cancelled 抛 OperationFailed；超时抛 WaitTimeout；不完整 SyncReport 直接报观察失败。revision 表示源目标版本，源未变的索引重建可以保留 revision，完成判断仍使用当前状态。相同内容的 sync 不自动重试失败目标。

不保存每次调用的操作历史。显式 idempotency_key 仍持久去重：相同请求重放首次接收结果，不同请求冲突；重放不会覆盖后来更新的文件状态，wait(report) 仍等待当前工作。

Processor 可使用 ProcessingContext 提交进度、checkpoint，检查协作取消，运行受管理子进程。重启恢复未完成阶段；外部引用已失效就报错，不能恢复外部原件副本。失败/暂停/重试等待的文件让出 worker；持久 checkpoint 也可以触发协作让出，再次调用 Processor 时通过 resume_state/resume_files 恢复。Adapter 应在有限工作单元后 checkpoint，并正确跳过已完成单元。

## 6. 替换、删除与可见性

确认源替换、删除或被规则排除时，旧文字、附属产物和索引立即退出当前读取/搜索入口，不等待新 Processor 成功。新源失败时显示失败，不回退旧 PDF。

物理删除异步且可重放。搜索对 Milvus 候选核验当前有效索引代；旧行尚未删除也不能返回。过滤失效候选后，在预算内继续取候选，不能用旧内容凑满 top-k。

删除先在同一 SQLite 事务撤销文档入口、持久保存删除工作，再通知 worker。崩溃后继续删除；Milvus 已删而 SQLite 未标完成时重复删除。恢复删除工作不等于恢复已删源文件。

同路径删除后重新出现属于新代，单 worker 保证先清理旧数据，再发布新数据。清理使用 namespace 创建代定位 collection；namespace 删除后同名重建也不能接受旧任务写入。GC 只删 MFS 拥有的文件，保护有效共享引用和已打开句柄，不删用户源文件和借用的应用产物。

sync 的缺失判断只覆盖成功观察的范围。整个 root 不可读、扫描不完整等不是所有文件被删除；保留未确认区域。明确观察到的排除、类型变化和缺失则撤销相应旧结果。

## 7. Namespace 规则与索引控制

Ignore 是 namespace 自己的一份有序规则，无全局继承，也不隐式读取 .gitignore。每条规则有稳定 rule_id、include/exclude 动作和模式，支持文件、目录、后缀及 glob；后匹配规则覆盖前规则。支持原子增删改和排序。

- 无斜杠模式匹配任意层级名称；带斜杠模式相对 namespace root，前导 / 表示根锚定。
- 目录规则覆盖后代；支持 *、?、字符集合和 **。
- 父目录排除后仍允许显式包含后代，扫描不能直接剪掉可能被重新包含的目录。
- expected_revision 只用于规则更新的冲突检查；不匹配立即报错，不阻塞等待。
- 规则提交后读路径和任务提交使用新规则；新增排除立即失效并清理，重新包含通过 sync/reconcile 重新接收。
- StashBase 特有的 sibling/派生文件关系由应用转成明确规则，不把应用业务回调塞进基础 glob。

namespace_configuration(namespace) 返回只读配置副本：namespace/kind/root、indexing、paused、max_file_bytes、完整 manifest 和 pending_manifest。它不包含运行对象或凭据；未绑定 Adapter 也可读取。规则继续通过 rules(namespace) 获取。

索引策略为 off/bm25/hybrid，并独立提供 paused：

| 策略 | 行为 |
| --- | --- |
| off | 保留文件观察、处理和 grep，清除排名索引，不调用 Embedder |
| bm25 | 只构建 BM25，不调用 Embedder |
| hybrid | 构建 BM25 和 dense，必须绑定相容 Embedder |
| paused | 暂停新增索引工作；源观察、必要文字处理、逻辑失效和旧代清理继续 |

search(namespace, text, mode="bm25") 只选择这次查询通道，不会关闭后台 embedding。取消单文件也不等于 namespace 暂停。

## 8. 搜索与读取

删除公开 query 名称，不保留别名。非排序文字、文件名和路径匹配统一使用 grep；已知文档读取使用 read。排名检索使用 search(namespace, text, mode="bm25" | "vector" | "hybrid")。search 和 grep 的第一个参数必须是一个已存在的 namespace；不从 Filter 推断 namespace，也没有省略后搜索全实例的默认值。ByNamespace 已删除，ByDocumentId/UnderPath/AnyOf 的身份必须属于这个 namespace，越界报 InvalidFilter。

grep 逐文档读取，具有文档数、匹配数和读取量预算，超过预算返回 truncated；不能先加载全库全文再应用 limit。路径筛选无需读取正文。源后缀、namespace 和路径筛选在排名后端 top-k 之前应用。

search 默认 consistency="eventual"、timeout=5.0 秒。eventual 查询当前有效索引，允许结果尚未补齐，但不能返回已失效旧源。显式 strong 目前等待指定的这一个 namespace 的索引工作。timeout 是调用方搜索的总等待预算，包含查询执行容量等待、一致性等待、query embedding、Milvus 调用、候选扩充及合并。到期抛 WaitTimeout；None 不设期限，0 立即超时。grepping 文字不需要等待 embedding。目录级 strong 尚未实现，讨论方案见下文。

SearchExecution 使用单调时钟建立一个 deadline；等待和阶段边界复用它，Milvus 接收剩余时间。查询使用至多 4 个执行线程，与单个后台文件处理 worker 分开；超时调用返回后，尚未退出的 Adapter 继续占用容量及资源租约，返回后丢弃结果，不执行后续阶段，避免连续超时无限增加线程或任务。close 唤醒调用方并等待实际执行退出，再关闭存储。不能强杀不响应取消的 Python Adapter；这个限制不延长搜索调用方的等待预算。

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

SQLite catalog schema 7 保存上述状态；元数据表结构可升级，但旧 namespace 需要调用方使用 migrate_namespace 显式提供适配器和索引模式。迁移撤销旧处理文字和索引，再从 Internal 原件或 External 原路径处理，不回放旧正文缓存。缺少可用原件或处理实现时明确报错。旧布局中的受管理文件在引用释放后由 GC 回收。

## 10. 验收

必须验证实际用户行为：External 未产生原件副本；原文变动后 grep 读当前文件；替换/删除/排除后旧结果立即失效；新处理失败、迟到结果、重启与重建均不能复活旧源；删除后重建不误删新数据；GC 不删外部引用。

namespace 验证包含不同 dim/model 共存、重开不匹配报错、规则原子更新与 include 后代、独立暂停/关闭索引、同根/嵌套根的独立生命周期及状态目录保护。后台验证包含反复 sync 合并、单文件顺序、单 worker、崩溃恢复与当前任务等待。搜索验证包含显式单 namespace、拒绝跨 namespace Filter、grep 预算、过滤下推、总等待超时及无公开 query。

MFS 测试不等于 StashBase 已迁移。对接实际转换器、检索效果和应用事件链路需要在 StashBase 单独验收。

## 11. 内部重构与当前状态迁移（REF-002 / RECEIPT-002）

一个文件处理 Worker、一个独立 GC；前台 SearchExecution 管理查询期限与资源租约。

| Module | 输入/输出与责任 |
| --- | --- |
| MFS | 公开 API、源观察入口、参数路由及实例资源生命周期 |
| Lifecycle | SourceInput 或状态命令 → 接收结果；领取 ExecutionPermit；统一提交 StepResult、失效、取消、规则和重建事务；维护等待条件 |
| Worker | 领取一次执行，调用 Preparation/Indexing，将结果交回 Lifecycle；持有唯一处理循环 |
| Preparation | 执行 Processor、校验文字/SourceMap/产物；进度和 checkpoint 回调进入 Lifecycle |
| Indexing | 切片、向量复用、Milvus 写入/清理/重建；返回阶段结果，不修改共享任务 |
| NamespaceRuntime | Adapter 绑定、collection 路由、模型匹配和向量校验 |
| Reader | 通过 ReadView 读取资格；执行 grep/read/search，返回前再次核验有效代 |
| ArtifactStore | 管理原件接收、产物持久化、文字引用解码和 GC；逻辑引用随 Lifecycle 事务提交 |
| Catalog | SQLite JSON 编解码、任务种类/阶段/状态校验、schema 升级及引用存储 |

ExecutionPermit 区分 FileWork 与 NamespaceWork，包含身份、revision、namespace 创建代、attempt token、取消信号及独立任务数据。Prepared/Chunked/Embedded/Published/Cleaned/Rebuilt 是明确的阶段结果。Lifecycle 提交时核对当前目标、许可身份、规则和取消状态，过期结果不能推进目标。慢调用在状态锁外。

Preparation、Indexing、Reader、ArtifactStore 不接收整个 MFS。执行模块使用 Lifecycle 方法读取任务副本、提交进度或查询资格；Reader 使用只读 ReadView。公开文件状态查询暂保留既有空 doc_id 的 namespace 控制任务表示，便于诊断和 retry；内部执行使用 NamespaceWork，控制任务不与文件处理混用。

SQLite 自动升级至 schema 7，移除 runs、wait_operations、wait_target_sets、run_dependencies。新增有界 vector_cache，保留 targets、取消门、已准备结果、引用及 operations 中显式 idempotency_key 的去重记录；后者移除旧 operation_id 字段。MutationReport/SyncReport/DropReport 不再包含 operation_id，wait 的字符串参数改为 namespace。调用方保存的旧 operation ID 不再受支持，需改为 DocumentId 或 namespace/path；不将它解释为历史完成。

| 文件操作 | 持久状态 | 重启后的动作 |
| --- | --- | --- |
| 新增 | 一个 upsert 目标、源版本和当前阶段 | 从当前阶段加入待办 |
| 更新 | 同一行覆盖为新源版本，并保留 cleanup 责任 | 清理旧索引，再处理最新内容 |
| 删除 | 同一行改为 delete，即文件 tombstone | 继续清理；已完成的幂等删除可以重放 |

恢复读取 targets，而非只读 documents：未提取文件、取消目标和删除任务都可能没有可读文档。更新不建立两条 delete/insert 历史事件；Milvus 清理旧片段再插入新片段属于实现步骤。文件粒度删除目标及 namespace 清理责任在重启后仍存在。删除完成目标保持终态，后续该文件重新出现会覆盖同一目标。

源版本以前成功过也不能证明当前索引构建完成。模型重建、无变化 sync、重复写入、取消恢复，以及接收/删除事务后进程退出都用当前目标验证；故障注入覆盖事务回滚、提交后失联和迟到执行。

## 12. 处理调度与后续扩展

### 目录级 strong

当前 search(namespace, text, consistency="strong") 只等待显式指定的一个 namespace。StashBase 一个 Folder 一个 namespace 已隔离其他 Folder，默认 eventual 适合交互搜索，本轮不增加目录级 strong。

未来若要“只等 notes/，不等 recordings/”，可按 UnderPath/ByDocumentId/AnyOf 选择当前目标，同时计入 namespace 重建；继续沿用同一次搜索总 deadline，避免目录外工作阻塞。现有 wait(namespace, path=...) 已能单独等待路径当前工作，但不提供搜索快照隔离，也不隐含改变 strong 的范围。

### 单 worker 协作让出（已实现）

方案 A 已实现。checkpoint 持久保存恢复数据后，根据当前可运行工作决定是否协作让出；每个阶段完成后也重新选择目标。实例仍只有一个文件 worker，不同时执行多个文件；方案 B、C 是尚未实施的替代或扩展方案。

目标是让新打开/导入的短任务能在长 PDF/转录的持久工作单元之间获得执行机会，同时保证替换、取消、重试、重建、进程恢复和 GC 不会复活旧目标。已有文字的 grep 和已有索引的 eventual search 继续独立执行。没有安全点或不响应取消的外部调用无法强制抢占；响应时间取决于 Adapter 单次调用期限和工作单元大小。StashBase 的十分钟音频单元表示源音频长度，不是处理耗时上限。

#### 三种 Interface 的比较

| 方案 | Interface 与调用方式 | 隐藏的工作及代价 |
| --- | --- | --- |
| A：一个 worker，在 checkpoint 协作让出（已实现） | 保留 process(path, media_type, context)、context.checkpoint(state, files=...)、set_active_scopes | Lifecycle 统一调度、恢复和退出；不新增公开优先级或线程池配置；不能恢复 StashBase light/heavy 的并行吞吐 |
| B：有界文件并发与资源容量 | 可选 SchedulingPolicy(file_concurrency, capacities)；Adapter 声明共享资源需求，命名如 light/heavy，由宿主决定 | 多个完整文件执行器共用一个当前目标集；需要文件执行租约、namespace 屏障、绑定快照、查询资源计费和精细 GC；不是把线程数改大 |
| C：宿主拥有 Preparation，MFS 接收完整文字 | 借用文字 Processor；宿主通过有条件的完成通知唤醒相同目标 | 保留 StashBase 的 2 light/1 heavy 和分段让出；跨进程取消、丢失唤醒、文字文件保留和整体等待的合同更复杂 |

A 让应用继续调用现有 checkpoint，把调度正确性集中在 Lifecycle；B 通过资源声明支持多个工作负载，但改动覆盖存储读写与运行对象的生命周期；C 容易复用现有 StashBase 实现，端到端状态则分布在两个进程。当前采用 A 的执行协议；需要同时处理轻重任务的吞吐指标时，再实施 B。C 是独立的迁移选择，不在一个 Processor 中隐式启动并等待另一套长期队列。

示例仍只声明完成的工作单元：

```python
for unit in units_after(context.resume_state):
    process_unit(unit, context.work_dir)
    context.checkpoint({"completed": unit}, files={"partial": partial_path})
return assemble_complete_document()
```

#### Module 与依赖

Lifecycle 拥有唯一的 Document Target、可执行性判断、执行许可和提交事务。Worker 只运行领取/执行/完成循环，不另外维护队列。Preparation 与 Indexing 返回阶段结果，不决定何时释放同一文件的执行权。内部使用统一的 finish_execution(permit, result, error) 收口阶段提交、让出、停止、失败和退休；不让 Worker 把 requeue 与 retire 随意组合。这是内部 Interface，不增加应用必须调用的方法。

调度与 Lifecycle 是 in-process 依赖；SQLite、文件和 Milvus 是可用本地实现验证的 local-substitutable 依赖；Processor/Embedder 是 true external 的注入 Adapter。仅方案 C 的 Node/Python 连接属于 remote but owned，使用生产 RPC Adapter 与测试 Adapter 验证通知协议，不新增可替换的 Scheduler Adapter。

#### 执行许可与不变量

1. 一个 DocumentId 至多一个**实际仍未退出**的执行。revision 被覆盖或取消后，其旧调用、finally 和受管理子进程仍持有执行租约；新目标可以接收，但要等旧执行退出并完成必要清理才能执行。
2. 许可验证同时检查活跃租约、DocumentId、Source Revision、namespace incarnation 和 attempt token。token 字符串相同但许可已退休，也不能提交 checkpoint、进度或结果。完成与退休必须撤销旧许可的权力。
3. 每次执行捕获不可变的 Adapter 绑定与 collection 路由；慢调用期间不按 namespace 名字重新查可能已替换的运行对象。重建另使用持久 index_epoch，因为源 revision 未变也可能已经是另一轮索引构建。epoch 只限制相关索引执行，兼容的准备文字和 checkpoint 可以保留。
4. 替换/删除/排除/drop 的接收事务立即撤销可见性，持久登记清理责任并使旧许可失效。迟到的 Milvus 写入可能已经发生；必须先退休旧执行、再清理旧写入、最后发布新代。提交 token 校验不能代替这个物理写入顺序。
5. 只有用户 cancel 创建持久取消门。调度让出、源替换、drop、close 都不创建它；自动唤醒不得清除它。显式 retry/reprocess 才解除用户意图。
6. checkpoint 只发布私有恢复数据，不发布部分 Document、Artifact 或索引。让出不算失败、不清空既有失败次数，也不补充自动重试预算；尝试启动次数与失败次数分别记录。

#### Checkpoint 与退休顺序

- 当前执行租约保护输入、工作目录和旧 resume_files。先复制不可变 checkpoint 文件并 fsync，再进入短事务。
- 在 Lifecycle 锁内验证许可，把 checkpoint 状态与文件引用原子提交。若提交成功但确认失败，重新读取持久状态确定结果；不能猜测未提交或丢掉引用。复制后未提交的文件作为无引用产物由 GC 回收。
- 使用与领取执行完全相同的可执行性和优先级函数，判断是否有更紧急的**可运行**目标。未到期重试、暂停的索引阶段、未绑定 Adapter 的工作不能导致无意义的让出。
- 若应让出，在许可上记录不可撤回的 yield_requested，再触发内部 _ProcessingYielded。此时目标仍处于执行中，租约尚未释放，继续等待 Python 栈和子进程退出。
- 实际退出后，finish_execution 在同一个有条件的完成流程中检查目标是否仍有效：有效则保留 revision、阶段、checkpoint 与失败次数，改为 pending；无效则只退休旧执行，不能覆盖新目标或取消状态。撤销 token、把引用交给持久目标，再释放执行租约并通知等待者。
- 下一次领取使用新 token、新 work_dir 和已保存的 resume_state/resume_files，不恢复 Python 调用栈。MFS 保证恢复数据持久、与目标匹配；Processor 负责根据这些数据跳过已完成工作单元。

Processor 可能通过 finally:return 吞掉内部异常，因此正常返回不能清除 yield_requested；让出后返回的“完整结果”必须丢弃。若退出清理真的抛出异常，且目标仍有效，按正常失败记录错误并保留 checkpoint，不能把错误伪装成成功让出。已失效执行的异常不得污染新目标。若退休事务失败，保留租约并重试/核对持久状态；连续三次无法完成后停止调度，wait 报 StorageFailed，保留租约阻止 GC。close 仍可关闭已退出执行的实例；重开从持久目标/checkpoint 恢复。

#### 并发操作与存储屏障

| 并发事件 | 顺序与结果 |
| --- | --- |
| sync 在 checkpoint 文件复制或事务前替换源 | 接收新 revision，撤销旧许可；旧 checkpoint 不得成为新目标的恢复输入 |
| sync 在 checkpoint 提交后、旧调用退出前替换源 | 新目标承担旧代清理；旧调用只能退休，新目标等实际退出后再运行 |
| 用户 cancel 与让出相遇 | cancel 优先；取消门保留，不能重新排为可运行工作 |
| cancel 后马上 retry | 可以接收新尝试意图；旧调用退出前不实际启动新执行，旧 token 始终不能提交 |
| reindex 时源 revision 不变 | 接收事务建立 namespace 索引屏障并推进 index_epoch；旧索引执行不能发布新构建，兼容准备结果仍可复用 |
| 重建被更新的重建覆盖 | 屏障保持关闭；旧控制执行实际退出后，再执行最新控制目标；旧完成不能切换活动 manifest |
| drop 后立即创建同名 namespace | 旧任务只使用捕获的 incarnation/collection；新 namespace 使用新 incarnation；旧删除不能路由到新 collection |
| 查询遇到重建/drop | 捕获 binding/collection 和取得查询租约，与建立重建/drop 屏障在同一 Lifecycle 锁内排序；租约覆盖 query embedding 及进入后端前的间隙。新查询遵守失效/重建状态；破坏性 drop/recreate 等所有已获准查询真实退出，不能持状态锁等待后端 |
| 搜索调用方已超时、外部调用仍运行 | 继续保留查询容量与 collection/文件租约，直到真实执行退出；迟到结果丢弃 |
| close 与 checkpoint/取消/索引调用相遇 | 停止新领取、唤醒等待者并发出协作取消；存储与文件租约保留到所有执行退出 |

A 继续使用粗粒度 GC 保护：实际执行尚未退出时不回收受管理文件；退休后依靠持久 target/prepared/document 引用保护 checkpoint 和完整结果。宿主拥有的借用文字不受 MFS GC 控制；宿主必须保证引用期间可读，或者明确接受读取失败。

B 还必须实现：全局有界执行容量；按 DocumentId 排他至实际退出；按 namespace incarnation 的读写/控制屏障；一次原子获取所有资源，避免互相持有部分资源造成死锁；共享 Adapter 对象的并发上限覆盖查询 embedding，等待计入搜索 deadline；把 Preparation.transient_text 单例改成受租约保护的每执行数据；以执行级文件引用代替“任何执行都阻止 GC”。最初可串行化共享 Milvus client 调用，慢调用不持 Lifecycle 状态锁。没有这些措施，不启用多个文件 worker。

#### 优先级、公平性与恢复

set_active_scopes 继续作为应用提示。清理和 namespace 控制工作的依赖关系先于优先级；每个 checkpoint 和阶段边界都重新选择，移除无条件 preferred。普通目标按现有 force/active/background 基础顺序，加只在“可运行且排队”期间累计的等待老化，平级 FIFO；一次领取后重置该轮排队年龄。执行中的目标以当前基础优先级参加让出比较，不永久携带第一次入队的年龄。Processor 负责在完成工作单元后 checkpoint，并正确恢复进度；MFS 无法从不透明 JSON 判断是否前进，同级任务和仅靠老化获得优先级的任务使用 50 ms 最小执行间隔，抑制反复空让出；更高基础优先级及清理/控制工作不受此间隔限制。

公平性依赖 Adapter 能在有限时间到达安全点，以及负载没有永久超出容量；不承诺对无限原生调用强制抢占。B 对需要多资源的老化目标还需停止持续填满其所需资源，避免大需求永远排不上。

重启时只恢复持久当前目标：checkpoint 事务前崩溃恢复上一份，事务后即使状态仍是 running 也恢复新 checkpoint；领取时始终产生新 token。不恢复执行中对象、用户回调或旧调用栈。strong/current wait 继续跟随当前工作和 namespace 控制目标，让出不会造成提前 ready；失败/blocked/cancelled 的报错合同不变。

#### 方案 C 需要额外完成通知合同

宿主准备完成后直接 retry(id) 存在并发问题：它会清取消门，没有 expected revision，且完成通知可能先于 MFS 写入 blocked 而丢失。若保留宿主准备，必须设计按 namespace incarnation、Source Revision、源 hash 与 Processor 配置限定的可重放通知，持久记录产物可用代；只唤醒相同且未取消的依赖等待目标。自动通知与显式用户重试分开。

宿主产物使用不可变发布路径，并保存到 MFS 引用及已开始读取退休；索引完成不能作为删除借用文字的信号。宿主释放准备 lane 的条件是完整产物和通知已持久保存，不等待 embedding。串行 RPC dispatcher 不执行阻塞自身后续通知的 wait/reindex；禁止 Python 等 Node 转换，而 Node 又等 Python 索引完成的循环等待。需要整体等待时，由宿主合成准备与索引等待，不能把 MFS 当前的 blocked 当作普通 pending。

#### 确定性交错验收

使用带 Event/Barrier 的测试 Processor/Embedder 与真实临时 SQLite/Milvus，按具体阶段放行。当前覆盖见 tests/test_scheduling.py、tests/test_extensions.py、tests/test_search_timeout.py 和 tests/test_recovery.py；下面列出协议的完整验收场景，方案 C 项仅在未来实施 C 时适用。

| 测试交错 | 可观察断言 |
| --- | --- |
| 紧急短任务进入后，放行长任务到 checkpoint | 短任务先完成；同时运行的 Processor 数量始终为 1 |
| 紧急短任务在 checkpoint 决策后才进入 | 长任务到下一个安全点才让出，不宣称已错过的安全点能够抢占 |
| 只有暂停/未来重试/无 Adapter 的高优先级目标 | 当前文件继续执行，不反复空让出 |
| 多次让出与恢复 | 配合正确实现 resume 的测试 Processor，完成单元不重复；部分文字和产物不可检索 |
| 复制、提交、退出三个时间点分别替换源 | 旧 checkpoint/进度/结果都不能推进新 revision |
| cancel 与让出，再自动 sync；随后显式 retry | 自动流程不复活；显式重试在旧调用退出后继续 |
| finally 吞掉让出或抛出新异常 | 吞异常不能发布；真实清理失败可见且保留 checkpoint |
| 让出与可重试失败交替 | 让出不重置失败预算 |
| 旧 insert/delete 即将执行时替换或 reindex | 先退出旧执行再清理；新代不会被迟到操作覆盖/删除 |
| 同 revision 重建、重复重建、drop 后同名重建 | index_epoch 与 incarnation 分别阻止旧提交/旧删除 |
| GC 与旧 resume_files/新 checkpoint/退出清理相遇 | 仍在使用的文件不删除；退休后无引用文件可回收 |
| checkpoint/退休事务提交前后 kill 进程 | 恢复最后一份已持久 checkpoint，已接收目标不丢失 |
| 搜索超时但后端未退出，再重建/close | 资源保留到真实退出，迟到结果不返回 |
| 持续紧急流量与有限安全点 | 等待老化让后台目标最终获得执行机会 |
| 方案 C：通知早于 blocked、重复通知、通知丢 ACK、取消与 R1/R2 完成交错 | 无丢失唤醒，不清用户取消门，不把旧产物关联到新目标 |
