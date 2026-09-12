# MFS 设计

本文是唯一设计正文，统一本轮已确认的行为。实现与验收结果见 [backlog](backlog.md)，StashBase 的具体接入见 [对接说明](stashbase-integration.md)。

## 1. 实例、namespace 与存储

MFS 是嵌入式 Python 库。保留现有实例：一个实例持有状态目录、数据库连接、内存待办和后台处理线程。业务配置属于 namespace，没有全局 Processor、Chunker、Embedder、ignore 或索引策略继承。

一个 namespace 有稳定身份、Internal/External 所有权类型及独立配置。DocumentId 是 (namespace, doc_id)，不同路径即使内容相同也是不同文档。计算结果可以复用，文档身份不合并。

- SQLite：每实例一个 catalog，共享元数据表。文档和任务按 namespace/doc_id 区分；回执和执行记录通过 ID 关联。
- Milvus：每实例一个数据库文件；每 namespace 一个活动 collection，支持不同向量维度。collection 使用不可复用的 namespace 创建 ID 命名。
- 文件：MFS 拥有的原件、派生文字、附属产物和临时文件按 namespace 整理。目录独立不表示能脱离共享数据库单独迁移。
- External root 与 MFS 状态目录分开，不能重叠。不同 namespace 的 External 真实根也不能重叠；同根的子目录是搜索/sync 范围。

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

SourceMap 描述处理文字的 UTF-8 字节范围对应源文件的页/行/时间，不是源二进制偏移。Milvus 一行对应一个片段。同一文档可有多行，重叠片段保留各自位置。

切片计划保存范围/hash等恢复信息，不再额外永久保存片段正文或向量 JSON。向量按 namespace、向量空间和片段内容复用；hash 相同但结果未完成不能当作缓存命中。最后一个有效引用消失后，不为旧源无限保留缓存。

直接文本的 grep 使用实际读取的行号和偏移。借用的提取文字仅在内容 hash 仍匹配时沿用保存的来源映射，已变化则退回当前文字行号。两次 sync 之间，grep 与索引不保证来自相同 bytes。

## 5. 接收、处理与恢复

```text
调用方 sync / internal upsert
  → 观察源、计算 hash、判断是否变化
  → SQLite 事务保存最新目标、撤销旧结果、登记清理责任
  → 更新内存待办，返回操作回执

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

重复 sync 未改变内容/配置时沿用任务。R1→R2→R3 尚未开始则只处理 R3；R1 正在运行时，允许该调用退出，旧结果不能提交成 R3。阶段提交前核对目标 revision、namespace 创建代和执行身份。已经承诺的旧代清理责任不能被最新目标覆盖。

sync 不等待 Processor/embedding；调用方可以继续 sync/grep。慢 Adapter 会推迟其他文件的后台处理和物理删除，所以对外失效必须在接收事务中完成。

每次写入有独立持久回执；相同目标集合复用一份持久记录，避免反复 sync 重复存整份列表。wait(receipt) 只等待该次操作，旧未完成目标被替换报告 Superseded，历史成功回执保持成功。revision 表示目标版本，不是历史文件备份，也不是阻塞等待机制。

Processor 可使用 ProcessingContext 提交进度、checkpoint，检查协作取消，运行受管理子进程。重启恢复未完成阶段；外部引用已失效就报错，不能恢复外部原件副本。失败/暂停/重试等待的文件让出 worker。

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

索引策略为 off/bm25/hybrid，并独立提供 paused：

| 策略 | 行为 |
| --- | --- |
| off | 保留文件观察、处理和 grep，清除排名索引，不调用 Embedder |
| bm25 | 只构建 BM25，不调用 Embedder |
| hybrid | 构建 BM25 和 dense，必须绑定相容 Embedder |
| paused | 暂停新增索引工作；源观察、必要文字处理、逻辑失效和旧代清理继续 |

search(mode="bm25") 只选择这次查询通道，不会关闭后台 embedding。取消单文件也不等于 namespace 暂停。

## 8. 搜索与读取

删除公开 query 名称，不保留别名。非排序文字、文件名和路径匹配统一使用 grep；已知文档读取使用 read。排名检索使用 search(mode="bm25" | "vector" | "hybrid")。

grep 逐文档读取，具有文档数、匹配数和读取量预算，超过预算返回 truncated；不能先加载全库全文再应用 limit。路径筛选无需读取正文。源后缀、namespace 和路径筛选在排名后端 top-k 之前应用。

strong 等待相应索引工作，eventual 立即查询当前有效索引，允许结果尚未补齐，但不能返回已失效旧源。grepping 文字不需要等待 embedding。

单 namespace hybrid 对 BM25/dense 使用 RRF。跨 namespace 分别执行各自模型/collection 检索，再按排名合并；不同模型和 collection 的原始分数不可直接比较。任一路失败默认明确报错，不静默漏掉一个 namespace。

保留 document_status、list_document_statuses、操作回执等已有状态。StashBase 自行判断文字是否可用并选择自己的 grep fallback；不新增 ready 订阅或额外范围就绪系统。

## 9. 内部职责与迁移

保留 MFS 入口和三个外部 Adapter interface。内部按职责拆分，不按职责启动线程：

| 内部职责 | 内容 |
| --- | --- |
| Lifecycle | 接收、失效、目标、执行许可、阶段提交、取消、回执和文件引用事务 |
| Preparation | Processor 调用、进度、checkpoint 和协作退出 |
| Indexing | 切片、embedding 复用、Milvus 发布/删除/重建 |
| Reader | 有界 grep、read、排名合并及有效版本检查 |
| ArtifactStore | 受管理文件持久化、引用、读句柄和 GC |

慢调用在状态锁外，提交在短事务内统一核验。执行模块不各自维护另一套目标或互相归并队列。

SQLite catalog schema 5 保存上述状态；元数据表结构可升级，但旧 namespace 需要调用方使用 migrate_namespace 显式提供适配器和索引模式。迁移撤销旧处理文字和索引，再从 Internal 原件或 External 原路径处理，不回放旧正文缓存。缺少可用原件或处理实现时明确报错。旧布局中的受管理文件在引用释放后由 GC 回收。

## 10. 验收

必须验证实际用户行为：External 未产生原件副本；原文变动后 grep 读当前文件；替换/删除/排除后旧结果立即失效；新处理失败、迟到结果、重启与重建均不能复活旧源；删除后重建不误删新数据；GC 不删外部引用。

namespace 验证包含不同 dim/model 共存、重开不匹配报错、规则原子更新与 include 后代、独立暂停/关闭索引、重叠根拒绝。后台验证包含反复 sync 合并、单文件顺序、单 worker、崩溃恢复与回执。搜索验证包含 grep 预算、过滤下推、跨 collection 合并及无公开 query。

MFS 测试不等于 StashBase 已迁移。对接实际转换器、检索效果和应用事件链路需要在 StashBase 单独验收。
