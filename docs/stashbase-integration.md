# StashBase 对接

MFS 合同统一见 [设计](design.md)，完成情况见 [backlog](backlog.md)。StashBase checkout 本轮未修改；这里记录接入方式，不能视为应用已迁移。

## 实例与源身份

Python daemon 生命周期内打开一个 MFS 实例。每个 Library Folder 对应一个稳定 namespace ID，包括父子 Folder；例如 /work 对应 folder-A，/work/project 对应 folder-B。两个 namespace 独立配置、同步和删除，不合并根、不迁移另一 Folder 的身份。源文件增删改由应用操作，再通过 sync 让 MFS 观察。Folder 内临时选一个子目录才使用 UnderPath。

同一原件可以分别登记为 (folder-A, project/a.md) 与 (folder-B, a.md)，各自处理和索引；只有 External root 与 MFS 状态目录的重叠会被拒绝。全 Library 检索是否合并这些命中属于应用结果呈现：现有 library-operations/index.ts 的 keyword 检索按 deepestOwnerIs 保留最深 Folder 的结果，可沿用同一产品规则。源变更后，应用扫描相应已登记 Folder；MFS 不自动把一个 namespace 的观察传播到另一个。

External 只记录原件指针，拒绝 upsert/remove；应用已有派生文件也可以借用指针。直接 Markdown/TXT 的 grep 读取外部文件，PDF 等需要当前提取文字。不能假定所有文件都生成一份新的 Markdown。

当前应用通过启动、打开/切换目录、窗口 focus、Agent turn end、手动 Sync 和 MCP reindex 等事件扫描，没有 filesystem watcher。接入时保留这些事件。

## 保留与移交的职责

| StashBase 保留 | MFS 承担 |
| --- | --- |
| 源文件操作、输入选择、产品可见性 | 文件观察、处理/索引任务、版本失效和恢复 |
| 播放转码、增强 PDF/HTML/OCR/转录实现 | Processor 调用、切片、向量复用、BM25/dense 写入 |
| 是否开始大批次索引的用户决策 | namespace 的 off/bm25/hybrid 与 paused |
| 自行判断 MFS 文字是否可用，必要时 grep fallback | grep、read、search、已有逐文档状态 |
| sibling/派生文件关系和旧规则导入 | namespace 有序规则及统一准入/读取资格 |

搜原视频时，播放副本通过规则排除，Processor 提供原视频的转录文字。搜生成视频时，应用负责生成输入随原源更新/删除，再 sync。MFS 不自动推断两个独立文件的业务关系。

## Processor 适配

旧 mfs-cli 自身已有一般文本读取及基础 PDF/DOCX converter；StashBase 实际 daemon 路径通常接收应用转换文字，然后调用旧 Chunker、Embedder 和 store，并非每次运行旧 converter。

新 MFS 保留 UTF-8/PDF，并提供基础 DOCX；应用已有增强实现经 Adapter 接入，同格式只选择一个实现。Adapter 可以返回源文字引用、应用已有派生文字引用，或新提取文字与 SourceMap。

HTML 可以保留原 HTML grep、提取后索引。SourceMap 描述提取文字的来源，不把旧索引映射用于后来已经改变的外部文件。播放附属文件不会自动加入搜索。

外部 Processor/Chunker/Embedder 对象由应用按 namespace 创建并传入，MFS 保存兼容清单，重开时核对。StashBase 当前全局模型配置可以由应用显式传给多个 namespace，不要求 MFS 提供全局继承。

以 Folder 中的 report.pdf 为例：MFS 的 sync 发现文件，worker 调用 StashBase 提供的 StashPdfProcessor.process(path, media_type, context)，这个方法执行 StashBase 的 PDF 提取并返回 ProcessedDocument；MFS 随后切片、embedding、发布。提取就在 process 里完成，没有调用完 Processor 以后再提取一次的步骤。

Processor interface 与 ProcessingContext 已足够对接，item 9 不构成 MFS 待修复。StashBase 如何复用 Python 脚本或 Node 函数属于它自己的 Processor 实现；只有需要跨进程时才使用 context.run_process，不强制额外 helper。播放转码、Viewer 和应用调度的改接属于 INTEGRATION-001。

Context 的取消、report_progress 和 checkpoint 都可选。应用 Processor 若按页/音频单元处理，可保存自己的中间文件，并从 resume_state/resume_files 继续；整体提取调用可以从头重跑。MFS 不自动拆 PDF/音频，也不要求先实现分段恢复才能接入。

缺依赖/模型可报告不可用，临时失败映射到 RetryableError，损坏内容报告处理失败；取消不应吞成普通成功。实际云端模型、转换输出及 SourceMap 的验收要在 StashBase 迁移时完成。

## 搜索映射

| 应用意图 | MFS 入口 |
| --- | --- |
| 精确文字、正则、名称/路径匹配 | grep(namespace, filters=...) |
| 查看已知文档 | read |
| 原 semantic/hybrid 意图 | search(namespace, text, mode="hybrid") |
| 只按词频排序 | search(namespace, text, mode="bm25") |
| 纯向量排序 | search(namespace, text, mode="vector") |

旧 public keyword 使用磁盘/派生文字 grep，并不是 BM25；旧 public semantic 通常是 dense + BM25 的 hybrid。新 MFS 与旧 daemon 的切片和候选量不完全相同，不能因使用 Milvus Lite 就宣称排名一致。

新实现每 namespace 独立 collection，search/grep 都必须显式传入一个 namespace；筛选在后端 top-k 前应用。MFS 不提供跨 namespace 查询和排名合并。应用若保留全 Library 入口，应在应用层定义 Folder 范围和结果呈现，不把跨集合的相关性排序交给 MFS。旧 daemon 的扩展名过滤、hybrid 候选数和旧 Chunker 窗口需要通过实际检索效果评估迁移。

## 规则与状态

旧 Python Scanner 会读取各根的 .gitignore/.mfsignore，其他应用搜索入口的解释并不统一。应用显式导入旧规则，并将 sibling、附件目录等特殊关系转成稳定规则；不能未经转换把旧简化 fnmatch 当成新规则语义。

同一有效规则用于 MFS 和应用 fallback。sync(path) 只是本次观察范围，不是永久白名单。应用选择只搜原件或派生件时，应登记相应规则。

用 document_status 的 revision/text_revision/indexed_revision、stage/state 恢复文件显示状态。整体 status.ready 不等于某个 PDF 的文字可用；grep 命中也不等于向量已完成。不新增 ready 系统。wait(DocumentId) 或 wait(namespace, path=...) 等待当前任务，模型重建也计入；wait(sync_report) 是相同范围的简写。无变化 sync 不创建历史记录，旧版本曾成功不能让当前重建提前通过。namespace_configuration 恢复 Folder 索引开关、暂停状态及 Adapter 清单，rules 读取独立规则。

## 应用验收

- 替换旧 daemon 调用，绑定实际 Adapter，移除已交给 MFS 的重复处理/索引调度。
- 保留播放转换、源操作、产品决策及 fallback。
- 验证多 Folder 范围、规则和原件点击跳转。
- 验证真实 OCR/转录/embedding 的线程调用、取消及失败恢复。
- 用代表性 corpus 运行 retrieval eval，并测量首次索引吞吐和检索延迟。

MFS 单元/集成测试不能替代这些应用验收。

## 模型配置与重建

必须区分 StashBase 的操作名称和 MFS 的方法名：server/library-operations/index.ts 中 MCP/Library reindex 调用 syncFolderNow，即重新观察目录、补处理变化及恢复转换，不会直接清空所有向量。接入新库时主要映射到 sync(namespace, verify="content")；对已有失败/blocked 目标需明确调用 retry/reprocess，单纯源内容未变的 sync 不自动重试失败转换。新 MFS 的 reindex 则是对已接收文档重建整个 namespace 索引，不负责发现尚未 sync 的文件。

| 当前应用动作 | 新 MFS 的调用与能力 |
| --- | --- |
| 手动 Sync、MCP reindex、重扫外部变化 | sync；需要重试的已有失败转换使用 retry/reprocess |
| 首次配置 embedding，给已接收文字补向量 | reindex(namespace, embedder=..., indexing="hybrid")；已支持 |
| 显式更换模型/维度、重新计算整个 namespace 索引 | reindex(namespace, embedder=...)；已支持；库内有能力不等于应用已有任意模型选择器 |

StashBase 当前允许切换 embedding 来源：OpenAI、OpenRouter、账户服务；所查配置中的前两者使用固定默认模型 text-embedding-3-small，并没有据此发现任意模型选择器。首次配置 embedding 会触发 backfill；同一模型换 API key 不应重新计算已有向量（shared/embedding.ts 已明确此区别）。

MFS 已有 reindex(namespace, embedder=..., indexing="hybrid")，可以显式换模型/维度；open_namespace 的不匹配拒绝是防止重开时静默混用旧向量，不代表不允许更换。接入时按 embedding_space、dimension 和 Chunker 配置判断是否重建，不把凭据轮换或同空间的 API 路由切换算成新模型。全局设置改变时由 StashBase 为各 Folder 请求重建；文字提取仍有效，不重新 OCR/转录，索引重建期间可走 grep fallback。

RECEIPT-002 已改为按当前目标等待，永久历史等待表已删除；源文字以前成功过，补向量尚未完成时 wait(sync(...)) 仍等待当前构建。StashBase daemon 尚未改接新库，其调用和结果映射仍为 INTEGRATION-001 的待实施工作。

## External 重命名与计算复用

External 重命名由覆盖新旧位置的 sync 观察：旧路径身份被删除，新路径成为新 DocumentId。这个文件生命周期不需要 rename 方法，内容相同的复制也可以走同一套计算复用。仅同步新位置不会自动观察范围外的旧位置。

向量计算缓存已独立于搜索可见性：同 namespace incarnation、index_epoch、dense 配置和片段 hash 可以复用已完整验证的向量，旧路径失效和物理删除不再清掉这份计算结果。缓存持久化，重开后仍可命中；每实例最多 32 MiB 逻辑数据，LRU 淘汰或损坏时重新计算。显式 reindex 清理旧缓存并推进构建代，drop 清理旧 incarnation，不跨 namespace 复用。

Processor 只有显式声明 cache_scope="content" 才能跨路径复用处理结果；应用借用的派生文字也必须满足引用有效性。升级时缓存为空、处理/模型配置改变或缓存淘汰，都可能发生重新计算，因此不承诺任意规模纯重命名零 embedding。文件生命周期仍由 sync 观察新旧位置完成。

## 扫描开销与处理调度

SYNC-002 已修复：首次或 content sync 列目录后，文件 hash 校验使用规范路径逐段安全 open 重查身份，不再每文件重新列完整父目录。保留 no-follow、inode、大小写和并发变化检查。stat 未变的后续 sync 仍走短路。仅 mtime 改变而 hash 相同时，会更新观察 stat 并保留当前任务；之后的 stat sync 恢复短路，执行中的旧任务副本不会覆盖新观察。

[单 worker 协作让出](design.md#单-worker-协作让出已实现)已启用：StashBase 的 Processor 可以在完成音频/页单元后调用 context.checkpoint；应用用 set_active_scopes 标出当前目录。MFS 先持久保存中间数据，等旧调用和子进程退出，再运行更紧急的可运行目标；恢复时提供 resume_state/resume_files。StashBase 的十分钟音频单元表示音频长度，不是执行耗时上限。

当前只有一个文件 worker，没有恢复 StashBase 的 2 light/1 heavy 并行吞吐；如果需要该容量，按设计第 12 节方案 B 单独实施。已发布文字的 grep 和已有索引的 eventual search 独立运行。重建/drop 等已经获准的查询真实退出后才破坏旧 collection，包括调用方已超时而 embedding 仍在运行的情况。

## 过渡与查询等待

若过渡期只借用 StashBase 已准备的文件，Adapter 必须先验证源 hash、转换配置与完成标记。相同源 bytes 的重复 sync 沿用目标，不会自动解除 blocked；用户明确要求恢复时可使用 retry/reprocess。自动完成通知不能直接调用无条件 retry：它会清取消门，且通知可能先于 blocked 提交。需要按 namespace incarnation/revision 限定的持久可重放唤醒与不可变产物保留合同，详见设计第 12 节方案 C。这个宿主准备方案的并发通知 Interface 尚未实现；MFS 调度 Processor 完整执行的方案不需要跨进程完成通知。

默认 search 为 eventual，不等后台索引补齐；所有 consistency 模式默认 timeout=5 秒，限制搜索调用方的总等待，过期返回 WaitTimeout。RPC 将应用传入的预算交给 MFS 并映射超时错误；不能在串行 dispatcher 中执行长期 wait/reindex，使后续取消、状态或准备完成通知无法处理。已经进入外部服务的调用未必立即终止，MFS 在阶段边界检查期限并丢弃迟到结果。strong 当前按 namespace 等待；一个 Folder 一个 namespace 已隔离其他 Folder，同一 Folder 内子目录是否另需 strong 暂不实施。
