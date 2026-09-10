# MFS 实现规范

2026-09-09。本文与 [生命周期设计](indexing-lifecycle.md) 描述当前实现；
原同步接口存档于 [V1 历史规范](design-v1.md)。运行环境为 Python 3.13、macOS/Linux。
Preparation、操作等待、内容复用和低频 GC 已实现，见 [生命周期补齐](lifecycle-extensions.md)。

## 1. 身份与存储

一个 MFS instance 独占一个状态目录，持有进程级文件锁。一个实例可有多个 namespace：

| 类型 | 原始输入的所有者 | 文档 ID | 修改入口 |
|---|---|---|---|
| internal | MFS | 调用方提供的非空 opaque ID | upsert / remove |
| external | 外部文件系统 | root 下真实拼写的 POSIX 相对路径 | sync |

Namespace 不是 ACL。调用方把授权范围转成检索过滤；无结果不能扩大范围。
MFS 不自带 watcher/daemon，也不隐式读取 external live file 更新索引。

SQLite 是输入目标、已提交文本与 SourceMap、阶段及重试状态的 source of truth。
原始输入与阶段产物作为文件持久化，SQLite 保存引用。Milvus 一个 collection 保存 Chunk，
包含 BM25、可选 dense、源过滤字段、完整 snippet 和定位。ranked search 不读取 SQLite。

`DocumentId(namespace, doc_id)` 表示逻辑文档；receipt 的 revision 表示接收版本；
`Document.snapshot_id` 表示处理输出。后者不是 Milvus Snapshot 或跨数据库备份。
SourceMap/SourceLocation 的范围使用处理文本的 UTF-8 byte offset，不能直接解释成变化后的 live file 偏移。

## 2. 接收、文本与索引

`upsert` 稳定读取/复制输入、计算 hash、持久化输入和 SQLite 任务后返回 `MutationReport`。
这时返回的 `revision` 已接收，`index_ready=False` 不代表写入失败。

后台执行顺序是 PROCESS → SQLite 提交文本 → CHUNK → EMBED → PUBLISH。
PreparationPolicy 默认 2 个 light、1 个 heavy worker 执行 PROCESS；一个索引线程执行 CHUNK/EMBED 与全部 Milvus 写操作。
回调仍是同步 Python 函数，不需要 async/await。dense 卡住不阻止准备线程提交其他文档文本。

- grep / query 读取已经提交的文本。OCR 尚未完成时无法查询新文本。
- 配置 dense 时，完整向量准备好才把该文档的新 BM25/dense rows 一起写入。
- dense 失败保留旧索引，初次接收则尚无索引；不写占位向量或提前发布 sparse。
- 索引完成后更新 SQLite 完成状态、内存 pending 集合并唤醒等待者。
- 删除先清除 SQLite 文本并记录索引清理任务，清理成功才算追上。
- 每次写入检查 revision/attempt，旧回调不能把后来的版本标成完成。

Milvus 大文档 upsert 和旧 rows 删除可分批，不承诺文档级原子替换。重试使用相同主键。
准备完整与一次数据库事务不是同一件事；并发搜索可以看到发布中间态。

## 3. 一致性

| 操作 | 语义 |
|---|---|
| query / TextMatch | SQLite 已提交文本，不等待索引 |
| search(consistency="strong") | 等实例 ready，再执行所选检索 |
| search(consistency="eventual") | 直接检索现有 Milvus 索引 |
| wait_ready(timeout) | 等待同一个实例级 ready |
| wait(receipt, timeout) | 仅等待回执的持久目标及其清理完成 |

BM25/vector/hybrid 共享一致性。任何未完成的处理、索引、删除、失败或取消任务都会使 ready 为 false。
ready 由持久目标推导，不是独立的永久布尔标志；启动时先恢复 pending，再启动线程。

默认 strong；`timeout=None` 一直等，超时抛 `WaitTimeout`，close 唤醒等待者并抛 `Closed`。
超时参数只约束 ready 等待，不中断回调或 RPC，也不取消后台任务。

strong 的保证是准入时已追上，放行之后允许新写入。搜索不持有 MFS 读写锁，
hybrid 两路不保证同一快照。eventual 允许旧结果、缺少结果和更新中间态；后端错误直接返回。

## 4. 生命周期接口

```python
MFS.open(path, processors=(), chunker=None, embedder=None, sync_policy=None)
mfs.create_namespace(namespace, kind, root=None)
mfs.get_namespace(namespace)
mfs.list_namespaces()
mfs.drop_namespace(namespace)
mfs.upsert(namespace, doc_id, data, media_type=None, idempotency_key=None)
mfs.remove(namespace, doc_id)
mfs.sync(namespace, path=".", verify="stat")
mfs.status()
mfs.document_status(document_id)
mfs.list_document_statuses(namespace=None, limit=100, offset=0)
mfs.wait_ready(timeout=None)
mfs.retry(document_id, stage=None)
mfs.reprocess(document_id)
mfs.cancel(document_id)
mfs.reindex(timeout=None)
mfs.close()
```

`upsert` 的 data 是 bytes 或 Path；internal 的原始 bytes 可用 `query(select="doc")` 读取。
external 返回已处理文本与定位，不返回 live original。

相同内容、media type、Processor 描述与 root binding 不重复生成；失败任务保持原状态，显式 retry。
`idempotency_key` 持久保存原回执：ACK 丢失后重复请求返回同一个回执，不覆盖后来接收的版本。
同 key 不同请求抛 `IdempotencyConflict`。

`DocumentStatus` 包含目标/文本/已完成索引 revision、stage/state、attempts、error、next_retry_at、
completed_batches/total_batches 和 executing。读取状态不加载原文或向量。
阶段有 process/chunk/embed/publish/delete/drop；状态有 pending/running/retry_wait/failed/blocked/cancelled/succeeded。
Namespace 清理任务在状态列表中用 `DocumentId(namespace, "")` 表示；失败后可把该 id 传给 retry。

可识别临时异常（`RetryableError`、网络/文件超时、连接/存储错误）最多自动重试 4 次并退避。
其他错误停在 failed；缺 Processor/Embedder 停在 blocked。retry 只继续当前阶段，
已保存 OCR 和成功 embedding 批次被复用；重新运行 Processor 用 reprocess。

cancel 持久保存用户取消门，并通知 ProcessingContext；executing 可以暂时仍为 true。
新 bytes 不解除取消，retry/reprocess 显式解除；取消不会假装数据已经完成，ready 仍为 false。
close 通知 Context、清理受管理子进程并等待执行/产物句柄退出，再释放 SQLite、Milvus 和实例锁。
任意 Python 回调不能强杀，Adapter 应检查取消并设置合适的网络超时。

## 5. 查询、下推与定位

`query(filters=(), select="doc_id", limit=None)` 支持 doc_id/doc/chunk 投影。
多个顶层 filter 是 AND；AnyOf 中是 OR。一个 ByNamespace/ByDocumentId/ByExtension/ByMediaType 内的值是 OR。

| 过滤器 | 含义 |
|---|---|
| AnyOf | 多个结构化过滤器的 OR，可用于多 Folder / Chat scope |
| ByNamespace | namespace 集合 |
| ByDocumentId | 完整 namespace + doc_id 集合 |
| UnderPath | external namespace 下 exact path 或目录 descendants，有 `/` 边界 |
| PathPrefix / PathSuffix | doc_id 路径的字面前/后缀 |
| NamePrefix / NameSuffix | doc_id 最后一段名称的字面前/后缀 |
| ByExtension | 源 ID 后缀，不区分大小写，可省略 `.` |
| ByMediaType | 规范化后的源 media type |
| TextMatch | SQLite 完整文本的字面或 RE2 正则匹配 |

前/后缀是区分大小写的字面条件，`%`、`_`、反斜杠不代表通配。
TextMatch 默认忽略大小写；case_sensitive 强制敏感，smart_case 根据 pattern 是否含大写决定。
whole_word 使用 Unicode 字母/数字/下划线边界。匹配可跨 Chunk，并返回 SourceLocation。
非法正则即使候选集合为空也报 `InvalidPattern`。

`search(text, filters=(), mode="hybrid", select="chunk", limit=10, consistency="strong", timeout=None)`
支持 bm25/vector/hybrid，select 只接受 chunk/doc_id。全文读取另行 query；TextMatch 不能用于 ranked search。

所有结构化条件直接进入每路 Milvus search 的 filter，在 top-k 前执行。
路径后缀使用反转字符串列的前缀区间，避免 LIKE 转义差异。大 ID 集合按 200 分批，多个 ID filter 先取交集。
命中直接返回 Milvus 保存的文本、snapshot_id 与 SourceLocation，不回 SQLite 组装结果。

BM25 使用标准 tokenizer/lowercase，dense 用 COSINE，hybrid 用 RRF。
文档投影去重后应用 limit；内部候选预算最多 1000，不足时返回 truncated，不承诺无界文档 top-k。
query 的 limit 为 1..100000 或 None，search 的 limit 为 1..1000。

## 6. 文件同步

SyncPolicy 提供 exclude_globs 与 max_file_bytes。root、子目录、exact file 使用相同祖先排除规则。
目录 sync 默认用 size/mtime 快速跳过；verify="content" 全量检查 hash，exact file 始终检查 hash。
内容未变不重新执行 Processor/Embedder。

保留 external root spelling，每次 sync resolve。root symlink 改目标后升级 binding 并完整 reconcile，
不复用旧 root 的 stat。内部目录 symlink 不递归；文件 symlink 归入 root 内真实路径、去重，跳过 root 外目标与环。
不允许借父目录 symlink 越界。缺失/不可读 root 或观察期间 root 变化报告 incomplete，保留未见旧文档。
路径大小写比较根据实际卷的 lookup 行为，文档 ID 保留真实拼写。

directory → file 替换的 PROCESS 失败保留旧 descendants；新文本提交后 grep 切换，
Milvus 中的旧 descendants 等新文件完整索引发布成功后才删除。

SyncReport.complete 表示本轮文件观察完整，不表示后台 PROCESS/索引完成。
changed 是已接收版本，后台错误看 DocumentStatus，index_ready 是返回时的实例状态。

## 7. 注入与恢复

Processor 的 id/version/options 描述在 open 时冻结，PROCESS 返回完整 text 与 SourceMap。
变更算法应更改版本/options。Processor 按其 workload/concurrency 声明调度；sniff 可与 PROCESS 并发，必须轻量且线程安全。
Chunker 与 query 的 Chunk 投影串行调用；文档级 grep 不调用 Chunker。
Embedder 的 embed_documents 与 embed_query 可以并发，适配器必须支持并发调用。

SQLite v1/v2 catalog 自动升级至 v3（任务结果、文件引用、checkpoint 和缓存）。缺失/旧 schema/中断的索引从 SQLite 处理快照恢复；不需要重新 OCR。
重新打开已有 collection 会显式 load。chunker/embedding 配置变化显示 mismatch，需显式 reindex。

reindex 不重新执行 Processor；它重建 collection 并重做 Chunk/embedding，仍允许 grep。
未配置 dense 的实例支持 BM25-only。配置 dense 后缺 Embedder 的新任务保持 pending/blocked，不自动关闭 dense。

未提交事务不 ACK；事务已提交但 ACK 丢失可重放回执。产物成功而完成状态未提交时按持久状态恢复，
稳定主键避免索引重放增加重复 rows。任意外部回调不承诺 exactly-once。

当前目标输入与产物保留用于恢复；无引用文件由低频维护或 collect_garbage 分批回收。备份/历史版本、
多进程共享写入、应用侧 daemon/ACL 不在本次接口中。
