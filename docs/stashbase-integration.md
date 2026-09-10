# StashBase 对接 MFS

本次完成 MFS 侧能力；StashBase checkout 未修改，下面是应用接入的具体映射。
已有 daemon 使用的是另一套 MFS 接口，不能只升级依赖而继续调用原来的 `hybrid_search`。

## 实例与文件身份

在 Python daemon 的生命周期中打开一个 MFS instance。每个 Library root 对应一个 external namespace，
使用稳定的 Library ID 作为 namespace；Folder 是该 namespace 下的 UnderPath，不额外创建 collection。
新增/删除/移动源文件仍由应用操作文件系统，然后调用 sync。watcher 只触发 sync，不再自行索引。

```python
mfs.create_namespace(library_id, "external", library_root)
report = mfs.sync(library_id)                    # 日常 stat 快速扫描
report = mfs.sync(library_id, verify="content")  # 手动完整校验
```

同一实例的 SyncPolicy 应来自应用共享的 ignore 规则，包括隐藏派生目录/文件；不要同时把派生 Markdown
作为独立文档同步。源 `paper.pdf` 的处理结果可以是 Markdown，但身份始终是 `paper.pdf`，
源后缀过滤与点击跳转无需再从 `.paper.pdf.md` 猜回原文。

MFS 当前 SyncPolicy 是实例级；应用若要按 Library 设置不同 ignore，需要在接入时统一为共同策略，
或后续增加 namespace policy。不能在 Node 忽略一份、MFS 仍扫描另一份。

## Processor 与 embedding

现有 OCR/PDF/HTML/DOCX/音频算法由应用包装成 Processor：

```python
class ApplicationProcessor:
    # id/version/options 和支持的媒体类型按既有适配器声明。
    def process(self, staged_path, media_type):
        text, locations = existing_conversion(staged_path, media_type)
        return ProcessedDocument(text=text, source_map=locations)
```

`staged_path` 是 MFS 已保存的稳定输入；格式判断使用 media_type，不依赖 staging 文件名后缀。
SourceMap 应保留页码、行号、时间范围等来源；结构化 source 可保存已有 heading 信息。
源身份来自 MFS DocumentId，定位描述来自该次处理文本。

Processor 只执行转换，不写另一套“完成/失败/已索引”状态。改变算法时修改版本或 options，
用户强制重新生成调用 `reprocess(DocumentId(...))`。MFS 负责源 hash 去重，失败 embedding 不重做已成功 OCR。
API 凭据、模型选择、外部进程与网络超时仍由适配器负责。

Embedder 提供 `embedding_space`、`dimension`、`embed_documents(texts)` 和 `embed_query(text)`。
查询调用与后台文档调用允许并发；适配器应使用可并发客户端或各自的客户端，避免把两者锁在同一个长请求后面。
MFS 不要求这些函数改为 async。

## 检索映射

| StashBase 参数/行为 | MFS 调用 |
|---|---|
| Library scope | ByNamespace(library_id) |
| Folder scope | UnderPath(library_id, relative_folder) |
| path_prefix | 先规范化为该 Library 下路径，再用 UnderPath 或 PathPrefix |
| extensions / types | ByExtension(exts)；业务类别先展开为扩展名集合 |
| 精确文件集合 | ByDocumentId(完整 ID 集合) |
| 文件名/路径前后缀 | NamePrefix/NameSuffix/PathPrefix/PathSuffix |
| caseStrict=true | TextMatch(pattern, case_sensitive=True) |
| caseStrict=false | TextMatch(pattern, smart_case=True) |
| wholeWord | TextMatch(..., whole_word=True) |
| 语义搜索 | search(mode="vector" 或 "hybrid", select="doc_id" 或 "chunk") |
| 原始全文 | query([ByDocumentId(id)], select="doc") |

外部权限范围始终与用户筛选相交。一个 filter 内的多个值为 OR，多个 filter 为 AND。
多个 Folder 范围使用 `AnyOf([UnderPath(...), UnderPath(...)])`；每路 Milvus 内执行 OR，
再与其他顶层过滤取交集。不要把多个 UnderPath 并排作为顶层过滤，那会是 AND。
全 Library OR 也可直接用 ByNamespace。

原 daemon `op_search` 对扩展名做最多 200 条候选的 over-fetch 后过滤。
改用 ByExtension 后，Milvus 的每一路都在 top-k 前过滤，稀有 PDF 不会先被其他类型挤掉。
输入值 `%`、`_`、引号、反斜杠和 Unicode 保持字面意义，不拼接原始 Milvus 表达式。

结果自带 Chunk 文本、snapshot_id 与 SourceLocation。UI 截取 snippet 或显示页码时不查询 SQLite。
若需要全文，在搜索完成后显式 query；返回的当前全文与较旧搜索 snapshot_id 可能不同，应用应保留这种区别。
检索不会读取 live 文件；点击打开源文件后的定位新鲜度仍由应用负责。

## 状态与调用顺序

1. watcher/用户写入后调用 sync 或 upsert；ACK 只表示已接收。
2. UI 读取 list_document_statuses，展示 PROCESS/EMBED/发布阶段、错误和已完成批次。
   stage=drop、doc_id 为空的是 namespace 清理任务，不作为普通文档展示；失败同样可 retry。
3. 文本搜索用 query(TextMatch)，即使 dense 失败仍能用新文本。
4. 交互式索引搜索通常用 eventual；确实要求本次索引追上时用 strong + timeout。
5. 可重试失败调用 retry；强制重新 OCR 调用 reprocess；取消调用 cancel。
6. daemon 退出时 close。被取消的外部函数可能仍在运行，executing 用来展示实际退出进度。

strong 是整个实例的 ready，任何 Library 的失败都会让 strong 等待；不按搜索范围或通道另算。
删除/移出授权范围时应用应立即缩小可搜索 scope；异步删除 ACK 不表示旧 Milvus rows 已经消失。

## 应用迁移验收

MFS 测试已覆盖联合发布、grep 独立、重试/崩溃恢复、源身份与前置过滤、symlink/ignore。
下面仍需要在 StashBase 应用迁移时执行：

- 删除 Node/daemon 中重复的 Preparation/indexing 调度与完成状态，以 MFS 状态为准。
- 包装真实 OCR/转录/embedding 客户端，检查线程调用、凭据、超时与取消退出。
- 对照 Library/Folder/Chat/MCP 授权范围与多 Folder OR 的实际调用。
- 在现有 corpus 上运行 retrieval eval，测量 grep 延迟、首次索引吞吐和失败恢复成本。

这部分需要改 StashBase 应用代码；当前 MFS 工作区的单元/集成测试不替代其端到端评估。
