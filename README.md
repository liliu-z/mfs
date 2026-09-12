# MFS

MFS 是嵌入式 Python 3.13 文件搜索库。一个实例管理多个 namespace：共享 SQLite 元数据，每个 namespace 使用独立的 Milvus collection、Processor、Chunker、Embedder 和规则。

External 只保存源文件指针，**不复制外部原件**，只支持 `sync`。Internal 保存原件，支持 `upsert` / `remove`。TXT/Markdown 直接使用原件文字；PDF/DOCX 等按需提取。SQLite 保存引用、SourceMap、hash 和任务状态，Milvus 保存片段文字及可选向量。

```python
from pathlib import Path
from mfs import MFS, DocumentId, TextMatch, Utf8TextProcessor

mfs = MFS.open(Path(".mfs-state"))
try:
    if not mfs.list_namespaces():
        mfs.create_namespace("notes", "internal", processors=[Utf8TextProcessor()])
    else:
        mfs.open_namespace("notes", processors=[Utf8TextProcessor()])

    report = mfs.upsert("notes", "hello.md", b"Hello, world!")
    mfs.wait(report.id, timeout=30)
    print(mfs.grep("notes", [TextMatch("Hello")]).items)
    print(mfs.search("notes", "hello", mode="bm25").items)
    print(mfs.read(DocumentId("notes", "hello.md")))
finally:
    mfs.close()
```

External 用法：`create_namespace("files", "external", root, processors=[...])`，然后 `sync("files", verify="content")`。状态目录和 External root 不能重叠；不同 namespace 可以指向同一个或互相嵌套的 External root，各自维护规则、处理和索引。namespace 内的子目录使用 `UnderPath` 或 `sync(namespace, path)` 指定范围。

`MFS.open` 不接收全局适配器。创建时默认使用 `DefaultChunker`；未提供 Embedder 默认 `bm25`，提供则默认 `hybrid`。重开 namespace 时传入相同声明的实现；库核对 Processor/Chunker 的 id、version、options 和路由，以及 Embedder 的 embedding_space、dimension。只有维度相同仍可能不兼容，不匹配立即报错。off/bm25 重开时可以省略不使用的 Embedder；若传入则仍核对声明。打开实例本身无需加载模型；已知后缀的 sync、删除、状态读取和已保存文字读取可以先使用。

`grep(namespace, ...)` 和 `search(namespace, text, ...)` 都必须显式指定一个 namespace，不提供跨 namespace 查询或排名合并。`ByNamespace` 已删除；`ByDocumentId`、`UnderPath` 及嵌套 `AnyOf` 中的身份必须属于指定 namespace，否则报 `InvalidFilter`。

`grep` 提供文字、正则、名称和路径匹配，`GrepBudget` 限制读取量、文档数和匹配数，达到预算返回 `truncated`。已知文档使用 `read`；`grep(namespace, select="doc")` 返回预算内文字，不加载 Internal 二进制原件。排名搜索使用 `search(namespace, text, mode="bm25" | "vector" | "hybrid")`，默认 mode 为 hybrid；只有 BM25 的 namespace 应显式选 bm25。公开 `query` 已移除。

写入返回接收结果，一个后台 worker 执行旧代清理、Processor、Chunker、embedding 和发布，在每个阶段边界重新调度。Processor 的 checkpoint 可在持久保存中间结果后让出给更紧急的可运行任务，旧调用和子进程退出后才释放执行权；再次调用时从 resume_state/resume_files 恢复。每文件保留一个最新目标，重复变更可以合并；已失效旧文件立即退出读取和搜索，失败不回退旧结果。`wait(DocumentId(...))` 等待文件当前任务，`wait(namespace, path="notes")` 等待范围当前任务；期间接受的新版本也要完成。传入写入/sync 返回值是相同文件/范围的简写，不等待历史版本。失败、blocked 或取消会抛 `OperationFailed`。`search` 默认 `consistency="eventual"`，查询当前有效索引。显式 `consistency="strong"` 等待这一个 namespace。`timeout=5.0` 是搜索调用方的总等待预算，包含排队、一致性等待、查询 embedding 和后端检索；到期抛 `WaitTimeout`，`None` 不设期限，`0` 立即超时。执行阶段检查同一 deadline，Milvus 接收剩余时间；不响应取消的外部调用可能继续运行到返回，届时丢弃结果，不开始后续阶段。借用文件可在两次 sync 间改变，grep 读当前内容，索引保留上次成功处理的版本，直到下次 sync 撤销它。

```python
from mfs import IgnoreRule

rules = mfs.rules("files")
mfs.update_rules(
    "files", expected_revision=rules.revision,
    add=[IgnoreRule("generated", "generated/"),
         IgnoreRule("keep-readme", "generated/README.md", action="include")],
)
mfs.configure_index("files", paused=True)  # 继续观察/处理/grep，暂停新增索引
mfs.configure_index("files", indexing="off")  # 清除排名索引，保留处理/grep
```

规则按顺序匹配，后匹配覆盖前匹配；支持增删改和排序，不隐式读取 `.gitignore`。新排除立即生效，External 重新包含后调用 sync。`expected_revision` 不匹配报 `RuleConflict`。

Processor 可使用两参数 `process(path, media_type)`，或额外接收 `ProcessingContext` 来提交 checkpoint、进度及检查取消。`ProcessedDocument(text, source_map, text_path=...)` 引用已有文字文件；未提供文字路径则保存必要派生文字。应用可用 `grep_path` 指定原 HTML，同时返回内存提取文字供索引。`artifacts` 发布附属文件，通过 `open_artifact` 读取。库提供 UTF-8、PDF、基础 DOCX；OCR、转录和播放转换可由应用适配。

`reprocess_namespace(namespace, processors=[...])` 显式更换 Processor 并重新处理；`reindex(namespace, chunker=..., embedder=..., timeout=...)` 显式重建索引。旧存储需要调用 `migrate_namespace(namespace, processors=[...], indexing="bm25" | "hybrid", ...)`，不会打开即自动换模型。失败可用 `document_status`、`list_document_statuses`和 `scope_status` 诊断，`retry` / `reprocess` 恢复。

`namespace_configuration(namespace)` 读取持久的索引模式、暂停状态、完整适配器清单、待生效清单与文件大小限制，无需先绑定模型；规则由 `rules(namespace)` 读取。

本轮等待 API 变更：返回值移除 `operation_id`，`wait` 的字符串参数现在是 namespace。SQLite 自动升级到 schema 7，删除历史等待表，保留当前任务、删除标记和显式 `idempotency_key` 去重记录。旧 operation ID 不再是等待凭证；请改为文件身份或 namespace/path。重复 idempotency key 仍返回首次接收结果，不覆盖文件的新状态；随后 `wait(report)` 等待文件当前状态。

独立 GC 线程回收无引用的受管理文件。可用 `GCPolicy(enabled=False)` 关闭自动 GC，由宿主调用 `collect_garbage()`。GC 不删除外部原件或借用的应用产物；已打开的产物句柄受保护。Adapter 应允许前台查询与后台调用重叠，或自行串行化。

- [统一设计](docs/design.md)
- [StashBase 对接](docs/stashbase-integration.md)
- [实施与验证进度](docs/backlog.md)

开发检查：`uv sync --locked --dev`，然后 `uv run pytest -q`、`uv run ruff check src tests`、`uv run ruff format --check src tests`、`uv run pyright`。测试包含真实 Milvus Lite、进程崩溃恢复和 Windows 专用路径用例。

完整向量的计算缓存与搜索可见性独立，按 namespace、索引构建代、模型配置和片段 hash 复用。缓存以带校验和的二进制形式保存，逻辑上限为每实例 32 MiB，按 LRU 淘汰；rename 通过覆盖新旧路径的 sync 处理，不需要 rename 方法。缓存命中可省去重复 embedding；淘汰或损坏时重新计算，显式 reindex 清理旧缓存并重建。
