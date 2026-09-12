# MFS

MFS 是嵌入式 Python 3.13 文件搜索库。一个实例管理多个 namespace：共享 SQLite 元数据，每个 namespace 使用独立的 Milvus collection、Processor、Chunker、Embedder 和规则。

External 只保存源文件指针，**不复制外部原件**，只支持 `sync`。Internal 保存原件，支持 `upsert` / `remove`。TXT/Markdown 直接使用原件文字；PDF/DOCX 等按需提取。SQLite 保存引用、SourceMap、hash 和任务状态，Milvus 保存片段文字及可选向量。

```python
from pathlib import Path
from mfs import MFS, ByNamespace, DocumentId, TextMatch, Utf8TextProcessor

mfs = MFS.open(Path(".mfs-state"))
try:
    if not mfs.list_namespaces():
        mfs.create_namespace("notes", "internal", processors=[Utf8TextProcessor()])
    else:
        mfs.open_namespace("notes", processors=[Utf8TextProcessor()])

    receipt = mfs.upsert("notes", "hello.md", b"Hello, world!")
    mfs.wait(receipt, timeout=30)
    print(mfs.grep([ByNamespace("notes"), TextMatch("Hello")]).items)
    print(mfs.search("hello", filters=[ByNamespace("notes")], mode="bm25").items)
    print(mfs.read(DocumentId("notes", "hello.md")))
finally:
    mfs.close()
```

External 用法：`create_namespace("files", "external", root, processors=[...])`，然后 `sync("files", verify="content")`。状态目录和 External root 不能重叠，不同 namespace 的真实 External root 也不能重叠。子目录使用 `UnderPath` 或 `sync(namespace, path)` 指定范围。

`MFS.open` 不接收全局适配器。创建时默认使用 `DefaultChunker`；未提供 Embedder 默认 `bm25`，提供则默认 `hybrid`。重开 namespace 时传入相同声明的实现；库核对 Processor/Chunker 的 id、version、options 和路由，以及 Embedder 的 embedding_space、dimension。只有维度相同仍可能不兼容，不匹配立即报错。off/bm25 重开时可以省略不使用的 Embedder；若传入则仍核对声明。打开实例本身无需加载模型；已知后缀的 sync、删除、状态读取和已保存文字读取可以先使用。

`grep` 提供文字、正则、名称和路径匹配，`GrepBudget` 限制读取量、文档数和匹配数，达到预算返回 `truncated`。已知文档使用 `read`；`grep(select="doc")` 返回预算内文字，不加载 Internal 二进制原件。排名搜索使用 `search(mode="bm25" | "vector" | "hybrid")`，默认 mode 为 hybrid；只有 BM25 的 namespace 应显式选 bm25。公开 `query` 已移除。

写入返回持久回执，一个后台 worker 依次执行旧代清理、Processor、Chunker、embedding 和发布。每文件保留一个最新目标，重复变更可以合并；已失效旧文件立即退出读取和搜索，失败不回退旧结果。`wait(receipt)` 只等本次操作；`search(consistency="strong")` 等选中 namespace，`eventual` 立即读取当前有效索引。借用文件可在两次 sync 间改变，grep 读当前内容，索引保留上次成功处理的版本，直到下次 sync 撤销它。

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

`reprocess_namespace(namespace, processors=[...])` 显式更换 Processor 并重新处理；`reindex(namespace, chunker=..., embedder=..., timeout=...)` 显式重建索引。旧存储需要调用 `migrate_namespace(namespace, processors=[...], indexing="bm25" | "hybrid", ...)`，不会打开即自动换模型。失败可用 `document_status`、`list_document_statuses`、`scope_status` 和回执诊断，`retry` / `reprocess` 恢复。

独立 GC 线程回收无引用的受管理文件。可用 `GCPolicy(enabled=False)` 关闭自动 GC，由宿主调用 `collect_garbage()`。GC 不删除外部原件或借用的应用产物；已打开的产物句柄受保护。Adapter 应允许前台查询与后台调用重叠，或自行串行化。

- [统一设计](docs/design.md)
- [StashBase 对接](docs/stashbase-integration.md)
- [实施与验证进度](docs/backlog.md)

开发检查：`uv sync --locked --dev`，然后 `uv run pytest -q`、`uv run ruff check src tests`、`uv run ruff format --check src tests`、`uv run pyright`。测试包含真实 Milvus Lite、进程崩溃恢复和 Windows 专用路径用例。
