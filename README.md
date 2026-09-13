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

写入返回接收结果，默认 4 个后台 Worker 按文件顺序执行 Processor、Chunker、embedding 和发布；旧版本清理独立执行，在每个阶段边界重新调度。Processor 的 checkpoint 可在持久保存中间结果后让出给更紧急的可运行任务，旧调用和子进程退出后才释放执行权；再次调用时从 resume_state/resume_files 恢复。每文件保留一个最新目标，重复变更可以合并；已失效旧文件立即退出读取和搜索，失败不回退旧结果。`wait(DocumentId(...))` 等待文件当前任务，`wait(namespace, path="notes")` 等待范围当前任务；期间接受的新版本也要完成。传入写入/sync 返回值是相同文件/范围的简写，不等待历史版本。失败、blocked 或取消会抛 `OperationFailed`。`search` 默认 `consistency="eventual"`，查询当前有效索引。显式 `consistency="strong"` 等待这一个 namespace。`timeout=5.0` 是搜索调用方的总等待预算，包含排队、一致性等待、查询 embedding 和后端检索；到期抛 `WaitTimeout`，`None` 不设期限，`0` 立即超时。执行阶段检查同一 deadline，Milvus 接收剩余时间；不响应取消的外部调用可能继续运行到返回，届时丢弃结果，不开始后续阶段。借用文件可在两次 sync 间改变，grep 读当前内容，索引保留上次成功处理的版本，直到下次 sync 撤销它。

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

`configure_namespace(namespace, processors=..., chunker=..., embedder=..., indexing=...)` 一次接收配置，自动选择必要阶段，返回可等待的 ConfigurationReport。旧配置继续服务，候选配置全部完成才切换；失败/取消保留旧配置。`reprocess_namespace` 是强制重处理的兼容入口，返回 ConfigurationReport；`reindex(..., timeout=...)` 是等待完成的索引修复入口。旧存储需要调用 `migrate_namespace(namespace, processors=[...], indexing="bm25" | "hybrid", ...)`，不会打开即自动换模型。失败可用 `document_status`、`list_document_statuses`和 `scope_status` 诊断，`retry` / `reprocess` 恢复。

`namespace_configuration(namespace)` 读取持久的索引模式、暂停状态、完整适配器清单、待生效清单与文件大小限制，无需先绑定模型；规则由 `rules(namespace)` 读取。

本轮等待 API 变更：返回值移除 `operation_id`，`wait` 的字符串参数现在是 namespace。SQLite 自动升级到 schema 8，删除历史等待表，保留当前任务、删除标记和显式 `idempotency_key` 去重记录。旧 operation ID 不再是等待凭证；请改为文件身份或 namespace/path。重复 idempotency key 仍返回首次接收结果，不覆盖文件的新状态；随后 `wait(report)` 等待文件当前状态。

独立 GC 线程回收无引用的受管理文件。可用 `GCPolicy(enabled=False)` 关闭自动 GC，由宿主调用 `collect_garbage()`。GC 不删除外部原件或借用的应用产物；已打开的产物句柄受保护。ExecutionPolicy 默认 workers=4、queries=4、heavy=1、light=2。Processor/Chunker 默认串行，具体实现类可声明 concurrency；resources={} 表示无需本地计算资源。Embedder 不经过对象并发门或资源准入，后台与查询分别受各自执行池限制，同一个 Embedder 可以被并发调用，实现须自行保证线程安全及服务限流。SQLite 连接由 MFS 按短查询/事务借还，最多 8 个，不要求调用方固定使用同一线程。

`ExecutionPolicy(stage_timeout=300)` 限制每个后台阶段的执行时间；长音视频可显式增加预算。到期目标进入 failed，错误码为 `ExecutionTimeout`，可显式 retry；尚未退出的调用仍占用资源，同文件重试须等它退出。Processor 可用 `context.cancellation.remaining()` 给内部请求设置更短的超时，受管理子进程会在超时后退出。

`close(timeout=30)` 停止接收新请求并等待清理；在途 sync 在目录项和哈希分块之间检查关闭信号，中断时返回 `complete=False` 和 `Closed` 失败，不把未观察文件当作缺失删除。close 到期抛 `WaitTimeout`，清理继续、实例锁继续持有。可以再次 close 等待，`timeout=None` 则一直等实际退出。库不能强杀任意 Python/模型线程；daemon 宿主在关闭预算耗尽后可终止整个进程，再按恢复协议重开。

源文件改名、移动或删除前，可用临时租约等待相关执行和源文字读取退出。租约不会创建或清除用户取消门；重叠租约分别释放。原有索引搜索仍可使用，租约内新的 `read` 报 `CapabilityUnavailable`，需要文字的 `grep` 对相应文件返回部分失败。应用负责串行化磁盘操作及其 sync；同一源属于多个 Folder 时，要一起列出相关 namespace 范围。`wait` 应放在租约释放后：

```python
from mfs import UnderPath

with mfs.quiesce([UnderPath("files", "notes")], timeout=30):
    (root / "notes/old.md").rename(root / "notes/new.md")
    report = mfs.sync("files", "notes")
mfs.wait(report, timeout=30)
```

迁移时用 `create_namespace(..., processing_paused=True)` 持久阻止准备和新增索引，先 sync，再用 `restore_document_state(id, expected_revision=..., state="cancelled" | "failed", error=...)` 导入未执行目标的旧状态。failed 必须提供 `TaskError`。重复导入幂等，不覆盖后来的显式 retry。完成迁移后 `configure_processing(namespace, paused=False)` 开始处理；重启不会自动解除此门。它与只暂停索引的 `configure_index(paused=True)` 不同，已有执行的实际退出仍用 quiesce 等待。

External 输入会在处理开始、checkpoint 和结果发布时校验，索引读取也核对已准备文字 hash。检测到变化报 `SourceChanged`，不会缓存不匹配结果；源字节相同的显式 sync 仍可刷新 stat。外部路径没有快照隔离，宿主应使用上述租约保护自己的变更，不能假定任意外部编辑器受 MFS 锁控制。work_dir 内的 text_path/grep_path 都会复制为受管理文字并受 GC 引用保护。

POSIX 的 `run_process` 使用独立监督进程；宿主硬退出后，监督进程清理命令进程组，并在实际退出前保留实例锁。此时重开可能短暂报 `InstanceLocked`。命令及其子进程必须保留受管理进程组，不能自行 daemonize/脱离 session。冻结的 sidecar 入口需在应用初始化前调用 `mfs.run_process_supervisor()`，并打包 `mfs._process_supervisor`；普通 Python 程序无需这个启动步骤。Windows 继续使用原生 Job Object。

- [统一设计](docs/design.md)
- [StashBase 对接](docs/stashbase-integration.md)
- [实施与验证进度](docs/backlog.md)

开发检查：`uv sync --locked --dev`，然后 `uv run pytest -q`、`uv run ruff check src tests`、`uv run ruff format --check src tests`、`uv run pyright`。测试包含真实 Milvus Lite、进程崩溃恢复和 Windows 专用路径用例。

完整向量的计算缓存与搜索可见性独立，按 namespace、索引构建代、模型配置和片段 hash 复用。缓存以带校验和的二进制形式保存，逻辑上限为每实例 32 MiB，按 LRU 淘汰；rename 通过覆盖新旧路径的 sync 处理，不需要 rename 方法。缓存命中可省去重复 embedding；淘汰或损坏时重新计算，显式 reindex 切换缓存代并重建，旧缓存按有界 LRU 回收。

`configure_index` 的连续开关与 `configure_namespace` 合并到最后接收的配置；只改 paused 不改变正在构建的模式。暂停也约束从 off 开启的新候选索引。开启索引会追赶已接收文件，不会隐式 sync 外部目录；关闭保留搜索文字，排名索引在后台清理。更换 Embedder 直接调用 `configure_namespace`，旧候选会自动失效，无需先 cancel；`cancel(DocumentId)` 用于停止文件当前任务，不是取消 namespace 配置的入口。

配置替换可以先接收，再轮询/等待，不阻塞 daemon 的状态和取消入口：

```python
change = mfs.configure_namespace("files", embedder=new_embedder, indexing="hybrid")
# 当前查询继续使用旧模型及其索引。
mfs.wait(change, timeout=30)
```

重启后通过 `namespace_configuration` 读取 `active_revision/pending_revision` 和两份清单，分别用 `open_namespace(..., configuration_revision=...)` 绑定。绑定验证期间若发生配置切换，会报 `NamespaceCompatibilityError`，重新读取当前配置后再绑定。配置成员在后台分批补齐；`pending_error/pending_failures/pending_retry_at` 描述候选配置的维护故障，达到 5 次后停止自动重试，重新提交相同配置可恢复。取消不会删除源；普通 sync 即使发现新内容也保持取消，只有显式 retry/reprocess 恢复。

`grep(..., consistency="strong", timeout=5)` 只等待文字准备，embedding 失败不会阻止已准备文字。删除接收后 strong 查询立即过滤；`wait(删除报告)` 仍等待后台物理清理。`DocumentStatus` 保留 `active_run_id`、`attempt_token`、`attempts`、`stage/state` 和 `cleanup_pending`。

grep 的 timeout 同样覆盖排队、读取、匹配、来源定位和 Chunker 的总等待；超时抛 `WaitTimeout`。grep 与排名搜索各有独立的有界执行池，容量均由 `ExecutionPolicy.queries` 控制；已超时但未退出的调用仍占用其池和实际资源。单个文件丢失、被替换为链接/特殊文件或暂时 quiesce 时，grep 返回其他命中，并设置 `truncated=True` 和 `failures: tuple[GrepFailure, ...]`；调用方应展示部分结果，不能解释为完整无结果。已知文件的 read 仍直接报告读取错误。

宿主有未完成的磁盘事务时，可用 `MFS.open(state, start_paused=True)` 禁止后台执行及 GC；读取宿主持久日志、恢复磁盘操作、确认相关 sync 完整接收后调用 `resume_background()`。启动门不替代宿主日志，不能在 quiesce 内等待索引。详见 [并发与恢复合同](docs/design.md)。
