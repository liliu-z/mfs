# MFS 决策与依据

> 文档角色：内部讨论依据，不是实现规范。\
> 现行设计以 [design.md](design.md) 为准；如果两者冲突，必须先修改设计，再实施。

## 1. 维护规则

本文只保存以后仍可能有价值的信息：

* 已核实、重新调查成本较高的事实；

* 重要设计选择及其替代方案；

* 选择理由、代价和重新评估条件；

* 被新决策取代的历史结论。

不保存完整聊天记录、临时措辞和已经失去价值的中间草稿。

状态含义：

| 状态         | 含义            |
| ---------- | ------------- |
| Accepted   | 已进入现行设计       |
| Proposed   | 当前建议，编码前仍需确认  |
| Superseded | 曾经采用，已经被新决策取代 |
| Rejected   | 已评估但不采用       |

## 2. 已核实事实

F-001 ~ F-004 来自 2026-07-30 的上游与产品调查；F-005 ~ F-012 来自 2026-08-18 对 StashBase 源码与运行时落盘的逐项核实，每条均标注了文件与行号。

### F-001：MFS `v0.1.0`

核实日期：2026-07-30。

* 正式 GitHub Release，仍标记为 Alpha；tag `v0.1.0`，commit `7cd3c5b`，发布时间 2026-05-06。

* StashBase 两个 Python 环境安装的都是 `mfs-cli 0.1.0`。

* 没有公开稳定 Python SDK；StashBase 直接调用 `mfs.store`、scanner、chunker、converter 等内部模块。

* CLI 包含 `add/remove/status/search/grep/ls/tree/cat/config`。

* 默认状态位于同一个 `MFS_HOME`，包括 config、queue、status、converted cache 和 Milvus Lite DB。

* `add`、`watch` 可以接收多个路径，但没有持久 Source registry。

* 普通 CLI 每次启动后退出；detached worker 排空 `queue.json` 后退出。

* `mfs add --watch` 是长期前台 CLI，`Ctrl+C` 退出；它不是正式 daemon。

* 默认一个 Milvus DB/collection，不是每个目录一套。

数据路由：

| 操作        | `v0.1.0` 行为                        |
| --------- | ---------------------------------- |
| `ls/tree` | 读取真实目录                             |
| `cat`     | 读取文件；PDF/DOCX 先转文本                 |
| `grep`    | BM25 候选选择 + 真实文本 regex/system grep |
| keyword   | Milvus BM25                        |
| semantic  | query embedding + dense vector     |
| hybrid    | BM25 与 dense vector 融合             |

格式：

* 直接索引 Markdown、RST、TXT、常见代码、脚本、SQL、Proto、GraphQL、Terraform/HCL。

* PDF 通过 `pymupdf4llm` 转 Markdown；DOCX 通过 `python-docx` 转 Markdown。

* JSON/YAML/CSV/HTML/XML/CSS/log 等默认不进入向量索引，但可以走文本读取/grep 路径。

* 图片、音视频、压缩包和 binary 默认忽略。

* 默认跳过超过 10 MB 的文件。

主要来源：

* <https://github.com/zilliztech/mfs/tree/v0.1.0>

* <https://github.com/zilliztech/mfs/blob/v0.1.0/README.md>

* <https://github.com/zilliztech/mfs/blob/v0.1.0/docs/architecture.md>

### F-002：MFS `v0.4` 不是兼容升级

* 公开版本从 `v0.1.0` 跳到 `v0.4.0-beta.1`，没有公开 `v0.2/v0.3` release。

* `v0.4` 是整体重写：Rust CLI、FastAPI server、HTTP Interface、connector、持久任务、多种 metadata/artifact backend。

* 产品方向从本地文件搜索变成多数据源 Agent context harness。

* 本地文件只是 connector；当前重点是 connector、processing profile、多用户和分布式部署。

* 自动处理、任务恢复、cache 和 locator 比 `v0.1.0` 成熟，但文件 mutation 不是其核心方向。

* 调查时最新正式版为 `v0.4.5`；当时 main 仍在重构 engine/supervisor/orchestrator。

主要来源：

* <https://github.com/zilliztech/mfs/releases/tag/v0.4.5>

* <https://github.com/zilliztech/mfs/blob/v0.4.5/README.md>

### F-003：当前 StashBase 的运行方式

> 概览。代码级细节见 F-005 ~ F-012（2026-08-18 核实）。

* 一个 Node server 管理一个 Python sidecar/Engine 和一个全局 Milvus Lite DB/collection。

* 多个目录通过绝对路径 scope 绑定到同一个 Engine，不是每目录一个进程或 DB。

* StashBase 在 2026-06 删除了 filesystem watcher。

* 自己写文件时直接更新索引；外部变化在启动、目录打开/切换、窗口 focus、Agent turn end、手动 Sync 和 MCP reindex 时扫描 reconcile。

* 当前 StashBase 已经验证“显式更新 + event-point reconcile”的可行性。

### F-004：当前 StashBase 的格式处理

> 概览。代码级细节见 F-005 ~ F-012（2026-08-18 核实）。

当前不是 MFS 自动处理各种格式，而是 StashBase 先转换，再把统一文本交给 patched MFS。

| 格式            | 当前 StashBase 处理                                |
| ------------- | ---------------------------------------------- |
| Markdown      | 原文编辑、预览和索引                                     |
| HTML          | sandbox preview；提取正文后索引                        |
| PDF           | pdf.js 预览；PyMuPDF/pymupdf4llm 提取；扫描件 OCR       |
| PNG/JPEG/WebP | 显示原图；RapidOCR 后索引                              |
| DOCX          | Mammoth 转 sanitized HTML 后预览和索引                |
| 音频            | FFmpeg + whisper.cpp，生成 timestamped transcript |
| 视频容器          | 只提取音轨并转录，不理解视频画面                               |

这意味着 StashBase 当前拥有 format detection、conversion、derived store、任务调度和大量 readiness 逻辑。

### F-005：StashBase ↔ MFS 的实际接触面

核实日期：2026-08-18，基于 `/Users/liliu/Documents/li/stashbase` 源码。

单一 seam：`server/indexer.mfs.ts`(429 行) → `server/mfs-daemon.ts`(499 行，stdio JSON-lines) → `python/stashbase_daemon.py`(1741 行)。

Python 侧只 import 四个 MFS 内部模块，没有 SDK：`mfs.store.MilvusStore`、`mfs.ingest.chunker.chunk_file`、`mfs.ingest.scanner.Scanner`、`mfs.config`。

十四个 op：`bind_folder`、`unbind_folder`、`upsert`、`delete`、`delete_prefix`、`rename`、`rename_prefix`、`search`、`status`、`scan_diff`、`list`、`close_store`、`set_rules`、`reconcile_source`。

**写后同步的胶水占 21 个调用点**：`upsertFile` 6、`upsertConvertedFile` 1、`deleteFile` 8、`deletePathPrefix` 2、`renameFile` 3、`renamePathPrefix` 1。其中 4 个在 `sync.ts` 内部自用，另外 17 个是"应用改完磁盘后手动通知索引"。

### F-006：MFS 今天不读盘，也不拥有规则

* `upsert {path, content, ext, file_hash}` —— `content` 是字符串。MFS 从未打开过文件。
* `bind_folder {provider, api_key, model, dimension}` —— embedder 的选择权在调用方。
* `set_rules {excluded_dirs, max_indexable_bytes, include_extensions, ...}` —— daemon docstring 原文：*"receive indexing rules from Node (single source of truth there)"*。

MFS 今天的实际职责只有：chunk、embedding、向量库读写、hash diff。

写路径是两步且可漂移：`file-save.ts:97-99` 先 `saveText()` 落盘，再 `upsertSavedFile()` 通知索引；后者失败只返回警告 `"Saved, but semantic index update failed"`，索引漂移留给下一次 sync 修复。

### F-007：content hash 有三份实现，其中一份靠 monkey-patch

| 位置 | 算法 |
| --- | --- |
| `indexer.mfs.ts:78` `prepareForIndex` | `blake3(TextEncoder().encode(content))` —— 内存字符串 |
| `file-hash.ts:8` `blake3File` | 流式读盘 |
| `stashbase_daemon.py:459` `_patch_scanner_blake3` | 覆盖 `Scanner.compute_file_hash` |

第三份必须存在，注释原文：*"MFS upstream hard-codes SHA256... The two MUST use the same algorithm or **every file would forever look modified**."*

根因是 MFS 不读盘，算不了源文件字节的 hash。派生文件尤其明显：MFS 拿到的是提取后的文本，但必须存的是源文件字节的 hash，否则 `scan_diff` 每次都判定不一致而**无限重复转换**（`index.ts:88` 注释确认）。

### F-008：Milvus Lite 用 Parquet，不是 SQLite

核实安装包 `milvus_lite`：纯 Python，无 `.so`/`.dylib`，全包 `grep -rln sqlite` 零命中。

```text
milvus.db/collections/<name>/
  schema.json
  manifest.json + manifest.json.prev
  wal/
  partitions/_default/{data,delta,indexes}/*.parquet
```

`storage/manifest.py` 记录了原子更新协议：dump 到 `.tmp` → fsync → 备份 `.prev` → `os.replace` → fsync 父目录。

**这解释了两个 monkey-patch 的根因，都不是偶然缺陷：**

* `MilvusStore._query_all`：行按 immutable segment（Parquet 文件）顺序返回，不是主键序。`query_iterator` 用末尾主键当游标，超过一页必然静默漏段。patch 注释确认后果是"已存的文件报 pending 然后重复 embedding"。这是段式存储的必然结果。
* `Manifest.save`：`os.replace` 在 Windows 上替换 manifest 会失败。

### F-009：没有 embedding 缓存

```python
def _embed_with_cache(svc, path, embedder, texts) -> list:
    return embedder.embed(texts)          # daemon.py:851-852，函数体只有这一行
```

TS 侧 `embedCache|embeddingCache|vectorCache` 零命中。向量只在 Parquet 里一份。

唯一的复用是 `op_rename` 的 `_try_rename_without_reembed`：从向量库读出旧行、替换 `source`/`id`/`parent_dir`、保留 `dense_vector` 写回。只在 rename 时生效。

后果：chunk id 是 `blake3(f"{path}:{start_line}:{end_line}:{hash(text)}")`——**路径和行号在 key 里**，所以文件开头插一行会让后续每个 chunk 的 id 全变，全文重新 embedding。

### F-010：没有 watcher，也没有本地账本

`server/watcher.ts` 只剩一个计数器。文件头原文：*"2026-06 simplification: StashBase no longer watches the filesystem... That deleted the debounce window, the self-write suppression TTL, the watcher-vs-import race gate, and the whole class of 'fs event arrived at the wrong moment' bugs."*

七个触发点，全是事件，无定时器：app boot、打开/切换目录、窗口 focus（5 秒节流）、agent 回合结束、手动 Sync、MCP reindex、embedder key 或转录模型就绪。

**窗口 focus 不是 watch。** 它是窗口管理器事件，不携带任何文件系统信息。watch 回答"怎么知道变了"，focus 只决定"什么时候去看"；真正干活的始终是 `scan_diff`。

**StashBase 没有自己的文件树或 hash 账本：**

* `file-listing.ts` 每次实时 `readdirSync`；仅有的缓存按 mtime 失效，存 heading/snippet，不存 hash。
* `blake3File` 全代码库仅 2 个调用点（转换 hook、音频转录），算完即用，不落库。
* `state-db.ts` 注释确认已删除相关表：*"Earlier `files` and `index_queue` tables duplicated daemon/reconcile state **write-only** and were removed... the daemon/store owns the authoritative per-file hash (via `scan_diff`)."*

唯一的 `{path: file_hash}` 账本在 Parquet 里。`scan_diff` 全程在 Python 内完成（walk 磁盘算 hash + 读向量库账本 + 内存比对 + 按 hash 1:1 配对 rename），跨进程只传回几十个路径的 diff。

### F-011：现有落盘共五处，三种格式

| 位置 | 格式 | 内容 |
| --- | --- | --- |
| `~/.stashbase/config.json` | JSON | `recentFolders[]`、`embedder{provider,model,apiKey}`、`apiKey`、`mcpHttp{...}` |
| `<appData>/state/state.db` | SQLite (better-sqlite3, WAL) | **只有一张表** `conversions` |
| `<appData>/derived.nosync/` | JSON + 文件 | `manifest.json` = `{派生key: 源文件绝对路径}`；`<key>.md`；`<key>_files/` |
| `<appData>/vector-store.nosync/milvus.db/` | Parquet + JSON + WAL | 向量 **及全部文件元数据** |
| `<appData>/models/whisper/` | 二进制 | 转录模型 |

另有按需创建的 `<appData>/file-order/<root>.json`。

`conversions` 表只持久化失败行，注释解释：*"in-flight lives in process memory (a crash kills the conversion with us — persisting it only produced corpses needing a reclaim pass), and 'done' is recorded by the derived note's existence on disk."*

**三个错配：**

1. 派生结果按 `blake3(源文件绝对路径)` 寻址，向量按 `blake3(path:行号:文本hash)` 寻址，两个 key 空间无关联。改名一次：派生 miss（重新 OCR）而向量 hit（复用）——同一操作两个子系统给出不同答案。
2. `derived/manifest.json` 这张反向索引存在的唯一目的是补救第 1 条。`derived-store.ts` 注释坦承：*"a rename re-converts (rare); content-addressing would need a separate path→hash index just to support deletion."*
3. 唯一的元数据账本寄生在 Parquet 里——这是 F-008 两个 patch、F-010 无本地账本、以及 `files` 表被删的共同根因。

### F-012：转录依赖应用自建的 native 二进制

`native/transcription/README.md`：StashBase 为每个发布目标构建 `whisper-cli`、`ffmpeg`、`ffprobe` 三个可执行文件，覆盖 `darwin-arm64` / `linux-x64` / `win32-x64`，`toolchain.json` pin 了 source revision、build option 和平台基线，产物打包进 Electron resources。

这条构建与分发链无法迁入 MFS，是"注册"而非"配置"的关键证据。

### F-013：当前 StashBase 是一份全局 MFS 数据，多目录共享

核实日期：2026-08-19，基于 `/Users/liliu/li/stashbase` 源码和本机运行时目录。

* `server/local-data.ts:55-60` 明确将 `vector-store.nosync` 定义为 whole app 的 single global vector store；一个 daemon 和一个 Milvus collection 索引所有打开目录，以绝对路径区分文件。

* macOS 的 `appDataRoot()` 默认是 `~/Library/Application Support/StashBase`；环境变量 `STASHBASE_LOCAL_DATA_ROOT` 可以覆盖。

* 当前 MFS/Milvus Lite 数据实际位于 `~/Library/Application Support/StashBase/vector-store.nosync/milvus.db/`；核实时 collection 为 `vectors_openai_1536`。

* StashBase 派生内容位于同一 app data root 下的 `derived.nosync/`，但不归当前 MFS 管理；当前没有用于 Copy 原文件的 MFS object store。

* 当前全局 scope 依赖文件绝对路径，没有显式 `project_id`。所以它在物理上已经是“一份全局 Engine 数据、多目录共享”，在领域模型和隔离约束上还不是完整的 multi-Project Engine。

## 3. 设计决策

现行设计快速索引：

| 主题 | 现行决策 |
| --- | --- |
| 定位与全局拓扑 | D-005、D-007、D-027 |
| Link / Copy / sync | D-024、D-028、D-032 |
| Revision 与查询一致性 | D-020、D-029、D-030 |
| catalog、hash、缓存与事务 | D-016、D-017、D-018 |
| Processor 与 Provider | D-008、D-009、D-010、D-019 |
| 公开 Interface 与实现路线 | D-012、D-013、D-021、D-031 |

其他编号保留为历史推理；标为 Superseded 的结论不得作为实现依据。

### D-001：MFS 是智能文件数据层

状态：**Superseded**（被 D-022 取代，2026-08-19）。原对应设计：§1、§2。

背景：`v0.1.0` 已经不只是 Milvus 调用，但没有清晰拥有文件数据生命周期，导致 StashBase 自己补齐大量能力。

考虑过：

1. MFS 只包装 Milvus；
2. MFS 扩张成完整 StashBase 产品；
3. MFS 管理通用文件数据生命周期，StashBase 管理产品体验。

决定：采用方案 3。

理由：删除调用方重复复杂度，同时避免把 UI、编辑器和 Agent 产品逻辑放进基础软件。

代价：MFS 的范围扩大到文件操作、同步、处理任务和 readiness，不能只改几个 CLI 命令。

重新评估：如果未来项目明确只做检索 SDK，不再承担文件管理，应重新定义产品而不是悄悄缩小 Interface。

### D-002：普通文件是事实来源

状态：**Superseded**（被 D-014 取代，2026-08-18）。原对应设计：§1、§3、§11。

原决定保留了"MFS 只观察、不拥有写入"的前提。核实 StashBase 实现后发现这正是重复复杂度的根源，见 D-014。原文如下：

决定：V1 使用 Local Folder Source；普通文件保存用户内容。SQLite 记录已观察状态，Derived Representation 和 Index Projection 可重建。

理由：兼容 Finder、Git、VS Code 和现有目录；迁移 StashBase 风险最低；不需要重新实现文件存储系统。

代价：必须正确处理外部修改、离线变化、symlink 和路径身份。

重新评估：只有 MFS 开始自己保存内容，或主要 Source 变成远端对象时，才讨论 managed content store。

### D-003：统一使用 Library/Source/Entry

状态：**Superseded**（被 D-023 取代，2026-08-19）。原对应设计：§3。

决定：一个 Library 包含多个 Source；Source 包含 Entry。早期文档中的 `Workspace` 不再使用。

理由：`Workspace` 曾同时表示一个目录、多个目录或一个 DB，无法稳定表达查询范围和所有权。

代价：旧原型和文档中的 Workspace 命名需要迁移。

### D-004：先 Library，再 CLI，再 Engine

状态：**Superseded**（被 D-031 取代，2026-08-19）。原对应设计：§14。

取代：早期的 Engine-first 建议。

考虑过：

1. 一开始实现 Engine/RPC；
2. 先做 Python Library；
3. 继续让应用调用人类 CLI。

决定：先稳定 Core Library；再做一次性 CLI；最后做长期 Engine 和 TypeScript client。

理由：先验证领域模型和 Interface；测试不依赖 RPC；CLI 和 Engine 可以复用同一 Implementation。

代价：StashBase 是 TypeScript，必须等 Engine 完成后才能正式迁移。

重新评估：如果首个必须交付的使用者只能通过跨语言协议接入，可将 Engine 提前，但不能让 Core 依赖 Engine。

### D-005：一个 Engine 管多个 Source

状态：Accepted，由 D-027 细化为一个全局 Engine 管多个 Project 与 Source。对应设计：§1、§3、§12。

决定：未来一个 StashBase 实例启动一个 Engine；Source 是逻辑状态和后台 task，不是独立进程。

理由：共享 Python runtime、embedding model、worker pool 和 Milvus connection；避免多个进程争抢 Milvus Lite file lock；支持跨目录查询。

代价：需要明确 Source scope、资源公平性和失败隔离。

重新评估：不同 OS 用户、安全域、native dependency 或强故障隔离时，可以拆进程。

### D-006：scan/reconcile 保证正确，watcher 只优化延迟

状态：**Superseded**（被 D-015 取代，2026-08-18）。原对应设计：§7。

结论方向正确，但把 scan 定位成常态正确性机制；写入下移后 scan 只服务外部改动。原文如下：

取代：将“watch 或不 watch”视为二选一的早期理解。

决定：显式 mutation 直接记录；外部变化最终由 scan/reconcile 确认；V1 提供可选 watcher，watch event 只触发 scoped reconcile。

理由：watcher 会丢事件并产生 debounce、overflow、rename 和自写事件问题；scan 才能在离线修改和重启后恢复。

代价：需要维护 Entry inventory、hash 和扫描性能；fresh query 可能需要等待。

重新评估：不取消 scan。只能根据真实延迟需求决定 watcher 默认是否启用、扫描频率和 scope。

### D-007：V1 不 mount

状态：Accepted，由 D-024 增加可选 copied object，但不改变“不 mount”。对应设计：§1、§4。

考虑过：普通目录 + Library、只读 mount、可写 FUSE/WinFSP、NFS/WebDAV。

决定：V1 使用现有普通目录，不实现 mount。

理由：本地文件已经可以被 OS 工具访问；mount 会引入 inode、handle、random write、fsync、lock、权限、崩溃恢复和跨平台打包问题；ANNS 也无法自然映射到 POSIX。

代价：只支持 filesystem path 的远端/虚拟数据源暂时不能透明呈现。

重新评估：MFS 自己拥有内容、需要投影远端 Source，或出现明确第三方兼容需求时，先验证只读 mount。

### D-008：MFS 拥有处理，StashBase 拥有展示

状态：Accepted。对应设计：§2、§10。

决定：PDF/OCR/DOCX/audio 的提取、Derived Representation、任务、索引和 Evidence 迁入 MFS；StashBase 保留 viewer、播放器、交互和业务语义。

理由：处理能力对其他应用也有价值；由 StashBase 持有会形成重复数据层。Viewer 则高度依赖产品体验，不应进入 MFS。

代价：StashBase 现有 conversion/indexing 代码需要分阶段迁移，短期存在新旧实现并行。

### D-009：白名单 + Processor registry

状态：Accepted。对应设计：§10。

决定：所有文件可进入 Entry inventory；只有 Processing Profile 白名单启用的格式自动处理。Processor 可以显式注册和替换。

理由：修复 `v0.1.0` 扩展名常量和 converter `if/else` 写死的问题；允许应用提供自定义 PDF/OCR 等实现。

约束：覆盖必须显式；Processor 有稳定 id/version；配置不能直接执行任意 shell 命令；任务必须支持 timeout、取消和错误隔离。

重新评估：Engine 插件体系只在存在第二种真实部署方式时设计，Library 阶段先支持可信 Python 注册。

### D-010：模型按能力拆分 Provider

状态：Accepted。对应设计：§10。

决定：Embedding、Text Generation、Vision 和 Transcription 使用不同 Interface，通过统一 registry 查找。

理由：输入、输出、批处理和失效语义不同；一个万能模型 Interface 会把调用方迫使到大量 capability 判断中。

代价：Provider 数量增加，但每个 Interface 更小、更稳定。

约束：记录 model revision/dimension；MFS 统一管理 batch、rate limit、retry、timeout 和 cancel；secret 不写入普通配置。

### D-011：文件浏览、grep 和 ANNS 使用不同数据路径

状态：**Superseded**（被 D-029 取代，2026-08-19）。原对应设计：§8。

决定：

```text
ls/tree/stat/read(source) -> live filesystem
grep(source)              -> 普通文本原文
grep(text)                -> Derived Representation
keyword/vector/hybrid     -> Index Projection
```

理由：Milvus 不是目录事实来源；regex 不等于 BM25；索引滞后不能让 fresh grep 产生假阴性；PDF grep 应匹配提取文本并返回页码。

代价：需要明确 `live/available/at_least/fresh`，调用方不能假设所有查询都瞬时完整。

### D-012：新建核心，不直接选择某个上游版本继续开发

状态：Accepted。对应设计：§14。

决定：

* 当前 StashBase 在迁移前锁定 `mfs-cli==0.1.0`；

* `v0.1.0` 用作行为、CLI 体验和黑盒兼容参考；

* 固定 `v0.4.5` 用作 durable task、cache、pipeline、locator 等实现参考；

* 当前 upstream main 不作为依赖基线；

* 新 Library/Source/Entry Interface 和文件生命周期重新实现。

理由：`v0.1.0` 方向接近但架构不足；`v0.4` 工程机制成熟但产品已经转向 connector-first；直接 fork 任一版本都会继承错误的外部模型。

代价：需要承担新核心的实现和长期维护；复用 Apache-2.0 代码时必须保留许可证和 NOTICE。

### D-013：兼容 CLI 体验，不兼容内部模块

状态：Accepted。对应设计：§5、§14。

决定：保留 `ls/tree/cat/grep/search/status` 等体验；为 Source 和文件 mutation 增加无歧义命令。`remove` 默认不能删除原文件，真实删除必须显式调用 file delete。

理由：人类 CLI 不是应用 Interface；`add/remove` 在旧版中代表索引操作，在新系统中容易与文件操作混淆。

代价：不能保证所有旧脚本零修改，需要建立明确的兼容矩阵。

### D-014：MFS 拥有写入路径

状态：**Superseded**（最终被 D-027、D-028 取代，2026-08-19）。曾取代 D-002。原对应设计：§1、§4、§5。

背景：F-005 显示"应用改完磁盘再通知索引"的胶水占 21 个调用点，其中 17 个是应用侧；F-006 显示这两步会各自成败，形成漂移，只能靠后续 sync 修复。

考虑过：

1. 维持观察者模型，改进 sync；
2. MFS 拥有 namespace 与写入路径，落盘仍是普通目录；
3. content-addressed blob store，普通目录靠 materialize 导出。

决定：采用方案 2。`write` / `import_file` / `open_write` / `delete` / `move` 由 MFS 执行，落盘与索引在同一次调用内完成。namespace 权威在 SQLite，字节仍落普通可读文件树。

理由：删掉 17 个应用侧调用点和整类漂移；同时保住 Finder/git/编辑器兼容——这对最终用户是产品级需求，方案 3 会牺牲它却换不到当前需要的能力（见 D-018）。

代价：应用的保存路径需要改造，包括 `baseVersion` 冲突检测。旁观者仍可能看到多文件操作的中间态。

重新评估：出现真实的多文件原子需求、或需要投影远端 Source 时，再讨论 content-addressed 存储。

### D-015：不提供 watcher；sync 只服务外部改动，触发权在应用

状态：**Superseded**（触发权结论由 D-028 保留，scan Interface 被其重定义，2026-08-19）。曾取代 D-006。

背景：F-010 显示 StashBase 已在 2026-06 主动删除 watcher，并列出了随之消失的整类缺陷；改为在事件点主动 pull，已被验证可行。

决定：

* MFS 不提供默认 watcher；
* `sync(source)` 是外部改动的唯一入口；
* managed Source 正常情况下不需要 sync；
* **何时调用 sync 由应用决定**；
* MFS 的责任是让 sync 便宜：`entry` 表存 `size`/`mtime`，先 stat 预过滤，只对疑似变化的文件读内容。

理由：写入下移后，"外部工具改了文件"从主路径退化为边缘情况。而"用户什么时候需要新鲜数据"是产品知识——窗口 focus 之所以是好触发点，正因为它与"用户需要看到最新结果"的时刻天然对齐，这种判断不属于存储层。

代价：应用必须自己选触发点；选得不好就会有陈旧窗口。

重新评估：出现无人值守、无 UI 事件可依托的部署形态时，再考虑可选 watcher；但它只能降低延迟，不承担正确性。

### D-016：三层 content-addressed hash，位置信息不进 key

状态：Accepted（2026-08-18）。对应设计：§9。

背景：F-009 显示没有 embedding 缓存，且 chunk id 把路径和行号写进了 key——开头插一行导致全文重新 embedding。F-011 显示派生结果按源文件**路径**寻址，改名即失效。

决定：

| 层 | hash | 用途 |
| --- | --- | --- |
| 源文件 | `content_hash` | sync 判断变化、派生缓存 key、rename 配对 |
| 派生文本 | `text_hash` | Processor 升级后文本未变则跳过下游 |
| chunk | `chunk_hash` | embedding 复用 key |

规则：**缓存 key 只由内容决定，路径与行号是 payload。**

理由：一次修正三个问题——改一行只重算受影响 chunk；改名/移动/复制全部复用；Processor 版本升级不再必然导致全库重跑。

代价：多三张表和一次哈希计算。`derived-store.ts` 曾以"content-addressing 需要额外的 path→hash 索引"为由拒绝此方案，但 `entry` 表本身就是那张索引，该代价在新架构下消失。

约束：向量不额外存一份；命中时从向量库内部拷贝已有行，SQLite 只记 `chunk_hash → 所在 collection`。`op_rename` 的 `_try_rename_without_reembed` 已证明该路径可行。

### D-017：SQLite 是元数据账本，向量库只存向量

状态：Accepted（2026-08-18）。细化 P-002。对应设计：§11。

背景：F-008 确认 Milvus Lite 落盘是 Parquet 而非 SQLite；F-011 确认文件元数据（`file_hash`、`is_dir`、`embed_status`、`parent_dir`）寄生在 Parquet 列里。

决定：`catalog.sqlite` 保存 entry、derived、chunk、task、intent；向量库只保留向量与最小可过滤字段。

理由：Parquet 擅长批量扫描与近邻检索，不擅长点查与事务。把元数据放进去，"列出所有文件的 hash"这种最基本的操作就退化成全段扫描——现有实现正因此踩到 `query_iterator` 的分页缺陷并被迫打补丁（F-008）。这不是新增一个数据库，而是补上缺失的一层。

代价：多一个需要 migration 的持久化组件。

约束：single-writer；崩溃恢复不依赖向量库充当事务账本。Index Projection 可从 Canonical Text 重建；Copied 的 Canonical Text 可从 object 重建；Linked 只有在 live bytes 仍匹配目标 Revision 时才能重建。Project/Source/Entry identity 与 Copied object 必须纳入备份，不能宣称整个 `data_dir` 都是 cache。

### D-018：不做多文件原子事务

状态：Accepted（2026-08-18），由 D-024 澄清 Copy 专用 CAS 不用于实现多文件事务。对应设计：§4、§7、§11。

背景：讨论中曾把多文件原子提交当作"不 POSIX"的卖点，随后逐场景核对发现缺乏真实需求。

考虑过的场景：保存单文件、移动文件或目录、批量导入——都只需单个 rename。最接近的是改名时级联更新引用链接，但中断的后果是若干链接需要修复，不是数据丢失。

决定：**单个 copied object 的发布必须原子；多 Entry 内容写入不承诺原子。** 不为多文件事务引入 WAL commit protocol。D-024 的 content-addressed object 只用于 Copy 去重和耐久性，不提供事务快照。

单 object 协议是同一 `data_dir` 内临时写入 → hash/fsync → 原子 rename → catalog 事务加引用；崩溃可以留下待 GC 的无引用 blob，但 catalog 不能指向未完成文件。

理由：多文件原子性的实现代价（内容寻址存储、垃圾回收、materialize 层）远超收益，且会牺牲外部工具兼容。

重新评估：出现真正无法容忍中间态的场景时重开——但要先给出具体场景，不接受"更严谨"这类理由。

### D-019：配置与注册的判据是分发权

状态：Accepted（2026-08-18）。细化 D-009、D-010。对应设计：§10。

背景：F-006 显示应用传入的 `provider/model/api_key` 是配置而非注入；F-012 显示转录依赖应用自建的三平台 native 二进制，MFS 无法接管。

决定：以"这段实现的分发权在谁手里"为判据。

| 情况 | 配置 | 注册 |
| --- | --- | --- |
| MFS 自己带得了，只是参数不同 | ✅ | |
| 需要外部二进制或模型文件，由应用打包分发 | | ✅ |
| 需要应用的运行时上下文 | | ✅ |
| 换实现要改 MFS 代码 | ✅ 说明本该内建 | |
| 不同应用会给出不同实现 | | ✅ |

据此：索引规则、chunk 参数、模型选择、timeout/retry 是配置；PDF 引擎与 OCR 是配置；音视频转录是注册。

注册的形态是**进程契约**（`SubprocessProcessor`），不是 Python 对象——跨语言传不了，且转录器本身就是可执行文件。能力在 MFS（调度、缓存、超时、取消、错误隔离），二进制在应用。

补充约束：**Provider 契约要薄，外层要厚。** 实现者只写 `embed(texts) -> vectors`；timeout、重试、退避、批切分、token 上限、取消由 MFS 统一提供。现有实现是反例：MFS 的 `EmbeddingProvider` 只有三个成员，改不了 timeout，于是第一个真实用户直接绕过它自写了一份带重试与批切分的实现（F-006）。契约太薄而外层缺失，等于把复杂度推回调用方。

### D-020：grep 必须在 MFS 内部

状态：Accepted（2026-08-18），由 D-029 补充 active Revision 语义。细化 D-011。对应设计：§8。

背景：核实发现 keyword 检索完全绕过 MFS——`server/keyword-search.ts`(368 行) 直接调用 ripgrep 扫描原文与派生文件，还要自行处理 DOCX 派生 HTML、音频 transcript、转换未完成时的占位、HTML 正文提取。

决定：`search(mode="grep")` 由 MFS 提供，ripgrep 编排迁入。

理由：派生文本迁入 MFS 后，应用无从知道该 grep 哪些文件。把 grep 留在外面会强迫应用重新实现一份派生路径映射——那正是要消除的重复。这是硬耦合，不是可选项。

代价：MFS 需要打包或依赖 ripgrep，并承担其跨平台分发。

### D-021：公开 API 必须挡住内部实现

状态：Accepted（2026-08-18）。对应设计：§5。

背景：F-005 显示调用方直接 import 四个内部模块；F-007、F-008 显示它 monkey-patch 了两个 MFS 内部方法（`Scanner.compute_file_hash`、`MilvusStore._query_all`）和两个上游方法。

决定：Library 暴露稳定接口；调用方看不到 collection、chunk schema、向量、embedding 维度、派生缓存路径。hash 算法、分页策略等实现细节由 MFS 独占，不允许也不需要外部覆盖。

理由：每个 monkey-patch 都是接口缺失的证据，且会在 MFS 升级时静默失效。

验收：迁移后的应用侧代码中不应存在任何针对 MFS 内部的 patch。

### D-022：MFS 是多 Project 文件搜索 Repository

状态：**Superseded**（被 D-027 取代，2026-08-19）。曾取代 D-001、D-014。

背景：把 MFS 定义为“文件数据层并拥有写入路径”后，`managed/observed`、外部删除、scan 责任和原文副本位置持续互相矛盾。如果 MFS 既接管普通目录 CRUD，又保存中心副本和索引，它实际上已经成为 content database，却仍对外宣称是 filesystem。

考虑过：

1. MFS 只包装向量数据库；
2. MFS 接管所有普通目录文件 CRUD；
3. MFS 作为不 mount、不 POSIX 的文件搜索 Repository，集中管理账本、派生内容、索引和可选 Copy 对象；StashBase 管 Project、Source、扫描触发和产品体验。

决定：采用方案 3。MFS 不再承诺通用文件系统语义，也不接管 arbitrary external path 的写入；它提供 Entry ingest、处理、索引、grep、查询、状态与可选耐久内容。StashBase 是面向用户的多 Project File Search Engine。

理由：MFS Interface 仍隐藏 hash、去重、处理、索引、fallback 和一致性等深层复杂度，不退化成数据库透传；同时不把 Finder、编辑器、目录 CRUD 和产品触发策略强行拉进 MFS。

代价：外部目录直接变化只能最终一致；StashBase 继续负责枚举和触发 scan。原设计“写盘与索引一次调用同时成功”的保证只适用于 copied object 的内部写入，不适用于 linked 外部文件。

重新评估：只有产品决定所有内容必须通过 MFS 写入、外部目录不再是正常工作方式时，才重新讨论 managed content repository；届时应明确改名和迁移，而不是恢复模糊 mode。

### D-023：`state_root` 定义 Repository，Project 是逻辑隔离单元

状态：**Superseded**（被 D-027 取代，2026-08-19）。曾取代 D-003、细化 D-005。

背景：讨论“一张表一个 Project 还是多个 Project”时，发现物理部署、逻辑隔离和产品 Project 被混成一个概念。F-003、F-013 证明当前 StashBase 已是一份全局 Milvus store 服务多个目录。

决定：

* 一个 `state_root` 对应一个物理 Repository；
* 一个 Repository 可以包含多个 Project；
* Project 是查询、权限、生命周期和默认展示 scope；
* 一个物理 catalog/index 表默认保存多个 Project，所有键和过滤都带 `project_id`；
* 多个 Project 共用 `state_root` 是全局拓扑，各用独立 `state_root` 是 local/强隔离拓扑；
* StashBase 默认使用一个全局 Repository，不为每个 Project 创建 Engine/DB。

理由：共享 runtime、模型、worker、索引连接和内容缓存，保留跨 Project 显式查询能力；同时通过 Project namespace 阻止普通查询串库。物理隔离仍可通过独立 `state_root` 获得，不需要改变领域 Interface。

代价：全局 Repository 需要 single-writer 或 Engine 协调、Project 级资源公平和明确删除语义；跨 Project 内容去重不能泄漏访问权限。

重新评估：不同 OS 用户、安全域、加密密钥、地域或强故障隔离时，给相应 Project 使用独立 Repository。

### D-024：内容策略是 linked/copied，不是 managed/observed

状态：Accepted（2026-08-19），由 D-028、D-029 细化生命周期和查询语义。对应设计：§4、§8、§11。

背景：`managed/observed` 看起来像运行 mode，却同时暗含所有权、同步、删除传播和可用性，无法从字段名判断真实保证。保存 Canonical Text 已经产生派生副本；如果还要求原文件删除后 `read/grep` 不失败，就必须保存原始 bytes。

考虑过：

1. 所有 Entry 只保存外部 link；
2. 所有 Entry 都复制进中心 store；
3. 允许调用方明确选择 External link 或 Copy，并让查询结果暴露可用性差异。

决定：采用方案 3：

* `linked` 保存 `origin_uri`、观察到的 `content_hash`、完整 Canonical Text 和索引，不保存完整原始 bytes；
* `copied` 将原始 bytes 写入 Engine 的 content-addressed object store，并保留可选 `origin_uri` 作为 provenance；
* linked 外部删除由完整 scan 确认并 tombstone；
* copied 外部删除不传播，删除必须显式 `remove_copied`；
* 相同 bytes 跨 Entry/Project 可以物理去重，但 Entry identity 仍然独立；
* `logical_path` 在 Project 内唯一，Link/Copy 路径冲突必须显式解决，不能静默遮蔽或合并。

Linked Entry 不提供单条永久 `remove`：外部文件仍存在时，删掉账本只会在下次 sync 重新出现。单条排除通过 Source ignore 规则表达，整批移除使用 `detach_linked_source`。

理由：linked 保持普通目录低成本接入；copied 用明确的存储成本换稳定 read/grep。策略名称直接描述 MFS 保存了什么，不再暗示 MFS 接管外部目录。

代价：两种策略的 read/grep 可用性不同；copied 需要 object GC、容量管理和备份；linked fallback 只能恢复 Canonical Text，不能恢复完整二进制。D-018 拒绝的是“为多文件事务强制所有内容进入 CAS”，不禁止 Copy 专用的内容寻址去重。

重新评估：若实际使用中绝大多数 Entry 都通过 Copy 导入，可以考虑把 copied 设为默认；仍不删除 linked，除非不再支持外部工作目录。

### D-025：调用方枚举，MFS 用 scan generation 对账

状态：**Superseded**（被 D-028 取代，2026-08-19）。曾取代 D-015。

背景：StashBase 没有自己的 hash 账本；要求它从 MFS 拉全量 list/hash 再 diff 会复制状态和同步算法。另一方面，仅“无脑 add”无法识别删除，普通 add 也会把每轮扫描变成重复 Entry。

决定：StashBase/调用方决定触发点并枚举 Source；每轮调用 `begin_scan`，按稳定 `source_key` 对每个文件执行幂等 `scan.upsert`，最后完整 `commit`。MFS 计算并持有 content hash、去重、记录 `last_seen_generation`，并在 commit 时按 `missing=keep|tombstone` 处理未见 Entry。

约束：

* 扫描失败、权限错误、Source 未挂载或只完成部分枚举时必须 abort，不能删除；
* 严格变化判断依赖内容 hash，size/mtime 只能作性能 hint；
* linked 默认 tombstone missing；copied 默认 keep；
* watcher 只允许触发 scoped scan，不承担删除正确性。

理由：调用方 Interface 保持小且无状态；hash、去重、rename 配对和删除安全集中在 MFS 一处。修复一次即可惠及 StashBase、CLI 和未来调用方。

代价：未优化的完整 scan 会读取并 hash 全部内容；跨进程输入可能产生传输成本。后续可以增加客户端预计算 hash、可信 fingerprint、分块上传和 watcher，但不能改变 correctness path。

重新评估：文件规模证明全量 hash 不可接受时，先测量瓶颈，再选择增量优化；不能退回双份权威账本。

### D-026：搜索不依赖 linked 原文件，read/grep 必须 fallback + warning

状态：**Superseded**（被 D-029 取代，2026-08-19）。曾细化 D-011、D-020。

背景：如果索引只保存外部 link，文件在下一次 scan 前被删除或改写，搜索命中可能指向不存在的路径，live grep 也会失败。把这种情况称为“MFS 不一致”混淆了内部一致性和相对外部目录的新鲜度。

决定：keyword/semantic/hybrid 始终查询 Repository 内部索引，不在查询时依赖 linked origin。linked `read/grep` 优先访问 live origin；缺失、不可访问或已观察到变化时，回退最后一次成功索引的 Canonical Text，并返回 `last_indexed/source_missing/source_changed` 与 warning。copied `read/grep` 读取内部 object。

理由：MFS 可以保证索引、Canonical Text 与其当前 Entry revision 一致；外部随时可变时只能对新鲜度做最终一致保证。fallback 让搜索和精确文本能力可用，又不会谎称结果来自实时原文。

代价：linked 仍保存一份可搜索文本；对二进制格式，它不能替代完整原文件。结果消费者必须展示 warning。

重新评估：如果产品不需要 live grep，可以统一对当前 Entry revision 的 Canonical Text grep，进一步简化状态；如果要求完整原始字节 fallback，应 materialize 为 copied。

### D-027：一个 deployment 只有一个全局 MFS Engine

状态：Accepted（2026-08-19）。取代 D-022、D-023。对应设计：§1、§3、§5、§11、§12。

背景：将 `state_root` 建模为 Repository 并暴露 `open_repository(state_root)`，同时又声称 StashBase 使用一个全局 MFS，产生直接矛盾。F-003、F-013 已证明当前 StashBase 在启动时确定一个 app data root，由一个 sidecar 和一个 Milvus store 服务所有打开目录。

考虑过：

1. 每个调用方按需打开任意 Repository；
2. 一个 StashBase deployment 启动一个全局 Engine，`data_dir` 只在 Bootstrap 配置，Runtime Interface 只接受 Project scope；
3. 每个 Project 启动独立 Engine/data directory。

决定：采用方案 2。领域模型从 Project 开始，不包含 Repository。`data_dir` 是 Engine 私有部署配置；调用方连接 Engine，不能直接打开或共享该目录。独立测试或强隔离部署可以启动另一个 Engine，但不改变 Runtime Interface。

StashBase 迁移后的默认 `data_dir` 是 `<appData>/mfs/`；迁移前现状仍是 `<appData>/vector-store.nosync/milvus.db/`，两者必须在文档和迁移工具中明确区分。

V1 将一个 StashBase folder/library 映射为一个 Project；一个 Project 默认只有一个 primary Linked Source，并可包含 Copied Entry。多个 Project 共享 catalog、索引连接、worker 和内容缓存，所有可见性从显式 `project_id` 开始。

理由：删除 Repository 后，业务复杂度没有散落到调用方，说明它不是有价值的领域 Module；相反，`open_repository` 会把路径、锁、migration 和后端生命周期泄漏给所有客户端。全局 Engine 将这些复杂度留在 Bootstrap 内部。

代价：同一进程不能把多个任意 data directory 当作业务资源动态打开。需要强物理隔离时必须启动独立 Engine。

重新评估：只有出现真实的“一个宿主同时管理多个可热插拔 MFS 数据集”需求时，才重新设计多实例管理；不能仅为测试便利恢复 Repository 领域概念。

### D-028：StashBase 触发 sync，MFS 完成 scan；Copy 不参与 Source reconcile

状态：Accepted（2026-08-19）。取代 D-025，细化 D-024。对应设计：§2、§4、§5、§6。

背景：D-025 让 StashBase 枚举目录并编排 `begin_scan/upsert/commit`，把 ignore、symlink、稳定读取、删除确认和大量跨进程传输暴露到调用方。F-005、F-010 已确认当前实现恰好相反：StashBase 选择触发点，Python `scan_diff` 在内部 walk、hash 并与唯一账本比较。

决定：

* Linked Source 注册 root；StashBase 调用 `sync_source(project_id, source_id)`；
* MFS 内部完成枚举、fingerprint、hash、diff、rename 配对和完整扫描后的删除；
* rename 只在 filesystem identity 可确认或同 hash 配对唯一时保留原 `entry_id`，有歧义时按 delete + add；
* StashBase 不维护文件树/hash manifest，也不编排 scan session；
* Copy 是一次性 `copy_file`，写入 Stored Blob 后不再与 `origin_uri` 同步；
* Linked Source 删除只影响 Linked Entry，Source sync 永远不删除 Copied Entry；
* full sync 确认 missing 后 Linked Entry tombstone，默认查询立即排除；保留期内仅显式诊断/恢复查询可见；
* V1 不提供 linked→copied 的原地 `materialize`。

理由：MFS 已拥有 Source root、旧 hash 和处理规则，scan 放在内部形成更深的 Module；调用方只决定用户何时需要新鲜数据。Link 和 Copy 分开后，外部删除语义不再依赖一个混合 Source 的 `missing` flag。

代价：MFS 必须承担本地 filesystem scan 的正确性和性能；远端 Engine 不能直接读取客户端路径，Copy 必须上传 stream，未来远端 Source 需要真实 Adapter 后再扩展。

重新评估：只有出现 MFS 无法访问 Source、且调用方枚举是唯一可行方案的第二种部署时，才增加 manifest ingest Adapter；它不能替换本地 Source 的 `sync_source`。

### D-029：grep 查询 active Canonical Text，原始读取不做文本 fallback

状态：Accepted（2026-08-19）。取代 D-026，细化 D-011、D-020、D-024。对应设计：§7、§8。

背景：D-026 让 linked `read/grep` 先读 live origin，失败后回退 Canonical Text。它产生两个问题：`read` 有时返回原始 bytes、有时返回提取文本，类型不成立；grep 同时具有 live 和 snapshot 两套语义，结果无法稳定解释。

决定：

* `grep`、`read_text`、keyword、semantic 和 hybrid 都只查询 Entry 的 active Revision；
* 默认查询排除 tombstoned Entry；外部删除在 sync 前可能命中旧 active Revision，full sync tombstone 后隐藏；
* `grep` 精确匹配 active Canonical Text，不读取 live external file；
* `read_original` 单独读取原始 bytes：Copied Entry 读 Stored Blob，Linked Entry 读当前外部文件；
* Linked 原文件缺失、权限失败或 identity 变化时，`read_original` 返回 typed error，不用文本伪装 bytes；
* 从搜索结果读取原文件时可传 `expect_revision`；linked 当前 bytes 不匹配该 Revision 时返回 `SourceChanged`，不让旧 Evidence 指向新内容；
* 查询结果分别返回 active revision、Source observation、Readiness、最后检查时间和 warning；
* 操作系统意义的 Live grep 是另一个明确操作，不与 MFS snapshot grep 混用。

理由：MFS 是文件搜索核心，Canonical Text 才是统一可搜索数据。单一查询路径比“失败再 fallback”更深、更稳定，也让 PDF/OCR/transcript 与文本文件遵循同一语义。

代价：默认 grep 不保证命中外部磁盘上尚未 sync 的最新字节；需要实时结果时先 full sync 或使用产品侧显式 Live grep。

重新评估：如果以后保存完整原始 revision 并需要 byte-level regex，可以新增明确命名的查询模式，不能改变现有 grep 的 snapshot 语义。

### D-030：Observed Revision 与 Active Revision 分离

状态：Accepted（2026-08-19）。对应设计：§3、§7、§8、§11。

背景：原设计声称“索引、Canonical Text 与当前 Entry revision 一致”，但 schema 没有 Revision，也没有异步处理完成前后的发布点。新内容已经观察到而提取或 embedding 尚未完成时，查询语义未定义。

决定：Entry 保存 `observed_revision_id` 和 `active_revision_id`：

* 新内容创建 immutable observed Revision，状态为 pending；
* Processor、Canonical Text、Evidence 和必需 Index Projection 全部 ready 后，原子切换 active Revision；
* 查询、grep 和 `read_text` 只使用 active Revision；
* pending/failed 新 Revision 不污染旧 active Revision；
* 没有 active Revision 时，查询返回 pending/failed coverage，不返回部分结果；
* 旧任务完成时检查自己仍对应最新 observed Revision，不能覆盖更新内容。

理由：把“内部一致性”落实成可测试的不变量，同时允许 linked 外部内容比 active Revision 更新。后者是 freshness，不是数据裂开。

代价：多一层 Revision 账本、旧 revision 回收和原子 pointer publication；状态机比直接覆盖 Entry 行复杂。

重新评估：只有处理和索引全部变为同步、且文件规模证明不会造成不可接受延迟时，才可能合并 observed/active；当前格式处理和 embedding 明确是异步任务，因此不满足。

### D-031：先交付 Core + Engine vertical slice，再做 CLI 和产品迁移

状态：Accepted（2026-08-19）。取代 D-004。对应设计：§14。

背景：D-004 规定 Core → CLI → Engine，但 StashBase 是 TypeScript，真正需要验证的 seam 是长期 Engine 的跨进程 Runtime Interface。先做一次性 CLI 不能证明 single-writer、异步任务、事件和原子 Revision 发布成立。

决定：先实现无 RPC 依赖的 Core，再立即用最薄 Engine 暴露同一 Interface，完成 Linked Source sync + active Revision + grep 的 vertical slice。CLI 和 TypeScript client 都作为 Engine/Core 的 Adapter，随后实现；StashBase 最后按能力迁移。

理由：尽早验证最危险的跨语言、并发和生命周期边界，同时保持领域逻辑不依赖 RPC。CLI 仍然有价值，但不再阻塞 Engine。

代价：早期需要同时维护 Core contract 和最小协议；协议版本化必须从第一版考虑。

重新评估：如果 MFS 不再有跨语言长期调用方，可以省略 Engine Adapter，但不改变 Core Interface。

### D-032：Linked Revision 处理期使用短期 staging，不承诺原文留存

状态：Accepted（2026-08-19）。细化 D-024、D-028、D-030。对应设计：§6、§7、§11。

背景：scan 算出 linked 文件的 hash 后，异步 Processor 可能晚些才读取文件。如果外部文件在此期间变化或消失，只保存 hash 会产生“Revision 标识 A、Canonical Text 来自 B”，或者任务无法重试。把处理改成全同步又不适合 OCR、转录和 embedding。

考虑过：

1. Processor 之后直接重读 live 文件；
2. 所有 linked 原文件永久复制进 object store；
3. stable read 后为 pending Revision 保留短期 staging，发布或失效后回收。

决定：采用方案 3。MFS 校验读取前后 fingerprint，`content_hash` 和 Processor 必须消费同一份 bytes。异步处理可以持有 staging snapshot；ready、obsolete 或超出 TTL 后删除。staging 不提供 `read_original` fallback，也不纳入用户备份/耐久承诺。

理由：保证 Revision 内部一致性和可重试性，同时维持 Link 与 Copy 的核心区别：前者没有长期原文可用性，后者有。

代价：处理高峰会产生受控的临时磁盘占用，需要 TTL、启动清理、配额和磁盘不足错误。

重新评估：如果 Processor 全部能在同一次 stable stream 内同步完成，可以取消持久 staging；如果业务要求 linked 原文永久可回读，应显式 Copy，而不是延长 staging 生命周期。

## 4. Proposed：编码前确认

### P-001：Python 作为首版 Core 语言

理由：现有 MFS 和 StashBase 提取生态主要是 Python；PDF/OCR/embedding 库成熟；先实现无 RPC 依赖的 Core 最直接。

风险：StashBase 需要 Engine 才能跨语言调用；高性能扫描或 hash 未来可能需要 native 加速。

确认标准：完成阶段 0 原型后，证明 Python 能满足目标文件规模和延迟；否则只将热点下沉 Rust，不重写领域 Interface。

### P-002：SQLite + Milvus Lite 默认组合

职责划分已在 D-017 定死。待验证：keyword index 使用 SQLite FTS 还是向量库 BM25；同一 `data_dir` 的 single-writer lock；崩溃恢复与 migration。

注意 F-008：Milvus Lite 落盘是 Parquet + JSON manifest + WAL，不是 SQLite，两者不冲突。

### P-003：首批 Processor 范围

当前建议先做 Markdown/TXT/代码/JSON/YAML/CSV/HTML/PDF/DOCX；在 StashBase 迁移前补齐图片 OCR 和音视频转录。

待确认：是否先做最小 vertical slice（Markdown + PDF），再扩展格式，而不是同时实现完整列表。

### P-004：写入路径下移的迁移成本

状态：**Superseded**（D-014、D-022、D-025 均已被后续决策取代）。

现行迁移路径见 D-027、D-028、D-030 和设计 §14：不接管 linked 外部目录写入，先建立 Engine/Project/Entry/Revision 与 `sync_source`，再增加 copied object store。StashBase 现有文件保存路径不因 MFS 迁移而强制改造。

### P-005：转录能力的归属形态

建议用 `SubprocessProcessor` 契约（D-019），二进制构建与分发留在应用侧。

代价：转录能力在 MFS 内是空壳，未注册则不可用；MFS 无法独立提供音视频支持。

替代方案：MFS 自建三平台 whisper.cpp/FFmpeg 构建链——需要承接完整的交叉编译与 license gate，当前不值。

确认标准：确认应用愿意长期维护该构建链；否则重新评估。

## 5. 已明确拒绝或延期

| 方案                            | 结论                  | 原因                                             |
| ----------------------------- | ------------------- | ---------------------------------------------- |
| MFS 只包装 Milvus                | Rejected            | 扫描、处理、任务和一致性继续泄漏给调用方                           |
| MFS 包含完整 StashBase 产品         | Rejected            | 数据层与 UI/编辑器/Agent 产品耦合                         |
| 一目录一 Engine/DB                | Rejected as default | 重复 runtime，file lock，无法共享查询和批处理                |
| watch 替代 scan                 | Rejected            | 事件可能丢失，无法处理离线变化                                |
| V1 提供 watcher                  | Rejected            | 调用方只触发 sync；watcher 不能替代 MFS 的完整扫描（D-028） |
| semantic query 伪装成 POSIX grep | Rejected            | 无法表达 top-k、filter、ranking、freshness 和 Evidence |
| V1 可写 mount                   | Rejected            | 复杂度远超当前验证需求                                    |
| 直接基于 upstream main            | Rejected            | 产品方向和 Interface 持续变化                           |
| V1 OS 级共享 daemon / service   | Deferred            | StashBase sidecar Engine 足以；暂不承担系统级安装和多租户服务管理 |
| 多文件原子事务                       | Rejected            | 逐场景核对后无真实需求，代价远超收益（D-018）          |
| 强制所有内容进入 content-addressed blob store | Rejected | linked 必须保留外部普通目录；只有 copied 内容使用 CAS（D-024） |
| 元数据存在向量库里                    | Rejected            | 点查退化为全段扫描并踩到分页缺陷（F-008、D-017）      |
| 把 grep 留在应用侧                   | Rejected            | 派生文本归 MFS 后，应用无从知道该扫哪些文件（D-020）   |
| 缓存 key 含路径或行号                 | Rejected            | 插一行即导致全文重新 embedding（F-009、D-016）        |
| Runtime `open_repository(state_root)` | Rejected         | `data_dir` 只属于 Engine Bootstrap，不是业务资源（D-027） |
| 调用方枚举并编排 scan session          | Rejected         | 会泄漏同步算法并复制状态；本地 Source 由 MFS `sync_source`（D-028） |
| Copy 参与 Source reconcile             | Rejected         | Copy 是一次性 import，后续更新与删除都必须显式（D-028） |
| grep 先读 live、失败后文本 fallback     | Rejected         | 混合两套查询语义且会让原始 bytes 与文本类型混淆（D-029） |

## 6. 如何追加新决策

新决策使用以下格式，并同步修改 `design.md`：

```markdown
### D-XXX：标题

状态：Accepted / Proposed / Superseded / Rejected。
对应设计：§X。

背景：为什么需要决定。

考虑过：有哪些真实可行方案。

决定：选择什么。

理由：为什么。

代价：会失去什么、增加什么复杂度。

重新评估：什么条件变化后需要再讨论。
```
