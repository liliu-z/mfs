# MFS 当前决策与依据

> 本文只记录当前仍有效的事实、决策理由、代价和重新评估条件。实现语义以 [design.md](design.md) 为准。

## 1. 已核实事实

### F-001：StashBase 使用一个全局索引实例

StashBase 当前由一个 Node 进程管理一个 Python sidecar 和一份全局 Milvus Lite store。多个 Folder 共享该实例，通过路径范围过滤，不是每个 Folder 一套 database。

结论：MFS 一个 Instance 必须容纳多个逻辑 Namespace，同时保留跨 Namespace 搜索。

### F-002：External 变化由显式 reconcile 发现

StashBase 已删除 filesystem watcher。外部变化在启动、打开/切换目录、窗口 focus、Agent turn end、手动 Sync 和 MCP reindex 等事件点触发扫描。

结论：MFS 提供正确且可重复的 `sync`；何时调用由应用决定。

### F-003：格式处理必须属于 MFS 写路径

StashBase 当前分别处理 Markdown、HTML、PDF、图片 OCR、DOCX 和音视频转录，再把文本交给索引。若 MFS 只接收预制文本，调用方还必须同步 source hash、derived text、Chunk 和索引状态。

结论：MFS Processor 必须读取同一份 stable bytes并产生 Search Text/Source Map；应用只负责注册带外部模型或二进制的 Processor。

### F-004：Milvus Lite 不适合作为 catalog

Milvus Lite 的落盘主体是 Parquet segment、manifest 和 WAL。按 Document 点查、全量枚举和事务更新不是它的强项；现有实现把文件 metadata 放进 Milvus 后出现了分页漏行和重复处理问题。

结论：Namespace 与 Document snapshot 使用 SQLite；Milvus 只承担 Chunk search。

### F-005：第一阶段实际需要的格式

Core vertical slice 只需要 UTF-8 TXT/Markdown 与 PDF，即可覆盖原文处理、页/行 Source Map、Chunk、BM25/vector、External sync 和恢复路径。OCR、DOCX、音视频后续通过相同 Processor seam 接入。

### F-006：`zilliztech/mfs` v0.1.0 只证明搜索 happy path

`v0.1.0`（commit `7cd3c5b`，Apache-2.0）已经实现 Python Milvus wrapper、同 collection 的 BM25+dense/RRF、Embedder、PDF/DOCX 转换和 CLI。但它把 file hash、metadata、Chunk 与 vector 一起保存在 Milvus；scanner 算 hash 后 worker 会重新打开 live file；依赖只声明 `pymilvus>=2.5` 与 `milvus-lite>=2.5.1`。

结论：它适合作为局部代码 donor，不满足本文的 SQLite authority、stable bytes、Document publication 与 crash recovery 保证，也不能替代 exact-version conformance tests。

## 2. 当前决策

### D-001：Namespace 固定数据 authority

状态：Accepted。

决定：

* Internal Namespace 的原文由 MFS 保存，调用方以 opaque `doc_id` 执行 upsert/remove。
* External Namespace 绑定一个 directory root，Document ID 是 root-relative POSIX path，只允许 sync。
* Namespace 是 identity、authority 和 lifecycle 边界，不映射为 Milvus collection。

理由：一 root 一 Namespace 同时定义 membership、相对 identity、安全范围和完整 sync 的删除边界；Internal/External 不会在同一 ID 空间混合两种写入 authority。

代价：root 不能原地 rebind；Internal/External 之间迁移必须新建 Namespace 并显式复制。

重新评估：只有真实 connector 必须让一个 Namespace 原子管理多个不相关 root 时，才扩展 source model。

### D-002：公开读取只保留 query 与 search

状态：Accepted。

决定：

* `query` 负责确定性筛选、Document ID 点查与精确文本匹配。
* `search` 负责 BM25/vector/hybrid 排序。
* 二者共用 Filter 和 `doc_id/chunk/doc` projection。
* Namespace、Document ID、path 和 text match 都是 Filter；多个 Filter 默认 AND。

理由：读取、枚举、grep、path search 和 semantic search 的正交变化是“是否排序、如何筛选、返回什么”，不应各自形成顶层方法。

代价：Filter compiler 要协调 SQLite 文档筛选与 Milvus Chunk 搜索；TextMatch + ranked search 可能产生较大的 Document ID candidate set。

重新评估：只有出现无法表达为 Document predicate 的真实查询能力时，才新增顶层方法。

### D-003：SQLite 保存 Document，Milvus 保存 Chunk

状态：Accepted。

决定：

* SQLite 只有 Namespace 与 Document 两张 table；不需要查询或部分更新的字段整条存 JSON。
* Document JSON 保存 media type、content hash、snapshot ID、Processor 描述、完整 text、Source Map 和 source locator。
* Milvus 只有一个 Chunk collection，保存搜索所需 identity、ordinal、text/range 和 sparse/dense vector。
* SQLite 不保存 Chunk/vector；Milvus 不保存 Document metadata。
* Internal 原文使用 opaque object file，不做内容寻址或跨 Document 去重。

理由：SQLite 提供 Document 点查与事务，Milvus 提供 Chunk 排序。每项事实只有一个 authority；Milvus 中的 Chunk text/vector 是搜索结构，不再复制整个 Document record。

代价：Document JSON 整条更新；reindex 必须从完整 text 重新切 Chunk并可能重新 embedding。

重新评估：只有 benchmark 证明 Document JSON scan、全量 reindex 或重复 embedding 是主要瓶颈时，才分别增加专用 index、增量状态或 cache。

### D-004：用 dirty marker 处理跨库 crash

状态：Accepted。

决定：

* SQLite 是 committed Document snapshot。
* 每次 Milvus/SQLite publication 前持久化 `INDEX_DIRTY`，成功后删除。
* dirty marker 创建后的任何失败都不尝试读回旧 vector 或回滚 Milvus；Document query 继续读 SQLite，search 暂停，显式 reindex 从 SQLite 重建固定 collection。
* V1 reindex 可以阻塞，不维护 old/new 两张 collection。

理由：V1 没有历史读取、异步 publication 或无停机 rebuild 需求。一个 marker 加可重建 projection 足以区分“可服务搜索”和“必须重建”；放弃 rollback 同时移除了读取旧 dense vector 和大 Namespace 内存备份的脆弱假设。

代价：crash 后可能需要全量 reindex；大库恢复时间取决于 Chunk 和 embedding 数量。

重新评估：只有目标恢复时间无法通过全量 reindex 达成时，才引入增量 journal 或双 collection switch。

### D-005：V1 不做跨 Document cache

状态：Accepted。

决定：content hash 只判断同一 Document 是否 unchanged；不保存 text/map/chunk/profile hash，不复用跨 Document Processor output 或 embedding。

理由：cache 会引入 key、引用、失效、GC 和额外存储；目前没有数据证明重复计算已成为瓶颈。

代价：重复文件和重复 Chunk 会重复处理或 embedding。

重新评估：用真实库 benchmark，只有节省的调用成本显著高于 lookup/维护成本时才增加独立 cache。

### D-006：只有三个公开 adapter seam

状态：Accepted。

决定：Processor、Chunker、Embedder 是公开 adapter seam。Catalog、Milvus projection、Publisher 和 filesystem walker 都是 implementation，不预先抽象成公开 interface。

理由：前三者已有多个实现或由应用注入；后者在 V1 只有一个实现，公开只会增加调用方需要理解的表面积。

重新评估：出现第二个可工作的 catalog、vector backend 或 source implementation，并且替换不改变领域语义时再增加 seam。

### D-007：Python 3.12 greenfield Core

状态：Accepted。

决定：从空实现搭建同步 Python 3.12 Core Library，使用 `src/mfs`、uv lock、Hatchling、pytest、Ruff 和 Pyright；V1 支持 macOS/Linux，不做 CLI、RPC/sidecar、TypeScript client、StashBase 集成、Windows 或上游兼容。允许从 `zilliztech/mfs` v0.1.0 移植局部代码并保留适用 attribution。

理由：当前目标是先把领域 Interface、数据 authority 和恢复语义做正确；复用成熟叶子实现可以节省工作，但继承上游整体架构会把 CLI、queue 和 Milvus metadata 模型带回 Core。

代价：首版不能直接替换 StashBase 当前 sidecar，也不承诺读取上游 MFS state。

重新评估：Core Interface 与 crash tests 稳定后，再为真实调用方设计独立进程 adapter；Windows 在 lock/fsync/path 测试矩阵可落实后加入。

### D-008：Source Location 只属于 snapshot

状态：Accepted。

决定：Document 与 Chunk 返回由原始 content hash、media type 和 Processor 描述生成的 opaque `snapshot_id`；Source Map/Source Location 只解释该 snapshot 的 Search Text。External live file 在 sync 之间可能变化，V1 不提供 live-original read 或 freshness check。

理由：Core 可以对保存的 snapshot 作出完整保证，却不能同时保证外部 authority 没有被独立修改。显式 snapshot identity 防止调用方误把旧行号/页码解释为当前磁盘位置，又不提前增加尚无真实调用方的读取 seam。

重新评估：StashBase 或其他调用方确实需要从结果跳转 live original 时，增加以 snapshot expectation 为前置条件的读取能力。

### D-009：暂不声明产品规模与延迟范围

状态：Accepted。

决定：V1 不先设 Namespace/Document/单文件数量或 P95 SLA；保留 Interface 自身的 ID、Chunk、query/search limit，并要求发布 benchmark 记录实际表现，不宣称无限规模。

理由：当前没有代表性 corpus 与产品数据，先写数字会制造虚假保证；正确性测试与内存友好的实现仍然可以开始。

重新评估：取得真实 StashBase corpus benchmark 后，把可验证的支持范围和恢复时间写入规范。

## 3. 实现门槛

领域实现可以开始。进入 Milvus implementation 前必须：

1. 用 Python 3.12 选择 exact `pymilvus`/`milvus-lite` 组合，执行 design §15 的 BM25+dense、filter escaping、multi-segment 全量枚举、delete/flush/reopen 与 empty-index conformance tests。
2. 在 `pyproject.toml` 与 `uv.lock` exact lock PDF、regex、RFC 8785、BLAKE3、Milvus 和 file-lock 依赖，并验证 macOS/Linux 安装。
3. 建立 marker/Milvus/SQLite/fsync failure injection，以及 macOS/Linux 的 lock、file identity、path case 测试矩阵。
4. 首次发布前保存 benchmark 环境与结果；它是后续支持范围的依据，不是当前编码前置条件。
