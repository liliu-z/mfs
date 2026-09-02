# MFS V1 实现规范

> 状态：Core Implementation Specification，2026-09-02。本文是 MFS V1 的唯一规范；实现可以开始，后端验证门槛见 §15。

## 1. 产品与保证

MFS 是由 `mfs_path` 打开的进程内文件搜索 Library。它接收或观察文件，将其处理成 Document 与 Chunk，并提供：

* `query`：确定性筛选、点查和精确文本匹配；

* `search`：BM25、vector 或 hybrid 相关性搜索；

* `sync`：让 External Namespace 的 snapshot 与目录一致。

一个 Instance 可以有多个 Namespace：

| kind       | 原文 authority                       | `doc_id`                            | mutation          |
| ---------- | ---------------------------------- | ----------------------------------- | ----------------- |
| `internal` | MFS                                | caller 提供的 opaque ID                | `upsert`、`remove` |
| `external` | Namespace 绑定的 filesystem directory | root-relative normalized POSIX path | `sync`            |

Namespace 是 Document identity、数据 authority 和生命周期边界，不是 ACL、产品 Project、SQLite database 或 Milvus collection。

所有读取和搜索都针对最近一次成功写入的 MFS snapshot；不会隐式读取 External live file，也不会隐式执行 `sync`。

V1 是 single-process、single-writer Library。mutation 同步执行；`sync` 允许逐 Document 部分成功，`drop_namespace` 作为一个受锁保护的删除提交。不提供 caller-controlled 多 Document transaction、历史版本、daemon、watcher 或后台 Job。

V1 是 Python 3.12 的 greenfield Core Library，不兼容现有 `zilliztech/mfs` Interface 或落盘格式。实现可以在遵守 Apache-2.0 attribution 的前提下复用 `zilliztech/mfs` `v0.1.0`（commit `7cd3c5b`）中的局部代码，但不得继承其“Milvus 同时保存 Document metadata 与 Chunk”的存储模型。V1 只支持 POSIX：macOS 与 Linux；Windows 延后。

## 2. 公开 Interface

```text
MFS.open(
    mfs_path: Path,
    processors: Sequence[Processor] = (),
    chunker: Chunker | None = None,
    embedder: Embedder | None = None,
    sync_policy: SyncPolicy | None = None,
) -> MFS

mfs.create_namespace(
    namespace: str,
    kind: Literal["internal", "external"],
    root: Path | None = None,
) -> NamespaceInfo
mfs.get_namespace(namespace: str) -> NamespaceInfo
mfs.list_namespaces() -> tuple[NamespaceInfo, ...]
mfs.drop_namespace(namespace: str) -> DropReport

mfs.upsert(
    namespace: str,
    doc_id: str,
    data: Path | bytes,
    media_type: str | None = None,
) -> MutationReport
mfs.remove(namespace: str, doc_id: str) -> MutationReport
mfs.sync(namespace: str, path: str = ".") -> SyncReport

mfs.query(
    filters: Sequence[Filter] = (),
    select: Literal["doc_id", "chunk", "doc"] = "doc_id",
    limit: int | None = None,
) -> QueryResult

mfs.search(
    text: str,
    filters: Sequence[Filter] = (),
    mode: Literal["bm25", "vector", "hybrid"] = "hybrid",
    select: Literal["doc_id", "chunk", "doc"] = "chunk",
    limit: int = 10,
) -> SearchResult

mfs.status() -> Status
mfs.reindex() -> ReindexReport
mfs.close() -> None
```

`processors=()` 表示没有 Processor：已存在 snapshot 仍可查询，新文件均无法处理。调用方显式注册需要的 Processor。`chunker=None` 使用 §6.3 的 `DefaultChunker`。`sync_policy=None` 使用 §7.1 的默认值。

open 时复制并冻结 registry/config 描述，之后不能热更新。同一个 MFS object 可以并发执行多个 query/search；mutation 串行。MFS 不要求 adapter 自身 thread-safe：Processor/Chunker 只在 mutation mutex 内调用，所有 Embedder 方法再经过一个内部 embedder mutex；锁只包围单次 adapter call。`close()` 等待正在执行的调用结束，重复调用是 no-op；close 后其他调用返回 `Closed`。

## 3. 类型与返回值

```text
DocumentId = { namespace: str, doc_id: str }
SnapshotId = str
SourceSpan = { text_start: int, text_end: int, source: JSON }
SourceMap = { version: 1, spans: tuple[SourceSpan, ...] }
SourceLocation = { version: 1, sources: tuple[JSON, ...] }

Document = {
  id: DocumentId,
  snapshot_id: SnapshotId,
  media_type: str,
  text: str,
  source_map: SourceMap,
  original: bytes | None
}

Chunk = {
  document_id: DocumentId,
  snapshot_id: SnapshotId,
  ordinal: int,
  text: str,
  text_start: int,
  text_end: int,
  source_location: SourceLocation
}

Match = {
  text_start: int,
  text_end: int,
  source_location: SourceLocation
}

```

`Document.text` 是 Processor 生成的完整可搜索文本。`Document.original` 对 Internal Document 返回 MFS 保存的 bytes；External 原文不归 MFS 保存，因此为 `None`。调用方只有显式选择 `select="doc"` 时才承担完整文本和 Internal 原文的读取成本。

`SnapshotId` 是 64 字符 lowercase BLAKE3 hex，标识一次已处理 Document snapshot，计算规则见 §6.1。`text_start/text_end` 均为 `Document.text` UTF-8 编码后的半开 byte range。`source_location.sources` 依 Source Map span 顺序收集与该 range 相交的 `source`，按 JSON structural equality 去重；没有相交 span 时为空 tuple。

```text
NamespaceInfo = { namespace, kind, root? }

MutationReport = {
  id: DocumentId,
  outcome: added | updated | unchanged | removed | not_found,
  index_ready: bool
}

DropReport = { namespace, dropped: bool, index_ready: bool }

SyncReport = {
  namespace,
  path,
  complete: bool,
  changed: tuple[DocumentId, ...],
  removed: tuple[DocumentId, ...],
  failed: tuple[SyncFailure, ...],
  skipped: tuple[SyncSkipped, ...],
  index_ready: bool
}

Status = {
  namespace_count: int,
  document_count: int,
  index_state: ready | dirty | mismatch,
  dense_enabled: bool,
  dense_available: bool
}

ReindexReport = { documents: int, chunks: int, dense_enabled: bool }

SyncFailure = { path: str, code: str, message: str }
SyncSkipReason = excluded | too_large | symlink | special_file | unsupported_media_type
SyncSkipped = { path: str, reason: SyncSkipReason }
```

所有 Document 集合按 `(namespace, doc_id)` 的 UTF-8 bytes ascending 排序；Chunk 再按 ordinal 排序。Namespace 列表按 namespace 排序；report 中的 ID 不重复。`failed/skipped` 按 path 的 UTF-8 bytes、再按 code/reason 排序，同一 path/reason 或 path/code 只出现一次。

公开 record、Filter、report 与 error context 使用 frozen slotted dataclass；公开集合使用 tuple。输入与输出中的 JSON 由 MFS deep-copy，调用方 mutation 不得改变已提交 snapshot 或其他结果对象。本文的 `{...}` 是这些 Python value type 的字段表示，不表示返回裸 dict。

## 4. Identity、路径与 lifecycle

不同 `DocumentId` 永远是不同 Document，即使原文 bytes 相同。Chunk 没有跨 Document identity 或独立 lifecycle。

### 4.1 Instance

* `mfs_path` 在加锁前执行 absolute resolve。

* 不存在或为空的 directory 会初始化；非空且没有可识别 V1 layout 的 directory 返回 `CorruptState`。

* Instance 持有 `mfs_path/LOCK` 的 OS exclusive lock；第二个进程打开相同路径返回 `InstanceLocked`。

* SQLite `user_version > 1` 返回 `SchemaVersionUnsupported`。V1 不实现旧 schema migration。

* `mfs_path` 与任何 External root 不得相同或互相包含。

### 4.2 Namespace

* `namespace` 非空、不含 NUL、区分大小写，UTF-8 不超过 255 bytes。

* Internal Namespace 必须省略 root；External Namespace 必须提供可读 directory。

* External root 创建时执行 `resolve(strict=True)` 后持久化。

* kind 与 root 创建后不可修改。

* 相同 ID、kind、root 的重复 create 是幂等；其他同 ID create 返回 `NamespaceConflict`。

* 不同 External Namespace 可以绑定相同或重叠 root；它们独立生成 Document 和搜索结果。

* `drop_namespace` 不存在时返回 `dropped=false`。删除 External Namespace 绝不修改 root 中的文件。

### 4.3 Document ID

* Internal `doc_id` 非空、不含 NUL，UTF-8 不超过 2048 bytes；`/` 没有目录语义。

* External `doc_id` 是至少一个非空 segment 的 relative POSIX path。

* External path 拒绝 absolute path、NUL、反斜杠、空 segment、`.`、`..` 和 root escape。

* `"."` 只允许表示 sync root，不是 Document ID。

* External `doc_id` 使用 directory entry 的实际拼写；root containment 使用平台正确的 canonical path 比较。

`query(ByDocumentId(...))` 找不到 Document 时返回空结果，不抛 `DocumentNotFound`。`remove` 找不到 Internal Document 时返回 `not_found`。

## 5. Query model

### 5.1 Filter

`Filter` 是 sealed tagged interface。V1 内置：

```text
Filter = ByNamespace | ByDocumentId | UnderPath | TextMatch

ByNamespace(namespaces: str | Sequence[str])
ByDocumentId(ids: DocumentId | Sequence[DocumentId])
UnderPath(namespace: str, path: str = ".")
TextMatch(
    pattern: str,
    regex: bool = False,
    case_sensitive: bool = False,
)
```

* 不同 Filter 按 AND 联合；同一个 Filter 的多个值按 OR 联合。

* 空的 Namespace/Document ID 序列是 `InvalidFilter`；重复值去重。

* Filter 引用不存在的 Namespace 是 `NamespaceNotFound`。

* `UnderPath` 只接受 External Namespace。`"."` 匹配整个 Namespace；其他 path 匹配 exact ID 或以 `path + "/"` 开头的 descendants。

* `TextMatch.pattern` 必须非空，UTF-8 不超过 16 KiB。`regex=true` 使用 RE2-compatible syntax；不支持 backreference、lookaround 或能匹配空字符串的 pattern。

* `case_sensitive=false` 使用 regex engine 的 Unicode case-insensitive 语义。

* TextMatch 对完整 `Document.text` 求值，不用 BM25/vector 预筛；match offset 始终映射回原字符串的 UTF-8 byte range。

* 多个 TextMatch 必须在同一 Document 中分别命中。返回的 `matches` 是全部匹配 byte range 的集合并集：重叠或首尾相接的 range 合并为一个半开 range，再按 start/end 排序；MFS 不保留 match 属于哪个 Filter 的信息。

新 Filter 可以增加新的 tag，但 V1 不执行调用方提供的 Filter 代码。

`filters=()` 表示全部 Namespace/Document。MFS 不实现 ACL；调用方必须在构造 Filter 前完成 authorization。

### 5.2 Projection

```text
QueryItem[T] = { value: T, matches: tuple[Match, ...] }
SearchItem[T] = { value: T, score: float, matches: tuple[Match, ...] }
QueryResult[T] = { items: tuple[QueryItem[T], ...], truncated: bool }
SearchResult[T] = { items: tuple[SearchItem[T], ...], truncated: bool }
```

`select` 决定 T：

| select   | value        | 行为                                                                                                            |
| -------- | ------------ | ------------------------------------------------------------------------------------------------------------- |
| `doc_id` | `DocumentId` | 每个匹配 Document 一项                                                                                              |
| `doc`    | `Document`   | 每个匹配 Document 一项                                                                                              |
| `chunk`  | `Chunk`      | query 无 TextMatch 时返回匹配 Document 的全部 Chunk，有 TextMatch 时只返回与任一 match 相交者；search 返回匹配 Document 中的 ranked Chunk |

`query` 不计算 score，按 §3 排序。`search` 的 `doc_id/doc` 按 Document 聚合，score 取其最高 Chunk score；`chunk` 不聚合。

对于 `query(select="chunk")`，每个 item 只携带与该 Chunk 相交的 matches。对于 search，TextMatch 是 Document filter；返回 Chunk 不要求自身包含 exact match，item 携带所属 Document 的全部 matches。

`query.limit=None` 返回全部最终投影项；非空 limit 必须为 `1..100000`。`search.limit` 必须为 `1..1000`。limit 作用于最终投影，不是初始 Chunk candidate。query 在完整有序结果中仍有下一项时 `truncated=true`；search 因 final limit 或 candidate cap 未返回所有可能结果时 `truncated=true`。

`search.text` 必须非空，UTF-8 不超过 64 KiB；否则返回 `InvalidQuery`。

## 6. Processing

### 6.1 Processor

```text
Processor = {
  id: str,
  version: str,
  options: JSON,
  media_types: tuple[str, ...],
  suffix_media_types: Mapping[str, str],
  sniff(head: bytes) -> str | None,
  process(staged_path: Path, media_type: str) -> ProcessedDocument
}

ProcessedDocument = { text: str, source_map: SourceMap }
```

ID/version 非空；options 必须是无 NaN/Infinity 的 JSON。`media_types` 使用无参数、lowercase `type/subtype`；suffix key 必须是以 `.` 开头的 lowercase 单 suffix，value 必须属于同一 Processor 的 `media_types`。MFS 使用 `Path.suffix` 语义，不做 `.tar.gz` 最长后缀匹配。相同 media type 或 suffix 只能由一个 Processor 注册，否则 `InvalidConfiguration`。

本文中的 JSON structural equality 以 RFC 8785 JSON Canonicalization Scheme 的 UTF-8 bytes 相等为准；object key 顺序不影响结果，array 顺序影响结果。MFS 在持久化或调用 adapter 前复制并校验 JSON，调用方之后修改原对象不得改变已提交配置。

Media type 按以下顺序解析：

1. Internal upsert 的显式 `media_type`；
2. Internal `doc_id` 或 External `doc_id` 的小写 suffix；
3. Internal `data: Path` 的小写 suffix；
4. 每个 Processor 对 staging 前 64 KiB 执行 `sniff`。

显式 media type 去掉参数并将 type/subtype 小写；显式值没有对应 Processor 时不回退 suffix/sniff。sniff 返回值必须属于该 Processor 的 `media_types`；多个 Processor sniff 成功是 `InvalidConfiguration`。没有匹配时 Internal upsert 返回 `UnsupportedMediaType`，External sync 记录 `skipped`并保留已有 snapshot。

同一 Document 的原始 bytes BLAKE3 lowercase hex 为 `content_hash`。只有 content hash、resolved media type 和完整 Processor 描述 `{id, version, options}` 做 JSON structural equality 都相等才返回 unchanged。该 hash 不作为 identity、object path 或跨 Document cache key。

`snapshot_id = BLAKE3(JCS({"content_hash": ..., "media_type": ..., "processor": ...}))`。它标识原始 bytes 与处理语义的组合，不是 Document identity。External 文件在下一次成功 sync 前可能已经变化；`source_map` 与 `source_location` 只描述该 `snapshot_id` 对应的 `Document.text`，不保证仍对应 live file。V1 不提供 External 原文读取或 freshness check。

Processor 改变不是全局 index mismatch。External Document 在下一次 sync 时重处理；Internal Document 由调用方再次 upsert。MFS 已通过 `query(select="doc")` 返回 Internal original，V1 不另设批量 reprocess 操作。

### 6.2 Source Map

```json
{
  "version": 1,
  "spans": [
    {
      "text_start": 0,
      "text_end": 120,
      "source": {"kind": "pages", "start": 1, "end": 1}
    }
  ]
}
```

span 使用 Search Text UTF-8 byte range，必须位于 text 内、单调且不重叠；允许 gap 和空 spans。`source.kind` 可为 `lines`、`pages`、`time`、`region` 或 `opaque`。MFS 不解释 kind-specific payload，只返回与查询 range 相交的 source 值。

ProcessedDocument text 必须能严格编码为 UTF-8，不允许 lone surrogate；Source Map 和 options 必须是有效 JSON。

首批 package Processor：

* `Utf8TextProcessor`：`.txt` → `text/plain`，`.md` → `text/markdown`；strict UTF-8，允许并移除开头 BOM，其余文本不改写；每个 logical line 输出一个 span，包含其原有 line terminator（若有），source 为 1-based inclusive `{"kind":"lines","start":n,"end":n}`。空文件、仅 BOM 文件产生空 text/spans。

* `PdfProcessor`：`.pdf` 或 `%PDF-`；使用 pinned `pymupdf4llm` 按页提取 Markdown，以一个 `\n` 连接页面；每页提取文本产生一个 span，source 为 1-based inclusive `{"kind":"pages","start":n,"end":n}`，页间连接符是允许的 Source Map gap。空页没有 span。依赖升级必须改变 Processor version。

### 6.3 Chunker

```text
Chunker = {
  id: str,
  version: str,
  options: JSON,
  chunk(text: str, source_map: SourceMap) -> Sequence[ChunkRange]
}
ChunkRange = { text_start: int, text_end: int }
```

非空 text 的 ChunkRange 必须从 byte 0 覆盖到 text 末尾，不留 gap；每个 range 位于 text 内且非空，start 严格递增，允许 overlap。单个 Chunk text UTF-8 不得超过 65535 bytes。MFS 从原 text 切出 Chunk text并计算 source location。空 text 必须产生零个 Chunk。

`DefaultChunker` 的描述固定为：

```json
{"id":"utf8-window","version":"1","options":{"max_bytes":4096,"overlap_bytes":512}}
```

算法：

1. 每个窗口最大 4096 UTF-8 bytes。
2. 非末尾窗口优先在窗口后 1024 bytes 内选择最后一个 `\n\n`、`\n`、Unicode whitespace，并把 separator 包含在当前窗口末尾；否则退到不拆 UTF-8 code point 的最大边界。
3. 下一窗口从 `end - 512` 后第一个 code-point boundary 开始。
4. 输出 text 是原字符串对应 byte range 的精确切片，不添加 path/title 等隐式 prefix。

### 6.4 Embedder

```text
Embedder = {
  embedding_space: str,
  dimension: int,
  embed_documents(texts: Sequence[str]) -> Sequence[Vector],
  embed_query(text: str) -> Vector
}
```

`embedding_space` 必须随 provider/model/revision、document/query encoding 或 normalization 的变化而变化。MFS 校验数量、dimension 和所有值 finite。V1 不做 embedding cache；一个变化的 Document 会重新 embed 全部 Chunk。

MFS 按原 Chunk 顺序以 `1..128` 个 text 的非空 batch 调用 `embed_documents` 并拼接结果，从不调用空 batch；adapter 可以在内部进一步分批、重试或限流，但一次调用要么返回完整有序结果，要么抛错。`embed_query` 每个 search 调用一次。MFS 本身不重试外部 Embedder，以免把非幂等计费或长期故障隐藏在同步调用内。

### 6.5 Internal mutation

Internal `upsert` 的完整流程：

1. `bytes` 写入 staging；`Path` 必须是可读 regular file且不是 symlink，否则返回 `SourceUnavailable`。MFS 单次流式复制到 staging，并校验读取前后 descriptor/path 的 file identity、size、mtime。变化时重试一次，仍变化返回 `SourceChanged`。
2. 计算 content hash并解析 Processor；命中 unchanged 时删除 staging并返回。
3. Processor 生成完整 text/source map，Chunker 生成全部 ranges；空 text 合法并产生零 Chunk。
4. active index 为 dense 时调用 Embedder；未提供匹配 Embedder 返回 `CapabilityUnavailable`。
5. 保存新的 Internal object并执行 §9 publication。

`remove` 删除 exact Document；不存在返回 `not_found`。upsert/remove 均不影响同 Namespace 的其他 Document。

## 7. External sync

### 7.1 Policy

```text
SyncPolicy = {
  exclude_globs: tuple[str, ...] = (),
  max_file_bytes: int | None = None
}
```

glob 匹配 root-relative POSIX path；`* ? []` 不跨 `/`，`**` 可跨 segment。规则同时作用于 file 和 directory；命中 directory 时整个 subtree 是明确 non-member。V1 只处理 regular file，不跟随 file/directory symlink，不进入 socket、device 或 FIFO。

被 exclude、超过大小、symlink 或 special file 的 path 是明确 non-member；新 path 进入 `skipped`，已有 Document 在 observation 完整时删除。unsupported media 或本次未注册 Processor 只表示当前无法处理：进入 `skipped`，已有 snapshot 必须保留。

`SyncPolicy` 是 Instance 全局 immutable 配置，所有 External Namespace 共享；需要不同规则时应打开另一个 Instance。`max_file_bytes=None` 表示 MFS 不额外设置产品限制，不表示底层存储、内存或 Processor 没有物理上限。

### 7.2 Stable read 与 unchanged

`sync.path` 使用 §4.3 的 External path 语法，另允许 `"."` 表示 root；拒绝 absolute、反斜杠、空 segment、`.` segment、`..`、NUL 与 root escape。MFS 以 `lstat/scandir` 观察每个 segment，不通过 symlink 解析 containment；在大小写不敏感 filesystem 上，生成的 Document ID 使用 directory entry 返回的实际拼写。

`sync` 的 path 指向 exact regular file 时总是 stable-read 并核对 content hash。目录 walk 可以先比较保存的 `size/mtime_ns`，但只有 resolved media type 和当前注册 Processor 的描述也与 Document JSON 相同时才视为 unchanged；Processor 改变必须重新处理。stat 不同、没有记录或必须重处理时：

1. 打开文件并记录 file identity、size、mtime；
2. 单次流式读取到 staging，同时计算 content hash；
3. 比较打开的 descriptor 与读取后的 path 状态；
4. 不一致时重试一次，仍变化则记录 `SourceChanged`。

Processor 只读取 staging，不重新打开 source。content hash、text 和 Chunk 因而来自同一份 bytes。

目录 walk 的性能契约把相同 `size/mtime_ns` 视为内容未变；若外部工具可能保留这两个值，调用方必须对 exact file 调用 `sync`，该路径会强制核对 hash。

### 7.3 Reconcile

| observation                            | action                          |
| -------------------------------------- | ------------------------------- |
| regular file                           | 对齐 exact Document               |
| directory                              | 完整递归 walk 后 reconcile subtree   |
| 明确 ENOENT，且 root/最近存在 parent 可读        | 删除 exact Document 与 descendants |
| 从未存在的 ENOENT                           | no-op                           |
| root/parent 不可读、walk 中断、entry type 不确定 | `complete=false`，不根据未见 path 删除  |

walk 在处理前将 eligible file 加入 `seen`；处理失败的已见文件保留旧 snapshot。missing deletion 只在完整 observation 的最后执行。一次 sync 已成功发布的其他 Document 不因后续失败回滚。

明确观察到 excluded directory 或 directory symlink 时，`skipped` 只记录该 directory 一次；若本次 requested subtree 的其余 observation 完整，则删除它下面已有的 Document。walk 中出现任何无法观察的 entry/type/descendant 时，本次 requested subtree 的 missing deletion 整体取消；已经明确观察为 non-member 的 path 仍可删除。

rename 表现为 delete + add。调用方若已知 rename，应 sync 两端的最近共同 parent。

`complete` 只表示 filesystem observation 是否足以安全执行 missing deletion；单文件 Processing failure 不会令完整 walk 变成 incomplete。若某次 publication 使 index 进入 dirty，sync 立即停止、返回 `complete=false/index_ready=false`，并且不执行本轮 missing deletion。

root 自身缺失或不可访问时必须返回 `complete=false` 并保留整个 Namespace。file → directory 在完整 walk 后删除旧 exact Document；directory → file 在新 file 成功发布后删除旧 descendants。

## 8. 持久化

```text
mfs_path/
  catalog.sqlite
  catalog.sqlite-wal
  catalog.sqlite-shm
  index.json
  INDEX_DIRTY
  LOCK
  objects/<opaque-id>
  staging/<operation-id>/
  milvus.db
```

`objects/` 只保存 Internal 原文，使用随机 UUID 文件名，不做内容寻址或去重。`staging/` 和无 Document 引用的 object 可以删除。

### 8.1 SQLite

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA user_version = 1;

CREATE TABLE namespaces (
    namespace TEXT PRIMARY KEY COLLATE BINARY,
    value     TEXT NOT NULL CHECK (json_valid(value))
);

CREATE TABLE documents (
    namespace TEXT NOT NULL,
    doc_id    TEXT NOT NULL COLLATE BINARY,
    value     TEXT NOT NULL CHECK (json_valid(value)),
    PRIMARY KEY (namespace, doc_id),
    FOREIGN KEY (namespace) REFERENCES namespaces(namespace) ON DELETE CASCADE
);
```

Namespace JSON：

```json
{"version":1,"kind":"external","root":"/absolute/folder"}
```

Document JSON：

```json
{
  "version": 1,
  "media_type": "application/pdf",
  "content_hash": "...",
  "snapshot_id": "...",
  "processor": {"id":"pdf","version":"1","options":{}},
  "text": "...",
  "source_map": {"version":1,"spans":[]},
  "source": {"size":1234,"mtime_ns":123456,"object":null}
}
```

Internal `source.object` 是 `objects/` 下的 relative path，`mtime_ns` 为 null；External `object` 为 null。所有相对 object path 必须保持在 `objects/` 内。

Namespace/Document value 总是整条读取和替换；V1 不对 JSON 内容做 SQL 查询或部分更新。

### 8.2 Index config

`index.json` 是单个原子替换的 JSON：

```json
{
  "version": 1,
  "chunker": {"id":"utf8-window","version":"1","options":{"max_bytes":4096,"overlap_bytes":512}},
  "bm25": {
    "analyzer": {"tokenizer":"standard","filter":["lowercase"]},
    "k1": 1.2,
    "b": 0.75
  },
  "dense": {
    "embedding_space": "provider/model/revision",
    "dimension": 1536,
    "metric": "COSINE"
  }
}
```

BM25-only index 的 `dense` 为 null。文件只记录 active index 的完整语义配置，不存 hash。

新 Instance 按当前 Chunker 和 Embedder 创建配置。打开已有 Instance 时：

* 当前 Chunker 描述不同，index state 为 `mismatch`；

* 提供 Embedder 且 space/dimension 与 active dense 不同，或 active 为 BM25-only，state 为 `mismatch`；

* 未提供 Embedder 不会把已有 dense index 改成 BM25-only，但 `dense_available=false`；

* `index.json` 缺失/无法解析、collection 缺失/schema 不匹配或 `INDEX_DIRTY` 存在，state 为 `dirty`；

* `index.json.version > 1` 返回 `SchemaVersionUnsupported`。

dirty 条件优先于 mismatch；只有落盘 index 可完整识别且没有 dirty 条件时才报告 mismatch。`dense_enabled` 描述 `index.json` 中 active index 是否含 dense；无法识别 index config 时为 false。`dense_available` 仅在 dense enabled、当前提供匹配 Embedder 且 state 不是 mismatch 时为 true；dirty 不改变 adapter 是否匹配，但仍禁止 search。

dirty/mismatch 时，`query(select="doc_id"|"doc")` 和 Namespace metadata 操作可用；search、Chunk query 和 Document mutation 返回 `IndexUnavailable`。`reindex()` 是唯一修复操作。

这里的 Namespace metadata 操作是 create/get/list；drop 空 Namespace 可用，drop 非空 Namespace 需要 ready index。active dense index 下，只有实际插入新 Chunk 的 Document 才要求匹配 Embedder：unchanged、remove 和 drop 不需要；sync 中无法 embedding 的 changed file 进入 `failed` 并保留旧 snapshot。

### 8.3 Milvus

Instance 只有一个固定名为 `chunks` 的 collection，dynamic fields 关闭。

| field           | type                  | use                              |
| --------------- | --------------------- | -------------------------------- |
| `id`            | auto `INT64` primary  | row identity                     |
| `namespace`     | `VARCHAR(255)`        | Namespace filter                 |
| `doc_id`        | `VARCHAR(2048)`       | point/path filter 与按 Document 删除 |
| `ordinal`       | `INT64`               | Chunk order                      |
| `text`          | `VARCHAR(65535)`      | result 与 BM25 input              |
| `text_start`    | `INT64`               | Document UTF-8 byte range        |
| `text_end`      | `INT64`               | Document UTF-8 byte range        |
| `sparse_vector` | `SPARSE_FLOAT_VECTOR` | BM25 function output             |
| `dense_vector`  | `FLOAT_VECTOR(dim)`   | dense index 时存在                  |

`text` 启用 §8.2 的 analyzer；BM25 function 从 text 生成 sparse vector。sparse index 使用 `SPARSE_INVERTED_INDEX / BM25 / DAAT_MAXSCORE` 和配置中的 k1/b；dense index 使用 `AUTOINDEX / COSINE`。`namespace`、`doc_id` 建 scalar index。

Milvus 只保存搜索所需 Chunk。SQLite 不保存 Chunk/vector；Milvus 不保存 Document content hash、Processor、media type、Source Map、原文引用或 publication state。Namespace/Document ID 是搜索过滤与结果回查所必需的 locator，不是第二份 authority。

## 9. Publication、recovery 与 reindex

MFS 持有一个 mutation mutex 和一个 RW lock。`create_namespace/drop_namespace/upsert/remove/sync/reindex` 从调用开始到返回全程持 mutation mutex；query/search 全程持 read lock；SQLite metadata mutation 与 Milvus/SQLite publication 持 write lock。耗时的读取、Processing、Chunk 和 embedding 在 mutation mutex 内但在 write lock 外完成，因此 mutation 串行而现有 snapshot 仍可并发读取。`close()` 先禁止新调用，再等待已进入的调用结束。

### 9.1 Single Document replace

1. 在 write lock 外准备 staging、Document JSON、完整 Chunk 和 vectors。
2. 将 Internal staging 原文原子移动到新的 object path并 fsync；此时尚未被引用。
3. 取得 write lock；原子创建并 fsync `INDEX_DIRTY` 及父目录。
4. 删除该 Document 的旧 Milvus rows、插入完整新 rows、flush，并校验 ordinal/row count。
5. 在单个 SQLite transaction 中 insert/replace Document。
6. 删除 `INDEX_DIRTY` 并 fsync 父目录，释放 write lock。
7. best-effort 删除旧 object 和其他 orphan；失败不影响已提交结果。

SQLite commit 是 commit point。创建 marker 前失败不改变 committed snapshot；新 object 可能成为 orphan。marker 创建后、commit 前发生任何错误时，SQLite 保持旧 snapshot，不猜测或回滚 Milvus，保留 marker、将 Instance 置为 dirty并抛原错误。之后 Document query 仍返回旧 snapshot，调用方以 `reindex()` 恢复 search。

commit 后发生 marker cleanup 错误时不回滚 SQLite；返回成功且 `index_ready=false`，保留 marker。调用方随后执行 `reindex()`。

delete 与 drop 使用相同 marker、flush 和 commit point。创建/删除空 Namespace 只修改 SQLite，不需要 marker。

### 9.2 Open recovery

open 删除无引用 staging/object。存在 `INDEX_DIRTY` 时不猜测 Milvus 当前内容，也不自动执行昂贵工作；Instance 以 dirty 状态打开，Document query 可用，调用方显式执行 `reindex()`。

SQLite 引用的 Internal object 缺失或 Document JSON 无法解析是 `CorruptState`，open 失败。

### 9.3 Reindex

`reindex()` 在 mutation mutex 与 write lock 内：

1. 创建并 fsync dirty marker；
2. 根据当前 Chunker从所有 SQLite `Document.text/source_map` 生成 Chunk；
3. drop/recreate 固定 `chunks` collection；
4. 若目标 index 为 dense，必须提供与目标 space 匹配的 Embedder并生成全部 vector；目标为 BM25-only 时不需要；
5. 插入、flush，校验写入的 Chunk 总数和每个非空 Document 的连续 ordinal；零 Chunk Document 只存在 SQLite；
6. 原子替换 `index.json`，删除并 fsync dirty marker。

当前提供的 Embedder 可以把 BM25-only index 升级成 dense，或替换为新的 embedding space。V1 不提供 dense → BM25-only 的原地降级。

失败时 marker 保留，SQLite 不变，之后可以重试。reindex 阻塞所有 query/search，不维护第二张 collection。

## 10. Search execution

### 10.1 Query

`ByNamespace`、`ByDocumentId`、`UnderPath` 通过 SQLite primary key/range 选择 Document；`TextMatch` 扫描其完整 text。`doc_id/doc` 直接由 SQLite 返回；`chunk` 再从 Milvus读取。Chunk 全量读取必须使用经 §15 验证的 iterator/scan，不得假设 Milvus 返回 primary-key order，也不得用“上一批最后一个 ID”模拟游标；MFS 收齐候选后按 §3 排序并应用最终 limit。

### 10.2 Ranked search

| mode     | execution                        |
| -------- | -------------------------------- |
| `bm25`   | Milvus sparse search             |
| `vector` | `embed_query` 后 COSINE search    |
| `hybrid` | 两路各取 candidate，按下述 RRF `k=60` 融合 |

vector/hybrid 需要与 active index 匹配的 Embedder；否则返回 `CapabilityUnavailable`。BM25 与 Chunk query 不需要 Embedder。

所有 Filter 必须在最终 top-k 前生效。scalar Filter 下推 Milvus；TextMatch 先在 SQLite 求 Document ID 集合，按后端表达式限制分批搜索。每个 batch 对每个 channel 最多取当前 channel candidate budget，按原始 score 全局 merge 后形成过滤后的 channel ranking；不得直接拼接 batch-local rank。

每个 search channel 的初始 candidate 数为 `min(1000, max(100, limit * 10))`。`select="doc_id"|"doc"` 时若 distinct Document 不足，继续扩大候选直到得到 limit 个 Document、耗尽结果或达到 1000；达到 cap 仍可能有结果时返回 `truncated=true`。

hybrid 中每个 channel 先按原始 score descending、再按 `(namespace, doc_id, ordinal)` 排序并从 1 编 rank。候选 `c` 的 `RRF(c) = Σ_channel 1 / (60 + rank_channel(c))`；未进入某 channel candidate set 的贡献为 0。最终再按 RRF score 与相同 identity tie-break 排序。

BM25、COSINE 与 RRF score 只在同一 mode 内排序，不承诺跨 mode 可比较。相同文本出现在不同 Document 时分别返回。

Milvus vector/AUTOINDEX 与 1000 candidate cap 使 search 是 top-k retrieval，不承诺穷举或 100% recall；确定性排序只约束后端实际返回并经 Filter 保留的 candidate。BM25 的具体 tokenizer、IDF 与 segment 行为属于 pinned backend 语义，升级必须触发 §15 conformance tests。

SearchResult 按 score descending；平分时按 `(namespace, doc_id, ordinal)` 的 UTF-8 bytes ascending，Document projection 没有 ordinal。

## 11. Errors

| code                       | condition                                 |
| -------------------------- | ----------------------------------------- |
| `InvalidNamespace`         | Namespace ID 为空、含 NUL 或超过长度限制           |
| `NamespaceNotFound`        | Namespace 不存在                             |
| `NamespaceConflict`        | 同 ID create 使用不同 kind/root                |
| `WrongNamespaceKind`       | mutation 与 Namespace kind 不匹配             |
| `InvalidDocumentId`        | Internal ID 非法                            |
| `InvalidPath`              | External path 非法或逃逸 root                  |
| `RootOverlap`              | External root 与 mfs\_path 重叠              |
| `SourceUnavailable`        | root/parent/file 不可访问                     |
| `SourceChanged`            | stable read 重试后仍变化                        |
| `UnsupportedMediaType`     | 没有匹配 Processor                            |
| `ProcessingFailed`         | Processor/Chunker 输出非法或执行失败               |
| `EmbeddingFailed`          | Embedder 执行失败或 vector 非法                  |
| `StorageFailed`            | SQLite、object/staging、lock 或 fsync I/O 失败 |
| `IndexFailed`              | Milvus query/search/mutation 执行失败         |
| `InvalidFilter`            | Filter 值或组合非法                             |
| `InvalidPattern`           | regex 无法编译                                |
| `InvalidQuery`             | query/search 的 text、select、mode 或 limit 非法  |
| `IndexUnavailable`         | index dirty/missing/mismatch              |
| `CapabilityUnavailable`    | 当前操作需要未提供的 Embedder                       |
| `InvalidConfiguration`     | Processor/Chunker/Embedder/SyncPolicy 非法  |
| `InstanceLocked`           | 相同 mfs\_path 已由另一进程打开                     |
| `SchemaVersionUnsupported` | catalog/index schema 无法读取                 |
| `CorruptState`             | catalog/object/index invariant 损坏         |
| `Closed`                   | close 后调用                                 |

错误对象至少包含 `code/message`，并通过原始 exception chaining 保留内部 cause；Document 相关错误包含 `DocumentId`；External source 错误包含 normalized relative path。publication 中 Milvus 失败对外为 `IndexFailed`，同时按 §9 决定是否进入 dirty；SQLite/object/fsync 失败为 `StorageFailed`。调用方不得解析 message 判断类型。

## 12. Package seam

Package top-level 只导出 MFS、公开值类型、Filter、Processor、Chunker、Embedder、`Utf8TextProcessor`、`PdfProcessor`、`DefaultChunker`、SyncPolicy 和 typed errors。SQLite schema、Milvus row、object path、content hash、lock、dirty marker和 staging 都是 implementation。

Processor、Chunker 与 Embedder 是公开 adapter seam；Catalog、Milvus、Publisher 或 filesystem walker 没有第二个实现需求，不预先公开 interface。

实现仓库使用 `src/mfs/` layout，V1 `requires-python = ">=3.12,<3.13"`，以 `uv` 生成并提交 lockfile、Hatchling 构建、pytest 测试、Ruff lint/format、Pyright strict type check。公开 Interface 是同步 Python Interface；async、CLI、RPC、HTTP 和 TypeScript client 不属于 Core。Catalog、Milvus 与 filesystem 可以有只对 implementation/test 可见的 internal seam，用于真实 local adapter 测试和 failure injection，但不得从 package top-level 导出。

仓库从空实现开始，不保留 `zilliztech/mfs` 的兼容层、migration 或 CLI。允许从 Apache-2.0 的 `zilliztech/mfs` `v0.1.0`/`7cd3c5b` 移植 Embedder、PDF 与 Milvus 局部实现；移植时保留适用的版权与许可证说明，并用本规范的 Interface tests 替换上游行为测试。Scanner、Chunker、catalog 与 publication 语义以本文为准，不能因复用代码而改变。

## 13. StashBase mapping

| StashBase action            | MFS call                                                            |
| --------------------------- | ------------------------------------------------------------------- |
| bind Folder                 | `create_namespace(folder_id, "external", root)` + `sync(folder_id)` |
| full reconcile              | `sync(folder_id)`                                                   |
| 保存/删除一个文件                   | `sync(folder_id, relative_path)`                                    |
| rename 文件/目录                | `sync(folder_id, common_parent)`                                    |
| remove Folder but keep disk | `drop_namespace(folder_id)`                                         |
| Folder search               | `search(text, filters=[ByNamespace(folder_id)])`                    |
| library search              | `search(text, filters=[ByNamespace(visible_folder_ids)])`           |
| path-prefix search          | `search(text, filters=[UnderPath(folder_id, relative_prefix)])`     |
| exact/regex lookup          | `query(filters=[ByNamespace(...), TextMatch(...)], select="chunk")` |
| read indexed document       | `query(filters=[ByDocumentId(...)], select="doc", limit=1)`         |

StashBase 只传 namespace、路径、原始输入和查询，不传 authoritative hash、预制 text、Chunk 或 vector，也不维护第二份 MFS manifest。

## 14. Required tests

### Interface 与 query

1. query/search 共用 Filter 和三种 projection，点查、文本匹配与路径范围都只经过该 Interface。
2. ByDocumentId 点查、不存在返回空、多个 Filter AND、同 Filter OR、空 Filter value 校验正确。
3. UnderPath 使用 segment semantics；不同 Namespace、相似字符串 prefix 不串数据。
4. TextMatch literal/regex/case、UTF-8 byte offset、多 TextMatch、相邻/重叠 union、跨 Chunk 边界和 SourceLocation 计算正确。
5. query/search 的最终 projection limit、排序、Document 聚合与 truncated 正确；无 TextMatch 的 Chunk query 返回全部匹配 Chunk。

### Processing 与 sync

6. content hash + media type + Processor 描述相同不运行 Processor/Chunker/Embedder。
7. Processor 选择优先级、重复 registry、sniff conflict、invalid source map/chunk/vector 全部返回 typed error。
8. DefaultChunker 覆盖 ASCII、多字节 Unicode、无 whitespace、overlap、空文本和 4096-byte 边界。
9. stable read 变化重试；incomplete walk、权限失败和处理失败不误删 missing；sync path 规范化与实际 directory-entry 拼写正确。
10. excluded directory/file、oversized、symlink、special/unsupported path 的新增、报告排序与从 member 变 non-member 行为正确。
11. External query 不打开 live source；snapshot\_id 随 bytes/Processor 语义变化且 SourceLocation 只对应 snapshot；drop Namespace 不修改 root。

### Persistence 与 recovery

12. SQLite 只有 Namespace/Document；Milvus 只有 Chunk search fields；没有第二份 Chunk 或 Document metadata。
13. Internal add/update/remove 在 Processor、Embedder、marker、Milvus、SQLite 与 fsync 各 failure point 保留正确 SQLite snapshot；marker 后失败进入 dirty且不尝试解释 Milvus。
14. crash 注入 dirty marker 前后；open 后 Document query 正确且 search 不服务；reindex 后 row/doc 对齐。
15. commit 后 marker cleanup 失败返回成功 + `index_ready=false`；dirty 时 mutation 被拒绝，reindex 后重试 upsert/remove 幂等。
16. orphan staging/object GC 不删除被引用 object。
17. Chunk count 增大、缩小、归零后没有旧 ordinal。
18. index.json、Milvus schema、Chunker 或 Embedder mismatch 正确阻止 search/mutation。
19. BM25-only、dense unavailable、embedding-space upgrade 与 reindex failure 行为正确。
20. 多 query 并发、所有 mutation/reindex 串行、prepare 不阻塞旧 snapshot 读取、close/lock 生命周期正确。

## 15. 后端验证与发布门槛

领域 Interface 已冻结，可以实现 Core。Milvus implementation 开始前必须先在 Python 3.12 上选择一组 exact `pymilvus`/`milvus-lite` 版本并通过以下 executable conformance tests；通过的版本写入 `pyproject.toml` 与 `uv.lock`，不能保留无上界依赖：

1. 同一 collection 的 BM25 Function、dense COSINE 与两路 hybrid/RRF 可创建、写入、查询和 reopen。
2. namespace/doc ID exact filter、path prefix、包含 quote/backslash/newline/`%`/`_`/Unicode 的值、分批 ID filter 都不误匹配或注入 expression。
3. 多次 insert/flush 形成多个 segment 后，以非 primary-key 顺序写入超过 16,384 rows；全量 iterator/scan 不重不漏。不得以 batch 最后一个 ID 作为隐式 cursor。
4. delete/insert/flush 后，query 与 search 在 write lock 释放前可见新 projection；进程 reopen 后结果一致。
5. 空 collection、零 Chunk Document、单 Document Chunk 数增大/缩小/归零、1000 candidate cap 均不崩溃且符合 report/truncated 语义。
6. collection/schema/index introspection 能稳定识别 ready、dirty 和 mismatch；故障注入后保留 marker并可由 reindex 重建。

`pymupdf4llm`、BLAKE3、RFC 8785 canonical JSON、RE2-compatible regex、file lock 与 Milvus 的依赖及 native wheel 同样必须 exact lock，并在 macOS、Linux CI 安装。依赖升级必须重跑 Processor golden tests、Milvus conformance tests 和 crash tests；涉及处理或索引语义时同时提升相应 version。

V1 暂不设 Namespace/Document/单文件数量或 P95 产品目标，也不作无限规模承诺。首次发布前运行代表性 benchmark，记录机器、corpus、Chunk 数、query/search/sync/reindex 延迟和峰值内存，结果用于以后设定支持范围，不反向改变本文 Interface。RPC/sidecar 与 StashBase 端到端集成不属于 V1 Core。

## 16. 非目标

V1 不实现 POSIX mount、CLI、RPC/sidecar、TypeScript client、StashBase 集成、daemon、HTTP server、watcher、持久 Job、多进程共享 Instance、多 Document transaction、历史版本、External live-original read/freshness check、跨 Document 处理/embedding cache、per-Namespace embedding space、remote connector、tenant、ACL、Windows、后台无停机 reindex 或调用方自定义 Filter 执行代码。
