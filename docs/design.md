# MFS 设计

> 状态：讨论稿。本文件是设计与实现的唯一依据。
> 事实来源、决策理由和替代方案记录在 [decision-log.md](decision-log.md)。

## 1. 定位

MFS 是 StashBase 使用的**全局、多 Project 文件搜索核心**。它不 mount、不实现 POSIX，也不是让调用方反复打开的数据目录。名称里的 FS 表示处理文件内容与文件路径，不表示它向外提供 filesystem 语义。

“全局”指一个 StashBase local-data deployment 内只有一个 MFS Engine：Engine 启动时由宿主配置唯一 `data_dir`，随后服务多个 Project。普通调用方只提交 `project_id` 和业务操作，不知道也不能直接访问 `data_dir`。

这不表示整台机器只能有一个 Engine。不同 OS 用户、测试进程或强隔离部署可以使用不同 `data_dir` 启动彼此独立的 Engine；它们不是一个 Runtime 内可切换的 Repository，也不能直接跨库查询。

```text
StashBase
  └── one MFS Engine(data_dir)       # deployment bootstrap
        ├── Project A
        ├── Project B
        └── Project C
```

StashBase 是面向用户的 File Search Engine 产品；MFS 是其深层搜索 Module：调用方交付 Project、linked Source 或 copied 文件，MFS 隐藏扫描、hash、内容寻址、提取、索引、精确检索、状态和崩溃恢复。

`data_dir` 只是 Engine 的部署配置：测试可以注入临时目录，独立部署可以配置不同目录，但 Runtime Interface 中不存在 `open_repository(state_root)`，领域模型中也没有 Repository。

## 2. 模块 seam

| MFS 负责                               | StashBase/宿主负责                  |
| ------------------------------------ | ------------------------------- |
| Project、Source、Entry 和 Revision 账本   | 启动一个 Engine，提供 `data_dir` 和后端配置 |
| linked Source 的枚举、hash、diff、删除确认     | 创建 Project，决定何时触发 sync          |
| copied 原文件的落盘、去重和回收                  | 选择 Link 或 Copy 的产品入口            |
| 格式识别、提取、Canonical Text 和 Evidence    | UI、预览、播放、结果展示和跳转                |
| chunk、embedding、keyword/vector index | 产品元数据、权限和跨 Project 可见范围         |
| grep / keyword / semantic / hybrid   | 用户确认、进度和错误文案                    |
| readiness、source status 和事件          | 展示 stale/missing/failed warning |

> StashBase 决定“何时需要新鲜数据”；MFS 决定“如何观察、处理并查询文件”。

扫描实现必须在 MFS 内。StashBase 只调用 `sync_source`，不枚举文件、不维护 hash manifest，也不编排 `begin_scan/upsert/commit`。

## 3. 核心模型

```text
MFS Engine
  └── Project
        ├── Linked Source
        │     └── Linked Entry
        └── Copied Entry
              └── Stored Blob

Entry
  ├── observed_revision_id          # 最近观察到，可能仍在处理
  └── active_revision_id            # 已完成处理，可供查询

Revision
  ├── content_hash
  ├── Content Reference
  ├── Canonical Text + Evidence
  └── Index Projection
```

| 概念               | 含义                                            |
| ---------------- | --------------------------------------------- |
| Engine           | 一个 deployment 内唯一的 MFS 运行实例，独占 `data_dir` 写入权 |
| Project          | 查询、权限、生命周期和默认展示的逻辑隔离单元                        |
| Linked Source    | MFS 可访问的外部目录；MFS 对它执行 scan/reconcile          |
| Entry            | Project 中的文件记录，以稳定 `entry_id` 标识              |
| Linked Entry     | 属于 Linked Source，原始 bytes 仍在外部目录              |
| Copied Entry     | 原始 bytes 已进入 MFS object store，不再跟随外部路径        |
| `source_key`     | Linked Source 内的稳定映射键，V1 为规范化相对路径             |
| Revision         | Entry 某次已观察内容的不可变处理单元                         |
| Canonical Text   | MFS 保存的可搜索规范文本；文本文件通常是全文，二进制格式是提取结果           |
| Evidence         | Canonical Text 到原内容的行号、页码、区域或时间戳映射            |
| Index Projection | 从 ready Revision 派生、可以重建的关键词和向量索引             |
| Readiness        | observed Revision 是否已经成为 active Revision      |

### Project 与当前 StashBase 的映射

V1 中，一个 StashBase 打开的 folder/library 对应一个 Project。Project 可以包含：

* 一个 primary Linked Source，即用户当前文件夹；

* 任意数量的 Copied Entry，例如上传后需要长期可用的文件。

只有出现“一个 Project 聚合多个外部根目录”的真实需求后，才允许多个 Linked Source；Interface 已保留 `source_id`，但 V1 产品不暴露多 Source 管理。

## 4. Link 与 Copy

Link 和 Copy 是两个不同生命周期的写入操作，不是一个 Source mode，也不是 scan 中的布尔 flag。

### External link

```text
attach_linked_source(project_id, root_uri)
  → 注册 Source，并安排首次 full sync
  → Linked Entry 指向 root_uri/source_key
  → MFS 保存 Canonical Text 和索引，不保存完整原始 bytes
```

约束：

* 外部目录是原始 bytes 的事实来源，MFS 不保留可供长期读取的原文件副本；

* Source 完整 sync 确认文件缺失后，对应 Entry tombstone，默认查询不再返回；

* 外部文件在两次 sync 之间可以变化，因此查询结果可能 stale；

* 原始文件缺失时，`read_original` 返回 typed error，不能拿 Canonical Text 冒充原始 bytes。

### Copy

```text
copy_file(project_id, logical_path, input)
  → 计算 content_hash
  → 写入 content-addressed object store
  → 创建 Copied Entry 和 Revision
```

约束：

* Copy 是一次性 import，不建立持续外部同步关系；

* `origin_uri` 可以保存为 provenance，但不参与删除和更新；

* 外部原文件删除或改写不影响 Copied Entry；

* 更新 Copied Entry 必须显式再次调用 `replace_copied`；

* 相同 bytes 可以共享一个 Stored Blob，不同 Entry identity 仍然独立。

`copy_file` 先把 input 写入 `data_dir` 内的临时文件，计算 hash、fsync 后原子 rename 成 Stored Blob，再在 catalog 事务中创建引用。崩溃最多留下无引用 blob 供 GC，catalog 不能指向未完成文件。Processor 始终读取 Stored Blob，不重读 `origin_uri`。

V1 中 `logical_path` 在 Project 内唯一。Copied Entry 与 primary Linked Source 映射出的相对路径冲突时返回 typed `PathConflict`，不允许静默覆盖、遮蔽或合并；调用方必须选择新路径或显式删除旧 Entry。

V1 不提供 `materialize(linked_entry)`。它会涉及原 Source 是否继续跟踪、logical path 冲突和删除传播；等出现真实用户流程后，再决定它是“复制出新 Entry”还是“改变原 Entry 生命周期”。

## 5. Bootstrap 与 Runtime Interface

### Bootstrap：只供宿主

```python
start_engine(MfsConfig(
    data_dir=...,
    metadata_backend=...,
    vector_backend=...,
    processors=...,
)) -> EngineHandle
```

Bootstrap 发生一次。`data_dir`、后端选择、Processor 注册、single-writer lock 和 migration 不进入普通 Runtime Interface。

长期 Engine 是生产实现；测试可以用相同 Interface 启动临时 Engine。调用方连接 Engine，而不是多个进程共同打开 `data_dir`。

### Runtime：供 StashBase、CLI 和其他调用方

```python
# Project
ensure_project(project_id, profile=None) -> ProjectRef
delete_project(project_id) -> DeleteResult

# Linked Source
attach_linked_source(project_id, root_uri, profile=None) -> AttachResult
update_linked_source(project_id, source_id, profile) -> SourceRef
sync_source(project_id, source_id, verification="incremental"|"full") -> SyncResult
detach_linked_source(project_id, source_id) -> DetachResult

# Copy
copy_file(project_id, logical_path, input, origin_uri=None) -> EntryRef
replace_copied(project_id, entry_id, input, expect_revision=None) -> EntryRef
remove_copied(project_id, entry_id) -> RemoveResult

# Entry view
stat(project_id, entry_id) -> EntryStatus
list(project_id, prefix=None, include_tombstoned=False) -> list[EntrySummary]

# 内容
read_original(project_id, entry_id, range=None, expect_revision=None) -> OriginalContent
read_text(project_id, entry_id) -> CanonicalTextResult

# 查询
grep(scope, pattern, include_tombstoned=False, ...) -> Results
search(scope, query, mode="keyword"|"semantic"|"hybrid",
       include_tombstoned=False, ...) -> Results

# 状态
status(scope) -> Status
poll(scope, cursor) -> Events
```

`scope` 是带类型的 `ProjectScope(project_id)`、`SourceScope(project_id, source_id)`、`EntryScope(project_id, entry_id)` 或 `MultiProjectScope(project_ids)`。它必须显式：普通请求是一个 Project；跨 Project 请求由 StashBase 在鉴权后传入允许访问的 `project_ids`。MFS 不能用“未传 Project 表示全局”这种隐式规则，事件流也不能绕过 scope 泄漏其他 Project 状态。

`input` 可以是 Engine 可访问的本地路径或 byte stream。远端调用方不能假设 Engine 能打开客户端本地路径，必须上传 stream。

V1 的 Linked Source 只接受 Engine 所在机器上的本地目录。`AttachResult` 包含 `source_id` 和首次 full sync 的 job/status；注册成功不等于内容已经 active。

`update_linked_source` 只修改 profile 并安排 full sync；V1 不原地修改 `root_uri`，换根目录必须 detach 后重新 attach，防止两个文件树被误认为同一个 Source。

Linked Entry 的生命周期由 Source sync 驱动，不提供通用 `remove_entry`：要永久排除单个外部路径，应更新 Source profile 的 ignore 规则后 sync；要移除整个映射则 `detach_linked_source`。否则只删账本会让文件在下一次 sync 中重新出现。

`delete_project`、`detach_linked_source` 和 `remove_copied` 只删除 MFS 拥有的账本、派生数据、索引和 copied blob 引用，绝不删除 linked 外部文件。

调用方看不到 collection、表结构、chunk schema、embedding 维度、对象路径或派生缓存路径。

## 6. Source sync

StashBase 选择触发点，MFS 完成整个 scan/reconcile：

```text
sync_source(project_id, source_id)
  ├── 验证 root 可访问且身份符合 Source 注册信息
  ├── 枚举目录并应用 ignore / symlink / size / format 规则
  ├── 与 MFS catalog 中的 source_key、fingerprint、hash 对比
  ├── added / modified → 创建 observed Revision
  ├── missing          → 完整扫描成功后 tombstone
  ├── identity 或同 hash 的 delete/add → 尝试配对 rename
  └── 返回 SyncResult；处理和索引可以异步继续
```

对 added/modified 文件，MFS 必须执行 stable read：读取前后校验 fingerprint，并保证 `content_hash` 与 Processor 消费的是同一份 bytes。文件在读取中变化时重试或跳过本轮，不能用 A 版本的 hash 发布 B 版本的 Canonical Text。

异步处理需要在 Source 再次变化或消失后仍能完成，因此 pending Revision 可以持有短期 staging snapshot。它只服务该 Revision 的处理与重试，ready、obsolete 或超出保留期后回收，不作为 `read_original` fallback，也不改变 Linked Source 的事实来源。同步小文件处理可以直接消费同一次 stable read，无需持久 staging。

删除只在完整扫描成功后生效。以下情况必须 abort，不得将“未看到”解释成删除：

* Source 根目录不存在或未挂载；

* 权限不足；

* 枚举中断；

* Source root 的 filesystem identity 与注册时不符；

* 调用被取消。

### 变化判断

`content_hash` 是内容身份的权威证据；`size/mtime/file-id` 只是避免读盘的 fingerprint。

* `verification="incremental"`：fingerprint 未变时可以跳过完整 hash；适合 focus、目录切换等高频触发，但不能发现保留全部 metadata 的内容替换；

* `verification="full"`：读取并 hash 所有候选文件；用于首次 attach、手动 Reindex、恢复检查和周期性校验。

两种模式的保证必须在 `SyncResult` 中可见。不能把 incremental 的结果宣称为 cryptographically verified。

rename 只有在 filesystem identity 可确认，或一个 hash 恰好对应唯一 missing Entry 与唯一 added path 时才能复用原 `entry_id`。相同内容存在多个候选时不猜测，按 tombstone + add 处理。

同一 `source_key` 在 tombstone 保留期内重新出现时复用原 Linked Entry，并创建新的 observed Revision；跨路径只有满足上述无歧义 rename 条件才复用。Copied Entry 被 `remove_copied` 后重新导入则创建新 Entry。

MFS V1 不提供 watcher。StashBase 当前的触发点是应用启动、打开/切换目录、窗口 focus、Agent turn end、手动 Sync 和 MCP reindex。以后 watcher 只能触发 scoped incremental sync，不能承担删除正确性。

## 7. Revision 与原子可见性

Entry 同时记录两个指针：

```text
observed_revision_id   # 最近收到或扫描到的内容
active_revision_id     # Canonical Text 与全部必需索引已经 ready
```

处理协议：

```text
发现新 content_hash
  → 创建 immutable Revision(state=pending)
  → 提取 Canonical Text / Evidence
  → 以 revision_id 写入 keyword/vector projection，并等待后端持久化确认
  → 在 catalog 单个事务中标记 Revision(state=ready)
    并切换 Entry.active_revision_id
  → 异步回收未发布或旧 Revision 的 artifact
```

新 Revision 尚未 ready 时：

* 查询、`grep` 和 `read_text` 继续使用旧 active Revision；

* `status` 报告新 observed Revision 为 pending/failed；

* 旧任务完成时必须检查自己仍是最新 observed Revision，不能覆盖更新内容；

* 如果没有旧 active Revision，Entry 对内容查询表现为 pending，而不是返回部分索引。

因此 MFS 保证的是：一次查询中的 Canonical Text、Evidence 和 Index Projection 来自同一个 active Revision。linked 外部原文件仍可能比 active Revision 更新，这是 freshness，不是 MFS 内部数据裂开。

处理阶段写出的 artifact 在 `active_revision_id` 切换前都不可见；进程崩溃后可以作为 orphan 重试或回收。查询必须按 active Revision 过滤，不能因为向量后端已经存在 pending Revision 的行就提前返回它。

Entry 对外暴露两个正交状态，不能压成一个含糊的 `fresh`：

| 维度                 | 状态                                                                                   |
| ------------------ | ------------------------------------------------------------------------------------ |
| Source observation | `present` / `missing` / `unavailable` / `unknown`，并带最后检查时间                           |
| Readiness          | `never_active` / `current` / `updating` / `failed`，描述 observed 与 active Revision 的关系 |

## 8. 读取、grep 与搜索

```text
list / stat       → MFS catalog
read_original     → copied blob，或 linked 当前外部文件；可校验期望 Revision
read_text         → active Revision 的 Canonical Text
grep              → active Revision 的 Canonical Text，精确匹配
keyword           → active Revision 的 keyword projection
semantic          → active Revision 的 vector projection
hybrid            → keyword/vector 融合
```

`grep` 不直接读取 live external file，也没有“live 失败再 fallback”两套执行路径。它和其他搜索一样查询 active Revision，因此 linked 原文件突然消失不会让 grep 崩溃。

默认 `list`、`grep` 和 `search` 排除 tombstoned Entry。外部文件删除但尚未 sync 时，旧 active Revision 仍可能被命中；full sync 确认 missing 后立即 tombstone 并从默认结果隐藏。tombstone 保留期内，诊断或恢复工具可以显式 `include_tombstoned=True` 访问 Canonical Text；超过保留期后才回收旧 Revision 和 artifact。

如果产品需要操作系统意义上的实时 grep，应由 StashBase 提供单独的 “Live grep” 操作，或者先执行 `sync_source(..., verification="full")`；不能和 MFS snapshot grep 共用一个无标识结果。

`read_original` 的保证：

| Entry 类型     | 行为                                                             |
| ------------ | -------------------------------------------------------------- |
| Copied Entry | 从 Stored Blob 读取，除内部存储损坏外始终可用                                  |
| Linked Entry | 读取当前 `root_uri/source_key`；缺失、权限错误或 identity 变化时返回 typed error |

`OriginalContent` 返回实际 `content_hash`。传入 `expect_revision` 时，MFS 必须验证读取 bytes 与该 Revision 的 `content_hash` 一致；Linked Entry 已被外部改写时返回 typed `SourceChanged`，不能把不匹配的现场文件交给旧 Evidence。StashBase 从搜索结果打开文件时应传结果中的 `active_revision`；纯粹“打开当前文件”时可以不传。

所有查询结果包含：

```text
entry_ref
active_revision
indexed_at
source_state        # present | missing | unavailable | unknown；copied 为 durable
source_checked_at
readiness           # never_active | current | updating | failed
warning[]
snippet / evidence / score / match_type
```

`source_state` 描述 MFS 最后一次观察结果，不声称是查询瞬间的实时 stat。StashBase 必须根据检查时间、readiness 和 warning 展示 stale/missing/failed 状态。

## 9. Hash、缓存与复用

| 层         | key                                                     | 作用                           |
| --------- | ------------------------------------------------------- | ---------------------------- |
| 原始内容      | `content_hash = BLAKE3(bytes)`                          | 变化判断、Stored Blob key、派生输入    |
| 派生缓存      | `content_hash + processor_id + version + config_digest` | Canonical Text / Evidence 复用 |
| 规范文本      | `text_hash = BLAKE3(canonical_text)`                    | 判断不同 Processor 产出是否等价        |
| 切分计划      | `text_hash + chunker_id + version + config_digest`      | 稳定切分结果复用                     |
| chunk 内容  | `chunk_hash = BLAKE3(chunk_text)`                       | 相同文本片段复用；位置另存                |
| embedding | `chunk_hash + model_revision + dimension`               | 向量复用                         |

规则：

* `source_key`/path 标识文件映射，不标识内容；

* 路径、行号和 Project 不进入内容 key；

* 相同内容的不同 Entry 可以复用 blob 和派生结果，但 Entry identity 独立；

* 跨 Project 物理复用不改变权限，查询必须先通过 Project scope；

* effective Processor/chunker/model 配置不同，不能错误复用结果。

同一段文本在一个或多个 Revision 中可以有多个 chunk occurrence；occurrence 保存 revision、顺序和 Evidence，`chunk_hash` 只用于内容复用，不能充当位置 identity。

由此得到：改名和复制不重新提取；内容改回去可以复用；只修改局部文本时只重新 embedding 受影响 chunk。

## 10. 内容处理与扩展

```python
class Processor(Protocol):
    id: str
    version: str
    def supports(revision, profile) -> Match: ...
    def process(request) -> ProcessResult: ...
```

`ProcessResult` 含 Canonical Text、Evidence、内容类型、语言、元数据和 warning。

模型按能力拆分：`EmbeddingProvider` / `TextGenerationProvider` / `VisionProvider` / `TranscriptionProvider`。

Provider Interface 要薄，MFS 外层实现要厚。实现者只提供核心能力；timeout、重试、退避、批切分、token 上限、取消和错误隔离由 MFS 统一管理。

### 配置还是注册

判据是实现的分发权：

| 情况               | 配置     | 注册     |
| ---------------- | ------ | ------ |
| MFS 自己可分发，只是参数不同 | ✅      | <br /> |
| 需要宿主打包外部二进制或模型   | <br /> | ✅      |
| 需要宿主运行时上下文       | <br /> | ✅      |
| 不同宿主提供不同实现       | <br /> | ✅      |

索引规则、chunk 参数、模型选择、timeout/retry 是配置；PDF 引擎和 OCR 是内建可选配置；依赖 StashBase 三平台构建链的音视频转录通过 Bootstrap 注册 `SubprocessProcessor`。

### 格式计划

| 阶段  | 格式                      | 产出                       |
| --- | ----------------------- | ------------------------ |
| 首批  | Markdown、TXT、代码、脚本      | Canonical Text + 行号      |
| 首批  | JSON、YAML、TOML、CSV、HTML | Canonical Text + 行号/结构位置 |
| 首批  | PDF、DOCX                | Markdown/文本 + 页码/段落      |
| 迁移前 | PNG/JPEG/WebP           | OCR 文本 + 区域              |
| 迁移前 | 音频、视频音轨                 | transcript + 时间戳         |

未知格式仍可成为 Entry。Copied Entry 可以保存和 `read_original`，Linked Entry 可以打开原文件；没有 Processor 时不承诺 `read_text` 或内容搜索。

## 11. 数据目录与逻辑存储

目标布局：

```text
<data_dir>/
  engine.json
  catalog.sqlite
  objects/<content_hash-prefix>/<content_hash>   # Copied Entry 原始 bytes
  staging/                                       # pending Linked Revision 短期输入
  derived/<derived_key>/                         # Canonical Text / Evidence
  indexes/                                       # keyword / vector backend
  locks/
  logs/
```

StashBase 目标默认值为 `<appData>/mfs/`；`STASHBASE_LOCAL_DATA_ROOT` 覆盖 app data root 后，MFS 相应使用 `<override>/mfs/`。这与 §12 记录的当前 `vector-store.nosync/milvus.db/` 是迁移前后两套位置，不能混称为现状。

`data_dir` 是 Engine 私有目录：

* 只有 Engine 写入；

* 调用方通过 Runtime Interface 访问；

* Scanner 必须强制排除解析后的 `data_dir`；即使 Source root 是其祖先、ignore 配置遗漏或 symlink 指向它，也不能递归索引 object、index 和 WAL；

* `staging/` 有 TTL 与启动清理，只保证处理期输入一致，不提供用户可见的原文耐久性；

* 一个 deployment 只配置一个 `data_dir`，不是每个 Project 一个目录。

耐久性分级：

| 数据                        | 地位                                           | 恢复要求                                                        |
| ------------------------- | -------------------------------------------- | ----------------------------------------------------------- |
| catalog                   | Project/Source/Entry/Revision identity 的权威账本 | 必须 migration，并纳入备份                                          |
| Copied objects            | Copied Entry 原始 bytes 的权威内容                  | 与 catalog 一起备份；不能只备份数据库                                     |
| Canonical Text / Evidence | active Revision 的可搜索内容                       | 为搜索连续性持久保存；Copied 可由 object 重建，Linked 仅在 live bytes 仍匹配时可重建 |
| Index Projection          | 查询加速结构                                       | 可由 Canonical Text / Evidence 重建                             |
| staging                   | pending Linked Revision 的临时输入                | 可清理；丢失后重试或重新观察 Source                                       |

因此“linked 原文件是事实来源”不等于 data directory 全部可丢弃。丢失 `data_dir` 后可以从当前外部目录重新建一套搜索库，但旧 `entry_id`、旧 Revision、尚未保留的历史 Canonical Text 和 Copied Entry 无法凭 linked 目录恢复。

逻辑存储至少表达以下关系，具体 SQL 在原型验证后冻结：

```text
Project(project_id, profile)
LinkedSource(project_id, source_id, root_uri, fingerprint, sync_state)
Entry(project_id, entry_id, kind, source_binding?, logical_path,
      observed_revision_id, active_revision_id, tombstoned_at?)
Revision(revision_id, entry_id, content_hash, raw_ref, state, created_at)
Derived(derived_key, canonical_text_ref, evidence_ref)
Projection(revision_id, kind, backend_ref, config_digest)
StoredBlob(content_hash, blob_ref, size, ref_state)
Task(task_id, revision_id, kind, state, attempts, error)
```

一个物理 catalog/index 默认保存多个 Project。所有 Entry、Revision 可见性和向量过滤必须从显式 `project_id` scope 开始；Blob/Derived 可以按内容跨 Project 物理复用，但不能绕过 Project 授权查询。

## 12. 当前 StashBase 基线与迁移

当前 StashBase 已经符合“一份全局数据、多目录共享”的部署形态：一个 Node server 管一个 Python sidecar 和一个全局 Milvus Lite collection，多个打开目录以绝对路径共享它。

当前 macOS 默认位置：

```text
~/Library/Application Support/StashBase/
  vector-store.nosync/milvus.db/   # 当前 MFS/Milvus Lite 数据
  derived.nosync/                  # 当前 StashBase 派生内容
  state/state.db                   # 当前 StashBase 状态
  models/whisper/                  # 转录模型
```

`STASHBASE_LOCAL_DATA_ROOT` 可以覆盖根目录。Windows 默认 `%LOCALAPPDATA%/StashBase`，Linux 默认 `$XDG_DATA_HOME/StashBase` 或 `~/.local/share/StashBase`。

当前实现仍有这些差距：

* 没有显式 `project_id`，以文件绝对路径充当 scope；

* 没有 Copied Entry object store；

* 文件 metadata 寄生在 Milvus Lite；

* Derived Store、任务状态和向量库分散；

* StashBase 直接调用和 patch MFS 内部模块。

迁移保持全局 Engine 拓扑，不引入 `open_repository`：

1. 建立 Project/LinkedSource/Entry/Revision catalog；
2. 把当前每个打开 folder 映射为 Project + primary Linked Source；
3. 把 `scan_diff` 收入 `sync_source` Interface；
4. 迁移 Canonical Text、任务和索引；
5. 增加 Copied Entry object store；
6. 删除 StashBase 对 MFS 内部实现的调用和 monkey-patch。

## 13. 配置与并发

```text
内建默认 < Engine 配置 < Project Profile < Linked Source Profile < 安全的单次覆盖
```

Engine 配置包含 `data_dir`、后端、worker、Processor 和全局资源限制。Project/Source Profile 覆盖 ignore、格式白名单、大小限制、chunk、模型和索引策略。未知字段必须报错；secret 不写入普通配置；每个 Revision 保存 effective config digest。

Engine 持有 single-writer ownership；多个窗口、agent 和客户端可以并发发请求，由 Engine 串行化 catalog mutation 并调度后台处理。不得让多个进程直接打开同一个 SQLite/Milvus Lite/data directory。

Runtime Interface 从第一天起可远程化：只传可序列化 ID、请求和结果；内部对象、路径和数据库 handle 不跨 seam。

## 14. 交付顺序

| 阶段 | 交付                                                                     |
| -- | ---------------------------------------------------------------------- |
| 0  | 冻结 Engine/Project/LinkedSource/Entry/Revision 与 Link/Copy 语义           |
| 1  | Engine bootstrap、SQLite catalog、Project 隔离、Linked Source `sync_source` |
| 2  | Revision 原子可见性、Canonical Text、grep 和 typed status/error                |
| 3  | Copied Entry object store、BLAKE3 去重、GC                                 |
| 4  | Processor、Evidence、keyword/vector/hybrid、三层缓存                          |
| 5  | CLI、TypeScript client；StashBase 迁移到全局 Engine                           |
| 6  | 按测量结果优化 watcher、hash、分块上传和远端 input                                     |

## 15. 验收底线

* Engine 启动后，Runtime Interface 不出现 `data_dir`、Repository 或底层数据库；

* 一个 Engine 服务多个 Project，任何普通查询都不能越过显式 Project scope；

* StashBase 只触发 `sync_source`，不维护第二份文件树或 hash 账本；

* Linked Source 不完整扫描不会造成批量误删；

* linked 文件在扫描中变化时不会产生“hash 属于 A、Canonical Text 属于 B”的 Revision；

* Copy 不建立外部同步关系；已经 active 的 Copied Entry 在原文件删除后仍可 `read_original`、grep 和 search；

* Linked 原文件在 sync 前突然缺失时，`read_original` 返回 typed error，已有 grep/search 快照不崩；full sync tombstone 后默认查询不再返回该 Entry；

* grep、Canonical Text、Evidence 和 Index Projection 来自同一个 active Revision；

* 从搜索结果读取 linked 原文件时可以用 `expect_revision` 阻止旧 Evidence 跳到不匹配的新内容；

* pending/failed 新 Revision 不会污染旧 active Revision；

* 相同内容可以物理复用，但不同 Entry identity 和 Project 权限保持独立；

* effective Processor/chunker/model 配置不同不会错误复用缓存；

* Index Projection 可从保存的 Canonical Text 重建；Copied Revision 的 Canonical Text/Evidence 可从 Stored Blob 重建；Linked Revision 只有在 live bytes 仍匹配其 hash 时才能重建，否则必须观察为新 Revision；

* 调用方不需要也不能 patch MFS 内部实现；

* pending linked staging 在 ready、obsolete 或超期后可回收，不变成隐式 Copy；

* `data_dir` 不会被当作 Source 内容递归导入。

## 16. 编码前待确认

| 问题                   | 当前建议                                                   |
| -------------------- | ------------------------------------------------------ |
| Core 语言              | Python 先跑通 Interface 语义，不承诺最终实现语言                      |
| metadata             | SQLite                                                 |
| vector backend       | Milvus Lite，内部 Interface 隔离                            |
| keyword backend      | 原型比较 SQLite FTS 与向量库 BM25                              |
| V1 Project/Source    | 一个 Project 一个 primary Linked Source                    |
| 默认 sync              | 高频触发 incremental；首次 attach、手动 Reindex 和恢复使用 full       |
| Copied Blob GC       | 引用归零后进入宽限期，再异步删除                                       |
| Linked tombstone 保留期 | 默认查询立即隐藏，宽限期后回收旧 Revision/artifact                     |
| 备份单元                 | `catalog + objects + derived` 一致备份；indexes/staging 可排除 |
| 备份协议                 | 由 Engine 提供 quiesce/snapshot；写入进行时直接复制目录不受支持           |
| symlink              | V1 默认不跟随；确需支持时先定义越界与循环策略                               |
| path 比较              | Project 持久化大小写/Unicode 规范，必须与 primary Source 兼容        |
| V1 watcher           | 不提供                                                    |
| mount                | 不进入 V1                                                 |
