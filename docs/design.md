# MFS 设计

> 状态：讨论稿。本文件是设计与实现的唯一依据。\
> 事实来源、决策理由和替代方案记录在 [decision-log.md](decision-log.md)。

## 1. 定位

MFS 是一个**不 mount、不 POSIX、面向多个 Project 的文件搜索 Repository**。调用方打开一个 `state_root`，得到一个 Repository；Repository 集中保存文件账本、派生内容、搜索索引，以及选择 Copy 时的原始内容副本。

MFS 不是操作系统文件系统，也不负责 Finder、编辑器或 arbitrary path 的通用文件管理。它提供的是比“数据库表 + 若干调用”更深的文件搜索接口：调用方交付文件或链接，MFS 隐藏内容寻址、去重、提取、索引、精确检索、缺失回退和状态管理。

StashBase 是使用 MFS 的多 Project File Search Engine 产品：

* StashBase 管 Project、Source、扫描触发、UI、预览和产品元数据；

* MFS 管持久账本、内容策略、处理流水线、索引和查询；

* 外部普通目录仍可被 Finder、Git、VS Code 等工具直接使用；

* Copy 内容进入 Repository 后，不再依赖原始路径存活。

## 2. Repository 粒度与拓扑

`state_root` 定义一个物理 Repository。相同 `state_root` 的调用方共享同一份 catalog、对象、派生内容和索引；不同 `state_root` 彼此物理隔离。

```text
repo = mfs.open(state_root)
```

| 拓扑                           | 含义                       | 默认用途         |
| ---------------------------- | ------------------------ | ------------ |
| 多个 Project 共用一个 `state_root` | 全局 Repository            | StashBase 默认 |
| 每个 Project 使用独立 `state_root` | Project-local Repository | 强隔离、独立迁移或备份  |

共享 Repository 不等于默认跨 Project 可见。所有普通操作必须带 `project_id`，跨 Project 查询必须显式给出允许访问的 Project 集合。

```text
Repository(state_root)
  ├── Project A
  │     ├── Source A1
  │     └── Entry...
  └── Project B
        ├── Source B1
        └── Entry...
```

同一个 Repository 默认只允许一个 active writer，或者由一个 single-writer Engine 代表多个调用方写入。任何调用方都可以通过 MFS Interface `add/upsert`，但不得直接修改 `state_root` 内部文件。

## 3. 模块 seam

| MFS 负责                                  | StashBase/调用方负责               |
| --------------------------------------- | ----------------------------- |
| Repository、Project namespace 和 Entry 账本 | 选择 `state_root`，创建和授权 Project |
| linked/copied 内容策略                      | 注册 Source，决定默认策略              |
| 内容 hash、Copy 去重和对象生命周期                  | 枚举外部目录，决定何时开始扫描               |
| 格式识别、提取和 Derived Representation         | 提供应用自带的 native Processor      |
| chunk、embedding、keyword/vector index    | UI、预览、播放、结果展示和跳转              |
| grep / keyword / semantic / hybrid      | 产品元数据、用户确认和错误文案               |
| scan generation、删除对账和 readiness         | watcher/focus/启动/手动等触发策略      |
| linked 内容缺失时的 fallback 状态               | 把 warning 明确展示给用户             |

> StashBase 决定“什么时候看外部世界”；MFS 决定“看到的文件如何成为可用的搜索数据”。

## 4. 核心模型

```text
Repository
  └── Project
        └── Source
              └── Entry (linked | copied)
                    ├── Content Reference
                    ├── Derived Representation
                    └── Chunk → Index Projection
```

| 概念                     | 含义                                                |
| ---------------------- | ------------------------------------------------- |
| Repository             | 由 `state_root` 标识的一套物理账本、对象和索引                    |
| Project                | 查询、权限、生命周期和默认展示的逻辑隔离单元                            |
| Source                 | 一批 Entry 的来源及扫描范围；属于一个 Project                    |
| Entry                  | 由 `(project_id, source_id, source_key)` 稳定标识的文件记录 |
| `source_key`           | Source 内的文件身份，通常是规范化相对路径；不等于内容 hash               |
| Content Reference      | 外部 locator，或 Repository 内部对象引用                    |
| Derived Representation | PDF 文本、OCR、DOCX、transcript 等规范文本                  |
| Processor              | 把 Entry 内容转换为 Derived Representation 的实现          |
| Evidence               | 命中位置：行号、页码、区域、时间戳                                 |
| Readiness              | 提取和索引是否覆盖 Entry 当前已接收内容                           |

### 两种内容策略

`managed/observed` 不再作为 Source mode。MFS 只记录明确的内容策略：

| 策略                      | 原始内容位置                                      | 外部删除后的保证                                    |
| ----------------------- | ------------------------------------------- | ------------------------------------------- |
| `linked`（External link） | 外部 `origin_uri`                             | 索引仍可查；打开或 live grep 可能失败，回退到最后索引文本并 warning |
| `copied`（Copy）          | Repository 的 content-addressed object store | read、grep 和搜索不依赖外部路径                        |

`linked` 不保存完整原始字节，但保存完成搜索和 fallback 所需的最后一次 Canonical Text。对 PDF、DOCX、图片和音视频，它是提取结果，不等于原始二进制；因此 fallback 不能伪装成完整原文件恢复。

`copied` 把原始字节写入对象存储，`origin_uri` 只保留为 provenance。外部文件删除不会自动删除 copied Entry；删除 copied Entry 必须显式执行 MFS `remove`。

Source 可以提供默认内容策略，Entry 保存实际策略。以后可以通过 `materialize(entry)` 把 linked Entry 转成 copied Entry；反向转换会降低可用性，V1 不提供。

## 5. Interface

```python
# Repository / Project
open_repository(state_root) -> Repository
ensure_project(project_id, config=None) -> ProjectRef
drop_project(project_id)
close()

# Source 与全量扫描
attach_source(project_id, spec) -> SourceRef
begin_scan(project_id, source_id) -> Scan
scan.upsert(source_key, input, policy="linked"|"copied") -> EntryRef
scan.commit(missing="tombstone"|"keep") -> ScanResult
scan.abort()
detach_source(project_id, source_id)

# 单文件增量
upsert(project_id, source_id, source_key, input, policy=...) -> EntryRef
remove(project_id, entry_ref)
materialize(project_id, entry_ref) -> EntryRef

# 浏览与内容
stat(project_id, entry_ref)
list(project_id, scope=None)
read(project_id, entry_ref, range=None) -> ContentResult
read_text(project_id, entry_ref) -> CanonicalText

# 查询
search(project_id, query, mode="grep"|"keyword"|"semantic"|"hybrid", ...) -> Results
search_projects(project_ids, query, ...) -> Results

# 状态
status(project_id, scope=None) -> Status
poll(cursor) -> Events

# 扩展
configure(profile)
register_processor(processor)
```

`input` 可以是 MFS 可访问的本地路径，也可以是 byte stream。跨进程或远端调用方不能假设 MFS 能打开调用方的本地绝对路径，必须上传 stream。

调用方看不到 collection、chunk schema、向量、embedding 维度、对象路径或派生缓存路径。

## 6. Add、hash 与去重

StashBase 不维护第二份文件 hash 账本，也不先从 MFS 拉出全量 list/hash。它可以在扫描时对每个文件无条件调用 `scan.upsert`；MFS 根据 `source_key` 找到文件身份，根据内容 hash 判断内容是否重复。

```text
scan.upsert(source_key, input)
  ├── 读取稳定内容并计算 BLAKE3 content_hash
  ├── 查找 (project_id, source_id, source_key)
  ├── hash 相同：标记本轮已看到，其他操作 no-op
  ├── hash 不同：建立新内容引用并排处理任务
  └── copied 且对象已存在：复用 object，不重复存储
```

严格判断内容是否变化必须读取内容并计算 hash。`size/mtime/file-id` 只能作为性能 hint，不能成为跨平台正确性的唯一依据。V1 先保证 hash 正确；watcher、可信 fingerprint、客户端预计算 hash 和分块上传只作为以后优化。

必须区分两个 identity：

* `source_key` 标识“这是哪个文件”；

* `content_hash` 标识“这是哪份内容”。

两个路径下内容相同的文件是两个 Entry，但可以复用同一个 object、Derived Representation 和 embedding。

### 三层 hash

| 层     | hash           | 作用                                      |
| ----- | -------------- | --------------------------------------- |
| 原始内容  | `content_hash` | 变化判断、Copy object key、派生缓存 key、rename 配对 |
| 派生文本  | `text_hash`    | Processor 升级后文本未变则跳过下游                  |
| chunk | `chunk_hash`   | embedding 复用 key                        |

缓存 key 只由内容决定；路径、行号、Project 和 Source 是引用或 payload，不进入内容 key。

## 7. Scan 与删除

MFS 不主动扫描任意外部目录。StashBase/调用方决定扫描触发点，枚举 Source，然后用 scan session 把完整观察结果交给 MFS。

```text
scan = begin_scan(project_id, source_id)     # generation = G

for each external file:
    scan.upsert(source_key, input, policy)   # last_seen_generation = G

scan.commit(missing=...)
```

只有 scan 完整成功后才能 `commit`：

* `missing="tombstone"`：将 `last_seen_generation < G` 的 linked Entry 标记删除；

* `missing="keep"`：保留本轮未见 Entry；copied Source 默认使用该策略；

* 扫描中断、权限错误、Source 根目录未挂载或身份不符：必须 `abort`，不得删除任何 Entry。

StashBase 当前已经验证 event-point reconcile：应用启动、打开/切换目录、窗口 focus、Agent turn end、手动 Sync 和 MCP reindex。V1 不要求 watcher；watcher 以后只能减少延迟，不能替代完整 scan 的删除确认。

同一轮 scan 内可以按 `content_hash` 将一对 delete/add 配成 rename，从而复用派生内容和向量。

## 8. 查询、可用性与 fallback

```text
list / stat             -> MFS Entry 账本
read                    -> copied object，或 linked live origin
grep                    -> 精确匹配；优先当前可用内容，必要时回退 Canonical Text
keyword                 -> keyword index
semantic                -> query embedding + vector index
hybrid                  -> keyword 与向量融合
```

keyword、semantic 和 hybrid 查询只依赖 Repository 内部索引。linked 原文件突然删除不会让搜索请求失败，但结果的新鲜度可能落后于外部目录。

linked Entry 的 `read/grep`：

1. 尝试读取 live `origin_uri`；
2. 若文件缺失、不可访问或已观察到内容变化，使用最后一次成功索引的 Canonical Text；
3. 返回结果必须携带 warning，不能把 fallback 宣称为 live；
4. 如果缓存只覆盖派生文本，则不能承诺完整原始字节或未提取区域。

copied Entry 的 `read/grep` 始终读取 Repository object；外部路径只用于 provenance。

```text
content_status:
  live
  copied
  last_indexed
  source_missing
  source_changed
```

结果返回 EntryRef、snippet、EvidenceRef、位置、score、匹配类型、`content_status` 和 warning；不返回向量、chunk id 或存储层行。

这里必须区分：

* **一致性**：索引、Canonical Text 与 MFS 当前 Entry revision 一致；

* **新鲜度**：MFS 当前 revision 是否等于外部文件最新内容。

MFS 保证前者；linked 模式只能在完成下一次 scan 后保证后者。

## 9. 内容处理与扩展

```python
class Processor(Protocol):
    id: str
    version: str
    def supports(entry, profile) -> Match: ...
    def process(request) -> ProcessResult: ...
```

`ProcessResult` 含 Derived Representation、Evidence map、内容类型、语言、元数据和 warning。Processor 版本或有效配置变化时，相关派生内容失效。

模型按能力拆分：`EmbeddingProvider` / `TextGenerationProvider` / `VisionProvider` / `TranscriptionProvider`。

Provider Interface 要薄，MFS 外层实现要厚。实现者只提供 `embed(texts) -> vectors`；timeout、重试、退避、批切分、token 上限和取消由 MFS 统一管理。

### 配置还是注册

判据是实现的分发权：

| 情况               | 配置     | 注册     |
| ---------------- | ------ | ------ |
| MFS 自己可分发，只是参数不同 | ✅      | <br /> |
| 需要应用打包外部二进制或模型   | <br /> | ✅      |
| 需要应用运行时上下文       | <br /> | ✅      |
| 不同应用给出不同实现       | <br /> | ✅      |

索引规则、chunk 参数、模型选择、timeout/retry 是配置；PDF 引擎和 OCR 是内建可选配置；依赖应用三平台构建链的音视频转录使用 `SubprocessProcessor` 注册。

### 格式计划

| 阶段  | 格式                      | 产出                       |
| --- | ----------------------- | ------------------------ |
| 首批  | Markdown、TXT、代码、脚本      | Canonical Text + 行号      |
| 首批  | JSON、YAML、TOML、CSV、HTML | Canonical Text + 行号/结构位置 |
| 首批  | PDF、DOCX                | Markdown/文本 + 页码/段落      |
| 迁移前 | PNG/JPEG/WebP           | OCR 文本 + 区域              |
| 迁移前 | 音频、视频音轨                 | transcript + 时间戳         |

白名单只控制是否自动处理。未知格式仍可成为 Entry；linked 模式可打开原文件，copied 模式可读取副本，但没有 Processor 时不能承诺内容搜索。

## 10. 存储布局

目标布局：

```text
<state_root>/
  repository.json
  catalog.sqlite
  objects/<content_hash-prefix>/<content_hash>   # 仅 copied 原始字节
  derived/<content_hash>/                        # Canonical Text / Evidence
  vectors/                                       # keyword / vector backend
  locks/
  logs/
```

`state_root` 是 MFS 私有实现目录，不能位于未排除的 linked Source 扫描范围内，否则会递归导入自己的 object、index 和 WAL。

```sql
CREATE TABLE project (
  project_id TEXT PRIMARY KEY,
  config_json TEXT,
  created_at INTEGER
);

CREATE TABLE source (
  project_id TEXT,
  source_id TEXT,
  default_policy TEXT,
  locator TEXT,
  PRIMARY KEY (project_id, source_id)
);

CREATE TABLE entry (
  project_id TEXT,
  source_id TEXT,
  source_key TEXT,
  content_policy TEXT,
  origin_uri TEXT,
  content_hash TEXT,
  object_hash TEXT,
  text_hash TEXT,
  last_seen_generation INTEGER,
  state TEXT,
  updated_at INTEGER,
  PRIMARY KEY (project_id, source_id, source_key)
);

CREATE INDEX idx_entry_content_hash ON entry(content_hash);

CREATE TABLE derived (
  content_hash TEXT,
  processor_id TEXT,
  processor_version TEXT,
  text_hash TEXT,
  blob_rel TEXT,
  created_at INTEGER,
  PRIMARY KEY (content_hash, processor_id, processor_version)
);

CREATE TABLE chunk (
  project_id TEXT,
  source_id TEXT,
  source_key TEXT,
  chunk_index INTEGER,
  chunk_hash TEXT,
  evidence_json TEXT
);

CREATE TABLE scan_run (
  project_id TEXT,
  source_id TEXT,
  generation INTEGER,
  state TEXT,
  started_at INTEGER,
  completed_at INTEGER,
  PRIMARY KEY (project_id, source_id, generation)
);
```

一个物理表可以保存多个 Project，但所有主键、查询和向量过滤都必须包含 `project_id`。只有 object、Derived Representation 和 embedding 可以按内容 hash 跨 Project 物理复用；复用不改变 Project 权限。

SQLite 保存元数据账本；向量库只保存向量、检索文本和最小过滤字段（至少含 `project_id`、Entry locator、`chunk_hash`）。原始 bytes 不进入 SQLite 或向量表。

## 11. 当前 StashBase 基线与迁移目标

当前 StashBase 已经是全局拓扑：一个 Node server 管一个 Python sidecar 和一个全局 Milvus Lite collection，多个打开目录以绝对路径共享它。

当前 macOS 默认位置：

```text
~/Library/Application Support/StashBase/
  vector-store.nosync/milvus.db/   # 当前 MFS/Milvus Lite 数据
  derived.nosync/                  # StashBase 派生内容
  state/state.db                   # StashBase 状态
  models/whisper/                  # 转录模型
```

`STASHBASE_LOCAL_DATA_ROOT` 可以覆盖该根目录。Windows 默认 `%LOCALAPPDATA%/StashBase`，Linux 默认 `$XDG_DATA_HOME/StashBase` 或 `~/.local/share/StashBase`。

当前实现没有 Copy 原文件的 MFS object store，也没有显式 `project_id`；Milvus 行以绝对路径区分目录。迁移目标是在保持“一份全局 Repository、多 Project 共享”的拓扑下：

1. 用显式 Project/Source/Entry 取代绝对路径隐式 scope；
2. 增加 SQLite catalog；
3. 增加可选 `objects/`，只服务 copied Entry；
4. 把派生内容、处理状态和索引纳入同一 Repository Interface；
5. 删除 StashBase 对 MFS 内部模块的直接调用和 monkey-patch。

## 12. 配置与并发

```text
内建默认 < Repository 配置 < Project 配置 < Source Profile < 安全的单次覆盖
```

配置覆盖 ignore、格式白名单、默认内容策略、Processor、大小限制、chunk、模型、索引、并发和日志。未知字段必须报错；secret 不写入普通配置；每个处理任务保存有效配置摘要。

Repository Interface 从第一天起可远程化：不共享内存对象，使用显式 handle，可序列化请求与响应。单机默认 single-writer；多窗口、多 agent 和多个调用方通过同一 Engine 协调写入。

## 13. 交付顺序

| 阶段 | 交付                                                                           |
| -- | ---------------------------------------------------------------------------- |
| 0  | 冻结 Repository/Project/Source/Entry 与 linked/copied 语义                        |
| 1  | `state_root`、SQLite catalog、Project 隔离、Entry upsert、scan generation          |
| 2  | copied object store、BLAKE3 去重、linked fallback 与 warning                      |
| 3  | Processor、Derived Representation、三层 hash、grep/keyword/vector/hybrid、Evidence |
| 4  | CLI、Engine 与 TypeScript client；StashBase 迁移到全局 Repository                    |
| 5  | 按需优化：watcher、客户端预计算 hash、分块上传、远端 object backend                              |

CLI 保留 `ls/tree/cat/grep/search/status` 体验，但语义针对 MFS Entry。`remove` 只移除 Entry；linked Entry 默认不删除外部文件，copied Entry 释放 object 引用并由 GC 延迟回收。

## 14. 验收底线

* 同一 `state_root` 可承载多个 Project，普通查询绝不越过 `project_id`；

* 不同 `state_root` 物理隔离；

* linked 与 copied 的可用性差异在 Interface 和结果状态中明确可见；

* copied Entry 在外部原文件删除后仍可 read、grep 和 search；

* linked Entry 原文件缺失时，搜索请求不失败，grep 回退最后索引文本并返回 warning；

* 不完整 scan 不会造成批量误删；

* 调用方不需要拉取全量 list/hash 才能同步；

* 相同内容只保存一份 copied object，但不同 `source_key` 仍是不同 Entry；

* 删除派生与索引后可从 linked live 内容或 copied object 重建；

* 改名、移动、复制不重复提取或 embedding；

* Processor 与 Provider 可以显式注册和替换；

* 调用方看不到或 patch MFS 内部实现；

* `state_root` 不会被当作 Source 内容递归导入。

## 15. 编码前待确认

| 问题               | 当前建议                                  |
| ---------------- | ------------------------------------- |
| Core 语言          | Python 先跑通 Interface 语义，不承诺最终实现语言     |
| metadata         | SQLite                                |
| vector backend   | Milvus Lite，内部 Interface 隔离           |
| keyword backend  | 原型比较 SQLite FTS 与向量库 BM25             |
| linked fallback  | 保存完整 Canonical Text；不保存完整原始二进制        |
| copied object GC | 引用归零后进入宽限期，再异步删除                      |
| V1 watcher       | 不提供；调用方触发完整 scan                      |
| daemon / mount   | mount 不进入 V1；Engine 在 StashBase 迁移前提供 |
