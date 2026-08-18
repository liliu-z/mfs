# MFS 设计

> 状态：讨论稿。本文件是设计与实现的唯一依据。
> 事实来源、决策理由和替代方案记录在 [decision-log.md](decision-log.md)。

## 1. 这是什么

MFS 是一个**不 mount、不 POSIX 的文件数据层**。应用把文件交给 MFS，MFS 负责落盘、提取、索引和检索；应用自己不碰磁盘。

不 POSIX 不是妥协，是前提。正因为不受 syscall 形状约束，MFS 才能提供 POSIX 给不了的东西：

* 写入带 revision precondition，不是 `O_EXCL` 这种粗粒度
* 一次调用同时完成落盘和索引，不存在"写成功但索引失败"
* 富元数据和处理状态随文件一起查询
* 按内容检索（grep / keyword / 向量 / 混合），不用伪装成 `read()`

原文件仍是普通文件，Finder、git、VS Code 照常可用。MFS 拥有的是**写入路径和账本**，不是一套私有存储格式。

## 2. 边界

| MFS 负责 | 应用负责 |
| --- | --- |
| 文件落盘、移动、删除 | UI、编辑器、窗口、tab |
| 格式识别与提取（PDF/OCR/DOCX/转录） | 预览与播放 |
| chunk、embedding、索引 | 搜索结果的展示与跳转 |
| grep / keyword / semantic / hybrid | 用户确认、进度与错误文案 |
| 处理状态与 readiness | 产品元数据（排序、最近打开、描述） |
| 外部改动的 sync | **决定什么时候调 sync** |

> MFS 管文件如何变成可检索状态；应用管用户如何使用文件。

## 3. 核心模型

```text
Library
  └── Source (managed | observed)
        └── Entry
              ├── Derived Representation   （提取出的规范文本）
              └── Chunk → Index Projection （可重建）
```

| 概念 | 含义 |
| --- | --- |
| Library | 可统一浏览和检索的一组 Source |
| Source | 注册的目录。`managed` = 写入走 MFS；`observed` = 只读跟随 |
| Entry | Source 中的文件或目录 |
| Derived Representation | PDF 文本、OCR、DOCX、transcript 等提取结果 |
| Processor | 把 Entry 变成 Derived Representation 的实现 |
| Evidence | 命中位置：行号、页码、区域、时间戳 |
| Readiness | 提取和索引是否覆盖当前内容 |

**Source 的两种 mode 语义完全分开**，不做"既权威又跟随"的中间态——那是绝大部分复杂度的来源：

* `managed`：所有写入经过 MFS。不 watch，不周期 scan。
* `observed`：目录权威（别人的 git repo、Downloads）。只读，靠 sync 跟随。

## 4. 接口

```python
# 生命周期
attach(spec) -> SourceRef          # mode: managed | observed
detach(ref)
close()

# 写入（仅 managed）
write(path, bytes|str, expect=revision) -> Commit   # 内容在内存：编辑器保存
import_file(src_abs, dest, mode=copy|move) -> Commit # 文件已在盘上：上传、拖入
open_write(path) -> Writer                           # 流式：大文件
delete(path) -> Commit
move(src, dst) -> Commit

# 读取
read(path, range=None) -> bytes            # 原始字节
read_text(path) -> CanonicalText           # 提取后的规范文本 + Evidence
stat(path) / list(path)

# 检索
search(query, mode="grep"|"keyword"|"semantic"|"hybrid", ...) -> Results

# 状态与同步
status(scope) -> Status                    # readiness、pending、失败
sync(source) -> SyncDiff                   # 外部改动的唯一入口
poll(cursor) -> Events                     # 提取/索引完成通知

# 扩展
configure(profile)
register_processor(processor)
```

三个写入入口不能合并：`write` 会把 2 GB 视频读进内存，`import_file` 不会；`open_write` 服务边生成边写的场景。

调用方看不到 collection、chunk schema、向量、embedding 维度或派生缓存路径。

## 5. 写入路径

```text
write(path, bytes, expect=revision)
  ├── 检查 path containment、revision precondition
  ├── 写 temp → fsync → rename            （单文件原子）
  ├── 算 content_hash
  ├── 事务提交 entry 表
  └── 排处理任务，返回 { revision, indexing: pending }
```

**单文件写必须原子；多文件不承诺原子。** 真实场景里没有多文件"全或无"的硬需求——最接近的是改名时级联更新链接，中断的后果是几个链接需要修，不是数据丢失。因此不引入 WAL 事务、CAS 或 commit log。

崩溃恢复靠三件小事，不靠全盘扫描：

| 残留 | 处理 |
| --- | --- |
| 半写的 temp 文件 | 启动时清理 temp 目录 |
| `task.state = processing` | 重置为 pending。处理输入是 `content_hash`，幂等，重跑安全 |
| 磁盘操作完成但事务未提交 | `intent` 表记录意图；启动时对单条前滚或回滚 |

## 6. 缓存与复用：三层 hash

这是 MFS 相对现有实现最大的性能差异来源。

| 层 | hash | 存放 | 作用 |
| --- | --- | --- | --- |
| 源文件 | `content_hash` = 字节 BLAKE3 | `entry` | sync 判断变化；派生缓存 key；rename 按 hash 配对 |
| 派生文本 | `text_hash` = 提取结果 hash | `derived` | Processor 升级后重提取，文本没变则跳过全部下游 |
| chunk | `chunk_hash` = chunk 文本 hash | `chunk` | embedding 复用 key |

**关键规则：缓存 key 只用内容，位置信息（路径、行号）是 payload。** 一旦把路径或行号写进 key，在文件开头插一行就会让后面每个 chunk 的 key 全变，导致全文重新 embedding。

由此得到：

| 操作 | 结果 |
| --- | --- |
| 改一行 | 只重新 embedding 受影响的 chunk |
| 改名 / 移动 | 派生和向量全部复用 |
| 复制文件 | 全部命中 |
| 内容改回去 | 全部命中 |
| 跨文件相同段落 | 互相命中 |

向量不额外存一份：命中时从向量库内部拷贝已有行，SQLite 只记 `chunk_hash → 所在 collection`。

## 7. 内容处理与扩展

```python
class Processor(Protocol):
    id: str
    version: str
    def supports(entry, profile) -> Match: ...
    def process(request) -> ProcessResult: ...
```

`ProcessResult` 含 Derived Representation、Evidence map、内容类型、语言、元数据、warning。Processor 版本或有效配置变化时，相关派生内容失效。

模型按能力拆分：`EmbeddingProvider` / `TextGenerationProvider` / `VisionProvider` / `TranscriptionProvider`。

**Provider 契约要薄，外层要厚。** 实现者只写 `embed(texts) -> vectors`；timeout、重试、退避、批切分、token 上限、取消全部由 MFS 在外面包一层。契约太薄而外层缺失，调用方就会绕过它自己实现一份——这是现有实现已经发生过的事。

### 配置还是注册

判据是：**这段实现的分发权在谁手里。**

| 情况 | 用配置 | 用注册 |
| --- | --- | --- |
| MFS 自己带得了，只是参数不同 | ✅ | |
| 需要外部二进制或模型文件，由应用打包分发 | | ✅ |
| 需要应用的运行时上下文（用户选择、密钥托管、UI 取消） | | ✅ |
| 换实现要改 MFS 代码 | ✅ 说明本该内建 | |
| 不同应用会给出不同实现 | | ✅ |

> 一句话：MFS 能 `pip install` 进来的是配置；要应用把二进制递过来的是注册。

据此：索引规则、chunk 参数、模型选择、timeout/retry 是**配置**；PDF 引擎和 OCR 是**配置**（内建可选）；音视频转录是**注册**——whisper.cpp 与 FFmpeg 需要三平台交叉编译并随应用分发，MFS 接管不了这条链。

注册的形态因此不是"传一个 Python 对象"（跨语言传不了，且转录器是可执行文件），而是进程契约：

```python
register_processor(SubprocessProcessor(
    id="whisper", version="1.7.4",
    formats=[".mp3", ".m4a", ".wav", ".mp4"],
    cmd=[whisper_cli, "-m", "{model}", "-f", "{input}", "--output-json"],
    timeout=..., cancel="SIGTERM"))
```

能力在 MFS（调度、缓存、超时、取消、错误隔离），二进制在应用。覆盖内建 Processor 必须显式声明；配置不能直接执行任意 shell 命令。

### 格式计划

| 阶段 | 格式 | 产出 |
| --- | --- | --- |
| 首批 | Markdown、TXT、代码、脚本 | 规范文本 + 行号 |
| 首批 | JSON、YAML、TOML、CSV | 文本/记录 + 行号 |
| 首批 | HTML | 正文文本 |
| 首批 | PDF | Markdown + 页码 |
| 首批 | DOCX | 文本 + 段落 |
| 迁移前 | PNG/JPEG/WebP | OCR 文本 + 区域 |
| 迁移前 | 音频、视频音轨 | transcript + 时间戳 |

白名单只控制是否自动处理。未知格式仍是 Entry，仍可管理和 grep，只是标记 `unsupported`。

## 8. 查询

```text
list / stat / read        -> 真实文件系统
grep                      -> 原文 + 派生文本（正则/精确）
keyword                   -> keyword 索引
semantic                  -> query embedding + 向量索引
hybrid                    -> keyword 与向量融合
```

**grep 必须在 MFS 内部。** 派生文本由 MFS 拥有，应用无从知道该 grep 哪些文件；把 grep 留在外面会强迫应用重新实现一遍派生路径映射。

| Freshness | 含义 |
| --- | --- |
| `live` | 当前原文件；用于浏览和文本 grep |
| `available` | 立即查现有索引，同时报告 pending/failed 覆盖度 |
| `at_least(commit)` | 等索引覆盖到指定 commit |
| `fresh` | 等查询发起时的状态处理完成或超时 |

结果返回 EntryRef、snippet、EvidenceRef、位置、score、匹配类型；不返回向量、chunk id 或存储层行。

grep 是精确匹配，向量检索是近邻搜索，两者不能伪装成同一个操作。

## 9. 存储布局

```text
<state_root>/
  catalog.sqlite      ← 唯一的元数据账本
  derived/<content_hash>/
  vectors/            ← 向量库
  locks/  logs/
```

```sql
CREATE TABLE entry (
  source_id TEXT, rel_path TEXT, size INTEGER, mtime INTEGER,
  content_hash TEXT, state TEXT, updated_at INTEGER,
  PRIMARY KEY (source_id, rel_path));
CREATE INDEX idx_entry_hash ON entry(content_hash);   -- rename 配对

CREATE TABLE derived (
  content_hash TEXT, processor_id TEXT, processor_version TEXT,
  text_hash TEXT, blob_rel TEXT, created_at INTEGER,
  PRIMARY KEY (content_hash, processor_id, processor_version));

CREATE TABLE chunk (
  source_id TEXT, rel_path TEXT, chunk_index INTEGER,
  chunk_hash TEXT, start_line INTEGER, end_line INTEGER, content_type TEXT);
CREATE INDEX idx_chunk_hash ON chunk(chunk_hash);     -- embedding 复用

CREATE TABLE task (
  id TEXT PRIMARY KEY, source_id TEXT, rel_path TEXT, kind TEXT,
  input_hash TEXT, state TEXT, attempts INTEGER, error TEXT, updated_at INTEGER);

CREATE TABLE intent (
  id INTEGER PRIMARY KEY, op TEXT, payload TEXT, created_at INTEGER);
```

**向量库只存向量和最小可过滤字段**（`source_id`、`rel_path`、`chunk_hash`、BM25 文本）。文件元数据一律进 SQLite。

理由：向量库擅长批量扫描和近邻检索，不擅长点查和事务。把 `file_hash`、`is_dir`、`embed_status` 这类元数据放进向量库，会让"列出所有文件的 hash"这种最基本的操作退化成全表扫描——现有实现正因此踩到分页缺陷并需要打补丁。

派生结果按 `content_hash` 寻址，因此不需要额外的反向索引来支持删除清理：`entry` 表本身就是那张索引。

## 10. 外部改动

`sync(source)` 是外部改动的唯一入口：

```text
sync:
  walk 目录 → 先比 size/mtime → 只对疑似变化的文件算 content_hash
  与 entry 表比对 → added / modified / deleted
  按 content_hash 配对 → renamed（派生与向量全部复用）
```

**MFS 默认不 watch，也不决定何时 sync。** "用户什么时候需要新鲜数据"是产品知识，不是存储层知识。窗口 focus、打开目录、agent 回合结束这类触发点由应用选择。

MFS 的责任是让 sync 足够便宜：`entry` 表存了 `size`/`mtime`，扫描成本从 O(总字节) 降到 O(文件数)，只有疑似变化的文件才读内容。

可选 watcher 后续再评估，默认关闭，且只降低延迟、不承担正确性。managed Source 正常情况下不需要 sync。

## 11. 配置

```text
内建默认 < 用户配置 < Library 配置 < Source Profile < 安全的单次覆盖
```

覆盖 ignore/watch、格式白名单、Processor、大小限制、chunk、模型、索引、并发、日志。未知字段必须报错；secret 不写入普通配置；每个处理任务保存有效配置摘要。

同一个 state root 默认只允许一个 active writer。Library 之上很快需要一个 single-writer 进程（多窗口、多 agent 并发写），因此接口从第一天起就要可远程化：不共享内存对象、显式 handle、请求与响应可序列化。

## 12. 交付顺序

| 阶段 | 交付 |
| --- | --- |
| 0 | 冻结设计与基线 |
| 1 | Core Library：领域类型、SQLite、写入路径、hash/revision、sync、status |
| 2 | 处理与检索：Profile、registry、派生存储、三层缓存、首批 Processor、grep/keyword/vector/hybrid、Evidence |
| 3 | CLI：规范命令、JSON 输出、错误与进度 |
| 4 | Engine 与 TypeScript client；应用迁移，删除重复实现 |
| 5 | 按需扩展：daemon、MCP、更多 Processor/Provider、只读 mount 原型 |

CLI 保留 `ls/tree/cat/grep/search/status` 的体验，为 Source 与文件操作提供无歧义命令。`remove` 默认只取消管理，删除原文件必须显式使用 `file delete`。

## 13. 验收底线

* 路径不能逃逸 Source root
* 单文件写要么完整生效，要么完全没发生
* 写盘与索引不会各自成功或失败——不存在"写成功但索引失败"
* 崩溃后重启能自行恢复，不需要全盘扫描
* 旧任务不能覆盖新内容
* 删除派生与索引后可以完整重建
* 改名、移动、复制不触发重新提取或重新 embedding
* 改一行只重新 embedding 受影响的 chunk
* 未支持格式仍可管理和 grep
* grep 不因索引滞后漏结果
* PDF/OCR/音频命中能定位回原文件
* Processor 与 Provider 可以显式注册和替换
* 应用不需要 monkey-patch 任何内部实现
* Library、CLI、Engine 对同一操作语义相同

## 14. 编码前待确认

| 问题 | 当前建议 |
| --- | --- |
| Core 语言 | Python 先跑通接口语义，不承诺是最终实现 |
| metadata | SQLite |
| vector backend | Milvus Lite，内部 Interface 隔离 |
| keyword backend | 原型比较 SQLite FTS 与向量库 BM25 |
| **写入路径下移** | **建议下移**——消除写盘与索引之间的漂移。代价是应用的保存路径（含冲突检测）需要改造 |
| **转录集成方式** | **建议用 `SubprocessProcessor` 契约**，二进制构建与分发留在应用侧。代价是转录能力在 MFS 内是空壳，需注册后可用 |
| V1 watcher | 不提供，只提供 sync |
| daemon / mount | 不进入 V1 |
