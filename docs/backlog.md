# MFS 修复与验收记录

2026-09-10 更新。下方保留已有验收记录，并记录本轮修复和后续应用工作。
当前已实现契约见 [design.md](design.md)，StashBase 应用迁移的映射与边界见
[stashbase-integration.md](stashbase-integration.md)；已有验收不表示应用接入缺口已全部补齐。

## 本轮修复（2026-09-10）

用户已确认本轮整体实现，下列项目已纳入修复并落地：

- [x] **FIX-005：大小写重命名。** 不敏感卷上，实际拼写变化会清理旧 SQLite / Milvus 身份；
  保留敏感卷的独立路径。覆盖重命名后的内容修改与查询结果。
- [x] **FIX-006：格式资格预检查。** sync 先用后缀和最多 64 KiB 头部判型；不支持的文件不完整 staging。
  正式接收仍做稳定读取、源身份复核和判型。
- [x] **FIX-007：回执 index_ready。** 新回执统一使用实例 ready，幂等重放保留历史回执。
- [x] **PLATFORM-001：Windows 实现和 CI。** 原生句柄固定身份、重解析点防护、二进制读取、
  设备/alternate-stream 路径防护、实例锁、持久化适配和受管理 Job Object；CI 加入 windows-2025。
  锁定依赖已包含 Windows wheels，Milvus Lite 3.2.1 manifest 已使用 os.replace。
  本机无法执行 Windows 原生测试，原生验收留给 Windows CI，不把 macOS 结果记为 Windows 已通过。
- [x] **API-001：轻量元数据与结构化状态。** content_hash、源大小/mtime、源/文本/索引版本、
  progress、error_detail、附属产物名称；支持 namespace/path 分页（最多 1000）与 scope_status，
  index_configuration 返回实际索引配置，状态读取不加载完整原文或向量。
- [x] **LIFE-003：Preparation 完整生命周期。** ProcessingContext 的取消、进度、持久 checkpoint、
  受管理命令；原子附属产物发布与带租约读取；有界 light/heavy worker、Adapter 并发上限、
  active scope / reprocess 优先级和 aging、checkpoint 让出；用户取消跨新 bytes 和重开保留。
- [x] **LIFE-004：按回执等待。** Mutation/Drop/Sync 的 operation_id 与目标持久化，
  wait(receipt) 只等自己的目标及目录替换清理；超时可重用，失败及时报告，旧目标被覆盖返回 Superseded。
  删除优先且可在索引配置 mismatch 时执行；已有单 writer 约束保持。
- [x] **CACHE-001：内部内容复用。** PROCESS/CHUNK/EMBED 成功产物缓存、checksum 与失效回退；
  跨路径推理复用但保留独立身份。路径无关必须由具体 Adapter 声明；reprocess 绕过 PROCESS 缓存。
- [x] **STORE-001：低频在线 GC。** 实例维护线程或宿主 collect_garbage；强引用、读取租约、
  删除认领、宽限期、逐项 orphan inventory 和软预算；主链路不做 sweep，失败不更改任务状态。
  取消/失败 checkpoint 和稳定输入保留，open 完成引用恢复后才允许维护。

实现契约见 [生命周期补齐](lifecycle-extensions.md)。不增加 query cancellation 或 pause/resume。
StashBase 的应用状态和 daemon API 迁移仍位于下方“后续应用工作”，本轮不修改另一个 checkout。

## 已完成

- [x] **FIX-001：祖先 ignore 一致生效。** root、子目录、exact file 同一套排除规则；
  被排除的旧投影可清理，不再绕过父目录规则入库。
- [x] **FIX-002：directory → file 不提前删除 descendants。** PROCESS 失败保留旧文本和索引；
  新文本提交后 grep 切换，Milvus descendants 等新文件完整索引发布后再清理。
- [x] **FIX-003：大小写别名 reconcile。** 观察卷的大小写 lookup 行为、保留真实路径拼写，
  requested/seen/missing 使用一致身份比较；测试根据实际卷验证对应分支。
- [x] **FIX-004：`sync(verify="content")`。** 发现等长且恢复 mtime 的修改；exact file 始终校验 hash，
  未变内容不重跑 Processor/Embedder。
- [x] **ALIGN-001：symlink 与 StashBase 语义扫描对齐。** 接受并保留 root alias，按次 resolve，
  retarget 完整 reconcile；内部目录链接不递归，内部文件链接按 root 内真实路径去重，
  越界/环跳过，broken root 不触发批量删除。
- [x] **SEARCH-001：结构化过滤全部下推。** Namespace、ID、UnderPath、路径/名称前后缀、
  源后缀/类型以及 AnyOf 多 Folder 范围在 Milvus 每路 top-k 前执行。
  命中自带文本、快照和定位，ranked search SQLite 访问为零。
- [x] **SEARCH-002：精确文本独立。** SQLite 文档级 literal/RE2、smart-case、Unicode whole-word、
  跨 Chunk 匹配与 SourceLocation；空 Catalog 也验证非法正则，point query 使用主键筛选。
- [x] **LIFE-001：持久任务与阶段恢复。** SQLite 目标/状态、稳定输入、成功 OCR/embedding 批次产物、
  幂等回执、retry/reprocess/cancel、轻量状态查询及 executing；BM25/dense 同表完整发布。
- [x] **LIFE-002：准备/索引分离与全局 ready。** 准备/索引分开，单 Milvus writer；
  strong 用 Condition 等 ready，eventual 直接搜；无搜索读写锁或两路同快照要求。
- [x] **Python 3.13。** 依赖锁、Ruff/Pyright 和 macOS/Linux CI 配置对齐。

## 故障与并发验证

- [x] 真正终止子进程：SQLite 接收提交前、接收提交后但 ACK 前、OCR 产物持久化后、Milvus 发布成功后。
- [x] SQLite 文本提交失败复用已保存 OCR；Milvus 成功但完成状态失败按相同主键重放。
- [x] 丢失 ACK 后当前实例恢复 pending，相同 idempotency key 重开后返回原回执且不覆盖后来的版本。
- [x] 旧 revision / 取消前的旧 attempt 不清除新 pending；取消后可 retry。
- [x] namespace 删除/立即重建、失败清理的状态与 retry、异步文档删除阻止错误 ready。
- [x] dense 卡住时其他文档可完成 PROCESS；grep 新文本、eventual 旧索引，查询 embedding 不被后台锁住。
- [x] strong 准入后允许写入继续；close 唤醒等待者并回收线程；超时不取消任务。
- [x] 后端 cause 链自引用不会使错误分类死循环；跨进程重开显式 load collection。
- [x] V1 Catalog 与缺失索引从 SQLite 快照恢复，不重新 OCR。
- [x] 真实 Milvus Lite 的完整 rows 重放、BM25/vector/hybrid 下推、16,500 行跨段扫描与 reopen。

对应测试：`test_sync_regressions.py`、`test_search_filters.py`、`test_lifecycle.py`、
`test_recovery.py`、`test_publication.py` 与 `test_backend_conformance.py`。
历史基线：macOS / Python 3.13.12，46 个测试通过。本轮新增验收见 test_extensions.py、
test_windows.py；包含 checkpoint 真正跨进程恢复、回执跨重开、取消门、同内容复用、GC 租约和删除认领。
本轮完整验证：macOS / Python 3.13，65 passed、3 skipped（Windows 原生用例），109.64 秒；
7 条警告来自 PDF 依赖的 SWIG 弃用提示。随后补做 GC 链接清理的针对性测试通过，外部文件保持完整。
Ruff、格式检查、Pyright（当前平台及 Windows 分支）、直接与开发依赖的精确版本核验、
git diff --check 全部通过。Windows/Linux 原生运行由 CI 矩阵覆盖，本轮未在这两个平台执行。

## 后续应用工作

- [ ] **INTEGRATION-001：StashBase 应用迁移。** 按对接说明替换 daemon API，注入实际转换器，
  移除 Node/daemon 重复的 Preparation/indexing 状态与调度。MFS repo 的改动不自动迁移另一个 checkout。
- [ ] 运行 StashBase 实际 retrieval eval 和代表性 corpus，测 grep/搜索延迟、索引吞吐及失败恢复成本。

SyncPolicy 仍为实例级；namespace 独立策略可按应用需要评估。任意 Python 回调需要协作取消，
context.run_process 管理的子进程可由 MFS 清理。
