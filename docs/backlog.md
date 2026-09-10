# MFS 修复与验收记录

2026-09-09。MFS 侧实现已完成。当前契约见 [design.md](design.md)，
StashBase 应用迁移的映射与边界见 [stashbase-integration.md](stashbase-integration.md)。

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
- [x] **LIFE-002：两个后台线程与全局 ready。** 准备/索引分开，单 Milvus writer；
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
本地验证：macOS / Python 3.13.12，46 个测试通过（85 秒）；Ruff、格式检查、Pyright、
`uv lock --check --offline` 和 `git diff --check` 全部通过。7 条警告来自 PDF 依赖的 SWIG 弃用提示。
Linux 分支由 CI 配置覆盖，本轮未在 Linux 主机执行。

## 后续应用工作

- [ ] **INTEGRATION-001：StashBase 应用迁移。** 按对接说明替换 daemon API，注入实际转换器，
  移除 Node/daemon 重复的 Preparation/indexing 状态与调度。MFS repo 的改动不自动迁移另一个 checkout。
- [ ] 运行 StashBase 实际 retrieval eval 和代表性 corpus，测 grep/搜索延迟、索引吞吐及失败恢复成本。

后续可按实际需求增加在线 GC、namespace 独立 SyncPolicy、任务优先级或多 worker。
当前产物在下次 open 时 GC，SyncPolicy 为实例级；取消不强杀任意 Python/native 回调。
