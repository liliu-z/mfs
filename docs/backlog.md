# 实施进度

唯一设计依据：[MFS 设计](design.md)。这里只记录差距和验证结果，不重复设计正文。

## 本轮实现

- [x] **EXT-001** External 零复制；引用式文字输入/输出；SQLite 和恢复文件不重复保存正文；GC 只处理受管理文件。
- [x] **VIS-001** 源替换/删除/排除后立即撤销旧结果；持久清理旧代；失败、迟到结果、重启和 reindex 不复活旧源。
- [x] **NS-001** namespace 独立适配器清单与 fail-fast 绑定；每 namespace 一个 Milvus collection；独立索引模式/暂停。
- [x] **IGNORE-001** namespace 有序规则、增删改/排序及版本冲突检查；扫描、读取和提交统一资格判断。
- [x] **API-002** 移除 query 及公开 Query 类型，公开有界 grep 和明确 read，迁移示例/测试。
- [x] **REF-001** 单后台处理 worker + GC；每文件最新目标；集中生命周期事务并拆分执行/读取职责；受管理文件按 namespace 组织。
- [x] **ROOT-001** External root 不得与 MFS 状态目录重叠，保护源文件；原跨 namespace 禁止重叠的限制已由 ROOT-002 撤回。
- [x] **PROCESS-001** 保留 UTF-8/PDF，补基础 DOCX；支持应用提供的已有文字引用和增强处理。
- [x] **VERIFY-001** 行为回归、真实 Milvus 验证、重启/崩溃恢复、格式和类型检查。

## 验证记录

2026-09-11，本轮实现验证：

- 完整回归：**79 passed，3 skipped**；跳过的是 Windows 原生句柄测试，当前机器为 macOS。
- 最后调整 off/bm25 重开时省略 Embedder 后，namespace 与补充合同测试：**14 passed**。
- `ruff check`、`ruff format --check`、strict `pyright` 和 `git diff --check` 全部通过。
- 包含真实 Milvus Lite、不同维度共存、崩溃恢复、取消/清理重试、External 零复制、规则、DOCX 和 HTML 路径验证。
- PDF 依赖报告 7 条 SWIG 弃用警告，未影响测试。

实现前基线为 65 passed、3 skipped；不以旧基线替代本轮验收。

## 应用后续工作

- [ ] **INTEGRATION-001** StashBase daemon 迁移及实际转换器适配；沿用应用的输入选择、播放转换与 grep fallback。
- [ ] 在 StashBase 的代表性 corpus 上运行 retrieval eval 和端到端恢复验证。

READY-001 不在本轮范围：由 StashBase 使用已有状态选择 fallback，不另建 ready 系统；历史回执的简化归 RECEIPT-002。

## 2026-09-12 审视后的修复

- [x] **SEARCH-DEFAULT-001** search 默认 consistency="eventual"、timeout=5.0 秒；timeout 覆盖调用方的搜索总等待，阶段边界复用 deadline，后端接收剩余时间。慢 Adapter 期间也能超时返回，迟到执行保留容量/租约直到结束，不开始后续阶段。
- [x] **ROOT-002** 允许不同 namespace 使用相同或嵌套 External root；创建、根重定向和迁移均保留独立配置，仅拒绝与 MFS 状态目录重叠。
- [x] **TEXT-002** 统一 UTF-8 文字引用的换行读取和字节偏移语义，修复 CRLF TXT/Markdown 被内置 Processor 校验拒绝；覆盖 Internal/External、grep/read/search 与 SourceMap。
- [x] **RECEIPT-002** 按文件当前状态恢复待办，移除每次 sync/upsert 的永久操作回执及其历史等待依赖。保留文件粒度的删除标记、更新中的旧索引清理责任、当前任务和必要恢复数据；更新覆盖同一文件目标，不拆成独立 delete/insert 事件。重建完成判断读取当前任务，避免历史成功误判。已完成 schema 6 迁移；wait 使用当前文件/范围，返回值移除 operation_id，显式 idempotency_key 去重保留。
- [x] **SYNC-002** 去除首次/content sync 对每个文件重新枚举完整父目录的平方级开销；保留路径身份、大小写、符号链接和并发变动校验。已确认作为实现 bug 修复，不增加公开配置。
- [x] **REF-002** 按 [设计第 11 节](design.md#11-内部重构与当前状态迁移ref-002--receipt-002) 重构 Lifecycle、Worker 和执行模块：有类型的执行许可/结果、集中生命周期事务、显式模块依赖、移除对整个 MFS 私有状态的穿透。
- [x] **CONFIG-001** 补 namespace_configuration(namespace) 只读完整配置，包括 indexing、paused、Processor/Chunker/Embedder 清单和待生效配置；用于 StashBase 重启恢复 Folder 设置，不包含运行对象或凭据。

item 9 已确认不缺 MFS interface：StashBase 提供的 Processor 自己完成提取，MFS 随后切片/索引；撤销 INTEGRATION-002，实际应用改接统一归 INTEGRATION-001。

当时目录级 strong 和处理单元完成后按优先级调度仍为[讨论方案](design.md#12-处理调度与后续扩展)。建议暂不做目录级 strong；单 worker 让出可在现有 set_active_scopes 基础上讨论，不等同于恢复 StashBase 原有 light/heavy 并行容量。ProcessingContext.checkpoint/resume 已存在，含义是保存 Processor 自己的中间状态/文件；它不代表 MFS 已实现音频分段或抢占调度。ROOT-002 已实现并验证，继续保留在本表的完成项中。

旧 StashBase 对照：在其现有 Python 环境中调用真实 daemon upsert、旧 Chunker 和 Milvus，以本地确定性 Embedder 验证 CRLF TXT/Markdown 均成功写入。此结果证明旧写入路径没有新 text_path 校验错误，不代表穷尽旧库所有换行/定位行为；本次工作未修改 StashBase 文件。

概念复核：直接调用当前 MFS 和真实 Milvus，以确定性 Embedder 跑通 BM25 → 2 维 model-a → 3 维 model-b，每次重建后 vector 搜索均返回文档。BM25 升级、Context 中间结果的重开恢复和进程终止恢复 3 个现有测试再次通过；此结果证明库内能力，不代表 StashBase daemon 已接入，也不验证真实云端模型效果。

## 本次最终验收（2026-09-12）

- 完整回归：**94 passed、3 skipped**；跳过 Windows 原生句柄测试，当前为 macOS。
- 最后合并文字引用读取、校验旧 operation ID 和整理关闭路径后，CRLF/恢复/当前合同/搜索超时专项 **20 passed**。
- `ruff check`、`ruff format --check`、strict `pyright`、`git diff --check` 通过。
- 新增覆盖：CRLF+BOM 与 UTF-8 字节定位、80 文件目录枚举次数、未绑定时配置读取、当前重建等待、schema 5 历史表升级、更新/删除事务后进程终止及 idempotency 重放。
- 已确认的 MFS 修复项全部完成；StashBase 应用迁移、目录级 strong 和协作抢占仍按上述范围处理。

## 单 namespace 与调度复核（2026-09-12）

- [x] **SEARCH-SCOPE-001** search/grep 必须显式指定一个 namespace；删除 ByNamespace、跨 namespace 路由与排名合并。ByDocumentId、UnderPath 和嵌套 AnyOf 不能越出指定范围；strong 只等待这个 namespace，query embedding 只执行一次。公开调用示例和测试已迁移。
- [x] **SYNC-003** 内容 hash 相同但 stat 改变时，只更新观察 stat，保留 revision 和任务进度。阶段提交、进度和 checkpoint 回调合并最新目标，不能用执行开始时的快照覆盖新观察；覆盖处理/embedding 期间刷新和重开后的 stat 快速路径。
- [x] **REF-003** 删除 MFS facade 中重复的删除、取消、重试、规则更新和重建状态逻辑，统一由 Lifecycle 持有事务与状态变化。
- [x] **REUSE-002** External 重命名/复制复用经过验证的完整向量；缓存独立于搜索行删除，持久化、校验和、32 MiB 逻辑容量/LRU 淘汰，按 incarnation/epoch/模型/片段 hash 隔离。显式重建/drop 清理缓存；schema 7 自动迁移，不新增 rename 方法。见 [External 重命名与计算复用](stashbase-integration.md#external-重命名与计算复用)。
- [x] **SCHED-001** [单 worker 协作让出](design.md#单-worker-协作让出已实现)已实现：checkpoint 持久化与丢确认核对、统一完成/退休、活跃执行许可验证、不可撤回的让出意图、失败预算保留、可运行任务优先级/老化、捕获 Adapter、索引 epoch 和查询 collection 租约。退出前保留 GC 保护，连续完成事务故障停止调度并可重开恢复。多文件资源并发和宿主自动完成通知仍为方案 B/C，不在本次实施范围。

实施调度前验收：完整回归 **98 passed、3 skipped**（216.70 秒）；跳过的是 Windows 原生句柄测试。`ruff check`、`ruff format --check`、strict `pyright` 与 `git diff --check` 通过。新增 stat 用例先复现重复 hash 和回调覆盖新观察，再验证修复；checkpoint 取消/重开与进程终止恢复专项一并通过。该次验收尚不包括后来新增的调度测试。

实施后最终验收：完整回归 **119 passed、3 skipped**（276.38 秒），`ruff check`、`ruff format --check`、strict `pyright` 与 `git diff --check` 通过。新增 21 个用例覆盖执行退出前替换/取消重试/drop/reindex/close、finally 吞让出或抛异常、超时查询与重建/drop 的租约、checkpoint 与重建提交后丢确认、迟到索引写入、失败预算、暂停资格、等待老化、连续事务故障及重开恢复，以及重命名后跨重开复用、缓存损坏和 LRU 淘汰。Windows 原生句柄测试仅在原生 Windows 上执行；PDF 依赖的 7 条 SWIG 弃用警告未影响结果。
