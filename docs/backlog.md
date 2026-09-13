# 实施进度

唯一设计依据：[MFS 设计](design.md)。这里只记录差距和验证结果，不重复设计正文。

## 当前边界

- [x] **LIVENESS-005** External root 的 resolve/stat 在生命周期锁外完成；慢根观察不阻塞其他 namespace 的状态和 quiesce。
- [x] **CONFIG-005** 索引开关以最新候选为准，连续切换及重开收敛到最后请求；过时执行在阶段/协作检查点退出，实际退出前保留占用。
- [x] **STORAGE-005** SQLite 最多 8 个连接，按查询/事务借用；调用线程退出无需宿主回收，流式读取不跨调用方处理持有连接。
- [x] **TIMEOUT-005** 后台阶段默认 300 秒，到期失败并拒绝迟到提交；close 默认等待 30 秒，超时后继续清理并保留实例锁。进程内不可中断调用由宿主终止 daemon 回收。
- [ ] **BACKEND-005** Milvus Lite 3.2.1 的 BM25 按 segment 计算 IDF/avgdl，排名受 flush 边界影响；等待上游修复，不在 MFS 自建 BM25。确定性后端一致性测试保留为严格 xfail，升级后必须重新验收。
- [x] **CACHE-006** 处理缓存 I/O 移出生命周期锁；返回前复核条目与引用并取得 pin，GC/替换竞争退回重算。
- [x] **PAUSE-006** 候选索引按实际模式遵守暂停；off → bm25/hybrid 等待恢复，关闭索引仍可清理完成。
- [x] **SCAN-006** sync 在目录项和 hash 分块间响应 close，返回 incomplete，保留未观察文件。
- [x] **EMBED-006** Embedder 移除对象并发门与资源准入；后台和查询各自受执行池限制，实现负责线程安全及服务约束。

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
- [x] **REF-002** 按 [设计第 11 节](design.md#11-内部职责与状态迁移) 重构 Lifecycle、Worker 和执行模块：有类型的执行许可/结果、集中生命周期事务、显式模块依赖、移除对整个 MFS 私有状态的穿透。
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
- [x] **SCHED-001** [单 worker 协作让出](design.md#多-worker-与资源准入)已实现：checkpoint 持久化与丢确认核对、统一完成/退休、活跃执行许可验证、不可撤回的让出意图、失败预算保留、可运行任务优先级/老化、捕获 Adapter、索引 epoch 和查询 collection 租约。退出前保留 GC 保护，连续完成事务故障停止调度并可重开恢复。多文件资源并发和宿主自动完成通知仍为方案 B/C，不在本次实施范围。

实施调度前验收：完整回归 **98 passed、3 skipped**（216.70 秒）；跳过的是 Windows 原生句柄测试。`ruff check`、`ruff format --check`、strict `pyright` 与 `git diff --check` 通过。新增 stat 用例先复现重复 hash 和回调覆盖新观察，再验证修复；checkpoint 取消/重开与进程终止恢复专项一并通过。该次验收尚不包括后来新增的调度测试。

实施后最终验收：完整回归 **119 passed、3 skipped**（276.38 秒），`ruff check`、`ruff format --check`、strict `pyright` 与 `git diff --check` 通过。新增 21 个用例覆盖执行退出前替换/取消重试/drop/reindex/close、finally 吞让出或抛异常、超时查询与重建/drop 的租约、checkpoint 与重建提交后丢确认、迟到索引写入、失败预算、暂停资格、等待老化、连续事务故障及重开恢复，以及重命名后跨重开复用、缓存损坏和 LRU 淘汰。Windows 原生句柄测试仅在原生 Windows 上执行；PDF 依赖的 7 条 SWIG 弃用警告未影响结果。

## 并发与接入边界修复（2026-09-12）

依据 [本次审查](review-2026-09-12.md)，用户确认后的实施范围：

- [x] **RECOVERY-003** 状态命令提交后确认失败统一核对；配置绑定跟随持久结果；无法核对时停止调度/发布，重开恢复。
- [x] **SOURCE-003** 源身份、hash、变动时间保护 checkpoint/结果及缓存；索引读取核对准备文字 hash，阻止跨阶段源变化；处理缓存格式升级。
- [x] **PROCESS-003** POSIX 父死监督、进程组实际退休与继承实例锁；支持冻结 sidecar 的早期监督入口。
- [x] **REBUILD-003** 重建保持用户取消，同时丢弃旧索引阶段与批次，显式 retry 可恢复。
- [x] **WAIT-003** strong/current wait 统一识别失败、blocked、cancelled 终态。
- [x] **GC-003** 工作目录 grep 引用受管理保存，参与持久引用与缓存完整性检查。
- [x] **SYNC-004** 成功观察范围独立清理缺失，失败前缀保守保留。
- [x] **QUIESCE-001** 按 namespace 创建代和路径获取可释放退休租约，等待实际执行及源读取；处理嵌套、超时、迟到读取与同名 namespace 重建。
- [x] **MIGRATION-003** 持久处理暂停与初始失败/取消导入，校验 revision；重开保持暂停，重复导入不撤销用户 retry。

StashBase 本身未作修改。INTEGRATION-001、实际冻结打包、真实 OCR/转录/模型端到端验证与 corpus 吞吐验收仍须在应用接入时完成。方案 B/C 仍保持原范围。

验收：完整回归 **146 passed、3 skipped**（328.61 秒），比修复前新增 27 项。跳过的是当前 macOS 无法运行的 Windows 原生句柄测试；7 条 PDF SWIG 弃用警告不影响结果。新增用例覆盖丢确认及核对失败、源改变后恢复、跨阶段文字校验、SIGKILL 后子进程退出与重开、普通/模拟冻结入口分派、取消后重建、GC、扫描隔离、租约和迁移恢复。

最后收拢接收事务的重复核对、调整扫描覆盖查询及统一 read/grep 行定位后，相关读取/源/扫描/接收/租约专项 **19 passed**。`ruff check`、`ruff format --check`、strict `pyright`、`git diff --check` 通过；本地文档链接均可解析。模拟冻结入口测试不替代 StashBase 的实际打包验收。

## 2026-09-13 并发、配置与恢复重构

用户确认全部实施，并明确保留字段。当前 MFS 库侧实现：

- [x] **STATE-004 / item 1、4** latest target 合并与持久 active_runs；保留 active_run_id、stage/state、attempts、attempt_token。旧调用退出后执行最新目标，旧算子失败不污染新版本；同文件顺序，不同文件并行。
- [x] **EXECUTION-004** 默认 4 Worker、4 查询槽，heavy=1/light=2；Processor/Chunker 先原子申请资源再领取阶段，对象默认串行，具体实现类声明 concurrency 后并发。非阻塞 Admission 支持宿主共享容量，超时不释放实际调用的资源。Embedder 仅受执行池限制，见 EMBED-006。
- [x] **ADMISSION-004 / item 1** Internal 完整落盘再提交 SQLite；复制/待提交文件有 GC pin，原件 rename/fsync 在生命周期锁外。新建父目录同步，后来的 upsert/remove/reprocess 不被旧复制覆盖，丢确认按持久目标核对。External 不保存历史 bytes，不将新内容按旧 hash 缓存。
- [x] **CONFIG-004 / item 2** configure_namespace 一次合并 Processor/Chunker/Embedder/模式变化；有效文字与兼容切片可复用。G0 服务、G1 全员完成后切换；失败/取消保留旧配置。重启可分别绑定两套实现，连续变更只保留最新候选，退休代有界。旧查询捕获匹配的模型与 publication，真实退出后才清理；后台 collection 创建/退休也参与 namespace 执行屏障。
- [x] **SYNC-005 / item 3** protected/nonmember 集合与有限祖先查询消除剩余 O(N²) 检查；使用已有 SQLite namespace/path 索引，不维护第二份完整树。同 namespace 扫描串行，跨 namespace 并行；遍历/hash 不持全局 mutation 锁，提交复核 incarnation、root、绑定、规则与候选代，旧扫描不删除后来接收的目标。
- [x] **VIS-004 / item 5** 两种 namespace 的受管理派生物集中存放，借用路径仍由宿主保证。删除/更新/排除接收后立即过滤；strong grep 只等文字，strong search 等当前索引配置，均不等已删除文件的物理清理。wait(report) 保留清理等待。精确 cleanup debt 独立重试；GC 使用文件/读句柄 pin。
- [x] **INTEGRATION-004 库侧 / item 6** quiesce 覆盖真实源使用者及 namespace 后台操作；新增 start_paused/resume_background，宿主可在任何执行/GC 开始前恢复磁盘事务。已记录路径锁 → 全范围退休 → 磁盘操作 → 完整持久 sync → 释放的接入合同，后续索引失败不应回滚已接收磁盘操作。
- [ ] **INTEGRATION-001 应用侧** StashBase 当前旧 mfs-cli daemon、实际转换器、共享 RPC grant、宿主持久路径日志及 Viewer 生命周期未改接。该 checkout 本轮未修改，应用端到端和 corpus 验收不能用 MFS 库测试替代。

已修复并覆盖原先三个复现：cleanup 恢复覆盖取消、reindex 等待漏报致命存储故障、无变化 sync 平方级路径检查。历史操作回执仍不恢复。配置改动统一走候选代，reprocess_namespace 返回 ConfigurationReport；显式全 namespace reprocess 可解除取消，普通配置替换不能。

验证：完整回归 **162 passed、3 skipped**（388.88 秒）；随后补齐候选文字状态、index_ready 与 root 打开期间 namespace 重建的窗口，相关并发/等待/租约专项 **28 passed**（79.41 秒）。`ruff check`、`ruff format --check`、strict `pyright`、`git diff --check` 通过，本地文档链接检查通过。

3 项跳过为当前 macOS 不能执行的 Windows 原生句柄测试；PDF 依赖的 7 条 SWIG 弃用警告未影响测试。包含真实 Milvus Lite 和故障注入，不代表实际断电、StashBase 原生转换器或打包应用已验证。未创建 commit。

## 2026-09-13 边界修复与独立复审

- [x] 搜索去重保留 snapshot 身份；后端锁等待计入同一 deadline，超时排队请求不再启动。
- [x] grep 总期限覆盖全部阶段，来源定位按有序区间查找；单文件不可用返回结构化 failures 和 truncated；grep 执行池独立于排名搜索。
- [x] 安全打开源/借用文字，拒绝读取期间的链接/特殊文件替换；Internal 输入复制同样防止 FIFO 竞争。
- [x] 配置分批补齐成员、同进程故障与丢提交确认恢复、候选维护错误公开及相同配置重试；重绑提交时复核配置代。
- [x] GC 将 SQLite 忙竞争归类为 busy；后台老化不能超过交互优先级。
- [x] 独立复审四项：sniff 不持状态锁且只做一次；临时文字重建重新领取 Processor 额度；晋升连续故障准确达到五次上限；drop 后释放运行绑定和闲置许可引用。

修复前专项稳定复现缺陷；修复后 25 项相关回归通过。独立 agent 对四项原触发机制重新注入均通过，详见 [独立复审](review-2026-09-13-independent.md)。3000 个已接收、处理暂停文件的本机测量：新配置接收 0.00148 秒，接收及其后 0.2 秒内 52 次状态采样的最大耗时 0.01406 秒；这是合成数据测量，不能外推百万文件晋升或网络文件系统。

当前全量回归：23 个测试模块分配到 4 个独立进程，各模块执行一次，合计 **196 passed、3 skipped、1 xfailed**。3 项跳过需要原生 Windows 句柄；严格 xfail 对应 BACKEND-005；7 条 PDF SWIG 弃用警告。`ruff check`、`ruff format --check`、strict `pyright`、`git diff --check` 与本地 Markdown 链接检查通过。StashBase 仍未修改；daemon/RPC、真实格式 Processor、跨进程共享额度、宿主事务/迁移与打包验收仍属于 INTEGRATION-001。
