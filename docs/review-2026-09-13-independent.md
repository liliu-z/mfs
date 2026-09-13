# MFS 与 StashBase 独立复审（2026-09-13）

本报告从两个仓库当前源码和临时故障注入出发，覆盖完整相关模块，不以 git diff 或旧 review 报告作为事实依据。MFS 检查起点 HEAD 为 `a872bde037c55041d9adb20ebed2ea9952c96f00`，并包含其工作区改动。复审 agent 未修改 MFS 实现/测试或 StashBase；唯一仓库写入为本报告。

已阅读 StashBase `AGENTS.md`、`MAINTENANCE.md`、`code-review/README.md` 以及数据生命周期、文件事务和搜索产品合同。因为用户要求完整设计及实现复审，采用该仓库的 intent-first 路线；源码决定当前实现。MFS 无适用的 `AGENTS.md`，其 `CONTEXT.md`、`README.md`、`docs/design.md` 用于确认公开合同。

**最终结论（2026-09-13 12:23 UTC）：** 独立注入发现四处实现缺陷：两处 P1、两处 P2；主 agent 修复后，四项均已使用原触发机制独立复验通过。本轮没有另外确认的未修复 P1/P2。下列问题是本轮发现并关闭的记录，不是对当前代码仍有这些缺陷的断言。StashBase 仍使用另一套 `mfs-cli` 接口；不能直接替换依赖后宣称已满足所有 StashBase 需求，接入工作及未验证事项见后文。

## 独立确认的实现问题

### 1. P1：未知后缀接收再次打开源路径，特殊文件竞争会锁住整个实例

- 位置：`src/mfs/_sync.py:260`、`:280` 的 `file()` 在 `_open_canonical` 校验后调用 `_admit`；`src/mfs/_core.py:914` 的 `_admit` / `:1384` 的 `_select_processor`。发现时，二次 sniff 位于持有 `Lifecycle.condition` 的接收路径，使用 `staged_path.open("rb")`；行号指最终修复落点。
- 触发：External 文件使用未知后缀、依赖 Processor.sniff 路由；在已打开普通文件并校验身份之后，路径被其他进程替换为 FIFO。无需真实 PDF 解析或模型调用，内置 PdfProcessor.sniff 即可触发。
- 用户后果：`sync` 持有全局状态锁等待 FIFO，`status`、取消/生命周期入口和 `close` 无法推进。健康 namespace 的 grep 仍能按 deadline 抛错，但无法返回原本可用的结果；搜索总期限没有消除实例被锁住的问题。
- 独立证据：`/tmp/mfs_independent_fifo_probe.py`。本机真实 `os.mkfifo` 注入输出 `status_blocked_after_200ms True`、`healthy_grep WaitTimeout 0.103`、`close_blocked_after_200ms True`；向 FIFO 写入后才退出，且 sync 返回 `complete=True`。
- 修复要求：从已验证 descriptor 获取并复用 head/路由，不在状态锁内重新打开源或执行 sniff。其他源接收入口也要原子拒绝 FIFO/链接竞争。
- 最终复验：**通过，已关闭。** `/tmp/mfs_independent_fifo_verify.py` 保留同一 FIFO 注入，只允许清理时没有读者的 `ENXIO`；输出 `status_blocked_after_200ms False`、`healthy_grep_ok 0.001`、`close_blocked_after_200ms False`。源码确认复用 descriptor 的选择、锁内只核对声明；Internal sniff 移到锁外，`_stage_path` 也改用 `open_regular`。External 接收本身仍不承诺路径此后保持不变，因此 FIFO 替换发生在最后身份校验之后时，接收报告不是源快照保证。

### 2. P1：临时索引文字重建绕过 Processor 并发和资源额度

- 位置：`src/mfs/_preparation.py:267` 的 `read_text()` 原先在 transient text 缺失时直接调用 `prepare()`；`src/mfs/_runtime.py:76` 的 `acquire_stage()` 此时只为 chunk/embed 阶段领取对应 Adapter 的额度。最终修复落点为 `src/mfs/_indexing.py:63`、`src/mfs/_lifecycle.py:1008`、`src/mfs/_preparation.py:220`。
- 触发：Processor 返回 `grep_path` 和内存索引文字而不提供 `text_path`，例如 HTML 式处理。正常发布会释放内存文字；随后更改 Chunker/配置并复用已准备记录，chunk 阶段再次执行 Processor。进程重启后丢失内存文字也涉及同一分支。
- 用户后果：声明 `concurrency=1` / heavy 的处理器可在多个 chunk 阶段同时执行，非线程安全解析器或本地模型可能异常，重资源容量也会超发。单独给 chunk 阶段计费不满足实际执行的资源合同。
- 独立证据：`/tmp/mfs_independent_transient_admission_probe.py`。两个文件完成后只改 Chunker；实际输出 `two_processor_calls_at_once True maximum 2`、`execution_stages ['chunk', 'chunk']`、`processor_admission_used 0`。
- 修复要求：将临时文字重建纳入 Processor 的真实准入与退休流程；不能持有 Chunker 资源再阻塞等待 Processor 资源而形成新的资源死锁。
- 最终复验：**通过，已关闭。** `/tmp/mfs_independent_transient_admission_verify.py` 使用同一两文件、同一单并发 heavy Processor、同一 Chunker 变更，输出 `two_processor_calls_at_once False maximum 1`、`execution_stages ['process']`、`processor_admission_used 1`，释放阻塞后配置最终完成且最大并发仍为 1。源码确认缺文字的 chunk/embed 先返回 `NeedsPreparation`，退出原阶段后回到 process 重新领取额度，候选同步不会越过恢复标记。

### 3. P2：配置晋升连续失败永远达不到五次终止预算

- 位置：`src/mfs/_configuration.py:252` 的 `maintain()`，发现时在 `promote()` 前清除 `building.error/failures/next_run`；最终修复先执行 `:289` 的晋升，再清理已恢复的维护错误。
- 触发：候选 collection 初始化成功，但晋升时持久 namespace 写入持续失败；故障后仍能读取持久状态进行核对。
- 用户后果：每次晋升失败都重新计为第一次，状态长期停留在 building/retry；`wait` 持续超时，不会出现文档承诺的五次后终止失败，后台也不断重试同一故障。
- 独立证据：`/tmp/mfs_independent_promote_probe.py` 仅在晋升写入点注入 `StorageFailed`。实测 `promotion_attempts 7 pending_failures 1`，`wait_result WaitTimeout`。
- 修复要求：连续失败预算涵盖完整维护/晋升步骤；达到上限可见失败，重新提交相同配置后能在同进程恢复。
- 最终复验：**通过，已关闭。** `/tmp/mfs_independent_promote_verify.py` 保留同一晋升拒写注入，实际停在 `promotion_attempts 5 pending_failures 5`，`wait_result OperationFailed`；再等一秒没有第六次重试。撤销注入后重新提交相同配置，revision 保持不变，并在同进程成功晋升：`same_process_retry_promoted True`。

### 4. P2：删除 namespace 后仍长期持有已配置的模型对象

- 位置：`src/mfs/_core.py:754` 的 `drop_namespace()` 原先只移除 `runtime.bindings`；`src/mfs/_configuration.py:393` 的晋升保留 `runtime.build_bindings[(namespace, generation)]`，而退休只遍历仍存在的 namespace。最终回收位置为 `_core.py:761`、`_configuration.py:444`、`_runtime.py:96`，闲置 Worker 在 `_worker.py:42` 释放上个 permit 的引用。
- 触发：创建 namespace，配置并晋升新 Embedder，再 drop；重复同名创建、配置、删除。
- 用户后果：namespace 已删除且后台删除已完成，旧 Embedder/Processor 运行对象仍被库强引用。移除/重加 Folder、换本地模型的长期进程会积累模型内存及含凭据的对象；文件 GC 无法回收 Python 运行绑定。
- 独立证据：`/tmp/mfs_independent_drop_binding_probe.py` 连续执行三轮并删除应用自己的模型引用后 `gc.collect()`，输出 `namespaces ()`、`retained_model_instances 3`、`retained_build_bindings 3`。
- 修复要求：按配置代/namespace 生命周期移除已失效运行绑定；实际执行及查询继续自行持有必要引用直到退出，不能提前关闭宿主拥有的共享 Adapter。
- 最终复验：**通过，已关闭。** 未改变 `/tmp/mfs_independent_drop_binding_probe.py`，重跑相同三轮操作后输出 `namespaces ()`、`retained_model_instances 0`、`retained_build_bindings 0`。执行许可及查询局部变量继续持有正在使用的 Adapter，不依靠在删除 namespace 时强行关闭宿主对象。

## 其余边界的独立核对

- 发布与重绑：执行许可捕获 revision、attempt_token、incarnation 和配置代；提交统一核验。查询捕获当前发布身份及 input_version，防止旧代命中在已接收新源后复活。配置代查询计数保护退休；重绑在采用对象前复核代。没有额外确认旧查询覆盖新模型的剩余缺陷。
- 配置接收与恢复：候选声明持久化，成员分批补齐；成员目标及准备文字成对落盘，丢失提交确认后采用持久值。读取持久结果本身失败时停止发布并要求重开，是明确的失效保护，不应改成盲目重试。上述晋升失败计数问题属于可核对故障的同进程恢复缺口。
- 搜索与部分失败：grep 与 ranked search 有独立有界池；同一 deadline 包含排队、准备等待、Adapter 与后端阶段。超时调用不释放尚在使用的资源，迟到结果丢弃。grep 对单文件丢失/链接/特殊文件或临时 quiesce 返回其他结果及 failures；宿主仍必须保留这种部分失败语义。
- 源与生命周期：read/grep 文字读取使用普通文件检查并注册范围读取租约；已管理文字/产物有引用和 pin。quiesce 等待真实执行/读取退出，startup gate 可先恢复宿主磁盘日志再启动工作。临时租约不替代宿主源事务互斥，也不保护外部编辑器。
- 关闭与清理：close 等待真实工作退出，不能安全强杀任意 Python Adapter；宿主需要可取消 Processor 或受监督子进程。精确 snapshot 清理债务独立于最新源状态，GC 不删除 External 原件。文件回收不等同于 Adapter 运行对象回收，上述第 4 项因此需要单独修复。

独立选跑的相关测试为 **23 passed，64.82 秒**：`tests/test_review_boundaries.py`、`tests/test_search_timeout.py`，另加 `test_failed_candidate_keeps_active_search_and_retry_can_promote`、`test_boot_gate_allows_path_recovery_before_any_execution`、`test_quiescence_waits_for_source_reads_and_blocks_new_reads`、`test_managed_grep_text_survives_gc_and_reopen`。这组既有测试运行于上述最终四项修复前，之后由独立 agent 重跑四个原始机制的故障注入并全部通过。没有重跑主 agent 的全量测试，也不在本报告冒认它的验证结果。

## StashBase 接入：需求已存在，但应用适配尚未完成

| 真实需求与入口 | MFS 可提供的能力 | 仍由 StashBase 完成的工作 |
| --- | --- | --- |
| `python/requirements.txt:22` 依赖 `mfs-cli[onnx]`；daemon 导入旧 `mfs.store`、`mfs.ingest` | 当前独立嵌入式 MFS API | 新 daemon 适配和迁移，不能只改包版本；清点旧 Milvus 补丁是否仍必要，打包 Python 3.13 及监督入口 |
| `server/indexer.ts` 的绝对源路径、Folder bind、转换源 upsert、rename/delete-prefix | 每 Folder 使用稳定 External namespace；DocumentId 为相对源身份；SourceMap/artifacts；sync、规则、quiesce | 保留 Node 的授权/真实路径及平台比较身份；将源和当前派生输出交给 Processor；由宿主管理磁盘 rename/delete，再 sync 覆盖新旧范围 |
| `python/stashbase_daemon.py:1950` 串行逐行执行 handler；`server/mfs-daemon.ts:91` 统一十分钟超时 | 有界并发执行、可轮询接收报告、查询 deadline | 改成有界 RPC 并发和明确请求/进程代；长 sync/wait 不占唯一 stdin 处理通道；状态/取消保持可响应；不能把 MFS WaitTimeout/部分失败转换成空结果 |
| `python/stashbase_daemon.py:1498` 和 `server/indexer.ts` 的全库语义搜索；`server/library-operations/index.ts` 的按 Chat 授权范围 | 单 namespace 的 BM25/vector/hybrid、结构过滤、grep whole-word | 全库查询由宿主有界 fan-out、共享总 deadline、合并排名、去重嵌套 Folder 身份、汇总部分失败；不同 namespace 分数不能未经约定直接比较 |
| `server/indexer.mfs.ts:230` 转换源索引；Preparation 合同中的 PDF、OCR、DOCX、音视频完成标记 | Processor、checkpoint、SourceMap、grep_path/text_path、artifacts、处理暂停 | HTML flatten、JSON/TXT、PDF/OCR、清洗 DOCX、转录及来源定位 Adapter；校验源 hash 和终态完成标记；空 OCR 成功；播放/预览句柄仍由应用管理 |
| `server/library-file-mutations.ts:104` / `:248` 源移动/删除 | 多范围 quiesce、同步撤销搜索资格、清理债务、start_paused | Node→Python 租约协议、重叠 Folder 范围、断连回收与宿主持久事务日志；重启后先恢复源磁盘事务再 resume_background；接收回执不等于索引完成 |
| `code-review/data-lifecycle.md` 的共享 light/heavy 资源、持久取消、故障/重启后 reconcile | Admission、用户取消门、processing_paused、restore_document_state | 跨进程资源仲裁必须异步/预取，不能在 Lifecycle 锁内 RPC；旧失败/取消和迁移阶段由应用日志导入；规则和用户意图恢复后才解除启动/处理门 |
| `server/mfs-daemon.ts` 的凭据刷新、退休 barrier；搜索产品文档对 hosted source 的 Known Gap | 模型兼容清单、候选切换、显式暂停/重试 | 凭据撤销、Provider 请求超时/配额、不同用途标签及宿主模型实例寿命属于应用；不要把“旧配置继续服务”直接用作已撤销凭据继续可调用的授权。产品文档与 hosted 后端当前不一致也是 StashBase 工作 |

建议先替换 daemon/Indexer 内部的生命周期与发布职责，保持 Node 的源授权、文件事务和检索边界；逐个验证上述真实入口后再迁移格式处理与旧状态。MFS 没有必要接管用户目录写入、Viewer 或 MCP 权限。若希望整库搜索由库本身提供，则需要另行定义跨 namespace 过滤、部分失败及排名合同，当前 API 没有此承诺。

## 尚未验证

- Windows 实机的 Job Object、目录句柄和共享冲突；本次 FIFO 注入只在 POSIX 执行。
- 打包后冻结 sidecar 的完整启动、父进程硬退出和模型资源回收；现有相关代码已读，本轮没有运行桌面发布流程。
- 大规模真实目录、NAS/FUSE 持续不可返回 I/O，以及百万级配置晋升事务的交互延迟；现有 deadline 不等于能够中断任意 native I/O。
- 真实 StashBase 旧数据库迁移、Node/Python RPC 并发适配、跨进程 Admission，以及用户数据/网络模型/凭据服务。本次全部使用临时合成输入，没有访问这些外部状态。
- 并发协议检查与定向注入不构成对所有调度交错的证明；未确认的风险没有列为实现缺陷。
