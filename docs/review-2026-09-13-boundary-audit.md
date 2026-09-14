# MFS / StashBase 边界审查（修复前）

后续：用户确认的第 1、2、3、4、6、7 项已实施，最终行为与验收见 [2026-09-14 修复记录](review-2026-09-14-boundary-fixes.md)。以下内容保留当时的发现与处置，不代表修复后仍存在同样缺陷。

审查基线：MFS `9856ef9`，StashBase `7a6ac737`。两边开始时工作区干净。本文根据当前代码、实际测试和新增复现判断；历史审查的缺陷不自动算作当前缺陷。未修改生产实现或 StashBase 文件。

本轮处置以用户随后确认的 MFS 范围为准：R1、R2、R4 三项实现问题，以及 D1、D2 两项设计调整，已记入[待修复清单](backlog.md)。R3 改为并发方案讨论，不将 DDL 串行本身判为缺陷；D3 暂缓；任意 Python/native 调用不能强制停止是现有约束。本文中的 StashBase 接入分析保留为背景证据，不构成本轮 MFS 修复要求。

对应讨论序号：1 → R1、2 → R2、3 → R3、4 → R4、5 → S1、6 → D1、7 → D2、8 → D3、9 → D4 的搜索取消边界。第 10 项的跨进程资源共享仅为可选能力，默认 MFS 自行调度；第 11–19 项中的宿主工作移出本轮范围。第 13 项是假设采用“宿主准备产物”的过渡方式后才产生的协议工作，不是当前 MFS 缺陷。已有 BM25 上游限制继续按 BACKEND-005 记录。

新增证据：[复现脚本](review-2026-09-13-boundary-audit-repro.py)、[本轮原始输出](review-2026-09-13-boundary-audit-results.jsonl)。脚本只创建临时状态目录，不访问云端模型。它记录缺陷现状，不是“所有断言通过即已修复”的回归套件。

## 1. 当前仍可复现的实现问题

### R1 / P1：collection 已删除，失败清理责任未结清，并影响同名新 namespace

位置：[清理循环](../src/mfs/_cleanup.py) 25–26 行、[drop 执行](../src/mfs/_indexing.py) 76–93 行、[等待谓词](../src/mfs/_lifecycle.py) 302–317 行。

对一次旧 snapshot 清理连续注入五次 `OSError`，耗尽正常重试预算；撤销故障后执行 `drop_namespace`。实际 drop 已 succeeded，Milvus `list_collections()` 返回空，但 `wait(drop_report)` 仍抛原来的 `OperationFailed`。关闭重开仍复现。再创建同名 namespace 和同名文件：新文件成功索引，BM25 能返回它，`wait(new_upsert_report)` 仍抛旧 incarnation 的清理错误。

原因是预算耗尽的债务在检查 collection 是否仍存在之前被跳过，只有显式重置预算才能再处理；成功删除 collection 没有结清其覆盖的 snapshot 债务。等待按 namespace/path 匹配债务，旧 incarnation 的错误又进入新对象的等待范围。此时 `status()` 甚至可出现 `pending_count=0, failed_count=0, ready=False`。重建同名文件后，对这个已成功的新文件显式 retry 可以重置旧债务并恢复；当前状态没有自然指引用户执行这个动作。

**用户影响：** 删除 Folder、重新加入 Folder、重建后的完成状态无法恢复；普通重启无效。已有索引可以工作，但用户一直看到未完成或重复旧错误。

**建议：** collection 删除成功并确认旧执行退休后，持久结清该 incarnation/generation 覆盖的所有债务，包括已耗尽预算的记录；重开重放该收尾。不能在旧执行仍可迟到写入时提前清债。当前文件等待也应明确区分新 incarnation 与历史删除工作，提供可发现的清理诊断和恢复入口。

### R2 / P1：首次初始化中断，把可恢复的空状态变成无法重开的 CorruptState

位置：[MFS 初始化](../src/mfs/_core.py) 188–200 行、[Catalog 版本检查](../src/mfs/_catalog.py) 37–44 行。

初始化先创建 `LOCK`、`PROCESS_LOCK` 和受管理目录，再建立 catalog。若在此窗口失败，异常清理释放运行资源但留下目录；下次通过“目录非空且没有 catalog”判断为损坏。

**证据：** 在进入 Catalog 前注入一次临时 `OSError`，撤销后重开报 `CorruptState: non-empty mfs_path has no recognizable catalog`。另一个复现在同一窗口真实 `SIGKILL` 子进程，退出码 `-9`，重开得到相同错误。没有手工伪造状态文件。

**用户影响：** 首次启用、首次迁移或创建新存储时的一次中断，使后续每次启动都失败，需要清理目录才能继续；此案例尚未接收用户文档，不是已接收原件丢失。

**建议：** 定义可识别、可重放的 bootstrap 状态，或在独立临时目录完成初始化后发布。恢复只处理能证明由本次初始化拥有的文件，继续拒绝不相关的非空用户目录。catalog 文件已出现但 schema 尚未提交的窗口也应验收。

### R3 / 并发方案讨论：配置/清理维护串行，且后端调用跨 collection 共用锁

位置：[维护线程](../src/mfs/_core.py) 290–318 行、[配置循环](../src/mfs/_configuration.py) 266–303 行及 366–374 行、[后端调用锁](../src/mfs/_index.py) 102–110 行。

`stage_timeout` 的监督对象是文件 Worker 的 execution records。配置初始化、collection load/recreate/drop 和独立清理由一个维护线程依次调用；这些管理调用没有相应 deadline，namespace_executions 只统计占用，不负责超时。

**证据：** 设置 `stage_timeout=0.15`，以 Event 阻塞 namespace a 的候选 collection 创建，然后请求 b 的配置。0.4 秒后两者仍 pending，无 pending_error、无失败计数；b 的 wait 超时。释放 a 的调用后，两者都完成。这个夹具验证的是控制流对不返回后端调用的处理，不代表真实 Milvus 此次自己发生了挂起。

**用户影响：** 一次慢管理调用可以让后续所有 Folder 的配置、清理一直排队。短搜索期限和 close 期限只能结束调用方等待，不能自动恢复维护线程。已有状态读取在本复现中保持可用。

**修正判断：** 同一 collection 的 DDL 串行有合理性，不能仅凭后一个 DDL 排队就判为 P1。Lifecycle 状态锁已经不包住这些慢后端调用；还存在两层串行：单一维护线程依次处理 namespace，以及 `_SerializedClient` 用一把实例级锁包住所有 collection 的整次 Milvus 调用。后一层也供索引写入和排名检索使用，因此影响可能超出后续 DDL；搜索等锁仍受自己的 deadline 限制。本轮夹具实测的是维护排队，并未证明真实 Milvus 挂起或实测上述搜索竞争。

**建议方案（尚未实施）：** 用有界维护线程池执行独立 collection 的工作，同一 collection 的管理操作仍串行；状态锁只用于登记占用、检查当前代和提交结果。检查固定版本客户端及后端的并发能力后，去掉跨 collection 的整次调用互斥，保留必要的连接生命周期保护和 collection 销毁排他。drop 必须等该代实际读写退出，迟到的创建结果不得发布成当前配置。如果后端本身要求实例级串行，加线程无法使它并行，应保留必要保护并明确等待/失败状态，不能未经验证直接缩锁。管理调用应传递剩余期限；超时不提前归还尚在执行的占用。以上均在 MFS 内完成，不要求 Node 发许可或新增宿主恢复协议。

### R4 / P2：sync 接收 canonical 文件，却让 wait(report) 等待别名，提前宣告完成

位置：[sync 报告范围](../src/mfs/_sync.py) 137、161–171 行及 `link()`；[wait 解读报告](../src/mfs/_core.py) 407–410 行。

External root 内 `alias.txt -> real.txt`，暂停处理后调用 `sync(n, "alias.txt")`：报告 `complete=True`，`changed=[real.txt]`，但 `path=alias.txt`。`wait(report, 0.1)` 立即成功；同一时刻 `wait(DocumentId(n, "real.txt"), 0.1)` 超时，real.txt 仍 pending。

**用户影响：** 宿主可能提前报告同步完成，随后读不到本次接收的文字/索引。等待失败或恢复状态也会查错文件。

**建议：** 报告须保存实际 canonical 等待范围，并包含本次越出请求拼写范围的目标。一个路径不足以表达时使用范围/身份集合，仍等待当前目标，无需恢复历史操作表。同类代码中，root 重定向把实际扫描改为 `.` 却保留旧 report_path，也应补验证；本轮实际执行的是文件别名案例。

## 2. StashBase 当前仍存在的等待风险

### S1 / P1：排队中的 bind barrier 会再次挡住后续 status/scan，RPC 保底仍是十分钟

位置：`stashbase/python/stashbase_daemon.py:1935–1952`、`stashbase/server/mfs-daemon.ts:188–204, 361–382`、`stashbase/server/state.ts:274–300`。

当前 StashBase 已有独立 write/search/scan/status/probe 通道，慢索引和搜索正常情况下不占 status 槽。搜索异常现在也会向上抛出。MFS 原有对接文档中“Python 顺序执行所有 handler”和“搜索异常变空结果”的描述已过时，不能作为当前发现。

但 pending 队列遇到 `bind_folder` 等 barrier 就停止向后扫描。慢 upsert 在运行时，随后打开另一个 Folder 或再次绑定同一 Folder，会插入 barrier；后续 status/scan 即便有空闲槽也不能运行。Node 的 bindFolder 每次都会发 RPC，没有按已确认的配置和 daemon generation 消除无变化绑定。

**证据：** 用当前真实 `_RequestDispatcher` 和受控 handler，慢 upsert 尚未结束时，barrier 前的 status 正常完成；加入 bind 后，0.3 秒内后续 status/scan 均未执行，队列包含 bind/status/scan。释放 upsert 后能正常收尾。这是 dispatcher 层验证，没有运行 UI，也没有等待实际十分钟。

**用户影响：** 切换/打开 Folder、状态刷新和同步动作仍可能长时间等待。Node 当前正常 RPC 的 watchdog 为十分钟，且计时从 ensureReady 之后开始；不是一个覆盖就绪等待与排队的交互总期限。

**建议：** 合并同 generation、同有效配置的幂等 bind；将可安全读取的健康/任务状态快照放在不依赖 store barrier 的控制通路。真正跨 store 变更的读取继续遵守 generation 和退休约束。用户请求从 Node 接收时创建总 deadline，排队过期就不再启动；超时不应一律认定整个 daemon 已死。迁移到新 MFS 时仍需验收这条链路。

## 3. 仍需定清的设计与产品合同

### D1：失败文件是否有权阻止整个 Folder 首次开启新检索能力

位置：[配置晋升](../src/mfs/_configuration.py) 428–439 行；[当前设计](design.md) 249 行。

最新设计已经允许“被取消且两代均无有效文字”的文件缺席发布。但失败成员、已有文字的取消成员仍阻止整代切换。

本轮公开接口复现：BM25 下一个好文件成功、一个文件 Processor 失败；加入 Embedder 请求 hybrid。好文件在候选代 succeeded，坏文件 failed，活动配置仍是 BM25，vector 查询报 CapabilityUnavailable。BM25 仍能搜到好文件。

这符合现有 MFS 设计，应作为产品取舍讨论。StashBase 的 partial/failed/ready 体验要求用户能利用其他可用文件；如果一次坏 OCR 就阻止首次启用整 Folder 向量搜索，接入仍不满足该体验。建议分别规定首次启用与替换已有有效模型的发布策略，允许显式的部分发布、失败成员清单及后续补齐；保留取消意图，不混用不同 embedding space 的向量。

### D2：缺绑定、暂停、资源等待、正在退出，不能都显示成无错误 pending

位置：[运行准入](../src/mfs/_lifecycle.py) 774–779 行、[等待谓词](../src/mfs/_lifecycle.py) 326–335 行、[文件状态](../src/mfs/_core.py) 824–871 行。

绑定运行 Adapter 是宿主的责任。当前候选配置缺绑定会明确报 blocked，普通活动配置缺绑定则仅停止领取。

**证据：** 接收文件后关闭，重开但不绑定 Processor，文件长期保持 `pending, attempts=0, executing=False, error=None`；带 0.2 秒预算的 wait 超时。补 open_namespace 后约 0.014 秒完成。没有设置预算的 wait 没有自动完成条件，阶段期限也尚未开始。

建议在现有状态 Interface 中提供明确的 blocking reason、所属 generation 和可恢复动作，并对无法由后台自行解决的等待给出相应错误/状态。宿主仍可显式选择等待即将安装的绑定，不需要增加另一套 ready 状态机。

### D3：精确检索的故障隔离需要覆盖启动和进程恢复

位置：[运行对象初始化](../src/mfs/_runtime.py) 43 行、[MFS.open](../src/mfs/_core.py) 206–235 行。

运行中的 grep 与排名搜索有独立执行槽，这是已有能力。但 MFS.open 无条件先打开 Milvus，并验证/加载 collection；后台后端无法打开时，整个 MFS 无法打开。把所有 PDF/OCR 文字也迁入 MFS 管理后，应用不能仅凭“grep 不依赖向量”推断它在整个 daemon/backend 启动失败时仍可用。

需要在接入时明确一种可执行的降级方式：元数据/已准备文字可独立打开，或宿主保留有源 hash/配置校验的可读取产物。普通源文字可继续磁盘 fallback。此项是代码结构识别出的接入合同缺口，本轮未模拟真实 Milvus 数据库损坏。

### D4：调用方超时与实际资源退休必须分别向宿主呈现

搜索超时、阶段超时和 close 超时都不强杀任意 Python/native 线程；当前保留执行占用的做法保证生命周期安全。四个不退出查询仍可用尽四个排名搜索槽。Embedder 也不使用 Processor/Chunker 的对象并发门，默认可能同时收到四个后台调用与四个查询调用。

StashBase Adapter 必须处理线程安全、查询优先级、网络期限及提供方限流；本地 ONNX 还要控制 native 线程和内存。read/sync 没有 search 那样的统一调用期限，已准备文字的读取/输出大小也应保留应用的限制。把 timeout 返回直接当“可以覆盖文件/释放模型/启动第二个 daemon”会重新引入生命周期竞争。

## 4. 接入背景与覆盖矩阵（不作为本轮 MFS 待修复）

复用现有 Node → Python daemon 链路：一个 daemon 独占一个 MFS state，每个持久 Folder ID 对应 External namespace，doc_id 使用 Folder 内源相对路径。访问控制、用户源文件操作、Viewer/播放和产品决策继续由 StashBase 拥有。MFS Processor 完成真正的搜索文字准备，随后由 MFS 管理切片、向量、发布与任务恢复。

把 Node 与 Python 的复杂协调集中在一个宿主 Adapter 内：外部 Interface 围绕 Folder 绑定、观察、检索、状态和源操作退休；不要让每个 HTTP/MCP handler 自己编排 MFS 私有字段、版本号和清理规则。

| StashBase 需求 | 当前能力与接入工作 |
| --- | --- |
| 替换旧 mfs-cli | `python/requirements.txt` 仍是 `mfs-cli[onnx]`；daemon 使用旧 store/config/ingest/embedder 模块。必须重写调用适配，不能只换依赖版本。 |
| 多 Folder 与父子 Folder | MFS 支持独立且重叠的 External root。宿主持久保存 Folder→namespace 映射，正确归一化显示拼写与比较身份；同源操作覆盖所有相关 namespace。 |
| 文本、HTML、JSON、PDF/DOCX、OCR、音视频 | 内置 UTF-8/PDF/基础 DOCX 加应用实际 Processor。保留原件身份、页/行/时间 SourceMap、HTML 原文 grep，以及隐藏派生文件规则。原件大小与提取输出大小分别限制。 |
| 不配置 embedding 也能用 | off/bm25、grep/read 已有；完整故障降级补 D3。全量扫描/文字准备不应等待凭据和账户。 |
| 首次大批量索引先询问 | sync 是接收并调度，不是旧 scan_diff。接收前安装索引暂停门，仍允许文字准备；持久区分用户暂停和临时预算决策，防止临时门释放清掉用户暂停。 |
| light/heavy/播放共享容量 | MFS 默认通过 LocalAdmission 管理自己的 Worker，无需 Node 发许可。只有应用另外要求将播放和 MFS 处理限制在同一份全局预算时，才涉及可选的共享 Admission；本轮不要求此接入方案。 |
| 取消、重试、进度、迁移旧状态 | cancel/retry/checkpoint/progress 和 processing_paused/restore_document_state 已有。先暂停导入旧失败/取消，再解除迁移门；落实 D2，保留用户意图。 |
| 源改名、移动、删除 | 宿主路径互斥 → quiesce 所有关联范围并退休 Viewer/播放 → 持久记录并执行磁盘步骤 → 租约内 content sync 新旧范围、检查 complete → 释放租约 → 可选 wait。先修 R1/R4；后续索引失败不回滚已提交源操作。 |
| 源操作中途崩溃 | 宿主需要可重放路径事务日志；现有 recovery-journal.ts 是编辑草稿恢复，不能替代移动/删除日志。MFS.open(start_paused=True) 后先恢复磁盘、绑定并 sync，再 resume_background。先修 R2。 |
| 换模型/维度、轮换凭据 | configure_namespace 已支持候选代；同空间凭据轮换重新绑定，避免重新 OCR/embedding。D1 的部分发布已记为待修复；R3 为 MFS 内部并发方案讨论。 |
| 全 Library MCP 搜索 | MFS 仅单 namespace；宿主有界 fan-out、共享一次 deadline、处理部分错误与重叠源去重。不同 collection 的 BM25/hybrid 原始分数不能直接当全 Library 总排名，须定义合并并跑 eval。 |
| 准确错误与用户可操作状态 | 当前旧 daemon 搜索已抛错；新 RPC 还要保留 MFS 错误类型、retryable、grep.failures/truncated。Node 当前 onLine 只构造普通 Error，会丢掉 busy 等 code。 |
| 请求并发与切 Folder 响应 | 保留已实现的有界 dispatcher；修 S1，并为取消、健康状态、资源 grant 保留独立通路。长 wait 不应占唯一写入通道。 |
| 退出、重开、native 子进程 | MFS close 有限等待后宿主升级终止；重开等待实际进程与继承的 PROCESS_LOCK 退出，在有限启动预算内重试 InstanceLocked。冻结入口先调用 run_process_supervisor。 |
| Windows 与打包 | 新 MFS 要 Python 3.13；替换旧 hidden imports/ONNX 依赖。run_process Windows 分支没有显式 CREATE_NO_WINDOW，StashBase 隐藏转换控制台的要求需补原生验收；本机三个 Windows 测试跳过。 |
| 检索质量与吞吐 | 跑 StashBase 代表性 corpus、现有 retrieval eval、多 Folder fan-out 与实际 OCR/转录负载。当前 BM25 flush 分组排名一致性测试仍 xfail，不能用“同为 Milvus”替代迁移验收。 |

若采用“Node 完成准备，MFS 只借用产物”的过渡方式，还需要按 incarnation/revision 限定、可重放的完成通知；通知早到、迟到和用户取消都必须处理。无条件 retry 会解除取消门。把实际准备放进 Processor 可以避免这套双任务系统的同步合同。

## 5. 当前范围与验证边界

待修复范围仅为 R1、R2、R4、D1、D2，具体目标和待补验收见 backlog。R3 讨论线程与锁粒度，D3 暂缓，S1 及宿主接入工作不纳入本轮。没有因本次记录修改生产实现。

本轮验证：

- MFS `.venv/bin/python -m pytest -q`：**218 passed、3 skipped、1 xfailed**，560.50 秒。三个跳过均要求 Windows 原生句柄；xfail 为已知 BM25 flush 排名一致性；PDF 依赖有七条弃用警告。
- StashBase `python/.venv.nosync/bin/python -m unittest discover -s python -p stashbase_daemon_test.py`：**32 tests OK**，8.890 秒。
- 新脚本八个 probe 全部完成，产生十四条观察；包括实际首次初始化 SIGKILL、清理错误后的同实例/drop/重开/同名重建/显式 retry、别名等待、缺绑定恢复、维护阻塞、失败成员发布和当前 StashBase dispatcher barrier。
- 脚本格式和 lint 检查通过。现有通过测试不覆盖上述新增窗口；没有宣称已修复这些发现。
- 没有执行 StashBase 整个 Electron 用户旅程、真实云端请求、实际断电或 Windows 打包；不能据此保证“满足所有需求”。
