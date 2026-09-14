# MFS / StashBase：并发、故障恢复与接入复审

审查基线：MFS `6706f850ca941c5e9de62868379cb122c9b363c6`；StashBase `7c146e1c1aa1e017f45695ab49dd6f34136d9ee9`。两个工作区开始时均干净。审查以当前实现为准，覆盖相关模块的整体行为，不以旧报告中的“已修复”替代验证，也不局限于最近一次 diff。

基线审查确认 **4 个 P1 实现问题**，涉及无限等待、写失败重试风暴、状态轮询阻塞整个实例，以及提交后故障导致内存旧目标覆盖 SQLite 新目标。用户随后授权修复并 push；以下 R1–R4 和 D1 的证据保留为修复前观察，源码行号对应基线。

修复结果：

| 项目 | 当前行为 |
| --- | --- |
| R1 | 按文件使用现有清理索引判断存在性；同一合成夹具不再触发全队列读取，并发取消不再被长时间阻塞。 |
| R2 | 领取失败共用三次预算及 0.25/0.5 秒单调时钟退避；耗尽后停止调度，状态携带存储错误，wait 报 StorageFailed；故障撤销后重开恢复。短暂故障及确认丢失仍自动恢复。 |
| R3 | 最终接收重新读取 SQLite 规则，拒绝被排除输入；重开把旧版本留下的无效目标转成删除任务。 |
| R4 | 内存刷新与调度资格判断不做数据库写入；候选成员按 SQLite 差异异步恢复，成员写失败统一核对；领取/更新核对持久目标。过时或已删除的缓存目标不能被写回，核对读失败会明确停止。 |
| D1 | 两代均无有效准备文字的已取消输入不阻塞配置晋升，取消门保留；有准备文字的取消成员仍保留 G0。状态、取消和 retry 只选择匹配当前源版本和配置代的候选。 |
| StashBase 请求分发 | 请求接收与执行分离，写入串行、查询/扫描/状态有独立有界名额；规则、绑定及关闭保持独占顺序屏障，关闭等待真实退出。搜索故障返回错误。 |

新行为由 `tests/test_liveness_recovery.py` 的 13 项回归覆盖，包括二次复核发现的“旧候选遮蔽立即取消”“调度提示夹带退休写入”“SQLite 已删除目标但缓存残留”边界。复核的 Standards、Spec 两项均无未解决问题。原始[观测结果](review-2026-09-13-liveness-results.jsonl)、[修复后结果](review-2026-09-13-liveness-fixed-results.jsonl)和可在当前代码运行的[复现脚本](review-2026-09-13-liveness-repro.py)保留；相同状态夹具的整页查询及并发取消耗时约 7 ms，原先的 250 次清理队列读取降为零。这是合成负载对比，不是所有用户环境的延迟保证。

StashBase 仍使用旧 `mfs-cli`，本次修复其现有请求分发，不包含迁移到新 MFS。下文的格式 Adapter、跨 Folder 搜索、打包验证及 Milvus BM25 已知限制仍属于接入工作与验收边界。

## 1. 新确认的实现问题

### R1 / P1：状态轮询对清理记录做乘法级读取，长时间持有全局锁

位置：`src/mfs/_core.py:863`、`:874`；`src/mfs/_catalog.py:458`。

`document_status` 为求一个 `cleanup_pending`，调用 `cleanup_rows(namespace)`，读取并 JSON 解码该 namespace 的全部清理记录。`list_document_statuses` 持有 `Lifecycle.condition`，对整页每一个文件重复这个操作。查询页大小为 P、清理债务数为 D 时，扫描和解码约为 P×D；`any(...)` 并不会避免列表构建时的全量读取。

待清理记录在批量更新、重建、旧查询未退出或清理失败时存在，是正常生命周期的一部分。越需要显示进度/错误时，状态轮询反而可能越慢。

**证据：** 使用真实 Catalog，构造 250 个目标、3000 条精确 snapshot 清理记录，调用公开的 `list_document_statuses(..., limit=250)`。一次整页查询触发 250 次全量清理记录查询；本机最终观测约 **8.27 秒**，并发 `cancel` 在 200 ms 后仍未返回。夹具使用启动暂停门保持债务稳定，没有注入慢数据库或 sleep 来放大查询耗时。这是合成负载测量，期间另有测试运行，绝对时长不应外推为所有机器的性能；P×D 次处理与全局锁范围可直接由代码和 SQL 次数确认。

追加单独复测：没有并行运行全量测试，整页仍约 **8.18 秒**。对 `Catalog.query` 和 `cleanup_rows` 分别计时，250 次清理查询累计取数 0.443 秒，含 Python 解析的 cleanup_rows 累计 8.042 秒，合计解码 750000 条记录。`load_json` 还执行递归校验和深拷贝；主要成本是反复构造/校验 Python 对象，不是 SQLite 单次查 3000 行很慢。这是文件状态列表调用，不是 search 在执行这套查询；cancel 的等待发生在取得同一把 Python 生命周期锁之前，取消自身不等待 Processor 退出，也没有固定一秒等待。

**用户影响：** 在这一全局锁上等待的其他 Folder 状态、取消、同步接收和任务提交一起受阻；搜索调用者可按其 deadline 超时，但超时不能让其返回本来可用的结果。频繁轮询会反复制造阻塞。

**修复方向：** 利用现有 `cleanup_scope` 索引做按 namespace/doc_id 的 `EXISTS`，或为当前页批量取一次布尔集合；避免每个文件解码整个清理队列。整页响应应从短锁内取得快照，减少锁内拼装。验收同时检查 SQL 规模与另一 namespace 的取消/状态延迟，不只检查状态字段正确。

### R2 / P1：任务领取写失败没有终止预算，四个 Worker 相互唤醒形成重试风暴

位置：`src/mfs/_lifecycle.py:896`；资源归还通知位于 `src/mfs/_runtime.py:60` 附近。

`claim` 中 `persist` 失败后，仅归还资源、`condition.wait(0.25)` 再重试。失败不进入 `fail_job`，也不设置可见的 `storage_error`、失败计数或不可提前的 retry deadline。四个 Worker 的归还资源都会 `notify_all`，所以这个 0.25 秒等待可以被其他 Worker 立即唤醒，不构成真正的退避。

**证据：** 输入已持久接收后，仅在领取阶段的 `Catalog.put_active(non-None)` 注入持续 `StorageFailed`，读取保持正常。默认四个 Worker，观测两秒内 **7940 次**领取失败；公开状态仍为 `pending`，`error=None`，内部 `storage_error=None`，`wait` 只能超时。将 stage timeout 设为 0.2 秒没有帮助，因为实际执行及其计时器尚未建立。移除故障后同实例能完成到 `succeeded`。

**用户影响：** 磁盘/SQLite 写入故障时，进度永远等待而没有原因，且持续争抢 CPU 和数据库。现有五次阶段失败预算与三次完成提交预算不覆盖这里。

**修复方向：** 把领取持久化故障纳入明确的存储故障状态和有限恢复协议；使用单调时钟核对真正的重试时刻，不能将 Condition 的一次 wait 当退避。保持丢失提交 ACK 时按持久值核对的正确行为。若已无法持久写失败状态，至少通过内存中的实例故障让 status/wait 明确报错并停止密集调度。

### R3 / P1：Internal upsert 与排除规则竞争，留下无法重试的假 running

位置：`src/mfs/_core.py:903`、`:925`；`src/mfs/_lifecycle.py:1385`、`:609`、`:940`。

Internal `upsert` 只在复制前核对排除规则。复制和准备原件在锁外，重新加锁接收时复核了较新的写入序号和 namespace incarnation，却没有复核当前排除资格。若这期间新增排除规则，`update_rules` 当时还看不到这个新目标，随后 `accept` 仍返回成功。

Worker 可以领取目标，但 `current` 在执行/提交时发现它被排除而返回 False。结果不提交，`retire` 却没有把这个同 revision 的 running 目标转入可结束状态。实际执行集合已经清空，后台期限监督也不再覆盖它。

**证据：** 复现调用 Internal `upsert(..., data=b"secret bytes")`，先把 bytes 写入 MFS 自己管理的原件文件，用 Event 暂停在原件落盘之后、任务接收事务之前；另一线程完成排除规则更新后放行。传入 Path 时对应的是将文件复制进受管理原件目录。External sync 不复制用户原件。结果为 `MutationReport(outcome='added')`；稍后 `state='running', executing=False`。`wait` 超时，`retry` 报 `task is still executing`。**关闭并重新打开、重新绑定 Processor 后，同样回到这个状态。** stage timeout=0.2 秒也不能结束它。

**用户影响：** 这个 namespace 的当前工作等待、strong 索引等待及涉及它的配置切换不能收敛；常规 retry 和重启均无效。本次 grep 返回空，没有观察到被排除正文泄漏。StashBase 建议使用 External namespace，因此这不是推荐对接路径上的直接阻塞，但它违反 MFS 自己的 Internal 并发合同。

**修复方向：** 接收事务内统一复核最新规则，并在接收前返回 `SourceExcluded`；对已经持久化的无效目标提供恢复归一化，保证不存在无执行所有者的永久 running。补同类“复制期间规则变更、重启后恢复”回归。

当前规则更新的实际时序：`update_rules` 在生命周期锁内，同一事务写入规则、删除已排除文件的公开文档记录、将已有目标改为删除任务；上述逻辑同步完成，索引和无引用受管理文件的物理清理在后台完成。最终接收与规则更新共用已有锁即可：规则先提交，则接收复查后拒绝；接收先提交，则规则更新将其撤销并安排清理。无需把原件写入或 Processor 执行放进这把锁。

### R4 / P1：规则已提交，后续候选记录写失败，旧内存目标反过来覆盖 SQLite

位置：`src/mfs/_lifecycle.py:1370`、`:563`；`src/mfs/_configuration.py:151`、`:236`。

`update_rules` 的主事务提交后，逐个调用 `remember` 更新内存目标。`remember` 又同步调用 `Configuration.synchronize`，在另一个事务里更新候选配置成员。这一步可能抛异常，且已在 `state_transaction` 的故障恢复范围之外；于是更新循环中途退出，剩余目标的内存状态没有采用刚提交的 SQLite 状态。

**证据：** 暂停处理、接收 `a.txt` 和 `b.txt`，修改 Chunker 版本，等候选配置为两个文件建立成员。随后添加 `*.txt` 排除规则，只对主事务提交后的第一次候选成员删除注入一次 `StorageFailed`。调用报错时，SQLite 中两个 target 均为 `delete`，内存中却是 a=`delete`、b=`upsert`。撤销故障并恢复处理后，`wait(namespace, 5)` 仍超时；a 已完成删除，b 的公开状态仍为 pending、executing=False，且 SQLite 中 b 的目标也被旧内存任务写回了 `upsert`。本次没有验证该场景重开后的收敛情况。

**用户影响：** 一次已经结束的写失败，使已持久接受的删除意图被旧状态覆盖；用户收到规则更新错误，但规则实际上已经生效，随后任务等待不能正常完成。此规则与候选成员路径由 Internal/External 共用；本次实际复现使用 Internal namespace。

**修复方向：** 以 SQLite 为唯一持久事实。内存状态刷新不应夹带可失败的第二组持久写入；候选成员属于派生工作，可独立持久化并恢复。任何提交后异常都应先按已提交数据恢复完整的内存视图，再允许调度；领取/提交旧任务时也必须核对 SQLite 当前目标，不能用旧缓存无条件覆盖新意图。

## 2. 需要明确的设计取舍

### D1：已有用户取消，会阻止整个 Folder 首次获得新索引能力

位置：`src/mfs/_configuration.py:409`。目前配置代切换要求所有 upsert 成员在候选代 `succeeded`，并把持久取消状态带入候选。

**公开接口复现：** BM25 namespace 中，一个文件在处理前被用户取消，另一个正常完成；配置 Embedder 并请求 hybrid。正常文件在候选代已经 `succeeded`，取消文件仍 `cancelled`；namespace 永久保留 BM25 和 pending revision，向量查询报 `CapabilityUnavailable`。`wait` 会明确报取消，并非静默死锁，但正常文件也无法使用已经建好的向量。

这符合 MFS 当前“全员完成再切换”的设计，不能归为实现没有遵守设计。问题在产品组合：StashBase 要保留用户取消，同时让其他可用文件持续提供部分检索；首次加 key、换失效 provider、全局设置变更都会碰到这个取舍。

建议区别“从未提供有效文本且被用户取消的文件”和“正在更新有效已发布结果的文件”。前者可以保留停止状态、明确缺席候选发布集合；后者的失败是否保留 G0 应另定规则。需要显式定义候选成员、部分完成、失败清单、取消候选及回退的合同，不能通过清除所有取消意图解决。也不能把不同模型的向量混在一个 collection 里。

### D2：有界响应不等于有界实际占用，需要宿主完整落实

search/grep 的总期限、超时后保留真实执行租约、close 的等待期限已经明确，也有覆盖。普通 Python/native Adapter 不协作退出时，timeout 不释放模型或源句柄；连续四个不退出查询可以占满对应查询池。进程隔离、宿主退休升级、可见的暂时不可用状态依然必要。

Embedder 当前不使用 Processor/Chunker 的 resources/concurrency 门，后台与前台默认可分别占四个调用。StashBase 源码包含本地 ONNX 路径，不能假定所有 Embedder 都是轻量 HTTP。应用提供的本地模型 Adapter 必须定义线程安全、native 线程数、查询优先级和过载策略；云端 Adapter 要明确 HTTP 期限、额度错误及可重试错误。一个共享串行模型锁又可能把查询排在大批次后面，不能机械补锁。

sync、read，以及 reindex 等待以外的工作没有 search 那样统一的调用期限。`_core.py:1103` 的 reindex 结束统计还调用全量 `scan`，`_configuration.py:433` 的晋升在一个锁/事务中遍历全部成员。这里是后续应量化的规模和期限合同，本次未把它们算作另外已复现的卡死问题。

### D3：检索质量仍有已知后端缺口

`tests/test_backend_conformance.py:15` 的严格 xfail 仍生效：相同文本因 flush 分组不同而出现 BM25 排名不一致。本次全量回归再次确认这一已知缺口。hybrid 使用 BM25 与 dense 的 RRF，不能据此保证迁移前后效果相同。必须用 StashBase 的 corpus/eval 判断影响；本次没有对真实用户语料给出质量结论。

## 3. StashBase 应如何对接

推荐一个 daemon 独占 MFS state；每个稳定 Folder 身份对应一个 External namespace。源文件操作与访问控制留在应用，处理与索引的当前任务生命周期交给 MFS。选择 Processor 完成实际准备，避免 Node Preparation 队列和 MFS 同时拥有同一份处理任务。

这里的 daemon 是 StashBase 已有的 Python 子进程：Node 的 `server/mfs-daemon.ts` 启动 `python/stashbase_daemon.py`，通过 stdin/stdout 交换 JSON。建议复用这条进程链路、替换 Python 内部的旧 mfs-cli 调用，不是再加一个后台服务。一个进程拥有存储、Node 管理源文件、操作前等待旧转换退出，这些现有结构继续保留；变化是请求并发方式、处理任务归属、Folder 到 namespace 的映射，以及让已有文件操作流程覆盖新 MFS 的 Worker 和读取租约。

| StashBase 现有需求与证据 | 对接方式与尚欠工作 |
| --- | --- |
| `python/requirements.txt:22` 的 `mfs-cli[onnx]`；daemon 使用 `mfs.store/config/ingest/embedder` | 不是兼容升级。重写 daemon Adapter，显式提供实际 Processor/Chunker/Embedder；新库没有原来的那些模块。旧 ONNX provider 也不能假定随新包继续存在。 |
| `server/indexer.ts` 的绝对路径身份、Folder bind、源路径结果 | 持久保存 Folder ID → namespace；DocumentId 使用源相对路径；保持原件显示和平台路径比较身份。父子 Folder 重叠时，操作和检索均须处理多个 namespace 的同一源。 |
| `python/stashbase_daemon.py:1950` 逐条执行 handler；`server/mfs-daemon.ts:91` 十分钟超时 | 按 request ID 做有界并发、互斥 stdout；状态/取消/资源回调保留可运行通路。长 wait/sync/reindex 不能占唯一 dispatcher。预算从 Node 请求进入时开始，覆盖 RPC 排队，再把剩余预算传给 MFS。 |
| `python/stashbase_daemon.py:1545` 大部分搜索异常返回 `hits: []` | 保留结构化错误和部分结果：WaitTimeout、IndexUnavailable、CapabilityUnavailable、OperationFailed、grep.failures 不能变成“没有匹配”。这也是当前旧实现可直接影响用户的现存问题。 |
| `server/sync.ts:278` 先拿 syncDiff、估算规模，再询问/执行大批次 | MFS.sync 已接收并调度工作，不能当纯 diff 使用。接收前安装新增索引暂停门，保持文字准备可运行；观察/估算完成后按用户决定恢复。宿主必须串行化该决定与 Settings/用户 Pause，保存应恢复的意图，避免临时暂停覆盖持久用户暂停。 |
| `design-docs/design/preparation.md` 的 PDF、OCR、DOCX、音视频及 HTML/JSON | 提供各格式的真实 Processor、SourceMap、完成标记和恢复单元。HTML 可分 grep 原文与索引文字；派生文字和源 hash/处理配置绑定，UI/MCP 结果始终映射原件。源大小与提取文字大小分别设限，保留 8 MiB 直接文字约束。 |
| `server/conversion-scheduler.ts` 的两 light/一 heavy 与播放/原生任务 | 移除搜索准备的重复队列，保留播放/Viewer 工作。Node 与 Python 必须共享真正的资源 grant；分别创建 heavy=1 不是总共 heavy=1。跨进程 grant 应预取/异步通知，生命周期锁内不做 RPC。 |
| `server/file-operation-guard.ts:6`、`server/library-file-mutations.ts` 的源操作退休 | 路径互斥 → quiesce 全部关联 namespace 范围并退休 Viewer/播放句柄 → 磁盘操作 → 租约内 content sync 新旧范围并检查完整性 → 释放租约 → 可选 wait。不得用用户 cancel 代替临时退休。索引稍后失败不能回滚已提交的源文件操作。 |
| 中途崩溃、重开、旧 failed/cancelled 迁移 | 宿主持久保存可重放路径事务与迁移步骤；有未完成日志时 MFS.open(start_paused=True)，恢复磁盘状态、绑定实现、完整 sync 后 resume_background。导入旧用户状态使用 processing_paused + restore_document_state，完成后再开门。现有 `server/recovery-journal.ts` 是编辑草稿日志，不能充当 rename/delete 事务日志。 |
| 全 Library MCP 检索；`server/indexer.ts` 支持 folder 省略 | MFS 只提供单 namespace 搜索。宿主有界 fan-out，共用总 deadline，处理各 Folder 失败、重叠源去重及统一呈现。跨 collection 原始分数不可直接当全库排名；若使用同一空间 dense 重排或其他归并规则，必须定义并跑 eval。 |
| 配置变更、清除凭据、重新绑定与退出 | 同一向量空间的凭据轮换不重建；空间/维度变化用 configure_namespace。G0/G1 重启分别绑定匹配实现，不能拿新模型查旧 collection；D1 必须先解决或明确接受。daemon 重开要等待真实退出及 PROCESS_LOCK 释放，有限重试 InstanceLocked。 |
| 冻结 sidecar 与跨平台 | Python 要从当前“允许 3.10+”收紧到新库的 3.13 要求；替换旧 hidden imports、包含监督模块，入口先分派 run_process_supervisor。Windows helper 隐藏控制台、Job Object、句柄退休和实际打包均需原生验收；本次没有在 Windows 上运行。 |

若过渡期仍由 Node 准备、MFS 仅借用产物，还需要按 incarnation/revision 限定的持久完成通知，处理“通知早于 blocked 落盘”、迟到通知和用户取消。直接在完成事件里调用无条件 retry 会清取消门；MFS 当前没有这个宿主准备通知合同。采用完整 Processor 方案可避免这套双队列同步。

不建议为了接入再给 MFS 增加源文件 rename/delete、产品权限或另一套永久操作回执。它们会混淆源所有权和当前目标模型；真正要补的是上面的故障闭环及明确的宿主 Adapter。

## 4. 建议验收顺序

1. R1–R4 已修复并有竞争/故障回归；D1 采用上方取消成员规则。
2. 跑通最小应用链路：一个 Folder、真实文本和 PDF Processor、新 RPC、搜索/取消/状态；检索/状态繁忙时文件浏览仍立即可用。
3. 验证大批次暂停前不偷跑 embedding、重叠 Folder 的源删除/改名、持久用户取消与旧状态迁移；在磁盘操作及 sync 接收前后分别杀进程重开。
4. 用实际 OCR/转录/ONNX/云端模型验证慢调用、超时、断连、额度失败、退出和共享容量；不能只用立即返回的假 Adapter 验收。
5. 运行真实 corpus retrieval eval、规模负载与冻结 sidecar 检查，再决定全量迁移。

## 5. 基线审查的验证边界

- 本机 Python 3.13.12 / macOS，真实 SQLite 和 Milvus Lite。
- `.venv/bin/python -m pytest -q`：**205 passed、3 skipped、1 xfailed**，525.32 秒。3 项跳过均要求 Windows 原生句柄；xfail 为上述 BM25 flush 一致性缺口；7 条 PDF SWIG 弃用警告。
- 新脚本验证 R1、R2、R3、R4，以及 D1；R3 包括重开，R2 包括撤销故障后的同实例恢复，R4 包括撤销单次故障后旧目标覆盖 SQLite 的情况。运行方法：`.venv/bin/python docs/review-2026-09-13-liveness-repro.py`。所有测试数据位于自动清理的临时目录。
- 追加定向回归：新旧 Internal 写入/删除、External 根目录替换、适配器重新绑定与配置晋升、提交确认丢失后的删除/排除/取消，以及规则原子更新，**10 passed，24.06 秒**。这些通过项不覆盖新发现的 R3/R4 窗口。
- R1 使用合成 metadata/cleanup 夹具；R2 是领取写事务失败注入，未填满真实磁盘。其余并发由 Event 确定窗口。复现脚本记录当前缺陷，不加入既有 pytest 的通过断言。
- 没有修改 StashBase、运行真实云端请求、执行 StashBase 全应用端到端、模拟实际断电或验收 Windows 打包。通过库测试不等于上述应用要求已满足。

## 6. 修复验证

- 新增 13 项 MFS 故障与并发回归全部通过；相关生命周期回归曾通过 82 项。最终代码全量测试：**218 passed、3 skipped、1 xfailed**，563.39 秒；ruff、格式和 pyright 检查通过。
- StashBase：typecheck、192 项 conversion-scheduler 测试、22 项 retrieval 测试、42 项 Python 测试、文档检查、Electron 测试和构建后的 Electron smoke 均通过。本机最初的 Node/SQLite 原生 ABI 不匹配在重建 better-sqlite3 后消除，重跑相关测试通过。
- StashBase 真实子进程/真实 Milvus Lite 验证：一个索引与两个搜索的 embedding 请求同时被本地 HTTP 夹具阻塞时，状态和扫描约 5 ms 返回；放行后按顺序删除、关闭、重新绑定，旧删除行未复活。此验证不访问真实云服务，也不等同于整套应用业务端到端验收。
- D3 对应的后端 xfail 与 3 项原生 Windows 跳过仍保留；未宣称修复上游 BM25 排名或完成 Windows 验收。
