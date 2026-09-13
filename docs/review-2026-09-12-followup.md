# MFS 当前工作区复核与 StashBase 接入边界

实施状态：本文保留审查时的复现记录；后续修复与本轮验收见 [backlog](backlog.md)。

审查日期：2026-09-12。MFS HEAD 为 `a872bde037c55041d9adb20ebed2ea9952c96f00`，审查包含其全部未提交修复及新增源码；StashBase HEAD 为 `7c146e1c1aa1e017f45695ab49dd6f34136d9ee9`，读取当前工作区。此次只新增本报告，没有修改两个项目的产品代码或原有未提交改动。

结论：当前实现已经补齐上一轮提出的主要机制，但仍有 3 个可复现的实现问题。StashBase 的迁移还需明确共享资源容量、派生文件所有权及文件事务确认这 3 个接入合同。不能把已有 MFS interface 视为应用已经完成接入。

## 验证与限制

- 完整回归：`.venv/bin/python -m pytest -q`，**146 passed、3 skipped、7 warnings，324.48 秒**。跳过的是 Windows 原生句柄测试；警告来自 PDF SWIG 依赖。
- `ruff check .`、strict `pyright`、`git diff --check` 通过。
- 实现问题另外使用临时脚本复现；现有回归尚未覆盖这些交错。脚本只使用临时状态目录，前两项使用真实 SQLite/Milvus 并注入指定时序或故障。
- StashBase 部分为源码与合同交叉检查，尚未运行迁移后的应用、真实模型或冻结 sidecar。因此下面的接入后果是依据当前调用链作出的判断，不声称已在迁移产品中发生。
- [上一轮审查](review-2026-09-12.md) 中已修复的删除丢确认、源缓存污染、POSIX 宿主强杀、cancel→reindex、strong 失败、工作目录 grep 引用、局部扫描隔离，以及 quiesce/迁移准入，不重复列作未修问题。

## 实现轴：3 个剩余问题

### I1 · P1：清理旧索引期间取消，目标仍会恢复处理并发布

位置：[Lifecycle.claim](../src/mfs/_lifecycle.py)，621–628 行；同文件 `cancel`，874–879 行；`Cleaned` 提交，773–775 行。

确定性时序：

1. R1 已完成索引，接收同一文件的 R2。
2. 在清理旧索引的 `delete_document` 内暂停执行。
3. 用户 cancel R2；状态已经显示 cancelled。
4. 放行旧索引清理，继续等待后台调度。

实际最终结果为 `state=succeeded`，SQLite 的 cancel gate 仍为 True；read 返回 R2，BM25 也命中 R2。取消状态与实际执行已经分离。

原因是第一次清理领取保存了 `cleanup_restore_state="pending"`。cancel 只更新 state/token，没有改变这个恢复状态；后续领取通过 `setdefault` 保留 pending，再次将目标设为 running。清理完成后恢复 pending，继而执行 Processor 和索引。

这违反 [设计的取消意图不变量](design.md) 第 12 节：自动流程不能撤销用户取消，只有显式 retry/reprocess 才能恢复。它与已修复的“取消后重建沿用旧 publish 阶段”是不同问题。

修复方向：清理责任可以继续，清理后的处理资格必须根据当前取消意图决定；cancel、重领清理、清理提交和重开恢复使用同一规则。至少加入“清理运行时 cancel”和“cancel 后关闭重开”两种交错验收。

本机复现脚本：`/tmp/mfs_review_cancel_cleanup_current.py`。

### I2 · P2：worker 已因存储故障停止，reindex 仍继续等待

位置：[MFS._reindex](../src/mfs/_core.py)，835–856 行；[Lifecycle.finish_execution](../src/mfs/_lifecycle.py)，721–726 行。

对重建完成事务注入连续 StorageFailed。Lifecycle 在三次失败后保存 storage_error 并停止 worker，符合停止发布的合同；但 reindex 的独立等待循环只检查 `MFS._stopping` 和任务 state，没有检查 Lifecycle 的致命存储错误。

实测 `reindex(timeout=5)` 等满 5 秒后返回 WaitTimeout；此时 worker 已退出。同一实例的普通 `wait("n")` 立即返回 StorageFailed。根据无 deadline 的循环，默认 `timeout=None` 会一直等待，直到实例关闭等外部事件发生。

影响：应用不能获得真正的恢复原因；若直接运行在 StashBase 的串行 RPC dispatcher 中，还会挡住后续状态、取消及其他 Folder 请求。

修复方向：复用 Lifecycle 的故障和完成判断，保留重建控制任务的判断顺序。后台无法继续推进时立即报告 StorageFailed；不要把已知故障表达为仍在执行或普通超时。

本机复现脚本：`/tmp/mfs_review_reindex_storage_current.py`。

### I3 · P2：无变化的 stat sync 仍存在平方级遍历

位置：[sync_namespace](../src/mfs/_sync.py)，399–401 行；外层 [MFS.sync](../src/mfs/_core.py)，941–945 行。

每个 existing 文档都会遍历整个 protected 文件集合，以判断文件是否替代了旧目录。即使全部文件 stat 未变，`replacement_child` 的计算仍要完成。之前的父目录重复枚举确实已修复，但这里保留了另一处 O(N²)。

临时 External namespace 设为 processing_paused，文件内容稳定，第二次 stat sync 的 cProfile 结果如下：

| 文件数 | `under()` 调用次数 | 含 profiler 的 sync 时间 | changed |
| --- | ---: | ---: | ---: |
| 200 | 40,000 | 0.119 秒 | 0 |
| 400 | 160,000 | 0.452 秒 | 0 |
| 800 | 640,000 | 1.771 秒 | 0 |

这些时间包含 profiler 开销，不作为生产延迟预测；调用次数直接证明复杂度。整个 sync 持有实例的 `_mutation_lock`，所以大目录扫描也会推迟其他 namespace 的 sync、规则/配置变更等需要该锁的操作。cancel 不使用该锁，不能据此说所有入口都被阻塞。

修复方向：用 protected 的规范化集合检查当前路径的有限祖先，或建立前缀结构；同时处理 nonmembers 的同类逐项匹配，保留大小写和目录变文件的正确性。回归应断言工作量随 N 和路径深度增长，而不只统计 scandir 次数。

本机复现脚本：`/tmp/mfs_audit_scan_scaling.py`。

## 接入轴：3 个需要落实的合同

### S1 · 共享重任务容量和播放交接

StashBase [调度合同](../../stashbase/code-review/data-lifecycle.md) 52–58 行要求一个容量所有者。当前 [conversion.ts](../../stashbase/server/conversion.ts) 163–168 行配置 2 light / 1 heavy；[音频预览](../../stashbase/server/audio-transcription.ts) 545–570 行先中断同源转录，再用同一个 heavy lane 执行播放转码，结束后恢复转录。

若按方案 A 把转录交给 MFS、播放保留在 Node，两边会各自调度重任务。原来的播放交接找不到 MFS 中的转录，两个 heavy 工作也可能同时运行。这既关系到资源上限，也关系到用户点击播放后能否及时交接，不能只归入“新库只有一个 worker，吞吐不同”。

接入前要决定一个覆盖准备、播放及相关本地模型调用的资源容量所有者，明确准入、临时停止、实际退出、释放及崩溃恢复。quiesce 可以退休指定源，但本身不是全应用的 heavy 容量计数器；只对同源加租约仍允许另一份源的 MFS 重任务同时运行。

MFS 若继续拥有准备调度，需要与宿主明确共享资源准入的 interface。若保留宿主准备队列，则要正式实施方案 C 的持久完成通知，不能在 Processor 中隐式等待另一套长期队列。当前单 worker 方案可先接入文本和限定 PDF，不能据此宣称音视频行为等价迁移。

### S2 · Folder 退出、源删除与共享派生文件必须分开

[MFS 对接合同](stashbase-integration.md) 7–9 行要求父子 Folder 各自拥有 namespace，独立删除。实际 StashBase 的 [RecentFolder](../../stashbase/server/app-config.ts) 47–52 行目前只有路径；[derivedKey](../../stashbase/server/derived-store.ts) 33–38 行按源绝对路径定位共享产物；[移除 Folder](../../stashbase/server/routes/library.ts) 96–115 行取消、清状态并删除整个物理前缀下的派生文件。

因此，只把旧 `deletePathPrefix` 替换为父 namespace 的 drop，仍会删掉子 namespace 正在借用的文字。MFS 的 collection 可以保持独立，但借用文件的生命周期已被应用破坏；GC 和 quiesce 都不会替应用长期保留这些文件。

接入方案：持久 Folder UUID 映射 namespace；从 Library 移除 Folder 只撤销该成员的任务和引用，真实源删除才处理所有重叠成员。提取输出优先成为 MFS 每 namespace 的受管理产物；如果必须与 Viewer 共享应用产物，采用不可变版本路径和跨消费者引用保留。任何仍在引用或读取的消费者退出前，不删除共享文件。

### S3 · 文件事务的成功确认不能直接映射为 sync + wait

StashBase [文件事务合同](../../stashbase/code-review/file-transactions.md) 214–216 行要求确认前撤销旧身份，并允许报告新身份的索引滞后。[renameWithRollback](../../stashbase/server/rename-helpers.ts) 42–60 行及 [Folder rename](../../stashbase/server/routes/folders.ts) 118–181 行仍保留索引步骤失败后回滚磁盘的调用形态。

MFS 的 sync 确认持久接收，后续准备/索引异步执行；quiesce 则阻止租约内的后台执行。因此，在租约内把 doIndex 实现为 `sync + wait` 会等待自己禁止的工作；释放租约后再等、遇错直接回滚，又会在执行已经恢复后修改源。

建议以“磁盘操作完成，相关 namespace 的新旧身份已持久观察/失效”为文件事务提交点；新索引失败单独显示为滞后或失败。若业务仍要求等待后回滚，应重新取得全部相关范围租约，在租约内补偿磁盘并再次 sync。

RPC 中长期 wait/reindex 要放到有界执行层，快速接收、状态和取消保持可响应。当前 [Python stdin dispatcher](../../stashbase/python/stashbase_daemon.py) 1950–1969 行同步调用 handler；不能直接塞入这些阻塞方法。宿主还要保存文件事务/迁移步骤，在 daemon 重启并重新绑定 Adapter 前处理未完成操作；进程内 ScopeLease 本身不是跨进程的持久文件事务日志。

## 建议的接入结构与顺序

一个 Python daemon 打开一个 MFS 实例，每个持久 Folder UUID 对应一个 External namespace。MFS 拥有当前目标、准备、checkpoint、切片、索引及其恢复；StashBase 拥有 Folder 成员关系、磁盘事务、产品策略、Viewer 和全 Library 结果呈现。播放与准备的共同资源容量按 S1 明确后再接入。

| 应用动作 | 接入安排 |
| --- | --- |
| 首次迁移 | 保存 Folder ID、规则、模型配置、用户取消/失败；用 processing_paused 创建 namespace，sync 后按 revision 恢复状态，日志完成后解除暂停 |
| 正常启动/重启 | 打开 MFS，读取持久配置，恢复未完成宿主事务，再绑定相容 Adapter；POSIX 旧 native 执行退休时按有限预算重试 InstanceLocked |
| 文件更新/改名/删除 | 路径事务锁 → 全部相关范围 quiesce → 磁盘操作 → sync 新旧范围并处理 incomplete → 释放租约；需要时另行 wait |
| 移除 Folder | 按稳定成员 ID drop 对应 namespace，保留仍被其他 Folder/Viewer 使用的共享产物 |
| 手动 Sync/MCP reindex | 映射 sync；需要恢复失败任务时显式 retry/reprocess。不要按旧操作名称直接清空整个 namespace 索引 |
| 更换 embedding 模型/维度 | 对受影响 namespace 显式 reindex；同 embedding space 的凭据轮换只重新绑定，不重做 OCR/转录 |
| 精确/语义搜索 | 精确使用 grep 或相同规则的 fallback；语义使用单 namespace search；全 Library 的 fan-out、预算和重叠 Folder 去重由应用明确 |
| Python/sidecar | 替换旧 mfs 私有 imports 和 mfs-cli 依赖，使用 Python 3.13，加入冻结 supervisor 早期入口并实际打包验收 |

实施顺序建议：先修 I1/I2/I3；落实 S1/S2/S3 的责任与状态迁移；再以一个 Folder 的 TXT/Markdown/JSON/PDF 跑通完整链路，之后接 OCR 和音视频。不要先铺开全部格式再补生命周期。

接入验收至少包含：清理时取消及重启；重建持久化失败；大目录无变化扫描；转录运行时点击播放；移除父 Folder 后子 Folder 继续读取；租约中 daemon 退出及文件事务恢复；rename 观察失败的补偿；实际 native helper 运行中强杀并重开；查询超时后重建/退出；旧取消/失败状态迁移中断与重放。

实现轴共 **3 项**，最严重的是取消后自动处理并发布；接入轴共 **3 项**，共同重任务容量与播放交接的所有者仍需明确。现有全量回归通过，但没有覆盖上述新复现，也不能替代 StashBase 的应用验收。
