# MFS 生命周期补齐

2026-09-10 实现。本文说明 ProcessingContext、按回执等待、内容复用和在线 GC。
基本检索契约见 [design.md](design.md)，应用映射见 [stashbase-integration.md](stashbase-integration.md)。

## 1. 边界

SQLite 保存文本、任务、操作结果和文件引用；Milvus 用一个 collection 联合发布 BM25/dense，
仍只有一个索引 writer。`query(TextMatch)` 读取已提交文本；`search(strong)` 等实例 ready；
`search(eventual)` 直接查询后端，允许旧结果和更新中间态。

本轮不增加 query cancellation，也不增加 pause/resume 或独立 sparse/dense 发布状态机。
StashBase 的 ripgrep 子进程超时不等同于 Milvus 查询取消。索引缺能力、临时故障和永久失败
仍通过 blocked / retry_wait / failed 表达；应用主动暂缓可由 Adapter 抛 CapabilityUnavailable。

## 2. ProcessingContext

新的 Processor 签名为：

```python
process(staged_path: Path, media_type: str, context: ProcessingContext) -> ProcessedDocument
```

旧的两个参数的 Processor 继续可用。两种都是普通同步函数，由 MFS 后台执行，不要求 async def。

| Context 成员 | 用途 |
|---|---|
| document_id / revision / content_hash | 本次已接收输入的身份 |
| work_dir | 每次执行独立的可写临时目录 |
| cancellation.reason / check() / wait(timeout) | 协作取消及退出观察 |
| report_progress(completed, total=None, unit=None) | 内存更新，最多每秒持久化一次 |
| resume_state / resume_files | 上次成功 checkpoint 的 JSON 和只读文件 |
| checkpoint(state, files={name: path}) | 保存完整的恢复边界 |
| run_process(argv, timeout=None, env=None) | 无 shell 的受管理子进程执行 |

Processor 必须处理 staged_path 的稳定 bytes；不重新读取 live 源文件，也不自行保存另一套任务状态。
状态/checkpoint/文本提交均校验 revision 与 attempt token；旧执行不能覆盖新任务。
进度在 checkpoint 和后续阶段提交时一并持久化，查询状态不读取完整文本或向量。

checkpoint 先复制并 fsync 文件，再提交 SQLite manifest 和强引用。重试、close/open 或进程终止后，
同一 revision 从最后成功的 checkpoint 恢复。每次 checkpoint 完全替换恢复 state/files；省略 files 表示无文件。
resume_files 是不可变副本；再次提交或修改前先复制进 work_dir。
`reprocess` 接收一个新 revision，不继承旧 checkpoint，也不命中 PROCESS 内容缓存。

### 2.1 调度和取消

`PreparationPolicy()` 默认有 2 个 light worker、1 个 heavy worker。Processor 可声明
`workload = "light" | "heavy"` 与正整数 `concurrency`，未声明时为 light / 1。
内置 UTF-8 Processor 允许并发 2；PDF 为 heavy / 1。每个文档的旧执行退休后才启动下一次执行。

`set_active_scopes([UnderPath(...)])` 由宿主传入当前打开的范围。优先级为显式 reprocess、active scope、
背景任务；等待时间按 `aging_seconds=60` 提升优先级，避免持续的新交互使旧任务永远排不到。
同一个成功内容的并发 PROCESS 请求合并执行，再由各文档独立提交自己的版本。

在 checkpoint 处，若有同类且能够执行的更高优先级任务，MFS 可以用内部控制异常让出 worker。
再次调用 Processor 时通过 resume_state 恢复，不增加故障计数。Adapter 不应捕获并吞掉此控制异常。
不 checkpoint、也不检查取消的任意 Python 回调只能等其返回。

用户 `cancel(id)` 的意图持久化；之后自动接收新 bytes 不会解除取消。`retry(id)` 或 `reprocess(id)`
显式解除。supersede/drop/close 的停止信号不创建用户取消门。`executing` 表示旧执行是否仍未退出，
不能把 state=cancelled 当作子进程已经消失。

`run_process` 将输出暂存磁盘，避免 stdout/stderr 管道塞满。POSIX 使用独立进程组，退出时清理组内进程；
Windows 在恢复挂起进程前加入 Job Object，退出时终止 Job、释放进程句柄并等待 active process 计数归零，
见 [Job accounting](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_accounting_information)。
超时、取消、异常和正常返回都清理执行树。
Adapter 仍负责网络超时、命令参数、产物格式与输出大小策略。MFS 不强杀 Python 线程。

### 2.2 附属产物

```python
return ProcessedDocument(text, source_map, artifacts={"transcript": transcript_path})

with mfs.open_artifact(document_id, "transcript") as artifact:
    payload = artifact.read()
    snapshot_id = artifact.snapshot_id
```

产物必须是 work_dir 下的普通文件，不能经 symlink 或 `..` 越界。MFS 复制为唯一、不可变文件；
text/source_map/附属产物 manifest 与下一个索引阶段在同一 SQLite 事务发布。空文本仍是有效结果。
读取句柄绑定打开时的 snapshot_id 并持有租约；即使文档被删除或替换，句柄仍可完成读取。
关闭 MFS 前须先关闭应用持有的产物句柄。

## 3. 操作级等待

所有写操作仍立即返回 Accepted 回执。需要本次操作完成时：

```python
receipt = mfs.remove("notes", "a.txt")
mfs.wait(receipt, timeout=30)

receipt = mfs.sync("files")
mfs.wait(receipt, timeout=30)
```

`wait` 接受 MutationReport / DropReport / SyncReport。operation_id 和目标 revision 集合持久保存，
同一回执可以超时后重用，也可以关闭重开后继续等待。等待时不持有 mutation lock。

- 只等待这个操作的目标及其目录替换清理，不受不相关文档失败拖累。
- 删除完成表示旧身份/旧 incarnation 的 Milvus rows 已清理；之后开始的查询不再返回它们。
  已经开始的查询仍可以持有旧结果。随后创建的新版本不属于旧操作。
- timeout 抛 WaitTimeout，不取消任务；close 唤醒等待并抛 Closed。
- failed / blocked / cancelled 抛 OperationFailed，带 revision、state、error_code、retryable。
- 完成前被新目标覆盖，抛 Superseded；已经成功的历史操作不被后来更新改写。
- Sync 的集合在扫描完成时封口；后来的扫描不会扩展旧回执。观察不完整的回执不能因 wait 变成成功。
- 删除和 namespace 清理优先于尚未开始的普通索引工作，且不要求 Embedder 或匹配的索引配置。
  已经进入后端的写操作必须先退出，仍然只有一个 writer。

`index_ready` 统一表达回执生成时的实例 ready；幂等重放保留原来的历史值。
`wait_ready` 和 strong search 继续使用实例级就绪条件。

## 4. 内容复用

缓存是内部优化，不增加 rename API。多个路径拥有独立 DocumentId、源版本、SQLite 状态和 Milvus rows。
源 hash 相同仅意味着可能复用推理；路径、元数据和旧身份删除仍正常更新数据库。

| 阶段 | 缓存条件 |
|---|---|
| PROCESS | 源 hash、media type、Processor id/version/options，以及身份上下文 |
| CHUNK | 实际 text/source_map 和 Chunker 配置 |
| EMBED | 实际批次文本、embedding space、dimension、document 用途 |

默认把文档身份和 namespace incarnation/binding 纳入 PROCESS key。声明 `cache_scope = "content"`
表示 Processor 承诺输出与路径/身份无关；必须在具体 Adapter 类重新声明，修改行为的子类不会自动继承承诺。
内置 UTF-8/PDF Processor 已声明。相同批次输入可复用向量；query embedding 不使用这个缓存。

只复用成功的完整结果，checkpoint 只用于自己的 revision。显式 reprocess 重做 PROCESS；如果最终
embedding 输入没变，仍可复用向量。删除旧路径与发现新路径可以分属不同 sync，只要缓存还在即可复用。
缓存有 checksum；被清理、缺失或损坏时正常重新计算，不改变任务语义。

## 5. 在线 GC

作为库，MFS 不创建独立 daemon 进程。默认维护线程由 open/close 管理；宿主可以关闭自动维护：

```python
mfs = MFS.open(path, processors=processors, gc_policy=GCPolicy(enabled=False))
report = mfs.collect_garbage()
```

默认 interval=3600 秒、idle_seconds=30 秒、grace_seconds=3600 秒。
每批最多 32 个文件、软预算 50 ms，每次调用最多 256 个条目、软预算 1 秒。
状态轮询不刷新 idle 时间。存在执行或产物读取时跳过；新请求到来后不再认领下一份文件。
显式 collect 使用相同的空闲条件和预算。OS 调用无法保证硬实时上限。

SQLite registry 记录唯一文件身份、强引用、unreferenced_at 和删除认领。
当前文本、已发布索引的附属文件、目标稳定输入、未完成/失败/取消任务以及 checkpoint 都是强引用根。
引用随业务事务切换，缓存是弱引用。读取在查 SQLite 元数据之前取得租约，贯穿实际文件使用。

GC 只在短元数据事务中复核引用和宽限期、标记 deleting、失效缓存；文件删除在锁外进行。
新引用不能绑定已认领文件。认领后崩溃、unlink 后崩溃都可继续幂等回收。
SQLite busy 或 Windows sharing violation 进入 GC 自己的诊断/后续重试，不更改索引任务或 ready。

初始化先迁移和校验强引用，完成后才启动维护；包括旧版本在 PROCESS 文本提交前保存的隐含快照。
open 不再遍历磁盘删除孤儿。磁盘 inventory 低频逐项执行，首次发现的无主文件先获得新的宽限期，
不只凭 mtime 删除；不跟随目录链接，不递归一次删完非空目录。
GC 不删除外部源文件、历史幂等/操作元数据，也不做 SQLite VACUUM 或 Milvus compaction。
close 只停止维护并等待当前执行退出，不跑完整清理。

## 6. 平台和验收

Windows 使用原生句柄固定文件身份，读取前拒绝重解析点；路径拒绝设备名、alternate streams 和尾部点/空格别名。
文件 bytes 使用 binary descriptor，保留 CRLF；稳定读取另外核验原生 ChangeTime，避免将 Windows 的创建时间当成变更时间，
见 [FILE_BASIC_INFO](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_basic_info)。
文件 fsync、SQLite Windows VFS、os.replace 和实例锁共同持久化状态。
Windows 不提供 POSIX 目录 fsync 的同等接口。Milvus Lite 3.2.1 已使用 os.replace 保存 manifest，无需旧兼容补丁。

测试见 test_extensions.py、test_recovery.py、test_windows.py 和真实 Milvus conformance tests。
CI 配置 Linux/macOS/Windows；macOS 本地通过不代表已经在 Windows 主机完成执行。
StashBase 的 Node/daemon 生命周期迁移和实际 corpus eval 仍属于另一 checkout 的应用工作。
