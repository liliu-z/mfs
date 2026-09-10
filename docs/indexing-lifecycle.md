# MFS 文档生命周期与检索一致性设计

> 2026-09-09，已落实到 MFS 的生命周期设计。
> 当前接口契约见 [design.md](design.md)，验收与集成边界见 [backlog.md](backlog.md)。
> 本版替代先前的通道独立发布、dense 占位补齐、搜索读写门及独立 Text Projection 方案。

## 1. 已确定的方向

- SQLite 是文档文本、来源定位、目标版本和任务状态的 source of truth。
- grep 使用 SQLite 中已经提交的文本，独立于 Milvus 索引进度。
- BM25 和 dense 在同一个 collection 中，准备完整后一起写入；dense 失败时不发布新版本的 sparse。
- BM25/vector/hybrid 使用同一个实例级 ready；不再分别定义通道就绪条件。
- strong 等 ready 后执行；eventual 不等 ready，直接搜索现有索引。
- 搜索期间不加 MFS 读写锁，也不要求 hybrid 两路读取同一快照。
- MFS 继续负责已接收输入的处理、状态、重试和恢复；Processor 等仍由应用注入。
- Milvus 排序搜索的过滤与结果组装不经过 SQLite。grep 直接查询 SQLite 是独立路径，不是联查。

不再引入先发 sparse、占位向量、dense_ready 列、首次联合等待预算或单独的向量补齐协议。
后台 catch-up 是重试未完成阶段，然后发布完整索引。

## 2. 存储分工

| 存储 | 内容与职责 |
|---|---|
| SQLite | 文档身份、已处理文本/SourceMap、目标/完成版本、阶段、错误、重试及删除意图；支持文档 query 和 grep |
| 管理的文件产物 | 稳定输入、可重用的处理/Chunk/embedding 产物；SQLite 保存引用 |
| Milvus 单 collection | 完整 Chunk rows：源过滤字段、text/BM25、dense、snippet/定位；负责 BM25/vector/hybrid |

grep 扫描文档文本，不以 BM25 候选代替字面匹配；无需再造一套 Text Projection。
Milvus 保留返回结果所需字段，命中后不回 SQLite 拼装文档。

## 3. 最少必要状态

每个文档保留目标 revision、已提交文本 revision、已完成索引 revision，以及当前阶段/错误。
revision 区分源内容、处理配置或显式 Reprocess 的新目标；旧任务完成不能把新目标标成完成。
Snapshot 仍表示处理后的文本与来源定位，不是 Milvus 备份能力。

实例在内存维护未完成目标集合与 `ready`。有尚未完成的处理、索引、删除或恢复工作时 ready 为 false。
该集合从 SQLite 重建；内存变量不是持久任务存储，也不能以“线程队列空了”代替全部完成。
失败和取消的工作不自动算完成；显式 retry/remove 或接收新版本才改变其后续状态或目标。

## 4. 接收与 grep 可用时刻

1. stable-read 原始输入到管理的产物，计算 hash 并持久化。
2. SQLite transaction 记录目标版本与 pending 工作；同步更新内存 pending/ready 后返回 Accepted receipt。
3. PROCESS 完成后，将完整文本、SourceMap 与索引任务一起提交到 SQLite。此时新文本可 grep。
4. 后台索引完成后，更新索引完成版本；全部目标追上后 ready 为 true。

**“SQLite 入了就能 grep”指可检索文本已经入库。** 只有 PDF bytes、路径或任务记录入库，
尚未 OCR/提取文本时，还不能 grep 到新文本。grep 调用始终不等 embedding，读取当时已提交的文本。

普通文本和已经处理好的输入可以很快到达第 3 步；OCR 失败不阻止读取其他已有文档。
Accepted 仍表示数据和任务已持久接收，不保证 OCR 或索引已经完成。
External 输入需要保留可重试的稳定 bytes，未被任何阶段引用后再 GC。
ACK 丢失可能发生在 SQLite 提交之后；相同 idempotency key 返回原回执，不能据未收到 ACK 推断没有提交。
目录 sync 逐文档接收，不是多文档事务；未变文件跳过重复处理，但继续原有未完成任务。

## 5. 一条后台处理路径

```mermaid
flowchart LR
    A[持久接收输入] --> P[PROCESS]
    P --> T[SQLite 提交文本]
    T --> G[grep 可用]
    T --> C[CHUNK]
    C --> E[EMBED]
    E --> W[Milvus 一起写入 BM25 与 dense]
    W --> D[SQLite 确认索引完成]
    D --> R[全部追上后 ready]
```

- EMBED 未完成或失败时，保留已有 Milvus 索引，暂不发布这个文档的新 rows。
- 重试只从失败阶段继续：embedding 失败不重跑成功的 OCR；Milvus 失败不重新生成已持久化向量。
- 部分 embedding 批次成功时保存结果，重试剩余批次；全部向量准备好后才发布文档索引。
- empty text/零 Chunk 也是合法完成：确认旧 rows 删除后标记索引完成。
- 阶段状态保留 pending/running/retry_wait/failed/blocked/cancelled/succeeded；错误有退避或显式 retry。
- 回调完成但产物未持久化就崩溃，仍可能重复执行；不承诺任意外部函数 exactly-once。

这条联合发布规则针对已配置 dense 的 Instance：临时缺凭据或 dense 故障不把它自动变成 BM25-only 模式。

## 6. 写入与失败恢复

保留一条简单的持久顺序：

```text
SQLite 记录待发布版本及完整产物
→ 单个索引 worker 写入 Milvus
→ 确认写入可检索
→ SQLite 记录该版本完成
→ 更新内存 pending/ready 并通知等待者
```

Milvus 使用包含文档/版本/Chunk identity 的稳定主键；重试写相同主键。
新版本准备完成后才替换旧索引。大文档的多批次写入、删除不是一个数据库事务；
eventual 或已通过准入的并发搜索允许看到中间态，直到完整发布确认前 ready 一直为 false。

Milvus 写成功而 SQLite 完成记录失败时，pending 仍存在，重放该版本写入并核对结果。
若源版本已经更新，旧完成结果不能清除新 pending；旧任务写入前检查目标，删除/重建 Namespace 同样检查身份。
只有一个 Milvus writer，所有写调用顺序执行。RPC 错误保留 pending；未知错误停止在 failed，
明确的临时错误才自动退避重试。不能在 MFS 之外直接修改同一后端。
重启先恢复未完成任务和 ready=false；不能先宣告 ready 再补读任务表。

删除先在 SQLite 记录 tombstone/清除 grep 文本，并持久记录 Milvus 清理任务。
清理完成后才算索引追上；eventual 期间可能仍命中旧索引。移出 Library 等授权变化由 Adapter 立即缩小允许的 scope。
需要等待索引清理的调用者显式等完成；Accepted ACK 本身不承诺搜索结果立即消失。

不因普通 embedding 失败锁住搜索。eventual 仍直接调用后端；后端自身报错就返回错误，不伪装成空结果。

## 7. grep、strong 与 eventual

| 调用 | 行为 |
|---|---|
| grep / query(TextMatch) | 直接匹配 SQLite 已提交文本，不检查索引 ready |
| BM25/vector/hybrid + strong | 等实例级 ready，然后执行所选搜索 |
| BM25/vector/hybrid + eventual | 不检查或等待 ready，直接执行所选搜索 |
| query(Document ID) / status | 查询已提交文档或任务状态 |

ready 是实例级的，初版不按 namespace、path 或通道细分。任何已接收目标未追上都会让 strong 等待。
strong 可指定 timeout；默认等待，关闭实例时唤醒并结束等待。超时不取消后台工作。
后台失败保留可查询原因，不通过忽略失败文档把 ready 变成 true。

eventual 不承诺新鲜度、完整索引或同版本快照，不要求返回通道降级说明，也不自动改换搜索 mode。
它可以搜到旧内容、缺少新文档，或观察并发发布的中间结果。query Embedder/后端不可用仍按正常错误返回。

**strong 的保证止于准入检查：放行时所有已接收目标已经追上。** 放行之后不冻结写入；
如果又接收更新，搜索可能观察到这些并发修改，hybrid 两路也可以来自不同时间。
本设计不再承诺单次搜索的 snapshot isolation。Milvus 读路径仍需能看到已确认发布的数据，
不能只设置 Python ready 却把尚未可检索的写入当完成。

## 8. 写入、索引与等待的线程模型

Python 3.13 支持在同一进程内使用 `threading.Thread`，多个线程共享内存。
这里采用普通后台线程，不要求调用者或现有函数改成 async/await。

- 一个准备 worker 执行 PROCESS 并提交 SQLite 文本；一个索引 worker 执行 CHUNK/EMBED/Milvus 写入。
  这样 dense 卡住不会阻止后续文档进入 grep；初版每类一个 worker，不建立复杂并行 scheduler。
- 任务状态持久记录在 SQLite，线程队列/Condition 只负责唤醒。失败退避时可以处理其他可执行任务。
- 同一个索引 worker 顺序执行所有 Milvus mutation，无需另起一个补齐 writer 或 publisher。
- grep/search 在调用者线程执行，与后台索引并发；不持有跨越搜索或 embedding 的 MFS 读写锁。
- SQLite 连接按线程使用，事务写入做必要的串行协调。PROCESS 串行；CHUNK 与 query 的 Chunk 投影串行。
  Embedder 的文档调用与查询调用允许并发，注入实现必须支持这一点；sniff 也需可与 PROCESS 并发。
- CPU 密集处理是否能利用多核取决于实现；GIL 不妨碍线程等待网络/文件 I/O。
  对有独立进程要求的 Processor 使用其受管理进程入口，不把所有 native 函数都假定为线程安全。
- close 停止接收、通知等待者并等待正在执行的 worker 退出，未完成任务留待重启。
  普通线程不能安全强杀；取消意图与实际执行退出分开记录。

ready 使用一个 `Condition` 保护的实例变量：

```python
# 搜索端示意，省略 timeout/close 的错误转换
if consistency == "strong":
    with condition:
        if not condition.wait_for(lambda: ready or closed, timeout=timeout):
            raise TimeoutError()
        if closed:
            raise Closed()
# 此处已释放状态锁；eventual 直接到这里
return index.search(...)
```

接收新任务时将其放入 pending 并置 ready=false；完成时仅移除对应的当前目标，
重新计算 ready，并 `notify_all()`。这些操作与相应 SQLite 提交协调，不能在 ACK 后还保留错误的 ready=true。
例如 v1 完成前已经接收 v2，v1 完成不能使 ready=true；全局变量也不能由每个任务独立直接写 true。

Condition 的短状态锁用于避免丢失唤醒及错误计数，不覆盖搜索或远程工作。
`wait()` 会释放这个锁并睡眠，worker 因而可以更新状态；不使用轮询布尔量的 busy loop。

以上线程与等待模型已实现，原有搜索读写锁与共享 Embedder lock 已移除。
依据：[Python threading](https://docs.python.org/3.13/library/threading.html)、
[Condition](https://docs.python.org/3.13/library/threading.html#condition-objects)、
[SQLite 线程与连接](https://docs.python.org/3.13/library/sqlite3.html#sqlite3.connect)。

## 9. 检索执行与 StashBase 覆盖

Milvus rows 保存 namespace/incarnation、doc_id、source_path/name/ext、media_type、snapshot/chunk identity、
text、snippet 和定位。Namespace、多 ID、目录、路径/文件名前后缀、源后缀/类型在 top-k 前下推。
PDF 提取为 Markdown 后，source_ext 仍是 .pdf。UnderPath 区分 exact/descendant，不能误收相似前缀目录。
转义必须覆盖引号、反斜杠、换行、%、_、Unicode；literal 前后缀不自动扩展为任意 glob。

排序搜索不从 SQLite 枚举候选 ID，也不在命中后回查 SQLite。全文读取单独调用 query。
`search(select="doc")` 明确报 InvalidQuery；TextMatch 留在 grep/query 路径，
排序 search 不隐式执行 SQLite 全文过滤再搜 Milvus，不支持的组合明确报错。
实际提供的大 ID 集合才分批，namespace/path 不先展开成 IDs。
文档级 limit 需要按 Document 聚合后应用；候选预算不足时报告 truncated，不能承诺有界 over-fetch 等价于完整 top-k。

grep 对 SQLite metadata 先筛选，再读取/匹配完整文档文本，保留跨 Chunk literal/regex 与定位。
SQLite 负责已提交文本的可见性；整个 grep 不等待 Milvus catch-up。

| StashBase case | 对接与验收 |
|---|---|
| Library/folder/Chat scope | Adapter 解析授权，Milvus 每路下推；无结果不扩大范围 |
| path_prefix、types | 按源路径、源后缀筛选，稀有类型不能被其他候选挤掉 |
| caseStrict=false、wholeWord | grep 保留 smart-case 与 Unicode 字母/数字/下划线边界 |
| PDF/图像/DOCX/音频 | Processor 仍注入，结果返回源身份及页码/时间/行定位 |
| snippet、heading | Milvus 预存结果需要的字段，不在搜索时回查 SQLite 或 live file |
| 关闭 meaning / dense 缺能力 | grep 可用；新 BM25/dense 一起等待；eventual 可直接搜已有索引 |
| 删除/移出 Library | SQLite 文本与授权范围及时更新，索引清理异步且可等待 |
| 进度、失败、Reprocess | 使用 MFS 状态及 retry，Node 不再维护第二份完成状态 |

JSON/TXT/HTML 的处理契约与已有 Adapter 保持一致：JSON 不要求语法有效，TXT strict UTF-8，HTML 由 Processor 提取文本。
StashBase 当前 extension filter 是最多 200 的有界 over-fetch 后过滤；MFS 应实现真实下推。
原位置：`../stashbase/python/stashbase_daemon.py:1498`。

## 10. symlink 与同步规则

实测 StashBase `_walk_disk`：正常 root 与 symlink root 均只返回真实 `real.txt`；
root alias 改指向另一目录后，扫描返回该目录的 `outside.txt`。
内部 dir symlink 不递归；file symlink resolve 后按真实 path 去重，root 外目标被过滤。
Preparation 和 derived keyword walker 只接受普通 Dirent file/directory，因此跳过内部链接。
MCP 文档也明确内部 symlink 不作为普通文件入口；不能把这些事实说成全项目完全相同的实现。

MFS 当前统一契约：

1. 保留调用方选择的 root spelling；每次 sync 开始 resolve，固定本轮 canonical root。
   接受 symlink root，包括系统 `/var` 与 `/private/var` 一类路径别名。
2. root 改目标允许下次 sync 观察新目标；提升 binding epoch，废弃旧 epoch 的未发布工作，
   不复用旧 root 的 stat-only 快速判断。每次重新检查与 mfs_path 的重叠限制。
3. 内部目录链接不递归；内部文件链接不建立独立 alias Document。
   若显式同步安全的内部 file alias，解析为 root 内真实 Document，应用同样的排除/格式规则；
   不允许借 alias 绕过 excluded ancestor，也不接受 root 外目标。
4. realpath/root 指向在观察期间改变，当前 observation 变为 incomplete，不据未见文件删除。
   stable read 和路径访问应基于固定 root/目录 descriptor 与一致的 containment 校验，避免只做一次字符串检查。
5. root 缺失、broken symlink、不可读时保留已有数据并报告 incomplete；不能当空目录批量删除。
6. requested、canonical identity、seen、missing deletion 使用相同平台路径比较规则。
7. 显式 file 校验 hash；directory `verify="content"` 对每个 eligible file 校验 hash。
   stat 模式保留为快速路径，no-op 内容校验不重跑成功的处理/索引阶段。

上述规则在 MFS 中实现并有回归测试；未修改 StashBase 自身不同调用链的行为。

## 11. 已实现 Interface

```python
receipt = mfs.upsert(namespace, doc_id, data, idempotency_key=key)
receipt = mfs.sync(namespace, verify="content")
mfs.query(filters=[TextMatch("word")])  # grep，读取已提交文本
mfs.search("word", mode="bm25", consistency="strong", timeout=30)
mfs.search("word", mode="hybrid", consistency="eventual")
mfs.wait_ready(timeout=30)              # 与 strong 等待同一个实例状态
mfs.status()                           # ready、pending/failed 数量
mfs.document_status(document_id)        # 阶段、版本、错误、批次、executing
mfs.list_document_statuses(namespace)
mfs.retry(document_id, stage="embed")
mfs.reprocess(document_id)
mfs.cancel(document_id)
```

不提供按 sparse/dense 分开的 wait/pause/invalidate 或一致性参数。
Processor/Chunker/Embedder 的算法保持外部注入，执行状态及成功产物归 MFS。

## 12. 当前实现边界

- `timeout` 约束 ready 等待，不是回调或 Milvus RPC 的总时间限制；超时不取消后台任务。
- 可识别的临时错误最多自动重试 4 次；未知错误保留 failed，缺能力保留 blocked。
  `retry` 从当前失败阶段续跑，`reprocess` 建立新版本重新调用 Processor。
- `cancel` 记录取消并阻止后续提交；已进入回调的线程继续到函数返回，`executing` 表示仍有执行。
  应用负责让长函数有网络超时或可管理的子进程，`close` 会等待正在执行的函数退出。
- 没有全局 pause/resume、任务优先级或多 worker 调度。初版两条固定 worker 已覆盖当前要求。
- 当前版本保留目标输入与阶段产物用于重试、reprocess/reindex；失去引用的产物在下次 open 时回收。
  连续运行期间不会与查询竞争删除原文件，在线 GC 作为后续优化。
- `query(select="chunk")` 从 SQLite 文本调用 Chunker，文档级 grep 不调用 Chunker。
- 未配置 dense 的实例支持 BM25-only；已配置 dense 的实例暂时缺 Embedder 时不能只更新 sparse。
- SQLite schema v1 自动升级；旧索引或缺失索引从 SQLite 快照重建。配置变化显式 `reindex()`。
  Snapshot 是文本与 SourceMap 的标识，没有 SQLite/Milvus 跨库快照或备份承诺。

## 13. 验证与对接

正式测试覆盖真实 Milvus Lite、后台线程与搜索并发、阶段失败和取消、版本覆盖、namespace 重建、
进程在接收/产物/发布边界终止后恢复、SQLite 提交故障、过滤下推和 symlink/sync 回归。
测试入口与未进行的应用验证见 [backlog](backlog.md)。

StashBase 的应用迁移说明见 [stashbase-integration.md](stashbase-integration.md)。
本次实现 MFS 所需能力；没有修改 StashBase daemon/Node 调度或运行其真实 OCR/检索评估。
