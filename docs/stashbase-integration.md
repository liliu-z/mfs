# StashBase 对接

MFS 合同统一见 [设计](design.md)，完成情况见 [backlog](backlog.md)。StashBase checkout 本轮未修改；这里记录接入方式，不能视为应用已迁移。

## 实例与源身份

Python daemon 生命周期内打开一个 MFS 实例。为每个不重叠的物理源根分配稳定 namespace ID；嵌套 Folder 映射为同 namespace 的 UnderPath。源文件增删改由应用操作，再通过 sync 让 MFS 观察。

External 只记录原件指针，拒绝 upsert/remove；应用已有派生文件也可以借用指针。直接 Markdown/TXT 的 grep 读取外部文件，PDF 等需要当前提取文字。不能假定所有文件都生成一份新的 Markdown。

当前应用通过启动、打开/切换目录、窗口 focus、Agent turn end、手动 Sync 和 MCP reindex 等事件扫描，没有 filesystem watcher。接入时保留这些事件。

## 保留与移交的职责

| StashBase 保留 | MFS 承担 |
| --- | --- |
| 源文件操作、输入选择、产品可见性 | 文件观察、处理/索引任务、版本失效和恢复 |
| 播放转码、增强 PDF/HTML/OCR/转录实现 | Processor 调用、切片、向量复用、BM25/dense 写入 |
| 是否开始大批次索引的用户决策 | namespace 的 off/bm25/hybrid 与 paused |
| 自行判断 MFS 文字是否可用，必要时 grep fallback | grep、read、search、已有逐文档状态和回执 |
| sibling/派生文件关系和旧规则导入 | namespace 有序规则及统一准入/读取资格 |

搜原视频时，播放副本通过规则排除，Processor 提供原视频的转录文字。搜生成视频时，应用负责生成输入随原源更新/删除，再 sync。MFS 不自动推断两个独立文件的业务关系。

## Processor 适配

旧 mfs-cli 自身已有一般文本读取及基础 PDF/DOCX converter；StashBase 实际 daemon 路径通常接收应用转换文字，然后调用旧 Chunker、Embedder 和 store，并非每次运行旧 converter。

新 MFS 保留 UTF-8/PDF，并提供基础 DOCX；应用已有增强实现经 Adapter 接入，同格式只选择一个实现。Adapter 可以返回源文字引用、应用已有派生文字引用，或新提取文字与 SourceMap。

HTML 可以保留原 HTML grep、提取后索引。SourceMap 描述提取文字的来源，不把旧索引映射用于后来已经改变的外部文件。播放附属文件不会自动加入搜索。

外部 Processor/Chunker/Embedder 对象由应用按 namespace 创建并传入，MFS 保存兼容清单，重开时核对。StashBase 当前全局模型配置可以由应用显式传给多个 namespace，不要求 MFS 提供全局继承。

## 搜索映射

| 应用意图 | MFS 入口 |
| --- | --- |
| 精确文字、正则、名称/路径匹配 | grep |
| 查看已知文档 | read |
| 原 semantic/hybrid 意图 | search(mode="hybrid") |
| 只按词频排序 | search(mode="bm25") |
| 纯向量排序 | search(mode="vector") |

旧 public keyword 使用磁盘/派生文字 grep，并不是 BM25；旧 public semantic 通常是 dense + BM25 的 hybrid。新 MFS 与旧 daemon 的切片和候选量不完全相同，不能因使用 Milvus Lite 就宣称排名一致。

新实现每 namespace 独立 collection，筛选在后端 top-k 前应用；跨 namespace 使用排名合并。旧 daemon 的扩展名过滤、hybrid 候选数和旧 Chunker 窗口需要通过实际检索效果评估迁移。

## 规则与状态

旧 Python Scanner 会读取各根的 .gitignore/.mfsignore，其他应用搜索入口的解释并不统一。应用显式导入旧规则，并将 sibling、附件目录等特殊关系转成稳定规则；不能未经转换把旧简化 fnmatch 当成新规则语义。

同一有效规则用于 MFS 和应用 fallback。sync(path) 只是本次观察范围，不是永久白名单。应用选择只搜原件或派生件时，应登记相应规则。

用 document_status 的 revision/text_revision/indexed_revision、stage/state 和 wait(receipt) 判断具体操作。整体 status.ready 不等于某个 PDF 的文字可用；grep 命中也不等于向量已完成。不新增 ready 系统。

## 应用验收

- 替换旧 daemon 调用，绑定实际 Adapter，移除已交给 MFS 的重复处理/索引调度。
- 保留播放转换、源操作、产品决策及 fallback。
- 验证多 Folder 范围、规则和原件点击跳转。
- 验证真实 OCR/转录/embedding 的线程调用、取消及失败恢复。
- 用代表性 corpus 运行 retrieval eval，并测量首次索引吞吐和检索延迟。

MFS 单元/集成测试不能替代这些应用验收。
