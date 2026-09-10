# MFS

MFS 接收或观察源文件，将其处理成可检索的文档，并负责处理与索引工作的生命周期。
这里统一当前实现中的用语；接口契约见 `docs/design.md`。

## Language

**Source（源文件）**：提供文档内容的原始输入。External Source 由外部文件系统拥有，Internal Source 由 MFS 保存。
_Avoid_: 用“文档更新”同时指源文件更新和搜索结果更新。

**Document（文档）**：以 Namespace 与 Document ID 唯一确定的逻辑检索对象；内容变化不改变其身份。

**Source Revision（源版本）**：MFS 已经确认接收的一次源内容与处理配置组合。它不代表处理或索引已经完成。

**Snapshot（处理快照）**：源版本经过处理得到的完整文本与来源定位。Snapshot 不提供历史版本读取。
_Avoid_: Milvus Snapshot、数据库备份。

**Index Generation（索引代）**：由处理快照和索引配置共同确定的一代检索结果；配置变化也可以产生新索引代。

**Grep（文本匹配）**：在已经提交的文档文本上执行字面或正则匹配；不依赖排序索引完成。

**Indexed Search（索引检索）**：BM25、向量或 hybrid 排序检索；这些搜索共享同一个就绪条件。

**Accepted（已接收）**：MFS 已承担持久保存该请求及完成后续工作的责任。
_Avoid_: Indexed、Ready。

**Published（已发布）**：某一完整索引代已经可检索；BM25 与 dense 一起完成。

**Index Build（索引构建）**：从已接收输入生成文本、Chunk 和检索向量的工作；产物生成不代表已经可搜索。

**Ready（索引已追上）**：实例的所有已接收目标均已完成处理和索引更新，包括删除。

**Strong（强一致搜索）**：等待实例 Ready 后放行的索引检索；放行后允许并发更新，不提供一次搜索的快照隔离。

**Eventual（最终一致搜索）**：不等待实例 Ready，直接查询当前索引；允许旧结果、缺失结果及并发更新的中间态。
_Avoid_: 用 final 表示终止状态或已全部完成。


**Processing Attempt（准备执行）**：一个 revision 的一次执行，拥有独立 work_dir 和取消信号；
checkpoint 可让下一次执行从持久边界恢复。attempt token 阻止旧执行提交。

**Operation Receipt（操作回执）**：持久 operation_id 与有限目标集合；wait 只等待该操作，
不等同于整个实例 Ready。成功结果在后续覆盖后仍有效。

**Artifact（附属产物）**：随文本快照发布的不可变文件，经 open_artifact 读取；读取租约保护其生命周期。

**User Cancellation Gate（用户取消门）**：跨自动源更新保留的取消意图，retry/reprocess 显式解除。
内部 supersede/drop/close 的执行停止不创建此门。
