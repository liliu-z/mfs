# 设计决定

具体合同统一在 [design.md](design.md)，术语见 [CONTEXT.md](../CONTEXT.md)。本文件只保留决定和理由，不再维护逐轮替代方案。

| 决定 | 理由 |
| --- | --- |
| 保留薄 MFS 实例，业务配置归 namespace | 统一数据库/线程生命周期，同时允许各 namespace 的模型、规则和索引策略独立 |
| SQLite 共享 catalog；Milvus 每 namespace 一个 collection | 元数据结构相同，向量维度和模型可不同 |
| External 原件只存指针，Internal 保存原件 | 文件所有权决定复制、删除和恢复责任 |
| 只有必要转换才产生新文字文件 | 直接文本不复制，StashBase 已有产物也可借用 |
| 源替换立即失效，物理清理异步 | 新 PDF 处理失败不能让旧 PDF 继续被搜索 |
| 单后台处理 worker，GC 独立 | 每文件各阶段有先后关系，避免多阶段队列和额外合并 |
| 最新目标可覆盖，旧代清理责任必须保留 | 反复修改只处理最新内容，删除后重建仍能正确清理 |
| grep/read 与排名 search 分开，移除 query | 精确文字/路径匹配与 BM25/向量的合同不同 |
| 不新增 ready 系统 | 应用已有状态判断和 grep fallback，避免扩大 MFS 职责 |
| 播放转换、输入选择和产品可见性由 StashBase 负责 | MFS 提供文件搜索库，不接管应用业务 |
| 拒绝重叠 External 根 | 父目录和子目录可用同一 namespace 的范围表达，避免同文件登记两次 |

历史设计保留在版本控制中；已经撤回的 External 稳定副本、全局模型继承、多阶段线程池和 SQLite 全文方案不再作为当前设计。
