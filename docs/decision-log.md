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
| search/grep 显式单 namespace，移除 ByNamespace 与跨 namespace 合并 | namespace 有独立处理和索引配置，库不定义跨集合的排序或路由 |
| grep/read 与排名 search 分开，移除 query | 精确文字/路径匹配与 BM25/向量的合同不同 |
| 不新增 ready 系统 | 应用已有状态判断和 grep fallback，避免扩大 MFS 职责 |
| 播放转换、输入选择和产品可见性由 StashBase 负责 | MFS 提供文件搜索库，不接管应用业务 |
| 允许不同 namespace 的 External 根相同或嵌套 | StashBase 各 Library Folder 独立登记和配置；只有 External root 与 MFS 状态目录的重叠需要拒绝 |
| 当前文件任务替代永久操作历史 | StashBase 需要最新文件状态；无变化 sync 不应永久增加回执，删除责任由文件 tombstone 保存 |
| 同内容观察更新源 stat，阶段提交合并最新目标 | 只改 mtime 后恢复 stat 快速路径，执行中的旧副本不能覆盖新观察 |
| Lifecycle 集中状态事务，执行模块返回类型化结果 | 清晰核验 revision/attempt，防止迟到工作覆盖新目标 |
| search 默认 eventual，timeout 默认 5 秒并限制总等待 | 交互搜索读取当前有效结果，调用方可按预算退出；超时检查贯穿搜索阶段 |
| checkpoint 协作让出，统一完成与退休 | 先持久保存，再等调用真实退出；取消、替换和重建不能复活旧目标 |
| 查询租约覆盖 embedding 至后端退出 | 超时只结束调用方等待，重建/drop 仍须等待旧 collection 的使用者退出 |
| 有界完整向量缓存独立于索引行 | 删除旧路径后仍可复用同内容；容量限制、校验和与索引代隔离保证可回收和正确性 |

历史设计保留在版本控制中；已经撤回的 External 稳定副本、全局模型继承、多阶段线程池和 SQLite 全文方案不再作为当前设计。
