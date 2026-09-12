# 实施进度

唯一设计依据：[MFS 设计](design.md)。这里只记录差距和验证结果，不重复设计正文。

## 本轮实现

- [x] **EXT-001** External 零复制；引用式文字输入/输出；SQLite 和恢复文件不重复保存正文；GC 只处理受管理文件。
- [x] **VIS-001** 源替换/删除/排除后立即撤销旧结果；持久清理旧代；失败、迟到结果、重启和 reindex 不复活旧源。
- [x] **NS-001** namespace 独立适配器清单与 fail-fast 绑定；每 namespace 一个 Milvus collection；独立索引模式/暂停。
- [x] **IGNORE-001** namespace 有序规则、增删改/排序及版本冲突检查；扫描、读取和提交统一资格判断。
- [x] **API-002** 移除 query 及公开 Query 类型，公开有界 grep 和明确 read，迁移示例/测试。
- [x] **REF-001** 单后台处理 worker + GC；每文件最新目标；集中生命周期事务并拆分执行/读取职责；受管理文件按 namespace 组织。
- [x] **ROOT-001** 拒绝重叠真实 External 根，保护源文件，显式处理存量重复登记。
- [x] **PROCESS-001** 保留 UTF-8/PDF，补基础 DOCX；支持应用提供的已有文字引用和增强处理。
- [x] **VERIFY-001** 行为回归、真实 Milvus 验证、重启/崩溃恢复、格式和类型检查。

## 验证记录

2026-09-11，本轮实现验证：

- 完整回归：**79 passed，3 skipped**；跳过的是 Windows 原生句柄测试，当前机器为 macOS。
- 最后调整 off/bm25 重开时省略 Embedder 后，namespace 与补充合同测试：**14 passed**。
- `ruff check`、`ruff format --check`、strict `pyright` 和 `git diff --check` 全部通过。
- 包含真实 Milvus Lite、不同维度共存、崩溃恢复、取消/清理重试、External 零复制、规则、DOCX 和 HTML 路径验证。
- PDF 依赖报告 7 条 SWIG 弃用警告，未影响测试。

实现前基线为 65 passed、3 skipped；不以旧基线替代本轮验收。

## 应用后续工作

- [ ] **INTEGRATION-001** StashBase daemon 迁移及实际转换器适配；沿用应用的输入选择、播放转换与 grep fallback。
- [ ] 在 StashBase 的代表性 corpus 上运行 retrieval eval 和端到端恢复验证。

READY-001 不在本轮范围：保留已有状态和回执，由 StashBase 自行选择 fallback，不另建 ready 系统。
