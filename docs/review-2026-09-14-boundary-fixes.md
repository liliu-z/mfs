# 边界修复与验收

实施范围为用户确认的第 1、2、3、4、6、7 项；第 8 项暂缓，其余宿主接入工作不在本次实现范围。审查前行为见 [2026-09-13 记录](review-2026-09-13-boundary-audit.md)，原始复现重跑结果见 [修复后输出](review-2026-09-14-boundary-audit-results.jsonl)。

## 最终行为

| 问题 | 实现与用户结果 |
| --- | --- |
| 旧清理错误阻止删除完成、污染同名重建 | 删除等待旧创建/查询/文件写入实际结束，再删除 collection；完成事务结清精确 incarnation/generation 的清理责任。重开也补齐旧版本已成功 drop 的结账；同名新 namespace 的文件等待和状态不继承旧债务。 |
| 首次初始化中断后打不开 | `.mfs-initializing` 先于锁文件和 catalog 持久创建；schema 事务完成后移除。锁前、catalog 前、schema 前异常或 SIGKILL 可恢复；未知文件和无关 schema 保持拒绝，不覆盖用户内容。 |
| 一个慢维护调用牵连所有 namespace | 两个有界维护线程按 namespace 公平调度；同 namespace 同时一个所有者，每轮有最小间隔。Milvus 调用锁按 collection 划分，不同 collection 可并行，同 collection 的读写继续互斥。 |
| 管理超时与实际退出混淆 | 创建/加载、退休、snapshot 清理使用 stage_timeout 和独立期限监督。超时公开失败并拒绝迟到发布，真实调用退出前保留占用，drop/close 继续等它。 |
| sync 别名报告提前完成 | path 是 canonical 范围，wait_paths 包含范围外的别名目标；无变化扫描也等待这些当前目标。root 重定向的报告扩大为整个 namespace。 |
| 一个坏文件阻止新检索能力发布 | 当前成员全部到达成功/失败/取消/blocked 且执行退出后，原子发布成功集合；失败和取消成员保留状态与重试能力。模型替换也遵守此策略，不混用不同维度/模型的向量；新代零成功且旧代有可用结果时保留旧代。 |
| pending 不说明原因 | DocumentStatus.blocking_reason 区分绑定、暂停、资源、退休、quiescence、配置初始化、索引不可用和重试退避。缺绑定的 wait/strong 文字等待明确报 blocked，补绑定后自然恢复。 |

部分发布不等于全 namespace Ready。eventual 可检索成功成员；文件/范围的 wait 和 strong 仍报告所选失败或取消。已有取消意图不会被配置切换、重开或其他文件的 retry 清除。

## 后端依据与实际测试

固定依赖为 pymilvus 3.0.1、Milvus Lite 3.2.1。已检查安装包的 `milvus_lite/adapter/grpc/server.py`、`servicer.py` 和 `db.py`：请求经线程池执行，collection 写入要求单 writer，不同 collection 使用独立存储目录；MFS 保留每 collection 互斥与实际查询/执行租约。

Lite 当前 handler 不因 gRPC 取消而停止。向它设置 RPC timeout 会让客户端先返回、服务端仍执行，不能据此释放 collection 锁或允许 drop。因此 MFS 的外层 deadline 结束调用方等待，维护 watchdog 撤销提交资格；内部调用保留到 handler 返回。真实后端回归在服务端阻塞 A 的 create_collection，验证 A 超时仍持有创建占用、B 可检索、A 的 drop 等真实返回后才完成。

真实后端一致性回归同时创建四个 collection，用八个写入者交错 insert/flush/search，验证每 collection 单写约束、独立 collection 并发及关闭重开后的完整数据。原有 BM25 segment 评分差异仍是上游已知 xfail。

维护无进展时的空转曾被回归复现为 0.2 秒内 853 次维护；现已限制每 namespace 的维护频率，期限线程仅在实际超时时通知等待者，避免互相唤醒空转。

并发验收还修正了候选领取竞争：G0 刚准备好文字而维护尚未接用时，G1 等待接用，不再次运行兼容 Processor，保留 checkpoint 只恢复一次的约束。绑定状态在 Runtime 创建时接入实际绑定表，Worker 启动延迟不会使已经绑定的任务误报 blocked。资源等待诊断随目标删除回收，不保留历史任务。

测试更新保留原断言目的：缺绑定按新合同立即报 blocked；故障注入适配维护线程名和按 namespace 的维护参数；GC 按其已有非阻塞 busy 合同在有限预算内验证最终回收，不假定连续三次调用必然完成。

## 验证记录

最终代码全量：`.venv/bin/python -m pytest -q -W error::pytest.PytestUnhandledThreadExceptionWarning --tb=short --show-capture=no`，**241 passed、3 skipped、1 xfailed**，620.24 秒。跳过项均要求 Windows 原生句柄；xfail 为已有 Milvus Lite BM25 segment 评分差异。七条警告来自 PDF/SWIG 依赖弃用，没有后台线程异常。

新增 23 项回归包含三处真实 SIGKILL、两种初始化异常、陌生目录/schema 保留、范围外文件别名及根重定向、旧写入与 drop/同名创建交错、旧版本成功 drop 的跨重开结账、部分模型切换后失败和取消的跨重开重试、真实后端并发/期限、无进展维护频率，以及 Worker 启动前的绑定状态。重建竞争继续由原有 checkpoint 回归约束。

`ruff check`、`ruff format --check`、strict `pyright` 和 `git diff --check` 通过；所有修改文档的本地链接存在。修复前复现中的别名等待、未绑定等待、初始化异常/SIGKILL、清理失败后 drop/重开/同名创建、失败成员首次开启向量检索均已重跑，结果见修复后 JSONL。未修改 StashBase 实现，未调用云端模型。
