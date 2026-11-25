# GLOSSARY — 领域术语

湖仓项目的核心词汇。读到规则里认不出一个词时，用它在这里找定义。定义以代码实现/DDL 为准，更细节的语义见 [docs/ARCHITECTURE.md](ARCHITECTURE.md)。

| 术语 | 定义 |
|---|---|
| **ODS** | 写入最贴源，稳定不改的数据；对应表 `bronze_orders` |
| **DWD** | 稳定基础分析层，去重后的事实表；对应表 `silver_orders` |
| **ADS** | 面向分析的汇总表，一般一个度量一张表；对应表 `gold_daily_revenue`（按 天×商户×品类 聚合的 GMV / 退款 / 净收入） |
| **bronze_orders** | ODS 层表：带 `run_id` 回溯来源批次，同一天重跑稳定，保留 3% 重复 + 10% 迟到 |
| **silver_orders** | DWD 层表：`MERGE bronze→silver` 去重，一行一个订单；`WHEN MATCHED AND s.event_time > t.event_time` 只更新变化行 |
| **gold_daily_revenue** | ADS 层表：`INSERT OVERWRITE` 生成（按 天/商户/品类），口径由语义层维护 |
| **run_id** | 批作业运行 ID（`ops.batch_runs` 落位），落到 `bronze_orders.run_id` 可直接定位数据来自哪次批次 |
| **snapshot / 快照** | Iceberg 快照；`$snapshots` 是访问元数据的 Trino 表（要双引号）；时间旅行用 `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` |
| **TableRef / catalog.db.table** | 项目统一的表引用形式（platform-common `TableRef`） |
| **batch_runs** | 三张表：`lake_batch.batch_runs`（服务写的运行记录/终态）、`ops.batch_runs`（BatchApp 写终态，跨引擎归档）、`lake_batch.batch_log_lines`（每 run 尾部日志） |
| **quota** | 查询配额：全局并发上限 + 单账号滑动窗口，放在 Postgres `lake_batch.query_leases` / `query_events`，超限返回 429 |
| **CoW（copy-on-write）** | 默认写入模式（`platform.write-mode=copy-on-write`）；Trino 读 MoR 表跨引擎静默出错风险下，CoW 是更安全默认 |
| **epoch 毫秒** | 时间的权威表示；`spark.sql.session.timeZone=UTC` 钉死，不依赖会话时区 |
| **隐藏分区（days(event_time)）** | ODS/DWD 分区按事件时间路由，上游 `dt=` 目录只影响读取范围 |
| **语义层** | 指标口径单一源 `platform-common` 的 `SemanticCatalog`；调用方选指标 + 白名单维度，服务端生成 SQL（双方言） |
| **SSE / 流式推送** | 查询结果、作业跟踪长连接推送（`meta` → `rows` → `done`），不轮询 |
| **状态库** | Postgres（默认）承担运行状态、配额、审计原文保险箱；`BatchHistoryService` 兜底湖表归档 |
| **SqlGuard** | 只读守卫：默认放行 select/with/show/describe/explain，看穿 EXPLAIN ANALYZE 内层，单语句白名单 |
| **小文件治理** | `rewrite_data_files` / `rewrite_manifests` / `expire_snapshots`（`maintain` job 用） |
| **资产目录** | `platform-common` 的 `GovernanceCatalog`：每张湖表的 owner / 业务域 / 分层 / 敏感级别 / 保留天数 / 新鲜度 SLA，Java 常量单一事实源，单测守卫 |
| **敏感级别** | L1 公开 → L4 受限（`Sensitivity` 枚举）；页面按级别给徽标着色 |
| **新鲜度 SLA** | 一张表允许的最大更新间隔（小时）；用 Iceberg 最近快照提交时间判定 OK / WARN(2×SLA 内) / BREACH / UNKNOWN |
| **治理资产** | `/api/governance/assets` 的一行：`GovernanceCatalog` ∪ `information_schema` ∪ Iceberg 指标；未登记的表也会显示出来 |
| **质量检查** | `quality` 作业的六项检查：银层唯一性/非空/金额范围、金层退款率/净收入一致性/相邻天行数波动；SQL 在 `sql/quality/` |
| **检查级别** | ERROR 失败即作业 FAILED；WARN 只记录不阻断（行数波动用 WARN） |
| **dq_results** | `ops.dq_results`：一行一次检查，`run_id` 与 `ops.batch_runs` 对齐，`snapshot_id` 定位被检查表的最近快照，observed > threshold 即失败 |
| **注入故障** | `--inject-faults`：quality 作业演示开关，往 silver 写两条重复订单号 + 一条负金额，稳定产出失败证据 |

> 词条会迭代：新增设计决策的同时补充术语。
