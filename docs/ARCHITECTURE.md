# ARCHITECTURE — 数据流与设计决策

面向 AI 编程助手的架构说明。README 是操作手册（怎么做），这篇是关键设计的“为什么”和模块边界。

## 1. 总体数据流

```
上游落地文件 (Parquet/CSV/JSON, 按 dt=YYYY-MM-DD 分目录)
        │   Spark: ingest（生成文件 → 写入 ODS）
        ▼
ODS  bronze_orders      (append, 按 event_time 分区)
        │   Spark: transform — MERGE bronze→silver（去重 + upsert，只更新变化的行）
        ▼
DWD  silver_orders      (一行一个订单)
        │   Spark: transform — INSERT OVERWRITE silver→gold（按 天×商户×品类 聚合）
        ▼
ADS  gold_daily_revenue
```

Iceberg 是存储/表格式层（format-version=2）；catalog：本地为 hadoop，集群为 hive(HMS)；storage：本地/MinIO(s3a)。三条命令即可本地跑通。

各接入点：

- 批作业入口：`bin/dev-batch.sh --job all` → `spark-batch/BatchApp` → 各 `job/*Job`（`InitJob` / `IngestJob` / `TransformJob` / `EvolveJob` / `QualityJob` / `MaintainJob` / `ShowJob` / `VerifySqlJob` / `RetentionJob`）；`all` 的顺序是 init → ingest → transform → quality → maintain → show（加 `--evolve` 在 transform 后插入 evolve），`verify-sql` 与 `retention` 只在显式 `--job` 时跑。`ShowJob` 的巡检输出同时给出三张业务表和四张 `ops` 元数据表（`batch_runs` / `dq_results` / `lineage_runs` / `retention_audit`）的行数，运维表没配保留策略，清不清靠这个数判断。
- 即席查询：`lake-adhoc`（官方 trino-jdbc 薄封装 + `${var}` 预处理 + 表格输出），`bin/trino-query.sh`。
- 服务路径：`lake-service`（REST / SSE + 语义层 + 配额 + 审计）。

## 2. 时间模型（权威口径）

- 会话时区钉死为 UTC：`spark.sql.session.timeZone=UTC`（PlatformConfig 的 sparkConf 生成）。
- **时间的权威表示是 epoch 毫秒**（`bigint`），编进 schema，不靠会话时区解释。写入一律用 `unix_millis`；运行记录表 `ops.batch_runs` 的 `started_ms/finished_ms` 直接存绝对时刻。
- ODS/DWD 用 `days(event_time)` **隐藏分区**（不入湖日期）：迟到数据自动落到正确的历史分区；`order_id` 带业务日期，同一天重跑稳定。

## 3. 关键设计决策（为什么这样选）

1. **写入一律 copy-on-write，不用 merge-on-read**（`platform.write-mode=copy-on-write` 是默认）。
   Trino 的 Iceberg connector 读 MoR 表跨引擎容易踩坑：Spark 写出的 delete file（position/equality delete）版本不齐时，Trino 做错误合并会**静默出错**。CoW 以写放大换零兼容问题；之后所有读端验证过，才有底气切回 `platform.write-mode=merge-on-read`。

2. **去重用 `MERGE` + `row_number()`，不做全量覆盖**。
   `WHEN MATCHED AND s.event_time > t.event_time` 只更新真正变了的行；`WHERE event_time >= current_date() - INTERVAL 7 DAYS` 让每次只碰最近几个分区。

3. **快照和文件要定期治理**：`rewrite_data_files`（合小文件）、`rewrite_manifests`（控制元数据规模）、`expire_snapshots`（清历史）。不管的话元数据目录会越滚越大，Trino 查询规划也变慢。

4. **语义层口径只维护一份**。
   `platform-common` 的 `SemanticCatalog` 定义 GMV / 净收入等指标；调用方只能选指标 + 白名单维度 + 过滤，服务端生成 SQL；`SemanticSqlRenderer` 同时产出 Trino / Spark 两种方言（引号风格不同、口径一致）。

5. **服务无状态，作业由调度器编排**。
   作业状态不放内存：提交落 `lake_batch.batch_runs` 的 `QUEUED` 记录，`JobDispatcher` 按提交顺序认领；并发名额（`lake.batch.max-concurrent-jobs`）在同一把 Postgres 行锁里检查+认领，多实例共享。执行方式可切：`local`（本机子进程）/ `kubernetes`（K8s Job API + 终态归档 Pod 日志）。

6. **查询有配额，只读守卫严格白名单**。
   全局并发上限 + 单账号滑动窗口都放在 Postgres（`query_leases` / `query_events`），多实例共享；超限返回 **429**（不等待）。`SqlGuard` 默认只放行 `select/with/show/describe/explain`，特别看穿 `EXPLAIN ANALYZE`（Trino JDBC 的 `setReadOnly` 只是标记，内层语句没有服务端拦截）。

7. **服务层不侵入 Spark 代码**。
   Spring 只在 driver/服务侧；批处理 jar 走 `spark-submit` / `java -cp`，要序列化到 executor，塞 `ApplicationContext` 只会让集群问题更难查。

8. **阻塞 IO 一律 offload**（WebFlux 请求线程就是 Netty 事件循环）。
   Hikari 取连接、`rs.next()`、等子进程都要 `subscribeOn(blocking)` 到虚拟线程。

9. **治理档案是常量，不另起配置体系**。
    `platform-common` 的 `GovernanceCatalog` 和 `SemanticCatalog` / `LakeJobs` 一样是 Java 单一事实源：
    platform-common 被三个模块共用，为几行元数据引 YAML 解析只会多一份依赖和一份运行时失败可能。
    责任人 / 业务域 / 分层 / 敏感级别 / 保留天数 / 新鲜度 SLA 写错由 `GovernanceCatalogTest` 拦住。
    `/api/governance/assets` 只做装配：`information_schema` 说表在不在，目录说谁负责、什么级别，
    Iceberg 的 `$snapshots` 说多久没更新；三者不互相覆盖。

9. **长任务用 SSE，不用轮询**。查询结果和作业跟踪都是一条长连接推到底；客户端断开时服务端 `stmt.cancel()` / 停日志游标。

## 4. 模块依赖关系

```
                platform-common
        （DDL / SQL / 配置 / CLI / 语义 / 作业注册表）
              ▲          ▲           ▲
              │          │           │
       spark-batch   lake-adhoc   lake-service
              ▲                      │
              └──── 子进程调用 ──────┘
                     （+ Trino / 状态库）

lake-ui：独立 Vue 前端，构建产物进 lake-service 静态资源，不参与 Maven 依赖图
```

- `spark-batch`、`lake-adhoc`、`lake-service` 都依赖 `platform-common`。
- `spark-batch` 与 `lake-service` **互不依赖**：批作业经子进程接入（`BatchController` → `BatchJobService` / `JobDispatcher` → `LocalProcessJobExecutor` / `KubernetesJobExecutor`；本地默认 command-template 是 `bin/dev-batch.sh --job {job}`）；服务侧不养 SparkSession。
- 改动成本预估：改 `platform-common` 影响所有模块；改 `spark-batch` 只动批；改 `lake-service` 只动服务；改 `lake-adhoc` 只动 Trino 封装。

## 5. 状态与账本（运行数据在哪里）

| 数据 | 位置 | 说明 |
|---|---|---|
| 运行状态 | Postgres `lake_batch.batch_runs` | 服务无状态；每 run 一条运行记录（run_id/job/status/耗时/日志路径） |
| 全局湖表归档 | `ops.batch_runs` | `BatchApp` 写终态；`--no-record` 可关 |
| 作业日志 | 本地 `logs/batch/{id}.log` / K8s Pod stdout | 每 run 尾部 2000 行 |
| 查询租约/配额 | Postgres `lake_batch.query_leases` / `query_events` | 实例崩溃自动回收过期租约 |
| 审计事件 | `logs/audit.log` + 内存环形缓冲 | 只记身份/动作/结果 + SQL 指纹，不记 SQL 原文 |
| 查询原文保险箱（可选） | Postgres `lake_audit.query_text` | AES-256 加密；写入前经 `SqlRedactor` 对字面量脱敏 |

## 6. 外部系统的切换点（升级接入集群只需改 1-3 个配置项）

| 切换 | 配置项 |
|---|---|
| HMS + MinIO(S3)，Hive 表 | `platform.catalog.type=hive`、`platform.hive.uri`、`platform.warehouse=s3a://...`、`platform.s3.*` |
| Spark 提交方式 | `platform.spark.master=yarn` + `lake.batch.command-template`（换 yarn 只改这组） |
| Iceberg 存储包 | s3 模式需勾 `spark-batch/pom.xml` 的 `s3` profile |
| 配额/审计 | 通过 lake-service 启动配置（Basic Auth、quota） |

## 7. 常用术语

- **ODS/DWD/ADS**：写入→增量去重→汇总三层，对应表 `bronze_orders` / `silver_orders` / `gold_daily_revenue`。
- **run_id**：`ops.batch_runs` 的 run ID，也写进 `bronze_orders.run_id`，可直接定位数据来自哪次批次。
- **snapshot / 时间旅行**：Iceberg 快照，`$snapshots` 元数据表（Trino 访问要双引号），`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`。
- **TableRef / catalog.db.table**：所有作业与 DDL 的统一引用形式。
- **SSE / streaming**：查询与作业结果的长连接推送（`meta` → `rows` → `done`）。
- **quota**：查询并发/频率限制（429），放 Postgres。

## 8. 资产目录的数据来源

| 信息 | 来源 | 说明 |
|---|---|---|
| 表在不在、什么类型、字段注释 | Trino `information_schema` | 复用 `QueryService.listTables()`，不另存一份表结构 |
| owner / 业务域 / 分层 / 敏感级别 / 保留天数 / 新鲜度 SLA | `GovernanceCatalog`（Java 常量） | 单测守卫；未登记的表照样显示，标成「未登记」 |
| 文件数 / 快照数 / 最近更新时间 | `IcebergTableMetrics.snapshot()` | 从 `$files` / `$snapshots` 定时采集，关闭采集时显示 UNKNOWN |

表浏览页的徽标与档案条都读 `/api/governance/assets`，不在前端硬编码负责人或级别。

## 9. 数据质量检查流

```
QualityJob --job quality [--inject-faults]
    │  逐项执行 sql/quality/*.sql（common 的 QualityChecks 是注册表）
    ▼
observed / threshold / detail ──写──> iceberg.ops.dq_results（run_id + snapshot_id）
    │                                      │
    │ ERROR 项失败即抛异常 → BatchApp 落 FAILED ─┘
    └──────────────────────────────────────> GET /api/governance/dq → 治理页（点 run_id 跳作业日志）
```

设计约定：

- 所有检查统一产出「越大越差」的 `observed` 与允许上限 `threshold`，`observed > threshold` 即失败，
  Java 侧不需要为每种检查写一套比较逻辑；`detail` 存重复订单号/异常行示例作为证据。
- 检查项与级别只在 `QualityChecks` 登记，SQL 全外置到 `sql/quality/`；DDL 在 `sql/ddl/50_ops_dq_results.sql`，
  由 `LakeTables` 装载、`SqlResources` 登记，单测遍历保证没有漏模板。
- `--inject-faults` 只往 silver 写脏数据（不重跑 transform），所以能稳定命中唯一性与金额范围两项 ERROR；
  集成测试用真实 Spark 验证「干净数据全 PASS → 注入后失败并留下证据」这条闭环。
- 结果表读不到（没跑 init / Trino 不可用）时接口返回空列表，治理页照常打开。

## 10. 血缘与保留

```
写表作业（init/ingest/transform/evolve/maintain）
    └─ BatchApp 调 LineageWriter ──> ops.lineage_runs（run_id, table, snapshot_id）
sql/transform/*.sql 头部 @lineage 注释 ──LineageParser──> LineageCatalog（表级/列级声明）
                                                          │
                                    GET /api/governance/lineage?table=gold_daily_revenue
                                    → upstream: silver → bronze；runs: 哪次运行/哪个快照

retention 作业（默认 dry-run）
    ├─ 按 GovernanceCatalog.retentionDays 找旧分区 → 列出分区与行数
    ├─ AUDIT retention.start / retention.done（stdout + ops.retention_audit）
    └─ 仅 --execute 时 DELETE，不 ALTER 业务表
```

关键取舍：

- 血缘分两层：**设计层**声明在转换 SQL 注释里（改 SQL 忘改声明由单测拦住），**运行层**在 `ops.lineage_runs`
  （快照 ID 来自 Iceberg 元数据表）。两者都不改业务表结构。
- 写入级血缘由 BatchApp 在**每个作业成功后**统一记录，按 `LineageCatalog.writtenTables(job)` 决定记录哪些表；
  记账是旁路，失败只提示不改变作业退出码。
- 保留期是资产目录的一个字段（`retentionDays`），不在 retention 作业里重复维护；`--retention-days` 只作为演示/回归覆盖。
- dry-run 是默认，且同样写前后审计事件——审计的是「评估过什么」，不只是「删过什么」。

## 11. 敏感列分级与治理告警

```
敏感列口径（MaskingCatalog 单一事实源）
   ├─ Trino 视图：sql/view/*_masked（analyst）/ *_restricted（viewer）
   │       └─ 服务采样按角色选目标；AUDITOR 走单独授权读基础表明文
   └─ rules.json 的 columns[].mask：误给基础表 SELECT 时仍兜底（masked_reader 演示）

GovernanceCatalog.SLA + IcebergTableMetrics.snapshot ─┐
ops.dq_results（quality 作业写） ─────────────────────┴─> GovernanceMetrics
                                                            └─> /actuator/prometheus
                                                                  └─> lake-slo.yml 告警
```

关键取舍：

- 视图读基础表的最新快照，不存数据副本；`--evolve` 加的可选列不进明细视图，聚合口径继续走 gold。
- 视图里做的是可区分的哈希/分桶，access-control 兜底掩码直接给 `***`：即使有人误把基础表
  授权给分析角色，也拿不到明文；而正常走视图时哈希仍有区分度。
- 服务只暴露 gauge 数字，SLA 阈值只在 Prometheus 规则里维护一份，避免 Java 与 YAML 两处漂移。
- 新鲜度没有快照时补 -1，规则用 `>= 0` 跳过 UNKNOWN；质量检查恢复时 gauge 清零，告警才会自动消。
