# lake-service/AGENTS.md — 服务模块规范

构建/运行需 JDK 21（父 pom release 11，本模块覆盖成 21；`mvn -pl lake-service` 必须跑 21）。
根仓约定不重复（根 [AGENTS.md](../AGENTS.md)）。

## 1. 模块结构（AI 定位用）

```
lake-service/src/main/java/com/demo/lake/server/
├── LakeServiceApplication.java
├── config/LakeConfiguration.java       # 运行时配置（conf/platform.properties 回显、批作业开关）
├── controller/
│   ├── AuthController.java             # /api/auth/session|login：控制台登录态（公开，不弹原生 Basic 框）
│   ├── AuditController.java            # /api/audit*：最近事件 + 原文保险箱（原文需 AUDITOR）
│   └── GovernanceController.java       # /api/governance/{assets,dq,lineage,retention}：资产/质量/血缘/保留审计
├── query/
│   ├── QueryController.java            # /api/query、/api/query/stream（SSE）
│   ├── QueryService.java               # 同步查询执行、只读守卫调用、审计记账
│   ├── SqlGuard.java                   # 只读守卫：默认白名单 select/with/show/describe/explain，看穿 EXPLAIN ANALYZE
│   └── ...                             # QueryFailedException / InvalidQueryException / TrinoUnavailableException
├── batch/
│   ├── BatchController.java            # /api/batch/{job}（202 + runId）、/api/batch/runs
│   ├── service/                        # JobDispatcher / JobExecutor / JobCompletionService / BatchHistoryService
│   └── store/                          # RunStateStore / JdbcRunStateStore / LogStore / JdbcLogStore（状态库访问）
├── observability/                      # AuditService / AuditEvent / IcebergTableMetrics / LakeMetrics
├── support/StateDbDdl.java              # 状态库 DDL 模板装载（classpath:sql/state/）
├── support/StateDbTables.java           # lake_batch / lake_audit 表名单一事实源
├── health/TrinoHealthIndicator.java    # /actuator/health（trino 组件）
└── observability/SqlRedactor.java      # 审计原文保险箱写入前的 SQL 脱敏（保留结构，掩码敏感字面量）

lake-service/src/main/resources/sql/state/   # H2/Postgres 双兼容 DDL：schema / 各表 / 索引 / 迁移列定义
```

## 2. 工作流

- 改接口/配额/审计/批作业编排 → `mvn -pl lake-service package`（JDK 21）+ `bin/smoke.sh`
- 只动 `platform-common` → `mvn -pl platform-common test`，然后 `mvn -pl lake-service package`
- 服务行为验收用 `bin/smoke.sh`（健康/鉴权/配置回显/查询/SSE/语义层/审计/配额/指标等 22 项）

## 3. 本模块约定（重点项）

- **阻塞 IO offload**：WebFlux 请求线程就是 Netty 事件循环；Hikari 取连接、`rs.next()`、等子进程都要 `subscribeOn(blocking)` 到虚拟线程。改路径时别破坏这一点。
- **SSE 流式**：查询结果与作业跟踪都是 `Flux` 推送（`meta` → `rows` → `done`）；客户端断开时服务端 `stmt.cancel()` / 停日志游标，别“白算”。
- **批作业经子进程**：`JobExecutor`（`LocalProcessJobExecutor` / `KubernetesJobExecutor`）默认 `command-template = bin/dev-batch.sh --job {job}`（`java -cp` 跑 spark-batch）；**本模块不养 SparkSession**。
- **权限**：`/api/**` 走 HTTP Basic（`viewer/viewer` 只读，`operator/operator` + 批作业，`auditor/auditor` 审计原文）；静态页面、`/actuator/health`、`/actuator/info`、`/api/auth/session|login` 公开。服务无会话，控制台登录页把凭据放进每个请求头（SSE 走 fetch 流，EventSource 带不了头）；过滤器只在「没带凭据」时发挑战头，别让原生弹框盖掉登录页。
- **状态库语义**：`lake_batch.batch_runs`（共享）+ `ops.batch_runs`（归档）合并读；`history.status` 是兜底，不要编造状态。
- **OpenAPI/Swagger**：`/v3/api-docs` 公开；网关接管文档时设 `springdoc.api-docs.enabled=false`。
- **审计与原文保险箱**：`AuditService` 只记身份/动作/结果 + SQL 指纹（`logs/audit.log` + 内存环形缓冲）；原文保险箱 `AuditTextStore` 可选开启，写 Postgres `lake_audit.query_text`（AES-256），`SqlRedactor` 在写入前对 SQL 字面量脱敏。
- **状态库 DDL**：SQL 文本在 `resources/sql/state/`，由 `StateDbDdl` 装载，表名以 `StateDbTables` 为准（不要在各 Store 里硬编码）；索引单独成文件，便于老 H2 不支持 `IF NOT EXISTS` 时单独降级。新增模板要补 `StateDbDdlTest` 守卫。各 `@PostConstruct` 原有的 fail-closed / 降级策略保持不变。

## 4. 配置

- 运行配置：`conf/platform.properties`（不入库）+ `conf/platform.stack.properties`（`bin/stack.sh` 生成）。
- `lake.security.enabled=false` 关掉 Basic 认证（生产环境应由网关/IdP 接管）。

## 5. 常见踩坑

- Trino 没起：查询返回 503 并给出“先起 Trino”提示，健康检查如实报 DOWN。
- `SqlGuard` 改语义：`EXPLAIN ANALYZE` 真执行内层语句；`setReadOnly` 只是驱动标记。
- 配额/状态改动看 Postgres：`QueryQuotaService`、`JobDispatcher`、`BatchHistoryService` 的并发/超时规则放在 `lake_batch.*` 表里。
- `bin/smoke.sh` 失败最常见的是 401/429/公开接口不公开（security 关开状态不一致）。
