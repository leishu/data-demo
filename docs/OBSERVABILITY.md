# 可观测性：指标、审计与 SLI/SLO

lake-service 用 Micrometer 埋点，指标经 `GET /actuator/metrics`（已暴露）读取；接 Prometheus 时
直接用下面的指标名抓取即可（本项目没内置 Prometheus registry，避免锁死版本）。
审计事件写 `logs/audit.log`（按 90 天滚动），最近 200 条可通过 `GET /api/audit` 查看（需 BATCH 角色）。

## 指标清单

| 指标名 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `lake.batch.submitted` | counter | `job` | 提交的作业数 |
| `lake.batch.completed` | counter | `status` | 各终态作业数（SUCCEEDED/FAILED/TIMEOUT） |
| `lake.batch.duration` | timer | `status` | 作业从认领到终态的耗时 |
| `lake.batch.queue.wait` | timer | `job` | 在 QUEUED 停留的时间 |
| `lake.batch.queue.depth` | gauge | — | 当前排队数（每次调度扫描刷新） |
| `lake.query.executed` | counter | `outcome` | 用户查询次数（success/error） |
| `lake.query.duration` | timer | — | 查询耗时（JSON 与 SSE 都计） |
| `lake.query.rows` | counter | — | 累计返回行数 |
| `lake.query.quota.rejected` | counter | `reason` | 配额拒绝（rate-limit / concurrency） |
| `lake.auth.denied` | counter | `reason` | 鉴权拒绝（missing-credentials / bad-credentials / insufficient-role） |

Spring Boot 自带 `http.server.requests`、`jvm.*`、`hikari.*` 等基础指标，不再重复定义。

## SLI 与 SLO（demo 目标，不是生产承诺）

| SLI | 计算方式 | SLO 目标 | 观察窗口 |
|---|---|---|---|
| 查询可用性 | `sum(rate(lake_query_executed{outcome="success"}[5m])) / sum(rate(lake_query_executed[5m]))` | ≥ 99.5% | 30 天 |
| 查询延迟 | `histogram_quantile(0.95, rate(lake_query_duration_bucket[5m]))` | P95 ≤ 5s | 30 天 |
| 作业成功率 | `sum(rate(lake_batch_completed{status="SUCCEEDED"}[1h])) / sum(rate(lake_batch_completed[1h]))` | ≥ 99% | 30 天 |
| 排队等待 | `histogram_quantile(0.95, rate(lake_batch_queue_wait_bucket[5m]))` | P95 ≤ 60s | 7 天 |
| 依赖健康 | `up{job="lake-service"} == 1 and trino health == UP` | 可用性 ≥ 99% | 30 天 |

> 说明：本项目是单机 demo，Trino / MinIO / HMS 都在本机 Docker 里，延迟目标比生产宽；
> 这些数字的作用是**让「坏掉」有明确定义**，而不是声称达到某个 SLA。

### 建议告警

- **查询错误率 > 5% 持续 5 分钟**：先看 `/actuator/health` 的 trino 组件与 `lake.query.quota.rejected`；
  配额拒绝多说明是容量问题而不是依赖挂了。
- **队列深度 > 20 持续 10 分钟**：调大 `lake.batch.max-concurrent-jobs`，或检查是否有作业卡在 RUNNING。
- **作业失败率 > 10% 持续 15 分钟**：看 `logs/batch/*.log` 与审计里 `batch.completed` 的 status 分布。
- **鉴权失败突增**：`lake.auth.denied` 计数异常通常意味着有人在扫接口或凭据配置错了。

## 审计事件

统一字段：`at`（ISO-8601）、`event`、`actor`，外加事件字段。事件类型：

| event | 字段 | 触发点 |
|---|---|---|
| `auth.denied` | path, reason / required_role | 缺凭据、权限不足 |
| `auth.failed` | path, reason | 口令错误（actor 是尝试的用户名） |
| `batch.submit` | run_id, job, idempotent | 作业入队 |
| `batch.completed` | run_id, job, status, duration_ms | 作业终态 |
| `query.execute` | sql_fp, sql_len, outcome, rows | 查询完成/失败 |

**隐私约定**：SQL 只写 SHA-256 前 16 位指纹（`sql_fp`）和长度，不写原文、参数或结果；
口令和 Authorization 头永不入审计。字段值里的空白收敛成 `_`，避免伪造出额外的 key=value。

## 怎么验证

```bash
# 指标
curl -u operator:operator http://localhost:8088/actuator/metrics/lake.batch.submitted
curl -u operator:operator http://localhost:8088/actuator/metrics/lake.query.duration

# 审计
curl -u operator:operator 'http://localhost:8088/api/audit?limit=20'
tail -20 logs/audit.log
```
