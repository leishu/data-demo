# 可观测性：指标、审计与 SLI/SLO

lake-service 用 Micrometer 埋点，同时暴露 `GET /actuator/metrics`（人看）和
`GET /actuator/prometheus`（Prometheus 文本抓取）。两者都需要 BATCH 角色（demo 里用 `operator/operator`）。
仓库自带可选的 Prometheus + Grafana 栈（`bin/infra.sh up monitoring`），凭据、告警规则和面板都已预置；
不想起容器时直接用 curl 抓端点也一样。

审计事件写 `logs/audit.log`（按 90 天滚动），最近 200 条可通过 `GET /api/audit` 查看（QUERY 或 AUDITOR 角色都可以，三个演示账号都能看），按时间倒序返回。

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
| `lake.audit.vault.decrypt.error` | counter | — | 查询原文密钥缺失/解密失败（需要人工介入，不能当作“记录不存在”） |
| `lake.iceberg.files` | gauge | `table` | Iceberg 数据文件数（查 `$files`） |
| `lake.iceberg.avg_file_size_bytes` | gauge | `table` | 平均文件大小（小文件多会降低查询效率） |
| `lake.iceberg.total_size_bytes` | gauge | `table` | 数据文件总字节数 |
| `lake.iceberg.snapshots` | gauge | `table` | 快照数（查 `$snapshots`，暴涨说明该跑 `maintain` 了） |

表级指标由 `IcebergTableMetrics` 每 60s 从 Trino 查元数据表写入，可用
`lake.metrics.iceberg.enabled/interval-seconds/tables` 调整；表还不存在或 Trino 没起时保留上次的值，
不影响服务启动。元数据表在 Trino 里要写成 `iceberg.lake."silver_orders$files"`（`$` 必须加双引号）。

Spring Boot 自带 `http.server.requests`、`jvm.*`、`hikari.*` 等基础指标，不再重复定义。

**Prometheus 命名换算**：Micrometer 的 `lake.query.executed` 在抓取文本里是 `lake_query_executed_total`；
timer 会展开成 `lake_query_duration_seconds_bucket` / `_sum` / `_count`；gauge 保持
`lake_batch_queue_depth`。下面 SLO 表达式用的是抓取后的名字。

## 本地监控栈（可选）

```bash
bin/lake-service.sh build && bin/lake-service.sh start   # 被抓取的服务
bin/infra.sh up monitoring                              # Prometheus :9090 + Grafana :3000
```

- Prometheus 配置在 `infra/prometheus/prometheus.yml`：每 15s 抓 `127.0.0.1:8088/actuator/prometheus`，
  用 `operator/operator` 做 Basic 认证。容器用 `network_mode: host`，所以**宿主机服务和全栈模式下的
  lake-service 容器**（端口都发布在宿主机回环上）都能被同一个 target 抓到，不需要改配置。
- 告警规则在 `infra/prometheus/rules/lake-slo.yml`，对应下面的 SLO 与建议告警；改完可执行
  `curl -X POST http://localhost:9090/-/reload` 热加载。
- Grafana（`http://localhost:3000`，`admin/admin`）自动装载 `Lakehouse` 文件夹下的
  `lake-service SLI/SLO` 面板，数据源指向本机 Prometheus。
- 全栈一键起：`bin/stack.sh up --monitoring`；只想停监控：`bin/infra.sh stop prometheus grafana`。

> 如果改了 demo 账号，记得同步改 `prometheus.yml` 里的 `basic_auth`；生产环境请换成网关签发的 token。

## SLI 与 SLO（demo 目标，不是生产承诺）

| SLI | 计算方式 | SLO 目标 | 观察窗口 |
|---|---|---|---|
| 查询可用性 | `sum(rate(lake_query_executed_total{outcome="success"}[5m])) / sum(rate(lake_query_executed_total[5m]))` | ≥ 99.5% | 30 天 |
| 查询延迟 | `histogram_quantile(0.95, rate(lake_query_duration_seconds_bucket[5m]))` | P95 ≤ 5s | 30 天 |
| 作业成功率 | `sum(rate(lake_batch_completed_total{status="SUCCEEDED"}[1h])) / sum(rate(lake_batch_completed_total[1h]))` | ≥ 99% | 30 天 |
| 排队等待 | `histogram_quantile(0.95, rate(lake_batch_queue_wait_seconds_bucket[5m]))` | P95 ≤ 60s | 7 天 |
| 依赖健康 | `up{job="lake-service"} == 1 and trino health == UP` | 可用性 ≥ 99% | 30 天 |

> 说明：本项目是单机 demo，Trino / MinIO / HMS 都在本机 Docker 里，延迟目标比生产宽；
> 这些数字的作用是**让「坏掉」有明确定义**，而不是声称达到某个 SLA。

### 建议告警

下面四条已落成 Prometheus 规则（`infra/prometheus/rules/lake-slo.yml`），告警名带 `Lake` 前缀：

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

### 查询原文保险箱（合规可选）

普通审计只有指纹，无法回答"那条查询到底写了什么"。需要逐字回溯时，开启独立保险箱：

```yaml
lake:
  audit:
    sql-capture-enabled: true
    redact: true          # 字符串字面量掩码，日期/短数字/短 ID 保留
    retention-days: 30
    key-id: v1
```
密钥通过环境变量 `LAKE_AUDIT_KEY`（base64 的 32 字节 AES-256 密钥）注入。**开启了却没配密钥时服务启动失败**，
不会悄悄写明文。开启后每次查询在审计表里多出一个 `event_id`，原文经脱敏 + AES-GCM 加密后写入
`lake_audit.query_text`，与日志层通过 `event_id` 关联。

读取需要 **`AUDITOR` 角色**（`auditor/auditor`）：

```bash
curl -u auditor:auditor 'http://localhost:8088/api/audit/sql/<event_id>?reason=INCIDENT-123'
curl -u auditor:auditor -X POST 'http://localhost:8088/api/audit/sql/<event_id>/hold?hold=true'
```

**密钥轮换**：每条原文都会记录写入时的 `key_id`。轮换时保留旧密钥，旧记录才读得出来：

```yaml
lake:
  audit:
    key-id: v2                 # 当前密钥编号
    keys:                      # 也可以用环境变量 LAKE_AUDIT_KEYS_V1 / LAKE_AUDIT_KEYS_V2 ...
      v1: <base64 32 字节>      # 旧密钥
      v2: <base64 32 字节>
```

`LAKE_AUDIT_KEY` 始终表示“当前密钥”，优先级高于同编号的历史配置；若密钥环里没有当前
`key_id`，启动直接失败。读取时按记录里的 `key_id` 选密钥：缺密钥或 GCM 校验失败会抛明确错误，
并累加 `lake.audit.vault.decrypt.error`，**不再伪装成 404 “记录不存在”**。

每条原文记录带 `redacted` 字段说明是否脱敏；`legal_hold=true` 的记录不会被 TTL 清理。
**每次读取原文也会再写一条 `audit.text_access` 审计事件**（含 reason），响应带 `Cache-Control: no-store`。

> 脱敏是启发式规则：保留表/列/函数和日期、短数字、短字母 ID，掩码其它字符串字面量，
> 遇到 password/token/phone/id_card 等上下文一律掩码。如果法规要求逐字原文，
> 把 `redact` 设为 false，并按最高敏感级管理保险箱（独立数据库角色、不进备份、不进日志采集）。
> 更完整的生产方案是 Trino Event Listener：在引擎侧统一捕获所有客户端的查询事件。

## 怎么验证

```bash
# 指标：人看 / Prometheus 抓取
curl -u operator:operator http://localhost:8088/actuator/metrics/lake.batch.submitted
curl -u operator:operator http://localhost:8088/actuator/prometheus | grep lake_query_executed_total

# 监控栈（起起来之后）
curl http://localhost:9090/api/v1/targets | python3 -m json.tool | grep -A2 lake-service
curl -u admin:admin http://localhost:3000/api/dashboards/uid/lake-service-slo | python3 -m json.tool | head

# 审计
curl -u operator:operator 'http://localhost:8088/api/audit?limit=20'
tail -20 logs/audit.log

# 一条命令把上面这些（含 401/查询/SSE/语义层/审计/只读守卫/指标抓取）全验一遍
bin/smoke.sh                       # 宿主机模式 22 项；容器模式 19 项
```
