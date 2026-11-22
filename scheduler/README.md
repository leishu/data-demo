# 外部调度接入（Airflow / DolphinScheduler / crontab）

外部调度器不直接跑 Spark，而是调 **lake-service 的批作业接口**。这样做的好处是：
作业仍走服务层统一的白名单校验、全局并发闸门、幂等去重、运行态库、日志搬运和审计；
调度器只负责「什么时候跑、依赖谁、失败重试」。

## 调用契约

统一入口：`bin/scheduled-batch.sh`

```bash
bin/scheduled-batch.sh --job ingest --date 2026-11-22 --wait --timeout 3600
bin/scheduled-batch.sh --job show --dry-run     # 不联网，只看会生成什么请求
```

| 参数 | 说明 |
|---|---|
| `--job` | 作业名，必须在 `LakeJobs` 白名单内（`init/ingest/transform/maintain/show/...`） |
| `--date` | **逻辑日期（数据日期）**，`YYYY-MM-DD`；默认今天（UTC）。Airflow 传 `{{ ds }}`，DS 传 `${bizdate}` |
| `--wait` / `--no-wait` | 是否等到终态；默认等 |
| `--timeout` / `--poll` | 等终态的最长秒数与查询间隔（默认 1800s / 5s） |
| `--dry-run` | 只打印请求摘要，方便在调度器里核对幂等键 |

**幂等键**：`lake-<job>-<逻辑日期>`，通过 `Idempotency-Key` 头传给服务。
同一逻辑日的重试（调度器重试、任务重跑）只会拿到同一条运行记录，不会重复执行。

**退出码**（可直接当调度任务状态）：

| 码 | 含义 |
|---|---|
| 0 | 提交成功，且（若 `--wait`）终态为 `SUCCEEDED` |
| 1 | 提交被拒（HTTP 非 202）、查询失败、终态为 `FAILED`/`TIMEOUT`、或等待超时 |
| 2 | 参数/环境错误（缺 curl/python3、日期格式不对等）；这类错误重试也没用 |

失败时脚本会把作业 `message` 和日志尾部打到 stderr，调度器日志里直接能看到原因。

**凭据**：HTTP Basic，默认 `operator/operator`；用环境变量覆盖：

```bash
export LAKE_BASE_URL=http://lake-service:8088
export LAKE_SCHED_USER=operator
export LAKE_SCHED_PASSWORD=******
```

> 脚本只负责触发和等待，**不改数据日期语义**：`--date` 用于幂等键和审计关联；作业本身处理哪几天的数据
> 仍由 `lake.batch.command-template` / 作业参数决定（例如 `ingest --days 3`）。

## Airflow

1. 把 `scheduler/airflow/dag_lakehouse_daily.py` 和 `dag_lakehouse_maintain.py`
   复制到 `$AIRFLOW_HOME/dags/`（或把它挂进 Airflow 的 dags 卷）。
2. 在 worker 上设置 `LAKEHOUSE_HOME`（仓库根目录）以及上面的 `LAKE_*` 变量；
   `LAKEHOUSE_HOME` 也可以写进任务的 `env`，证书建议走 Airflow Connection/Secret。
3. 两个 DAG：

| DAG | 调度 | 任务链 | 说明 |
|---|---|---|---|
| `lakehouse_daily` | 每天 01:00 | `ingest → transform` | `max_active_runs=1`，失败重试 2 次 |
| `lakehouse_maintain` | 每周日 04:00 | `maintain` | **单独跑**，不和写入作业抢资源；失败重试 1 次 |

`{{ ds }}` 是 Airflow 的数据间隔起始日期（例如 `2026-11-22`），正好作为 `--date`，
所以同一天的 backfill/重试幂等。

## DolphinScheduler

1. 在 worker 组里准备同样的 `LAKEHOUSE_HOME` / `LAKE_*` 环境变量。
2. 新建「Shell」任务，脚本内容：

```bash
cd "$LAKEHOUSE_HOME"
./scheduler/dolphinscheduler/run_lake_job.sh ingest ${bizdate}
```

3. 工作流 `lakehouse_daily`：`ingest → transform` 串行；
   工作流 `lakehouse_maintain`：单独一个 `maintain` 任务，定时错开每日链（例如周日 04:00）。
4. 任务超时设成略大于 `--timeout`（默认 1800s），失败重试 2 次；
   脚本的退出码就是任务实例状态，DS 不需要额外解析日志。

`run_lake_job.sh` 接收两个参数：`$1=作业名`，`$2=业务日期`；没传日期时用 UTC 今天。

## crontab（最小例子）

```cron
0 1 * * * cd /opt/lakehouse && bin/scheduled-batch.sh --job ingest   --wait >> logs/scheduler.log 2>&1
0 2 * * * cd /opt/lakehouse && bin/scheduled-batch.sh --job transform --wait >> logs/scheduler.log 2>&1
0 4 * * 0 cd /opt/lakehouse && bin/scheduled-batch.sh --job maintain  --wait >> logs/scheduler.log 2>&1
```

crontab 没有逻辑日期传参，幂等键按「执行当天」生成；若某天补跑历史数据，显式给 `--date`。

## 验证

```bash
# 1. 不联网，确认幂等键
bin/scheduled-batch.sh --job ingest --date 2026-11-22 --dry-run
# 2. 对已启动的 lake-service 真触发一次（服务要开批作业：local 模式）
bin/scheduled-batch.sh --job show --date 2026-11-22 --wait --timeout 120
# 3. 同一 (job,date) 再跑一次：拿到的 runId 和上次相同，不会重复执行
# 4. 看运行记录
curl -u operator:operator http://localhost:8088/api/batch/runs?limit=5
```
