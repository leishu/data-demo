# bin/ — 项目便捷脚本

本目录是项目的操作入口。完整操作手册（跑第一步、接集群、排错）在 [README.md](../README.md)。
AI 助手改代码后需要重跑的命令见 [AGENTS.md「修改前的校验思路」](../AGENTS.md#6-修改前的校验思路最小可粘性)。

| 脚本 | 作用 | 典型用法 |
|---|---|---|
| `build.sh` | 全量构建（含测试）+ 预生成批处理/即席查询运行时 classpath | `bin/build.sh`；只构建一个模块 `bin/build.sh -pl spark-batch -am` |
| `dev-batch.sh` | 一键批作业（`java -cp`，依赖 build.sh 已生成 classpath） | `bin/dev-batch.sh --job all`；`--job all --evolve`（加 schema 演进）；`--job verify-sql --sql-file sql/adhoc/01_daily_revenue.sql` |
| `lake-service.sh` | 服务层打包/启动（JDK 21，默认 8088） | `bin/lake-service.sh build && bin/lake-service.sh start` |
| `smoke.sh` | 端到端冒烟（健康/鉴权/配置/查询/SSE/指标……） | `bin/smoke.sh` |
| `stack.sh` | 全栈模式（Docker infra + Trino + 服务容器） | `bin/stack.sh up` / `up --monitoring` / `smoke` / `batch --job all` / `down` |
| `infra.sh` | 起/停/诊断基础设施（Postgres + MinIO + HMS；Trino 按需） | `bin/infra.sh up trino`；`bin/infra.sh doctor`（端口/HTTP 自检） |
| `trino-query.sh` | 即席查询 CLI（`${var}` 占位符由 platform-common 替换） | `bin/trino-query.sh --list`；`--sql "select ..." --stats`；`--sql-file ... --params snapshot_id=123` |
| `submit-batch.sh` | 集群模式提交批作业（`spark-submit --packages` 拉 Iceberg runtime） | `SPARK_MASTER=yarn bin/submit-batch.sh --job all --catalog-type hive --warehouse s3a://lake/warehouse` |
| `scheduled-batch.sh` | 外部调度接入（以 `(job, 逻辑日期)` 为幂等键） | 给 Airflow/DolphinScheduler 等调度器调用 |
| `storage-lifecycle.sh` | 配置了 bucket lifecycle 模板 | `bin/storage-lifecycle.sh` |
| `install-docker.sh` | 一次性安装 Docker Engine + Compose v2 | `bash bin/install-docker.sh` |
| `build-ui.sh` | 前端构建（Vite → `lake-service/src/main/resources/static/ui/`，随 jar 打包） | `bin/build-ui.sh`；或 `npm --prefix lake-ui run dev` 开发模式 |

## AI 助手常用排查

| 现象/改动 | 应对命令 |
|---|---|
| 改了 Java 规则/工具 | `mvn -pl <模块> test`；改 `platform-common` 顺带 `bin/dev-batch.sh --job all` 再跑一轮 |
| 运行报 `NoClassDefFoundError` | 先跑 `bin/build.sh` 确认 classpath 已生成 |
| 跑批报错/卡 | `bin/dev-batch.sh --job all` 逐 job 归位：`init → ingest → transform → maintain → show` |
| 想看三层表/数据 | `bin/dev-batch.sh --job show`；`bin/trino-query.sh --list` 查表清单 |
| 语义层/审计异常 | `curl -u operator:operator http://localhost:8088/api/config` |
| 怀疑查询被拒 | `curl -u viewer:viewer -H 'Content-Type: application/json' -d '{"sql":"update t set x=1"}' http://localhost:8088/api/query` 应报只读拦截；注意 `bin/trino-query.sh` 直连 Trino，不经过 SqlGuard |
