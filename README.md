# lakehouse-demo

[![CI](https://github.com/leishu/data-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/leishu/data-demo/actions/workflows/ci.yml)

Spark 做批处理、Iceberg 做存储与表格式层、Trino 做即席查询的湖仓脚手架，外面套一个 Spring Boot 4
服务层做查询接口与作业编排。三条命令就能在本地跑通，不装 Hadoop、不装 Docker 也行；要接集群时只改 3 个配置项。

```
 上游落地文件(Parquet/CSV/JSON)          HTTP 调用方 / 控制台
        │                                      │
        │  Spark Batch  (spark-batch)          │  Spring Boot 4 (lake-service, JDK 21)
        ▼                                      ▼
┌──────────────────────────────────────────────────────────┐
│ Iceberg 表格式层                                          │
│   ODS  bronze_orders   →  DWD  silver_orders  →  ADS      │
│   (按事件时间分区)        (MERGE 去重)          gold_daily_revenue │
│   catalog: hadoop(本地) / hive(HMS)   storage: 本地/MinIO │
│   快照 · 时间旅行 · schema 演进 · 隐藏分区 · 小文件治理    │
└──────────────────────────────────────────────────────────┘
        ▲ 写入                                 ▲ 即席查询
        │                                      │
   Spark 3.5                              Trino 476 (JDBC)
```

## 版本矩阵

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 11（批处理）/ 21（服务层） | Spark 3.5 官方支持到 17，所以批处理模块产出 Java 11 字节码；`lake-service` 用 21 |
| Spark | 3.5.1 (Scala 2.12) | 批处理 |
| Iceberg | 1.5.2 | `iceberg-spark-runtime-3.5_2.12` |
| Trino | 476 | 服务端镜像 `trinodb/trino:476` + 官方 `trino-jdbc`（lake-service 查询与 lake-adhoc 即席查询共用） |
| Spring Boot | 4.1.2 | WebFlux + Actuator，Spring Framework 7 / Reactor Netty / Jakarta EE 11 |
| 控制台 | Vue 3.5 + vue-router 4 + Vite 7 | 独立工程 `lake-ui/`，构建产物进 `static/ui/` |
| 日志 | log4j2（批处理）/ logback（服务层） | Spark 发行版绑死 log4j2；Web 服务用 Boot 默认的 logback |
| Hadoop | 3.3.4 | 由 Spark 传递依赖，无需单独装 |
| Lombok | 1.18.34 | 编译期生成访问器 / 构造器 / `log`；`provided` 不进产物，fat jar 单独 exclude |
| scala-collection-compat | 2.11.0 | Iceberg 的 `CALL` 存储过程在 Scala 2.12 下必需 |

## 目录结构

```
lakehouse-demo/
├── platform-common/          公共层：配置、Iceberg DDL、转换 SQL、SQL 脚本解析
│   └── src/main/java/com/demo/lake/common/
│       ├── config/PlatformConfig.java    本地/集群配置归一化
│       ├── catalog/LakeTables.java       三层表 DDL（含表属性）
│       ├── catalog/TransformSql.java     MERGE / 汇总 SQL
│       ├── catalog/TableRef.java         catalog.db.table
│       ├── sql/SqlScript.java            读脚本、按分号切分
│       ├── cli/Args.java                 极简参数解析
│       └── cli/SqlParams.java            即席查询 ${var} 替换 + 缺参跳过（供 lake-adhoc 调用）
├── spark-batch/              批处理：init / ingest / transform / evolve / maintain / show / verify-sql
├── lake-adhoc/               即席查询：官方 trino-jdbc 薄封装 + SqlParams 预处理 + 表格输出
├── lake-service/             Spring Boot 4 服务层：查询/作业 REST + SSE 流 + 控制台页面
├── lake-ui/                  Vue 3 控制台（Vite）：查询台 / 表浏览 / 批作业 / 指标语义 / 配置
├── sql/adhoc/                可以直接丢给 Trino 的查询脚本
├── infra/                    docker-compose: Postgres + MinIO + Hive Metastore + Trino
│   └── lake-service/         全栈模式的 lake-service 镜像（jar + JRE 21）
├── .github/workflows/ci.yml  CI：构建/单测 + 主分支 E2E
├── bin/                      便捷脚本（build / stack / smoke / dev-batch / infra ...）
└── conf/platform.properties.example   集群模式配置模板
```

## 快速开始（本地，免 Docker）

```bash
# 1. 编译 + 跑测试（会真实建表、MERGE、加列、清理快照）
bin/build.sh          # 自动找 ~/soft/jdk-21* / /usr/lib/jvm/java-21*；也可 export JAVA21_HOME 指定
                      # 整个 reactor 用 JDK 21 构建，但 spark 模块编译目标仍是 Java 11

# 2. 一条命令跑完 init → ingest → transform → maintain → show
bin/dev-batch.sh --job all

# 3. 想连 schema 演进一起看（加 channel 列 + 回填 + 时间旅行前的快照对比）
bin/dev-batch.sh --job all --evolve

# 4. 没有 Trino 也能校验 SQL（用 Spark 执行同一份脚本）
bin/dev-batch.sh --job verify-sql --sql-file sql/adhoc/01_daily_revenue.sql

# 5. 起服务层（查询 REST + 作业编排 + 控制台）
bin/lake-service.sh build && bin/lake-service.sh start   # http://localhost:8088/

# 6. 或者：一键全栈（Docker infra + Trino + 容器化服务），再跑端到端冒烟
bin/stack.sh up          # 首次会构建 jar、起 infra、跑一遍批处理、起服务
bin/stack.sh smoke       # 端到端冒烟（容器模式自动跳过批作业项）
```

> `bin/build.sh` 只收到 `-DskipTests` / `-pl ...` 这类参数时会自动补 `package`，不用手写生命周期目标。
> 只动批处理时 `mvn -pl spark-batch -am package` 用 JDK 11 也能构建；只有 `lake-service`
> 要求 JDK 21（模块内已挂 enforcer，版本不对会直接给出提示而不是 javac 的晦涩报错）。

本地模式用 **Hadoop catalog**：元数据就是 `warehouse/lake.db/*/metadata/*.metadata.json`，
数据是 Parquet，`data/raw/orders/dt=*/` 是模拟的上游落地文件。全程不需要任何外部服务。

跑完之后你会看到（默认 7 天 × 2000 单）：

```
bronze_orders         14,385 行   ← 含 3% 重复 + 10% 迟到数据
silver_orders         14,000 行   ← MERGE 去重后，一行一个订单
gold_daily_revenue       783 行   ← 按 天 × 商户 × 品类 聚合
```

再跑一次 `--job ingest --days 3` 然后 `--job maintain`，就能看到小文件治理的效果：
bronze 从 18 个文件（每分区 2 个）合并回 9 个（每分区 1 个，平均 30KB → 更大文件）。

单独跑某一步：

```bash
bin/dev-batch.sh --job init
bin/dev-batch.sh --job ingest --days 7 --orders-per-day 2000
bin/dev-batch.sh --job transform
bin/dev-batch.sh --job maintain --retain-last 5
bin/dev-batch.sh --job show
```

## 服务层（Spring Boot 4 + JDK 21）

`lake-service` 把「查」和「跑」包装成 HTTP：查询走 Trino（Hikari 连接池），批作业走独立进程。

```bash
export JAVA21_HOME=~/soft/jdk-21.0.12      # 也可以不设，脚本会自动找 ~/soft/jdk-21*
bin/lake-service.sh build                  # 打包（release 21）
bin/lake-service.sh start                  # 后台启动，默认 8088
open http://localhost:8088/                # 内置查询控制台
open http://localhost:8088/swagger-ui.html # 接口文档（OpenAPI 3 / Swagger UI，可在线调试）
```

接口文档：`/swagger-ui.html` 是可视化页面，OpenAPI JSON 在 `/v3/api-docs`（YAML 在 `/v3/api-docs.yaml`）。
文档本身公开；点右上角 **Authorize** 填 `viewer / viewer`（或 `operator / operator`）后就能 Try it out，
真正的 `/api/**` 仍走 Basic 认证与角色校验。

| 接口 | 说明 |
|---|---|
| `POST /api/query` | `{"sql":"select ...","maxRows":100}` → 列定义 + 行 + 耗时 + `truncated` |
| `POST /api/query/stream` | 同上，但走 SSE：`meta` → `rows`… → `done`，出错则发 `error` |
| `GET /api/semantics` | 指标目录：口径表达式、来源表与允许的维度 |
| `POST /api/semantics/render` | 按指标/维度/过滤生成 SQL，`engine=trino\|spark` 可看两种方言 |
| `POST /api/semantics/query` | 执行语义查询（固定 Trino 方言），同样受配额与审计约束 |
| `GET /api/tables` | 当前库的表清单 + 字段清单（读 Trino 的 `information_schema`，字段 `comment` 即 DDL 里的中文名） |
| `GET /api/tables/{t}/sample?limit=10` | 采样一张表 |
| `GET /api/config` | 生效配置回显（S3 密钥打码）；`batch.jobs` 带作业卡片的标题/描述/分组/是否仅命令行 |
| `POST /api/batch/{job}` | 触发批作业，202 + runId；job 走白名单；带 `Idempotency-Key` 头可去重 |
| `GET /api/batch/runs[/{id}]` | 运行记录与日志尾部；列表 = 状态库 `lake_batch.batch_runs` ∪ `ops.batch_runs` 历史（按 id 合并，库内记录优先），历史读不到时降级返回 `history.status` |
| `GET /api/batch/runs/{id}/events` | 作业跟踪 SSE：`status` / `log` / `ping` / `end` |
| `GET /actuator/health` | 带 `trino` 组件的健康检查 |

> 除静态页面、接口文档与 `/actuator/health`、`/actuator/info` 外，所有接口都要 HTTP Basic 认证；配额超限返回 429。
> 生产环境由网关 / IdP 接管文档访问：设 `springdoc.api-docs.enabled=false` 与
> `springdoc.swagger-ui.enabled=false` 即可关掉公开端点。
> 指标、审计与 SLI/SLO 定义见 [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)。

几个刻意的设计：

- **SQL 默认只读**。`SqlGuard` 只放行 `select/with/show/describe/explain`，且必须单条语句；
  `EXPLAIN ANALYZE` 会把内层语句真实执行一遍，所以只读模式下会继续按白名单校验内层 SQL
  （Trino JDBC 的 `setReadOnly` 只是驱动标记，不做服务端拦截）。
  要放开把 `lake.query.allow-writes` 置 true。给外部一个「能跑任意 SQL」的接口太危险。
- **时间是权威 epoch 毫秒，不靠时区约定**。`ops.batch_runs` 的 `started_ms` / `finished_ms`
  直接存绝对时刻（BatchApp 写入，InitJob 为老表按 UTC 墙钟回填）；旧的 `timestamp` 墙钟列已废弃。
  绝对时刻编进 schema，JVM / 会话时区怎么变都不会让历史列表偏移 8 小时。
- **有语义层，口径只维护一份**。`platform-common` 的 `SemanticCatalog` 定义 GMV / 净收入 /
  退款率等指标；调用方只能选指标、白名单维度和过滤条件，服务端生成 SQL 并执行（也走配额与审计）。
  `SemanticSqlRenderer` 支持 Trino / Spark 两种方言（引号风格不同、口径一致），
  方便把同一份定义下推到别的引擎的调用方。
- **API 需要鉴权**（demo 级）。`/api/**` 走 HTTP Basic：`viewer/viewer` 只读查询，
  `operator/operator` 额外能触发作业和看 actuator 指标；页面、`/actuator/health`、`/actuator/info` 公开。
  生产环境请设 `lake.security.enabled=false`，由网关 / IdP 做认证授权（静态口令只适合本机演示）。
- **查询有配额**。全局并发上限（`max-concurrent-queries`）+ 单账号滑动窗口
  （`max-queries-per-minute`）都放在 Postgres（`lake_batch.query_leases` / `query_events`），
  多实例共享同一份额度；并发名额是带过期时间的租约，实例崩溃没归还也会自动回收；
  等不到名额返回 **429** 而不是一直挂着；内部读运行历史不占配额。
- **Trino 挂了服务照常启动**（连接池 `initializationFailTimeout=-1`），查询返回 **503** 并给出
  「先起 Trino」的提示，而不是 500 或起不来。健康检查则如实报 `DOWN`。
- **批作业用子进程**，不在 Web 容器里养 SparkSession：内存、线程、崩溃隔离都更省心，
  换成 `spark-submit --master yarn ...` 也只改 `lake.batch.command-template`。
- **作业走调度器，不再由请求线程直接 fork**。提交只落一条 `QUEUED` 记录；`JobDispatcher`
  按提交顺序认领，名额检查与认领放在同一把数据库行锁里，多实例共享同一个并发上限
  （`lake.batch.max-concurrent-jobs`）。执行方式可切：
  `local` 是本机子进程（默认），`kubernetes` 通过 K8s Job API 提交、轮询终态并把 Pod 日志搬进状态库，
  且可带 `Idempotency-Key` 去重——抢占幂等键和写入 `QUEUED` 在同一事务里，同一 key 并发重复 POST
  也只会得到同一条运行记录。
- **服务无状态**。批作业的 RUNNING 状态不放内存，而是写进 infra 里已有的 Postgres
  （`lake_batch.batch_runs`）；只有执行器还活着的 run 才续心跳（`RunningRunRegistry`），
  服务重启或开多个实例都能看到同一份列表。终态写库失败时心跳也会停，对账会按时把僵死作业
  先尝试从湖表终态归档恢复，否则落「实例失联」终态（对账周期默认 60s）。
  湖表 `ops.batch_runs` 仍由 BatchApp 写终态，作为跨引擎的长期归档。
  本地和 K8s 执行器都会把日志行实时搬进状态库的 `lake_batch.batch_log_lines`（每 run 只留尾部 2000 行，
  默认保留 7 天）。本地执行器读 `logs/batch/{id}.log`；K8s 执行器按 `lake-job` 标签找到 Pod，
  用 follow 流读 stdout/stderr，作业结束/超时/取消时关流。SSE 首帧共享日志为空时会惰性重探，
  所以请求落到非执行实例也能看到后续日志；老记录仍可退回本机日志文件。
- **服务层不侵入 Spark 代码**。Spring 只在 driver/服务侧；批处理 jar 要进 `spark-submit`、
  要能序列化到 executor，塞一个 `ApplicationContext` 只会让集群问题更难查。
- **阻塞 IO 一律 offload**。WebFlux 的请求线程就是 Netty 事件循环，Hikari 取连接、`rs.next()`、
  等子进程都得 `subscribeOn(blocking)` 到虚拟线程上，否则一个慢查询就能让整个服务不响应。
  JDBC 本身没变成响应式（Trino 没有 r2dbc 驱动），响应式只加在 Web 层与流控上。
- **长任务用 SSE，不用轮询**。查询结果和作业跟踪都是一条长连接推到底，前端不再定时打接口；
  客户端断开时服务端 `stmt.cancel()` / 停掉日志游标，不白算也不空转。
- **运行历史落湖表，但不依赖它**。作业结束时 BatchApp 往 `ops.batch_runs` 写一行终态
  （`--no-record` 可关），服务端用 `information_schema` 探测表在不在，读不到就只降级提示；
  记账失败也不会把作业退出码改掉——它与作业结果无关。


## 控制台（Vue 3 + Vite）

`lake-service` 自带单页控制台，源码在 `lake-ui/`（Vue 3.5 + vue-router 4 + Vite 7），
构建产物写到 `lake-service/src/main/resources/static/ui/`，所以 jar 里自带页面，不需要额外的 Web 服务器。

| 页面 | 能做什么 |
|---|---|
| 查询台 `#/query` | SQL 编辑器（Ctrl/⌘+Enter 执行）、结果边到边渲染（SSE）、表格（类型标注、数值右对齐、NULL 置灰）、耗时/截断提示、QueryStats、复制 TSV |
| 表浏览 `#/tables` | 三层表清单 + 采样预览（20/50/200 行，表头带字段中文名），一键「在查询台打开」 |
| 批作业 `#/jobs` | 按白名单触发作业、运行记录自动刷新、点开看退出码/耗时/日志尾部（RUNNING 时用 SSE 实时跟） |
| 指标语义 `#/semantics` | 选指标/维度/过滤，预览 Trino / Spark 两种方言 SQL，或直接执行语义查询 |
| 配置 `#/config` | 生效的 catalog / 仓库 / 写模式 / Trino 连接 / 查询上限 / 作业命令模板，可展开原始 JSON |

```bash
bin/build-ui.sh                    # 首次会 npm install，然后 vite build
npm --prefix lake-ui run dev       # 开发模式 5173，/api 与 /actuator 代理到 8088
npm --prefix lake-ui run mock      # 没有 Trino / Docker 时：4173 起个假后端，纯验前端
```

`bin/build.sh` 会先构建前端再打包；只编后端时用 `SKIP_UI=1 bin/build.sh`（认不到 node 也会自动跳过）。

几个实现上的取舍：

- **同源托管，不做 CORS**：前端和接口同一个端口，省掉跨域配置；开发期靠 Vite proxy，而不是给后端开 CORS。
- **hash 路由**：`#/query`，静态资源托管不需要服务端做 history fallback。
- **`/ui/` 需要显式跳转**：Spring 只有根路径有 welcome page，目录请求不会自动找 `index.html`。
  WebFlux 没有 `ViewControllerRegistry`，所以用一条 `RouterFunction` 把 `/ui`、`/ui/` 307 到 `/ui/index.html`。
- **缺资源返回 404**：`NoResourceFoundException` 是 `ResponseStatusException` 的子类，`ErrorMapper` 统一按
  状态码翻译，兜底的 `Exception` handler 就不会把它吞成 500（前端加载旧 assets 时会踩到）。
- **深链可分享**：`#/query?sql=select...&run=1` 打开即执行，`#/jobs?run=<runId>` 直达某次作业的日志。
- SQL 存 localStorage，结果只在内存里，不落盘。

## 接集群（HMS + MinIO + Trino）

```bash
# 0. 首次：装 Docker Engine + Compose v2（含镜像加速，约 3 分钟）
bash bin/install-docker.sh        # 装完 newgrp docker 或重新登录

# 1. 起基础设施（Postgres 存 HMS 元数据，MinIO 存数据；Trino 按需另行拉起）
bin/infra.sh up                   # 起 Postgres + MinIO + HMS；首次构建 HMS + 拉镜像，约 5-10 分钟，常驻约 0.75 GB
bin/infra.sh doctor               # 端口 / HTTP 自检（Trino 未起时自动跳过相关项）

# 2. 用集群配置跑批（Hive catalog + s3a 仓库）
cp conf/platform.properties.example conf/platform.properties
bin/build.sh                     # 预生成 classpath（含 -Ps3 的 iceberg-aws-bundle + hadoop-aws）
bin/dev-batch.sh --job all       # 运行时只做 java -cp，不再每次 mvn package

# 3. Trino 即席查询（默认不起 Trino，常驻约 0.8 GB，用完 bin/infra.sh stop trino 回收）
bin/infra.sh up trino
bin/trino-query.sh --list
bin/trino-query.sh --sql-file sql/adhoc/01_daily_revenue.sql
bin/trino-query.sh --sql "select count(*) from iceberg.lake.silver_orders" --stats
# 占位符参数化：${var} 由 platform-common:SqlParams 预处理，缺参的语句自动跳过
bin/trino-query.sh --sql-file sql/adhoc/04_time_travel.sql --params snapshot_id=123456
```

> 即席查询由 `lake-adhoc` 模块承载：协议层用**官方 `trino-jdbc`**（与 lake-service 同一套 client，只要 JDBC URL，不依赖 `trino` 命令或 `docker exec`），
> 项目只保留 `${var}` 预处理（`platform-common` 的 `SqlParams`，复用 `SqlScript.split()` 切句）和表格显示层（`TableFormatter`，处理中文对齐）。
> 一次 `mvn exec:java` 完成预处理 + 执行 + 输出；`--limit`（默认 200）做客户端截断。

> 安装、镜像加速、端口、扩展新依赖（Kafka/ClickHouse/Redis……）和排错，全部见
> **[`infra/README.md`](infra/README.md)**。
>
> 两点与老版本的差异：MinIO 社区镜像 2026 年已从 Docker Hub / Quay 下线，compose 改用
> `bitnamilegacy/minio` 归档镜像（仅限本地开发）；HMS 镜像在 `infra/hive/` 里补了 Postgres
> 驱动和幂等的 `-initOrUpgradeSchema`，不会再出现「第二次启动因表已存在而退出」。
> 生产上通常换成企业自己的 HMS / 对象存储，配置只改 `platform.catalog.type / hive.uri / warehouse`。

### S3 依赖：为什么 IDE 会报「找不到依赖项」

这两个包只在 `spark-batch` 的 `s3` profile 里声明，**默认构建不激活、也不会下载**，所以第一次在 IDE 里勾上 profile 就会报红：

| 坐标 | 作用 | 体积 |
| --- | --- | --- |
| `org.apache.iceberg:iceberg-aws-bundle:1.5.2` | Iceberg 的 `S3FileIO`，读写 metadata / data 文件 | ~30 MB |
| `org.apache.hadoop:hadoop-aws:3.3.4` | Spark 的 `s3a://` 文件系统，传递引入 `com.amazonaws:aws-java-sdk-bundle:1.12.262` | 1 MB + 268 MB |

坐标本身在中央仓库（和阿里云镜像）都存在，缺的是本地仓库副本。两条命令解决：

```bash
# 1) 首次：父 pom 和 platform-common 得先进本地仓库，否则依赖树解析不出来
mvn -N install -DskipTests
mvn -pl platform-common install -DskipTests
# 2) 预下载 s3 相关依赖（含传递依赖）
mvn -Ps3 -pl spark-batch dependency:go-offline
```

IDEA 侧同时确认：Maven 面板勾选 `s3` profile、`Settings → Build Tools → Maven → Local repository` 指向 `~/.m2/repository`、没开 Offline work，然后 `Reload All Maven Projects`。

`hadoop-aws` 的版本必须与 Spark 发行版自带的 Hadoop 对齐（Spark 3.5.1 官方包是 3.3.4），不一致会在运行期报 `NoSuchMethodError`；换集群时用 `-Dhadoop.version=` 覆盖。

已经有 Spark 集群时，用 `bin/submit-batch.sh` 提交（自动 `--packages` 拉 Iceberg runtime）：

```bash
SPARK_MASTER=yarn bin/submit-batch.sh --job all \
  --catalog-type hive --hive-uri thrift://hms:9083 --warehouse s3a://lake/warehouse
```

## 工程化：CI、全栈与冒烟

功能能跑不等于可重复验证。这一层把「三条命令跑通」变成机器保证：

```bash
bin/stack.sh up          # 一键全栈：构建 jar → 起 infra/Trino → 跑批 → 起服务容器
bin/stack.sh smoke       # 端到端冒烟（容器模式，批作业检查自动跳过）
bin/stack.sh batch --job all   # 批作业在宿主机跑（容器内没有 Maven/Spark 运行时）
bin/stack.sh down        # 停服务与 infra，保留数据卷
```

- **CI（`.github/workflows/ci.yml`）**：PR / push 跑 JDK 21 + Node 22 的 `mvn verify`
  （含 Spark 真实建表/MERGE/加列/清理快照的集成测试），并归档 surefire 报告与 jar；
  `master/main` 和手动触发额外跑 E2E：起 Postgres/MinIO/HMS/Trino、跑
  `init → ingest → transform`、启动服务、执行 `bin/smoke.sh`。
- **全栈模式（`infra/docker-compose.full.yml`）**：在原有 infra 之上加 `lake-service` 容器。
  容器里的服务关闭批作业（`LAKE_BATCH_ENABLED=false`），因为镜像只带 JRE，没有
  `bin/dev-batch.sh` 需要的 Maven/Spark classpath；批作业用 `bin/stack.sh batch` 在宿主机执行。
  `bin/stack.sh` 会生成 `conf/platform.stack.properties`（集群模式：HMS + MinIO），
  不覆盖你自己的 `conf/platform.properties`。
- **冒烟脚本（`bin/smoke.sh`）**：健康检查、401 鉴权、配置回显、表清单、同步查询、SSE、
  语义层 render/query、审计事件、只读守卫；服务开了批作业时还会真的提交一次 `show`
  并等到 `SUCCEEDED`。容器模式用 `SMOKE_BATCH=0` 跳过批作业项。
  失败非零退出，可直接放进 CI。

## 批处理作业

| job | 做什么 | 关键点 |
|---|---|---|
| `init` | 建库 + 三层表 + `ops.batch_runs` | 幂等；表属性含 `format-version=2`、CoW 写模式；运行历史表放独立 `ops` namespace，不进业务表清单 |
| `ingest` | 生成落地文件 → 写入 ODS | 只读本次生成的 `dt=` 目录；`order_id` 带业务日期，同一天重跑数据稳定；每行带 `run_id` 可回溯批次；故意的重复单(3%)和迟到数据(10%)验证按事件时间入分区 |
| `transform` | ODS→DWD `MERGE`，DWD→ADS `INSERT OVERWRITE` | 只扫近 N 天分区；只更新变化的行 |
| `evolve` | 加 `channel` 列并回填 | 纯元数据变更 + CoW 重写，用来演示 schema 演进 |
| `maintain` | `rewrite_data_files` / `rewrite_manifests` / `expire_snapshots` | 建议按天调度 |
| `show` | 快照、历史、分区布局、数据抽样 | 只读巡检 |
| `verify-sql` | 用 Spark 执行 ad-hoc 脚本 | 跳过 `@trino-only` 语句，其余做最小方言适配 |

每个 job 跑完（无论成功失败）都会往 `ops.batch_runs` 落一行终态：`run_id / job / status / 耗时 / 日志路径`。
`run_id` 由 lake-service 生成并通过 `BATCH_RUN_ID` 环境变量注入，所以服务重启后从表里读回的记录
还能和原来的日志文件对上；不想记账加 `--no-record`。

`bronze_orders` 每行的 `run_id` 用的是同一个值，可以直接 join `ops.batch_runs` 定位数据是哪次任务导入的
（`maintain` 清理快照后仍可查）。老仓库首次跑 `ingest` 会先 `ADD COLUMNS` 补列，并删掉 `run_id` 为空的历史行。

作业名和页面文案只在 `platform-common` 的 `LakeJobs` 里定义一次：`BatchApp` 的注册键、`--job` 的取值、
服务端 `lake.batch.allowed-jobs` 的默认白名单都取它；标题/描述/分组/是否仅命令行在 `LakeJobs.METAS`，
由 `GET /api/config` 的 `batch.jobs` 下发给页面，前端只负责渲染。改名字时不会出现「页面还认、进程不认」的错位。

## 即席查询脚本（`sql/adhoc/`）

| 脚本 | 内容 |
|---|---|
| `01_daily_revenue.sql` | 每日 GMV / 退款 / 净收入 / 退款率 |
| `02_top_merchants.sql` | 商户 GMV 排行 |
| `03_partition_pruning.sql` | 两条 `EXPLAIN` 对比分区裁剪 |
| `04_time_travel.sql` | `$snapshots`、`FOR VERSION AS OF`、`FOR TIMESTAMP AS OF` |
| `05_table_health.sql` | `$files` / `$partitions` 小文件与分区巡检 |
| `06_refund_by_category.sql` | 品类退款率（经方言适配可用 Spark 校验） |
| `07_channel_mix.sql` | 渠道结构（需先跑过 `--job evolve`） |

## 几个关键设计决定

**1. 行级写一律 copy-on-write，不用 merge-on-read。**
读取侧（Trino）处理 MoR 的 delete file 跨引擎容易踩坑：Spark 写出来的删除文件，
Trino 的 Iceberg connector 必须按 position/equality delete 正确合并，版本不齐时结果会静默出错。
CoW 的代价是写放大，换来的是「Spark 写、Trino 读」零兼容问题。如果以后只让 Spark 读，
或者两端版本都验证过，把 `platform.write-mode=merge-on-read` 打开即可。

**2. ODS/DWD 用 `days(event_time)` 隐藏分区，不用入湖日期。**
上游落地文件按 `dt=YYYY-MM-DD` 分目录，但写入时 Iceberg 按 `event_time` 自己路由——
迟到数据会进正确的历史分区，不需要重跑整个目录。

**3. 去重用 `MERGE` + `row_number()`，不做全量覆盖。**
`WHEN MATCHED AND s.event_time > t.event_time` 保证只更新真正变了的行；
`WHERE event_time >= current_date() - INTERVAL 7 DAYS` 让每次只碰最近几个分区。

**4. 快照和文件要定期治理。**
`rewrite_data_files` 合并小文件、`rewrite_manifests` 控制元数据规模、`expire_snapshots` 清历史。
不管的话元数据目录会越滚越大，Trino 查询规划也会变慢。

**5. 统一 UTC。**
`spark.sql.session.timeZone=UTC`，避免「按天汇总」在不同时区下结果对不上。

## 踩过的坑（都在代码里做了兜底）

- **JDK11+ 反射限制**：`.mvn/jvm.config` 已配好 `--add-opens`；用 `spark-submit` 部署时放进
  `--conf spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions`。
- **`CALL ... rewrite_data_files` 报 `NoClassDefFoundError: scala/jdk/CollectionConverters`**：
  Iceberg 的 SQL 扩展编译时用了 Scala 2.13 的 API，Spark 2.12 发行版不带兼容包，
  需要显式引入 `scala-collection-compat`（pom 里已加，`bin/submit-batch.sh` 的 `--packages` 也带了）。
- **Hadoop catalog 不支持 namespace 元数据**：`CREATE DATABASE ... COMMENT 'xxx'` 会报
  `metadata is not supported`，所以建库 DDL 故意不带 COMMENT。
- **Trino 查元数据表**：`silver_orders$snapshots` 里的 `$` 要加双引号：
  `FROM iceberg.lake."silver_orders$snapshots"`。
- **Trino 脚本想在 Spark 上校验**：`date_add('day', -6, current_date)` 这类 Trino 语法 Spark 不认，
  `TrinoDialect` 只做最小替换（`date_add` / 带引号的 `INTERVAL` 字面量），
  带 `@trino-only` 标记的语句（`$snapshots` 元数据表、`FOR VERSION AS OF`）直接跳过——
  别指望它当通用方言翻译器用。
- **跑批前先构建**：`bin/dev-batch.sh` 现在是 `java -cp` 薄封装，依赖 `bin/build.sh`
  生成的 jar 和 `spark-batch/target/classpath.txt`；没构建过会直接报错退出，
  不会再偷偷跑一次 `mvn package`（并发作业抢同一个 `target/` 的老问题已消除）。

## 生产化 TODO

- 调度：把 `ingest/transform/maintain` 拆成 Airflow / DolphinScheduler 的日任务，`maintain` 单独跑。
- 权限：HMS 接 Ranger / Trino 接 LDAP + 系统访问控制，Iceberg 表级授权。
- 并发写：Spark 与 Flink 同时写同一张表时，确认 catalog 支持乐观锁（HMS + Iceberg 1.5 已支持）。
- 监控：把 `$files`、`$snapshots` 的指标打到 Prometheus（文件数、平均文件大小、快照数量）。
- 存储：生产用对象存储时开 `write.metadata.delete-after-commit.enabled`（已开）并配合生命周期规则。
