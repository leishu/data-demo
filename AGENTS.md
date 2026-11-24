# AGENTS.md — AI 编程助手入口文档

本文件是 AI 编程助手在本仓库（data-demo / lakehouse-demo）里的标准入口。修改代码前先读：
更细的数据流与设计决策见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)，操作手册与排错都在 [README.md](README.md)。

## 1. 项目概览

Spark 做批处理、Iceberg 做存储与表格式层、Trino 做即席查询的湖仓脚手架，外面套一个 Spring Boot 4 服务层（查询接口 + 作业编排）。三条命令就能在本地跑通，不需要装 Hadoop / Docker。

**版本矩阵**：JDK 11（批处理产出 Java 11 字节码）/ 21（`lake-service`）；Spark 3.5.1（Scala 2.12）；Iceberg 1.5.2；Trino 476 JDBC；Spring Boot 4.2.0（WebFlux + Actuator）；日志 log4j2（批）/ logback（服务）；Hadoop 3.3.4（由 Spark 传递）；Lombok 1.18.34（provided）。

## 2. 目录与模块地图（Maven 多模块）

```
lakehouse-demo/
├── platform-common/          公共层（其他模块都依赖它）
│   └── src/main/java/com/demo/lake/common/
│       ├── config/PlatformConfig.java     配置归一化（-Dplatform.* > LAKE_* 环境变量 > conf/platform.properties > 默认值）
│       ├── catalog/LakeTables.java        三层表 DDL（ODS/DWD/ADS，含表属性）
│       ├── catalog/TransformSql.java      MERGE / 汇总 SQL
│       ├── catalog/TableRef.java          catalog.db.table
│       ├── sql/SqlScript.java             读脚本、按分号切分
│       ├── cli/Args.java / SqlParams.java CLI 参数 + ${var} 替换（供 lake-adhoc 调用）
│       ├── semantic/SemanticCatalog.java  GMV / 净收入等指标口径（Trino / Spark 双方言渲染）
│       └── batch/LakeJobs.java            作业名/页面文案注册表（单一事实源）
├── spark-batch/              批处理：init / ingest / transform / evolve / maintain / show / verify-sql
│   └── src/main/java/com/demo/lake/batch/
│       ├── BatchApp.java                  作业入口
│       ├── SparkSessionFactory.java       SparkSession 组装
│       ├── RunRecordWriter.java           写终态到 ops.batch_runs
│       └── job/{InitJob, IngestJob, TransformJob, EvolveJob, MaintainJob, ShowJob, VerifySqlJob}.java
├── lake-adhoc/               即席查询：官方 trino-jdbc 薄封装 + ${var} 预处理 + 表格输出
├── lake-service/             Spring Boot 4 服务（查询/作业 REST + SSE + 控制台）
│   └── src/main/java/com/demo/lake/server/
│       ├── query/{QueryController, QueryService, SqlGuard, ...}   查询 + 只读守卫
│       ├── batch/service/                 作业调度（JobDispatcher / JobExecutor / 状态库）
│       ├── observability/{AuditService, IcebergTableMetrics, ...} 审计 / 指标
│       └── config/LakeConfiguration.java  运行时配置（conf/platform.properties 回显）
├── lake-ui/                  Vue 3 控制台（独立工程，构建产物进 lake-service/.../static/ui/）
├── scheduler/                外部调度示例（Airflow / DolphinScheduler / crontab，非 Maven 模块）
├── sql/adhoc/                可丢给 Trino 的即席脚本（01_daily_revenue … 07_channel_mix）
└── bin/                      便捷脚本（build / build-ui / dev-batch / stack / smoke / infra / lake-service …）
```

**非 Maven 模块**（注意：父 pom 的 `<modules>` 只含 4 个）：`scheduler/`、`infra/`、`lake-ui/`、`sql/adhoc/` 是单独的目录。

## 3. 开发命令

```bash
# 完整构建 + 单测（整个 reactor 用 JDK 21 构建；spark 模块仍产出 Java 11 字节码）
bin/build.sh                  # 自动找 JDK 21；可用 export JAVA21_HOME=<dir> 指定

# 批作业快速验证（依赖 bin/build.sh 已生成的 classpath）
bin/dev-batch.sh --job all                 # init → ingest → transform → maintain → show
bin/dev-batch.sh --job all --evolve        # 额外跑 schema 演进（加 channel 列 + 回填）
bin/dev-batch.sh --job verify-sql --sql-file sql/adhoc/01_daily_revenue.sql

# 服务层（JDK 21，默认 8088）
bin/lake-service.sh build && bin/lake-service.sh start   # http://localhost:8088/

# 冒烟脚本（结束非零退出，可直接放 CI）
bin/smoke.sh
```

单独构建/测试：

```bash
mvn -pl platform-common test            # 单测最快（platform-common 的全部单测都在这里）
mvn -pl spark-batch -am package         # 只动批处理，用 JDK 11 也能
mvn -pl lake-service package            # 必须 JDK 21（模块内已挂 enforcer）
```

## 4. 代码与测试约定（观察到的，建议约束）

- **包结构**：按模块 + 领域分层，根包 `com.demo.lake`，子包 `common.*` / `batch.*` / `server.*`。
- **注释/文档主语言是中文**（javadoc、SQL 注释、日志消息）。
- **Lombok**：注解处理器在父 pom 的 `pluginManagement` 里显式声明（不依赖类路径隐式扫描）；`lombok.config` 里 `config.stopBubbling=true`（只作用于本项目）。
- **异常风格**：`PlatformConfig` 的约定——参数非法抛 `IllegalArgumentException`（"缺少必填配置: ..."），运行期状态错误抛 `IllegalStateException`；中文消息。
- **日志**：CLI 批作业用 `System.out.println` / `System.out.printf("%n")` 打印步骤日志（如 `[transform] silver: 14000 -> 13987 行（新增 0）`）；服务层用 slf4j（logback）。批作业里不要引入服务层依赖（Spring 只在服务侧）。
- **SQL**：Iceberg DDL 走 `platform-common` 的 `LakeTables` / `TransformSql`（不要手写重复 DDL）；spark-batch 不直接手写 MERGE，走 `TransformSql.mergeBronzeIntoSilver`；`@trino-only` 标记的语句只能跑在 Spark 上校验。
- **测试**：JUnit 5；`platform-common` 是纯单测；`spark-batch` 集成测试真实建表/MERGE/加列（`IcebergLocalRoundTripTest`、`IcebergConcurrentWriteTest`）；修改共享逻辑时优先补平台级单测。

## 5. 常见踩坑（已记录在 README 但 AI 每人都会重掉）

- **JDK 11+ 反射限制**：`.mvn/jvm.config` 已配 `--add-opens`，跑测试时由父 pom 的 surefire `argLine` 提供；若用 `spark-submit` 部署，写进 `spark.driver/executor.extraJavaOptions`。
- **`rewrite_data_files` 报 `NoClassDefFoundError: scala/jdk/CollectionConverters`**：必须挂 `scala-collection-compat`（根 pom 已配版本管理）。
- **s3 依赖默认不下载**：`iceberg-aws-bundle`（约 30MB）和 `hadoop-aws`（约 270MB）只在 `spark-batch/pom.xml` 的 `s3` profile 里声明；若忽然解析失败，先确认 profile 未勾。
- **`hadoop catalog 不支持 namespace 元数据`**：CREATE DATABASE 不带 COMMENT（Iceberg 的 hadoop catalog 会报 metadata not supported）。
- **Trino 元数据表名带 `$`**：要双引号（`FROM iceberg.lake."silver_orders$snapshots"`）。
- **`platform.stack.properties` 是 stack.sh 生成的**（集群模式），不入库；`conf/platform.properties` 是纯人工配置，也不入库。
- **跑批前先构建**：`bin/dev-batch.sh` 是 `java -cp` 薄封装，依赖 `bin/build.sh` 生成的 jar 和 `spark-batch/target/classpath.txt`。

## 6. 修改前的校验思路（最小可粘性）

| 改了什么 | 跑哪条 |
|---|---|
| platform-common 代码 | `mvn -pl platform-common test` |
| platform-common DDL/SQL | 重跑 `bin/dev-batch.sh --job all`（集成测试也覆盖真实建表） |
| spark-batch 作业逻辑 | `mvn -pl spark-batch -am test` + `bin/dev-batch.sh --job <job>` |
| lake-service 行为 | `mvn -pl lake-service package`（JDK 21）+ `bin/smoke.sh` |
| lake-ui 页面 | `npm --prefix lake-ui run build`；本地联调用 `npm --prefix lake-ui run dev`（5173，/api 代理 8088）或 `npm --prefix lake-ui run mock`（无后端时 4173） |

## 7. 相关文档指针（已存在）

- [README.md](README.md)：操作手册（快速开始、集群接入、CI、排错）
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)：数据流 + 设计决策（CoW、UTC、语义层、只读守卫）
- [docs/CODING_STANDARDS.md](docs/CODING_STANDARDS.md)：命名/异常/SQL/测试规范
- [docs/GLOSSARY.md](docs/GLOSSARY.md)：领域术语
- [CONTRIBUTING.md](CONTRIBUTING.md)：变更校验与文档同步纪律
- [bin/README.md](bin/README.md)：脚本一览 + AI 排查表
- [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) / [docs/SECURITY.md](docs/SECURITY.md) / [docs/STORAGE.md](docs/STORAGE.md) / [docs/CONCURRENCY.md](docs/CONCURRENCY.md)
- [infra/README.md](infra/README.md)、[scheduler/README.md](scheduler/README.md)

> 其他 AI 助手：本文件已放在仓库根，各产品（Cursor/Copilot）可按各自惯例读取。
