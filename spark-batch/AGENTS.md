# spark-batch/AGENTS.md — 批处理模块规范

本模块产出 **Java 17 字节码**（`maven.compiler.release=17`），Spark 4.1.4 / Iceberg 1.11.0（Scala 2.13）。根仓约定不重复（根 [AGENTS.md](../AGENTS.md)）。

## 1. 模块结构（AI 定位用）

```
spark-batch/
├── pom.xml                                  # 依赖 platform-common；可选 s3 profile
├── target/classpath.txt                     # bin/build.sh 生成，bin/dev-batch.sh 的运行时依赖
└── src/main/java/com/demo/lake/batch/
    ├── BatchApp.java                        # 入口：解析 --job/--days/... → 创建 SparkSession → 分发 job → 记账
    ├── SparkSessionFactory.java             # SparkSession 组装（PlatformConfig.sparkConf）
    ├── RunRecordWriter.java                 # 落 ops.batch_runs 终态（run_id / job / status / 耗时 / 日志路径）
    ├── LineageWriter.java                   # 每个写表作业后落 ops.lineage_runs（run_id/table/snapshot_id）
    ├── LakeJob.java                         # 接口：name() + run(SparkSession, PlatformConfig, Args)
    └── job/
        ├── InitJob.java                     # 建库建表（LakeTables.all）；老表 ADD COLUMNS + UTC 回填
        ├── IngestJob.java                   # 生成落地文件 → ODS；按事件时间分区
        ├── TransformJob.java                # MERGE bronze→silver（去重）+ INSERT OVERWRITE silver→gold
        ├── EvolveJob.java                   # 加 channel 列 + 回填（纯元数据变更）
        ├── MaintainJob.java                 # rewrite_data_files / rewrite_manifests / expire_snapshots
        ├── ShowJob.java                     # 只读巡检（快照/分区/小文件）
        ├── VerifySqlJob.java                # Spark 执行 sql/adhoc 脚本（跳过 @trino-only）
        ├── QualityJob.java                  # 六项质量检查，结果写 ops.dq_results；--inject-faults 注入演示脏数据
        └── RetentionJob.java                # 按资产保留期删旧分区，默认 dry-run；前后写 ops.retention_audit
```

## 2. 工作流

- 改作业代码 → `mvn -pl spark-batch -am test`；然后 `bin/dev-batch.sh --job <job>` 验实际行为
- 改 `LakeTables` / `TransformSql`（platform-common）→ 重跑 `bin/dev-batch.sh --job all`（真实建表/MERGE/加列）
- 改 SQL 方言/`@trino-only` → `bin/dev-batch.sh --job verify-sql --sql-file sql/adhoc/<script>.sql` 用 Spark 校验

## 3. 本模块约定

- **作业入口形式**：实现 `LakeJob`；`name()` 必须取自 `platform-common` 的 `LakeJobs` 常量——作业名/文案/白名单是单一事实源在 LakeJobs，不要在 Job 里自定义字符串。
- **写入**：所有 DDL/MERGE/迁移只走 `LakeTables` / `TransformSql` / `Migrations`（不要手写重复 SQL）；SQL 文本在 `platform-common/src/main/resources/sql/`，写模式默认 CoW。
- **输出**：CLI 风格按 `[job]` 前缀打步骤日志（`[init]`、`[transform]` 等）；结果用 `.show(false)`；不要在批作业里引入服务层依赖（Spring 只在服务侧）。
- **记账**：每个 job 结束调用 `RunRecordWriter`（流程已统一处理，不要手动绕过）。
- **集成测试**：`IcebergLocalRoundTripTest` / `IcebergConcurrentWriteTest` 用真实 Spark + 本地 Iceberg warehouse；新增行为优先补同类“真实建表测试”，而不是 mock。

## 4. 集群提交

`bin/submit-batch.sh` 走 `spark-submit --packages` 拉 Iceberg runtime；`SPARK_MASTER=yarn bin/submit-batch.sh --job all --catalog-type hive --warehouse s3a://lake/warehouse`。

## 5. 常见踩坑

- `target/classpath.txt` 缺失 → 先跑 `bin/build.sh`（dev-batch 应直接报错，不偷偷再打包）。
- s3 profile 依赖解析失败 → `mvn -Ps3 -pl spark-batch dependency:go-offline` 预下载。
- 只想验证行为改动 → `bin/dev-batch.sh --job transform`；想看三层表状态 → `bin/dev-batch.sh --job show`。
