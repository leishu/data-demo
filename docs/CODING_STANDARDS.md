# CODING STANDARDS — 开发约定（观察到的风格，建议约束）

简短，且每条都有真实例子可以对照。更细的数据流、设计决策见 [docs/ARCHITECTURE.md](ARCHITECTURE.md)；日常操作见 [README.md](../README.md)。

## 1. 命名与包结构

- 包根 `com.demo.lake`；子包（各模块按领域分）：
  - `common.config` — 配置（PlatformConfig）
  - `common.catalog` — 三层 DDL / SQL 生成（LakeTables / TransformSql / TableRef）
  - `common.sql` — 通用 SQL 工具（SqlScript / TrinoDialect）
  - `common.cli` — CLI 参数解析（Args / SqlParams）
  - `common.semantic` — 指标口径（SemanticCatalog 等）
  - `common.batch` — 作业注册/状态模型（LakeJobs / BatchRunStatus）
  - `batch.*` — Spark 批作业
  - `server.*` — Spring 服务（controller / query / batch / observability / config）
- 工具类用 `final` + 私有构造器（如 `PlatformConfig`）；避免无状态工具还挂着成员变量。
- 谓词命名：布尔判断用 `is*`（如 `PlatformConfig` 的 `isHiveCatalog()`、`isObjectStorage()`）；取值没有前缀（`catalogName()`、`writeMode()`）。
- 配置读取优先用 `PlatformConfig.get(key, default)` / `getRequired`（缺必填抛 `IllegalStateException`），不要在各处直接读 `System.getenv` 判断。

## 2. 异常与错误处理

- 参数非法 → `IllegalArgumentException`，消息中文且带关键上下文（如 `PlatformConfig`：`"缺少必填配置: " + key`）。
- 运行时状态错误 → `IllegalStateException`。
- 不要：空 `catch` 吞异常；这里没有对外错误码体系，`IOException` 等标准异常即可。
- **Java 语言级别按模块区分**：`platform-common` / `spark-batch` / `lake-adhoc` 是 `release=17`（Spark 4.1 要求 Java 17+，record、switch 表达式、text blocks 均可用）；`lake-service` 是 `release=21`。改代码前先看所在模块的 pom。跨模块不可变值对象优先用 record（如 `JobMeta` / `QualityCheck`），不要再手写「不可变类 + Lombok getter」。

## 3. SQL 与方言约定

- 关键字大写（看 `sql/adhoc/01_daily_revenue.sql`：`SELECT ... FROM ... WHERE ... GROUP BY ...`）。
- 标识符用双引号/反引号；Trino 元数据表名带 `$` 必须双引号（`FROM iceberg.lake."silver_orders$snapshots"`）。
- `@trino-only` 标记的语句跑不了 Spark，`verify-sql` 用 `TrinoDialect` 做最小适配，不保证全量方言转换。
- DDL/写入语句只走 `platform-common` 的 `LakeTables` / `TransformSql`；MERGE 逻辑集中在 `TransformSql`（不要在 Job 里手写一次性 SQL 变体替代）。

## 4. 输出与日志

- 批作业（CLI）：`System.out.println` / `printf("%n")`，步骤前缀 `[init]` / `[transform]` / `[ingest]`……；结果查询结束用 `.show(false)`。
- 服务层：slf4j（logback），不手写 `System.out`；日志消息中文。
- 插件/日志冲突场景已有兜底（见 README 踩过的坑），改日志依赖时看根 pom 的 comment。

## 5. 配置

- 运行配置走 `platform.*`；优先级（PlatformConfig）：`platform-defaults.properties` < `conf/platform.properties` < 调用方指定文件 < 环境变量 `LAKE_*` < `-Dplatform.*`。
- `conf/platform.properties` 与 `conf/platform.stack.properties` 是私有环境，**不入库**；样例文件在 `conf/platform.properties.example`。

## 6. 测试

- JUnit 5，测试文件 `XxxTest.java` 放 `src/test/java/com/demo/lake/<模块>/...`。
- `platform-common` 是纯单测；`spark-batch` 新增逻辑若要验真实建表/MERGE/加列，写集成测试（`IcebergLocalRoundTripTest`、`IcebergConcurrentWriteTest` 风格）。
- surefire 的 `--add-opens` 参数已统一配置在父 pom，不要自行覆盖移除。

## 7. 提交/修改前的校验纪律

引用 [AGENTS.md「修改前的校验思路」表](../AGENTS.md#6-修改前的校验思路最小可粘性)，改哪个模块跑哪条；最低要求：

- 改 `platform-common` → `mvn -pl platform-common test`，且跑一轮 `bin/dev-batch.sh --job all` 确认三层表仍可建。
- 改 `spark-batch` → `mvn -pl spark-batch -am test`。
- 改 `lake-service` → `mvn -pl lake-service package`（JDK 21）+ `bin/smoke.sh`。
- 改 SQL 口径（语义层/`TransformSql`）→ 重跑 `bin/dev-batch.sh --job all --evolve` 并确认 gold/silver 结果。

## 8. AI 协作纪律（AI 助手改代码时按此做）

- 先对照 [AGENTS.md 的校验表](../AGENTS.md#6-修改前的校验思路最小可粘性)，把改了哪里、跑什么命令列出来，再动手改。
- 新增/改动的设计决策写进 [docs/ARCHITECTURE.md](ARCHITECTURE.md)，并同步补充文末「常用术语」里的新词条。
- 不扩大范围：改 `platform-common` 时顺手优化其他模块用法算扩大范围，应避免；要改反馈。
- 改 SQL/DDL 口径前确认口径变化的业务含义，必要时跑校验脚本 `verify-sql` 审查生成 SQL。
