# CONTRIBUTING — 变更与验证纪律

面向人类贡献者与 AI 助手。**变更前校验的标准入口**是 [AGENTS.md「修改前的校验思路」](AGENTS.md#6-修改前的校验思路最小可粘性)：改了什么、跑了哪些检查、哪些部分没跑且为什么。

## 1. 必跑的检查（按改动面）

| 改了什么 | 跑哪条 |
|---|---|
| platform-common 代码/DDL/SQL | `mvn -pl platform-common test`，并重跑 `bin/dev-batch.sh --job all` 确认三层表仍可建 |
| spark-batch 作业/SQL 逻辑 | `mvn -pl spark-batch -am test`；`bin/dev-batch.sh --job <job>` 验实际行为 |
| lake-service 接口/配额/审计 | `mvn -pl lake-service package`（JDK 21）+ `bin/smoke.sh`（22 项宿主机 / 19 项容器模式） |
| 语义层（口径/维度/渲染） | `mvn -pl platform-common test` + 重跑 `bin/dev-batch.sh --job all --evolve` |
| lake-ui 控制台页面 | `npm --prefix lake-ui run build`（前端编译基线） |
| bin/ 脚本本身 | 跑一遍对应脚本，见 [bin/README.md](bin/README.md) 的 AI 排查表 |

完整引擎基线：`bin/build.sh`（全 reactor + 全测试，JDK 21）。

## 2. 文档更新纪律（改行为时同步文档）

| 改动 | 该补的地方 |
|---|---|
| 设计决策/数据流变化 | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| 规范变化（编码风格、命令、调试流程） | [docs/CODING_STANDARDS.md](docs/CODING_STANDARDS.md) |
| 新词条/术语 | [docs/GLOSSARY.md](docs/GLOSSARY.md) |
| bin 脚本新增/改用法 | [bin/README.md](bin/README.md) |
| 项目事实更新 | [AGENTS.md](AGENTS.md) |

## 3. 提交信息规范

- 格式：`<module>: 简洁描述`（如 `spark-batch: make TransformJob idempotent`、`lake-service: return 503 when Trino is down`）。
- 附**校验摘要**：运行了哪条命令、结果如何、没跑的部分为什么没跑（如：已跑单测；集群模式验证在运维审核后跑）。
- 若有兜底/降级改动，说明它退化时的行为。

AI 助手参与时，**校验记录应在提交信息/PR body 里原样可见**——比如“`bin/smoke.sh` 宿主机 22/22 通过；容器模式未跑（需 Docker）”。

## 4. 常见误区

- 不要在没 `bin/build.sh` 的情况下直接跑 `bin/dev-batch.sh`（classpath 文件缺失，应直接报错退出）。
- `verify-sql` 不是通用方言转换器：`@trino-only` 语句无法在 Spark 上校验；`TrinoDialect` 只做最小适配（`date_add`、带引号的 `INTERVAL` 字面量等）。
- `platform.write-mode=merge-on-read` 只在“Spark 写、Trino 读”且所有读端都验证过才开（参见 ARCHITECTURE 设计决策#1）。
- `conf/platform.properties`、`platform.stack.properties` 是私有环境，**不提交**。

## 5. 最务实的 AI 贡献准则

> 你的输出应让审核者（reviewer）能指出：**改了什么、哪些检查真跑过了（附实际命令与结果）、还没跑的部分为什么没跑**。把质疑设计的理由写出来。
