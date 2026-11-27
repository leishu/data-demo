# NAMING — 命名规范（类名 / 变量名）

本文件是命名的**单一事实源**，给人和 AI 助手同时用。根 [AGENTS.md](../AGENTS.md) §4.1 只放 5 条硬规则和指针；细表放这里。
机器门禁：`config/checkstyle/naming.xml`（源码层，main + test 都扫）+ `config/checkstyle/naming-suppressions.xml`（当前只有 2 条测试专用豁免）+ `lake-service/src/test/java/com/demo/lake/server/NamingArchitectureTest.java`（字节码层）。
改规则必须同时改这三处和本文件，不允许只改一边。

## 1. 类名：后缀即角色

AI 写新类时按下面的决策树选后缀，不要在表里「凭感觉挑一个像的」：

```
有 @RestController        → *Controller
有 @Service               → *Service
只做持久化                 → *Store（接口）/ Jdbc*Store（实现）
是批作业入口               → *Job（实现 LakeJob）
是给前端的只读模型         → *View（优先 record）
是静态目录/白名单          → *Catalog
是内存中的活跃集合         → *Registry
是文本→结构                → *Parser
是拒绝非法输入             → *Guard
是文本脱敏                 → *Redactor
是构造重对象               → *Factory
是单向写回                 → *Writer
是定时扫描/认领            → *Dispatcher
是轮询外部状态             → *Watcher
是有推进位置的小状态机     → *Cursor
都不是                     → 领域名词，不加后缀（如 BatchIdentity、TableRef）
```

| 角色 | 后缀 | 范本（照抄这个） |
|---|---|---|
| 批作业 | `Job` | `TransformJob` |
| 用例编排 | `Service` | `LineageService` |
| 持久化 | `Store` | `JdbcLogStore` |
| 静态目录 | `Catalog` | `LineageCatalog` |
| 运行态注册 | `Registry` | `RunningRunRegistry` |
| 对外模型 | `View` | `JobView` |
| HTTP 入口 | `Controller` | `QueryController` |
| 配置 | `Config` / `Properties` | `PlatformConfig` / `LakeProperties` |
| 执行器 | `Executor` | `KubernetesJobExecutor` |
| 调度 | `Dispatcher` | `JobDispatcher` |
| 旁路监听 | `Watcher` | `BatchWatcher` |
| 游标 | `Cursor` | `LogTailCursor` |
| 写回 | `Writer` | `RunRecordWriter` |
| 指标 | `Metrics` | `LakeMetrics` |

**禁用后缀**：`Manager / Helper / Util / Impl`（机器门禁强制）；`Dto / Vo / Processor / Data / Bean` 同样不要用，对外模型统一叫 `View`。
`TableInfo` 这类领域元数据名词是既有术语，不算含糊后缀。确实需要上述禁用词通常说明角色没想清楚，回到上面的树里选一个。
历史遗留的 `QuotaHelper`（`@Service`）已改名为 `QueryQuotaService`。

**一个概念只用一个词**：`Store` 不再叫 `Repository/DAO`；对外模型叫 `View`，不叫 `DTO/VO`；批作业叫 `Job`，不叫 `Task`。

**缩写白名单**：`Id`（不是 `ID`）、`Sql`、`Ddl`、`Dq`、`Api`、`Jdbc`、`Http`、`Url`、`Db`。其余缩写一律展开
（`catalog`/`database`/`table` 写全，**不**写成 `cat`/`db`/`tbl`、`cfg`/`props`、`serv`）。

## 1.1 HTTP DTO（RestController 层）

Controller 里就地声明的 record 也按角色固定后缀，不再另造 `*Dto / *Vo / *Payload`：

| 用途 | 后缀 | 范本 |
|---|---|---|
| `@RequestBody` 请求体 | `*Request` | `QueryController.QueryRequest`、`AuthController.LoginRequest`、`SemanticController.SemanticRequest` |
| Request 内部的嵌套条目 | `*Item` | `SemanticController.FilterItem` |
| 响应体 | `*View` | `AuthController.SessionView` / `LoginView`、`BatchController.RunDetailView` |

- `@RequestBody` 的参数名一律叫 `request`（不要 `body` / `dto` / `payload`）。
- `@RequestParam` / `@PathVariable` 维持标量类型（`String` / `int` / `boolean`）；确有必要打包成对象时，按上表选后缀，不引入第四种叫法。
- ArchUnit 的 `controllerDtosHaveRoleSuffix` 和 `requestBodiesEndWithRequest` 两条会拦，改 DTO 名必须同步改测试。

## 1.2 lake-service 领域包分层

`batch / governance / query / observability` 四个领域包下只允许三层子包，领域根包不再直接放类：

| 子包 | 放什么 | 例子 |
|---|---|---|
| `model` | 领域数据、视图、身份对象 | `BatchRun`、`JobView`、`QueryResult`、`BatchIdentity` |
| `service` | 用例编排、守卫、领域异常 | `QueryService`、`SqlGuard`、`InvalidQueryException` |
| `store` | 持久化访问、内存态存储 | `JdbcLogStore`、`AuditBuffer`、`RunningRunRegistry` |

- 原来 `query` 根目录下的 `SqlGuard` 和 5 个领域异常已移入 `query/service`；跨切面包（`controller / config / exception / filter / security / support / health`）不属于这套分层，保持原样。
- ArchUnit 的 `domainPackagesUseModelServiceStore` 会拦：新类不要放到领域根包，也不要自造第四层。

## 2. 变量名：三条硬规则

1. **字段名说角色，不说类型缩写**。字段/参数/局部变量共用同一套黑名单：
   `cfg / props / queries / svc / mgr / repo / tmp / res / req / resp / val / obj / str / num / rv / cp / at`。
   - `cfg` → `platformConfig`；`props` → `properties`；`queries` → `queryService`
   - `at` 是无单位的时间点 → `recordedAt` / `probedAtNanos`
2. **单字母只放行 `i` / `j`（下标循环）和 `e`（catch）**；`v / n / c / t / m / p / s / w / g / a / b` 全部要改成有意义的全名。
   `out` 作为局部累加器允许，但不允许作为字段。
3. **数值带单位，Map 带键，集合用复数**：`dispatchIntervalMs` / `probedAtNanos` 是范本；
   `Map<String, JobExecutor> executorsByName`；`List<BatchRun> runs`。时间点用 `...At`，时长用 `...Ms/...Nanos`。

布尔统一 `is/has/can/should/allow/enabled`；禁止 `flag`、`ok`（测试断言除外）、`notXxx` 否定式。

### 反例 → 正例（改名前的真实代码）

| 改前 | 改后 | 原因 |
|---|---|---|
| `QuotaHelper`（`@Service`） | `QueryQuotaService` | 注解是 Service，且测试名本来就叫 `QueryQuotaServiceTest` |
| `private final PlatformConfig cfg` | `platformConfig` | 字段说角色，不说类型缩写 |
| `private final LakeProperties props` | `properties` | 同上 |
| `private final QueryService queries` | `queryService` | 单数服务不是集合 |
| `int n = Math.clamp(limit, ...)` | `int cappedLimit = ...` | 说明是哪个 limit |
| `long at = probedAtNanos` | `long probedAt = probedAtNanos` | 时间点要有业务含义 |
| `String v = cell(value)` | `String text = cell(value)` | 单字母无信息 |
| `int cp` / `String rv` | `codePoint` / `resourceVersion` | 缩写展开 |
| `for (Throwable t = e; ...)` | `for (Throwable cause = e; ...)` | 说明它在 cause 链上 |

## 3. 机器门禁

| 检查 | 管什么 | 什么时候跑 |
|---|---|---|
| Checkstyle（只开 naming 模块） | 类名/方法名/字段名/参数/局部变量的格式、缩写黑名单、连续大写 | `mvn validate`（`verify` 会带），失败即停 |
| ArchUnit `NamingArchitectureTest` | `@Service → *Service`、`@RestController → *Controller`、禁用含糊后缀 | `mvn -pl lake-service test` |
| `docs/NAMING.md` + AGENTS.md §4.1 | 上面管不到的语义（比如 Map 键名、布尔语义） | 人/AI 自检 + review |

Checkstyle 同时扫 `src/main/java` 和 `src/test/java`。测试源码里中文方法名、`@ArchTest` 小驼峰常量、以及中文方法名里的 `SQL` 缩写走 `naming-suppressions.xml` 的测试专用豁免；其余命名规则对测试一视同仁。

单跑命名检查：

```bash
JAVA_HOME=<jdk21> mvn -B -ntp validate
```

**不要绕过门禁**：`naming-suppressions.xml` 目前只有上面那两条测试专用豁免。确实需要新例外时，必须在该文件里逐条写清文件、检查项和原因，并在 PR 里说明；
改了一个被豁免的文件就要把对应条目删掉。新文件一律不许豁免。

## 4. AI 协作纪律

新增/改名任何类或变量前，先按这 7 条自查（同时写在 AGENTS.md §4.1）：

1. 新类型必须落到 §1 后缀表里的一个角色；表里没有 → 先在 PR 里提案，不许发明后缀。
2. 字段/参数/局部变量禁用 §2 的缩写黑名单。
3. 单字母只留 `i/j/e`；数值带单位，Map 带键。
4. 一个概念只用一个词，不许同义混用。
5. 改名和逻辑改动分开提交；交付说明里逐条列出新增/改名的名字和命中的规则。
6. Controller 的 DTO 按 §1.1 取后缀：请求体 `*Request`、嵌套条目 `*Item`、响应 `*View`；`@RequestBody` 参数名一律 `request`。
7. lake-service 的领域包按 §1.2 只放 `model / service / store`；领域根包不放类，跨切面包不套这套分层。

**照抄范本**（AI 的模仿倾向比规则遵从强得多，先打开对应文件再动手）：

| 要写 | 先打开 |
|---|---|
| Service | `LineageService` |
| Store | `JdbcLogStore` |
| Controller | `QueryController` |
| Job | `TransformJob` |
| View | `JobView` |

交付自检模板（放 PR body / 提交信息）：

```
## 命名自检
- 新增类型：XxxService（角色：用例编排）
- 改名：QuotaHelper → QueryQuotaService（原因：@Service 与测试名对齐）
- 新增字段/变量：platformConfig / cappedLimit（无缩写、带单位）
- 校验：mvn -B -ntp verify（checkstyle + ArchUnit 通过）
```
