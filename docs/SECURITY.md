# 安全与权限：从 demo 静态账号到生产网关 + 引擎授权

这个仓库有两层鉴权，别混为一谈：

| 层 | 管什么 | demo 默认 | 生产做法 |
|---|---|---|---|
| lake-service | REST 接口谁能调、谁能触发批作业 | HTTP Basic + 静态账号 | `mode=external`，网关注入身份头，服务校验共享密钥 |
| Trino / HMS | 引擎侧谁能读哪张表、谁能建表 | 无（本地信任） | Trino LDAP + 文件/系统访问控制；HMS 接 Ranger；表级授权对齐 |

## 1. lake-service 的两种模式

### local（默认，仅 demo）

```yaml
lake:
  security:
    mode: local
    enabled: true
    realm: lakehouse
    accounts:
      - { username: viewer,   password: "{noop}viewer",   roles: [QUERY] }
      - { username: analyst,  password: "{noop}analyst",  roles: [QUERY, ANALYST] }
      - { username: operator, password: "{noop}operator", roles: [QUERY, BATCH] }
      - { username: auditor,  password: "{noop}auditor",  roles: [AUDITOR] }
```

角色是接口级的：`QUERY` 可查表/跑 SQL（采样走最严档脱敏视图），`ANALYST` 额外可在采样里看 `merchant_id` 明文，
`BATCH` 才能触发作业和看 actuator，审计事件列表 `QUERY` / `AUDITOR` 都能看，`AUDITOR`
才能读查询原文保险箱和采样基础表明文。健康检查、静态页面和接口文档公开，便于探针和浏览器使用。

**控制台登录**：`POST /api/auth/login`（body `{username,password}`，成功返回 `{user,roles,mode}`）
和 `GET /api/auth/session`（带 Basic 头回显当前身份，没带只回 `authenticated=false`）是公开的，
不触发 `WWW-Authenticate` 挑战——否则控制台自己的登录页会被浏览器原生 Basic 弹框盖掉。
服务仍然**无会话**：登录后前端把凭据放进每个请求（含 SSE）的 Basic 头；「退出」只清前端凭据，
不会让服务端失效。鉴权过滤器只在「根本没带凭据」时发挑战头，口令错或过期时返回 401 但不弹框。

### external（生产）

网关（nginx / oauth2-proxy / API gateway）完成认证后，把身份注入头，服务只认带共享密钥的请求：

```yaml
lake:
  security:
    mode: external
    enabled: true
    shared-secret: ${LAKE_SECURITY_SHARED_SECRET:}   # 必填，不写进配置文件
```

```bash
export LAKE_SECURITY_SHARED_SECRET="$(openssl rand -hex 32)"
```

请求头契约：

| 头 | 含义 |
|---|---|
| `X-Auth-Shared-Secret` | 网关与服务的共享密钥；缺失或错误一律 401 |
| `X-Forwarded-User` | 用户名（也接受 oauth2-proxy 的 `X-Auth-Request-User`） |
| `X-Forwarded-Roles` | 逗号分隔角色，如 `QUERY,BATCH` |

**fail closed**：没配 `shared-secret`、密钥不对、没有身份头，请求都会被拒绝。
网关必须**剥离客户端自带的这些头**再注入自己的值，否则等于把授权交给调用方。

nginx 示例：

```nginx
location / {
    proxy_pass http://lake-service:8088;
    proxy_set_header X-Auth-Shared-Secret "__SHARED_SECRET__";  # 由配置管理注入，不进代码库
    proxy_set_header X-Forwarded-User    $remote_user;
    proxy_set_header X-Forwarded-Roles   "QUERY,BATCH";          # 实际应来自 IdP 的 group claim
    proxy_set_header Authorization       "";                      # 不让客户端 Basic 冒充
}
```

> 静态 Basic 账号在 external 模式下完全不生效。审计事件里的 `actor` 用网关注入的用户名，
> 查询配额也按它分桶。

## 2. Trino：LDAP + 系统访问控制

`infra/docker-compose.security.yml` 是**生产配置模板**，不是本地启动方式
（本地没有 LDAP，启用后 Trino 起不来）：

```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.security.yml \
  --profile security up -d trino
```

它做了三件事：

- `infra/trino/auth/config.properties` 打开 `http-server.authentication.type=PASSWORD`；
- `password-authenticator.properties` 指向 LDAPS（**必须 TLS**，别用明文 ldap://）；
- `access-control.properties` + `rules.json` 实现 catalog/schema/table 三级授权 + 列掩码：
  `etl` 可读写 `iceberg.lake`，`admin` 全部权限；分析/普通角色只能查脱敏视图，见下一节。

使用前按实际环境改 LDAP 地址、bind pattern 和 `rules.json` 的用户/角色。
更集中化的做法是 Ranger Trino 插件或 OPA，文件规则适合先跑起来、可审计的最小版本。

## 3. HMS + Iceberg 表级授权

- **HMS**：见 `infra/ranger/README.md`，安装 Ranger Hive 插件管建表/改表/删表。
- **Iceberg 数据文件**：HMS 不控制对象存储里的 Parquet 文件，需要 bucket policy / IAM
  或 Iceberg REST Catalog 的授权层。
- **对齐**：同一批用户/角色在 Ranger（HMS）、Trino 规则、对象存储三处要一致，
  否则会出现「一个层放行、另一层拒绝」的诡异现象。

## 4. 敏感列分级：视图 + 列掩码

两层同时生效，任一层单独配错都还有另一层兜底：

| 层 | 在哪 | 作用 |
|---|---|---|
| Trino 视图 | `platform-common/src/main/resources/sql/view/*.sql` | 明细表按角色给两份视图，掩码表达式写在 SQL 里；分析角色只授权视图 |
| 列掩码 | `infra/trino/auth/rules.json` 的 table rule → `columns[].mask` | 即使误给基础表 SELECT，敏感列仍返回掩码值 |

列口径（单一事实源是 `MaskingCatalog`，SQL 与 rules.json 都按它维护）：

| 列 | analyst | viewer | auditor |
|---|---|---|---|
| `user_id` | SHA-256 前 16 位 | 同左 | 明文 |
| `order_id` | 前 6 位 + `***` | 同左 | 明文 |
| `amount` | 向下分桶到 100 | 同左 | 明文 |
| `merchant_id` | 明文 | SHA-256 前 12 位 | 明文 |

- `bronze_orders` / `silver_orders` 对 analyst / viewer 只有 `GRANT_SELECT`（查视图需要），**没有 SELECT**，
  直连 `SELECT *` 会被引擎拒绝；`gold_daily_revenue` 是聚合结果，原样给 analyst。
- `*_masked` 给 analyst（merchant_id 明文），`*_restricted` 给 viewer（merchant_id 也哈希）。
  服务侧 `/api/tables/{table}/sample` 按角色自动选目标：`QUERY` → restricted，
  `QUERY,ANALYST` → masked，`AUDITOR` → 基础表明文；没带身份时走最严档。
  采样端点同时接受 `QUERY` / `AUDITOR`，其余 `/api/query`、`/api/tables` 仍只要 `QUERY`。
  服务 Trino 账号（`lake_service`）默认只需视图 SELECT + 基础表 GRANT_SELECT，不需要 DDL 权限；
  但 AUDITOR 档采样走的是基础表，**默认 rules.json 只给 GRANT_SELECT 会被 Trino 拒绝**。
  需要在服务里演示 AUDITOR 明文采样时，须显式给 `lake_service` 基础表 SELECT——
  这也意味着 `/api/query` 能用该账号读到明文，属于策略取舍，生产默认不开。
- `masked_reader` 是演示列掩码的角色：它能直连基础表，但四个敏感列全部返回掩码值。
- 视图由 lake-service 启动时创建，也可手动 `bin/init-views.sh --user etl`；安全 profile 下
  服务账号没有 OWNERSHIP，需由 etl / admin 先把视图建好。
- AUDITOR 看明文走单独授权（对应查询原文保险箱的 legal_hold 流程），不给 analyst 开后门。

验证（启用 security profile 后）：

```bash
# analyst 直连基础表：拒绝
trino --server https://trino.example.com --user analyst \
  --execute 'select user_id from iceberg.lake.silver_orders limit 1'
# analyst 走视图：user_id 哈希、merchant_id 明文
trino --server https://trino.example.com --user analyst \
  --execute 'select user_id, merchant_id from iceberg.lake.silver_orders_masked limit 1'
# viewer 走受限视图：merchant_id 也变成哈希
trino --server https://trino.example.com --user viewer \
  --execute 'select merchant_id from iceberg.lake.silver_orders_restricted limit 1'
# masked_reader 直连基础表：四列都是掩码值
trino --server https://trino.example.com --user masked_reader \
  --execute 'select user_id, order_id, amount, merchant_id from iceberg.lake.silver_orders limit 1'
# auditor 走单独授权：明文
trino --server https://trino.example.com --user auditor \
  --execute 'select user_id from iceberg.lake.silver_orders limit 1'

# 服务侧采样对比：viewer / analyst / auditor 的 merchant_id 依次是哈希 / 明文 / 明文
curl -u viewer:viewer   'http://localhost:8088/api/tables/silver_orders/sample?limit=1'
curl -u analyst:analyst 'http://localhost:8088/api/tables/silver_orders/sample?limit=1'
curl -u auditor:auditor 'http://localhost:8088/api/tables/silver_orders/sample?limit=1'
```

## 5. 验证清单

```bash
# local 模式：角色边界
curl -u viewer:viewer -X POST http://localhost:8088/api/batch/show   # 403
curl -u operator:operator http://localhost:8088/api/batch/runs       # 200

# external 模式：无密钥 / 无身份头必须 401
curl -H 'X-Forwarded-User: alice' -H 'X-Forwarded-Roles: QUERY' \
     http://localhost:8088/api/config                              # 401
curl -H 'X-Auth-Shared-Secret: <secret>' -H 'X-Forwarded-User: alice' \
     -H 'X-Forwarded-Roles: QUERY' http://localhost:8088/api/config # 200

# Trino 授权（启用 security profile 后）
trino --server https://trino.example.com --user analyst \
  --execute 'select * from iceberg.lake.silver_orders limit 1'      # 放行
trino --server https://trino.example.com --user analyst \
  --execute 'create table iceberg.lake.t (a int)'                  # 拒绝
```

审计事件 `auth.denied` 会记录 `reason`（`bad-shared-secret` / `missing-identity` /
`insufficient-role`），外网扫描和凭据配错都能在 `logs/audit.log` 里看到。
