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
      - { username: operator, password: "{noop}operator", roles: [QUERY, BATCH] }
      - { username: auditor,  password: "{noop}auditor",  roles: [AUDITOR] }
```

角色是接口级的：`QUERY` 可查表/跑 SQL，`BATCH` 才能触发作业和看 actuator，`AUDITOR`
才能读查询原文保险箱。健康检查、静态页面和接口文档公开，便于探针和浏览器使用。

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
- `access-control.properties` + `rules.json` 实现 catalog/schema/table 三级授权：
  `analyst` 只能 SELECT 白名单表，`etl` 可读写 `iceberg.lake`，`admin` 全部权限。

使用前按实际环境改 LDAP 地址、bind pattern 和 `rules.json` 的用户/角色。
更集中化的做法是 Ranger Trino 插件或 OPA，文件规则适合先跑起来、可审计的最小版本。

## 3. HMS + Iceberg 表级授权

- **HMS**：见 `infra/ranger/README.md`，安装 Ranger Hive 插件管建表/改表/删表。
- **Iceberg 数据文件**：HMS 不控制对象存储里的 Parquet 文件，需要 bucket policy / IAM
  或 Iceberg REST Catalog 的授权层。
- **对齐**：同一批用户/角色在 Ranger（HMS）、Trino 规则、对象存储三处要一致，
  否则会出现「一个层放行、另一层拒绝」的诡异现象。

## 4. 验证清单

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
