# 本地湖仓基础设施（Docker）

这份文档解决三件事：

1. 在这台机器上装好 **Docker Engine + Compose v2**（含镜像加速，否则拉不下镜像）；
2. 一条命令起齐 **Postgres + MinIO + Hive Metastore + Presto**，并验证「Spark 写、Presto 读」；
3. 说明**怎么加新依赖**（Kafka、ClickHouse、Redis……），不用每次重造环境。

> 设计取舍：**只把基础设施放进容器**。JDK 11 / JDK 21 / Maven / Node 仍然用宿主机的
> （`~/soft/jdk-21.0.12`、`/usr/lib/jvm/java-11-*`）。Spark 的 driver 就在 IDE 里跑，
> 把它塞进容器只会让断点、堆栈、`spark-submit` 变难查。

---

## 1. 装 Docker

### 1.1 一条命令

```bash
bash bin/install-docker.sh
```

脚本会提示输入 sudo 密码（当前用户 `leishu` 在 sudo 组里）。它做这些事：

| 步骤 | 内容 | 为什么 |
|---|---|---|
| apt 源 | 默认阿里云 `docker-ce`，不可达时自动试清华，都失败才报错退出 | Ubuntu 自带的 `docker.io` 版本旧、没有 compose v2；本机到 `download.docker.com` 的 TLS 会被重置，官方源不可用 |
| 装包 | `docker-ce`、`docker-ce-cli`、`containerd.io`、`docker-buildx-plugin`、`docker-compose-plugin` | Engine + CLI + 运行时 + 构建插件 + `docker compose` |
| `/etc/docker/daemon.json` | 镜像加速、日志轮转（20m×3）、`live-restore`、可选代理 | **本机直连 `registry-1.docker.io` 是被拒的**，不配加速拉不了镜像 |
| 权限 | 把当前用户加入 `docker` 组 | 免 sudo 用 docker |
| 验证 | `docker version`、`docker compose version`、`docker run hello-world` | 确认 Engine、Compose、拉镜像、运行四件事都通 |

脚本是幂等的：重复跑不会重复装，已有 `daemon.json` 会先备份再合并。

### 1.2 让 docker 组生效

```bash
newgrp docker          # 当前终端生效
# 或者退出重新登录（所有终端生效）
docker run --rm hello-world
```

### 1.3 镜像加速与代理（重点）

脚本默认写入两个加速器：`docker.m.daocloud.io`、`dockerproxy.net`。确认生效：

```bash
docker info --format '{{json .RegistryConfig.Mirrors}}'
docker pull alpine:latest
```

如果公司有内网仓库，重跑脚本时覆盖：

```bash
DOCKER_REGISTRY_MIRRORS=https://harbor.example.com bash bin/install-docker.sh
```

脚本支持的参数：

| 参数 | 作用 |
|---|---|
| `DOCKER_APT_MIRROR` | apt 源列表，逗号分隔；默认 `阿里云,清华`，第一个不可达自动试下一个 |
| `DOCKER_REGISTRY_MIRRORS` | 镜像加速器，逗号分隔；默认 `docker.m.daocloud.io,dockerproxy.net` |
| `DOCKER_USER` | 要加入 docker 组的用户；默认取 `SUDO_USER` / `logname`，取不到用当前用户 |
| `DOCKER_SKIP_PROXIES` | 置 1 则跳过写 `daemon.json` 的 `proxies` |
| `DOCKER_SKIP_PULL_TEST` | 置 1 则跳过 `hello-world` 拉取测试 |

**关于代理**：如果当前 shell 设了 `HTTP_PROXY` / `HTTPS_PROXY`，脚本会把代理写进
`daemon.json` 的 `proxies` 字段。注意这只对 **systemd 托管的 dockerd**（Docker ≥ 23.0）生效；
用 `dockerd &` 或 `service docker start` 起的不读这个配置。脚本结尾会 `systemctl restart docker`，
所以在这条路径上是有效的。不想写代理：`DOCKER_SKIP_PROXIES=1 bash bin/install-docker.sh`。

镜像源偶尔会失效。拉取报 `failed to resolve reference` / `403` / `429` 时，按顺序试：

```bash
# 1) 换镜像源顺序
DOCKER_REGISTRY_MIRRORS=https://dockerproxy.net,https://docker.m.daocloud.io bash bin/install-docker.sh
# 2) 临时用镜像前缀直接拉，再改回标准 tag
docker pull docker.m.daocloud.io/library/alpine:latest
docker tag  docker.m.daocloud.io/library/alpine:latest alpine:latest
```

> 阿里云「容器镜像服务 → 镜像加速器」给的是个人专属地址（`https://<id>.mirror.aliyuncs.com`），
> 登录后复制过来最稳，尤其适合长期使用。

### 1.4 需要什么资源

| 项目 | 数值 |
|---|---|
| 镜像总体积 | 约 2.5–3.5 GB（Presto 最大） |
| 磁盘 | 建议预留 20 GB（镜像 + 数据卷 + 构建缓存） |
| 内存 | 容器峰值约 2.5–3.5 GB；本机 15 GB，但日常已用 8–9 GB |
| 端口 | 5432 / 9000 / 9001 / 9083 / 8080（+ 8081 Adminer），全部只绑 `127.0.0.1` |

内存紧张时优先降 Presto：`infra/presto/etc/jvm.config` 的 `-Xmx1G` 和
`infra/docker-compose.yml` 的 `presto.mem_limit` 一起改，两边保持「容器上限 = 堆 + 1G 左右」。

---

## 2. 起停与验证

```bash
bin/infra.sh up            # 起 Postgres + MinIO + HMS + Presto（首次会构建 HMS、拉镜像）
bin/infra.sh up tools      # 额外起 Adminer（Postgres 网页客户端）
bin/infra.sh ps            # 看健康状态
bin/infra.sh doctor        # 端口 / HTTP / Presto 端到端检查
bin/infra.sh logs presto   # 跟某个服务日志
bin/infra.sh down          # 停容器，保留数据
bin/infra.sh clean         # 停容器 + 删数据卷（元数据、对象存储全清）
```

`up` 结束后会打印端口清单：

| 服务 | 地址 | 凭据 |
|---|---|---|
| Presto | http://localhost:8080 | 任意用户名（`demo`） |
| MinIO API | http://localhost:9000 | `admin` / `password` |
| MinIO 控制台 | http://localhost:9001 | `admin` / `password` |
| Hive Metastore | `thrift://localhost:9083` | — |
| Postgres | `localhost:5432`，库 `metastore` | `hive` / `hive` |
| Adminer | http://localhost:8081 | 服务器填 `postgres` |

`doctor` 会依次验证：Docker → 容器状态 → MinIO/Presto HTTP → HMS 端口 → Postgres →
`select 1` → `show tables from iceberg.lake`。最后一项在还没跑过批处理时会是 WARN，属于正常。

---

## 3. 接 Spark 跑一遍

```bash
cp conf/platform.properties.example conf/platform.properties

# s3 profile 会带上 iceberg-aws-bundle + hadoop-aws（第一次要下载 ~300MB）
mvn -Ps3 -pl spark-batch dependency:go-offline

MVN_PROFILE=s3 bin/dev-batch.sh --job all
bin/infra.sh doctor

# 即席查询
bin/presto-query.sh --sql "select count(*) from iceberg.lake.silver_orders" --stats
```

要点：

- `conf/platform.properties.example` 里的 `s3a://lake/warehouse`、`localhost:9000`、`localhost:9083`
  和 compose 完全对得上，不用改。
- Presto 0.290 的 Hive 文件系统同时注册了 `s3://` / `s3a://` / `s3n://`（`fs.s3a.impl`），
  所以 Spark 用 `s3a://` 写、Presto 直接读没问题。
- 行级写保持 `copy-on-write`，别开 merge-on-read —— 0.290 的 Iceberg connector 读 delete file 容易踩坑。

服务层（`lake-service`）这时 `/api/query` 才真正可用；没起 Presto 时它仍然能启动，但查询返回 503。

---

## 4. 加新依赖（可扩展性）

### 4.1 临时用：独立容器接进同一网络

网络名固定是 `lakehouse-net`，任何容器都能加入：

```bash
docker run --rm -it --network lakehouse-net \
  bitnamilegacy/minio-client:2025.7.21 --version
```

### 4.2 长期用：写 `docker-compose.override.yml`

Compose 会自动合并同目录的 `docker-compose.override.yml`，**不用改基础文件**，适合个人/临时依赖：

```yaml
# infra/docker-compose.override.yml
services:
  redis:
    image: redis:7-alpine
    container_name: lake-redis
    profiles: ["tools"]
    ports:
      - "127.0.0.1:6379:6379"
    mem_limit: 256m
    restart: unless-stopped
    networks: [lake]
```

`bin/infra.sh up tools` 就会把它一起带起来。

### 4.3 团队共用：直接加进 `docker-compose.yml`

照抄现有服务的四件套即可：

```yaml
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    container_name: lake-clickhouse
    profiles: ["olap"]
    ports:
      - "127.0.0.1:8123:8123"
      - "127.0.0.1:9002:9000"
    volumes:
      - chdata:/var/lib/clickhouse
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8123/ping | grep -q Ok"]
      interval: 5s
      timeout: 3s
      retries: 20
    mem_limit: 1g
    restart: unless-stopped
    logging: *default-logging
    networks: [lake]
```

然后在 `volumes:` 下加 `chdata:`，用 `docker compose --profile olap up -d` 启动。

约定（保持一致性）：

| 约定 | 说明 |
|---|---|
| 端口 | 一律 `127.0.0.1:宿主:容器`，不要暴露到 `0.0.0.0` |
| 镜像 tag | 钉版本，不用 `latest` |
| 健康检查 | 每个有依赖关系的服务都写，`depends_on` 用 `condition: service_healthy` |
| 资源 | `mem_limit` 给上限，避免一个服务把宿主打爆 |
| 网络 | 加入 `lake`（即 `lakehouse-net`） |
| 可选服务 | `profiles: ["tools"]` 或自定义 profile |
| 日志 | `logging: *default-logging`（20m×3 轮转） |

### 4.4 想装命令行工具（aws-cli、kcat、psql 客户端……）

不用污染宿主机，起一个一次性 toolbox：

```bash
docker run --rm -it --network lakehouse-net \
  -v "$PWD:/work" -w /work alpine:3.20 sh
```

或者做成 `tools` profile 里的常驻容器。宿主机只保留 JDK / Maven / Node 这些
**IDE 直接依赖**的运行时。

---

## 5. 版本与镜像现状

| 组件 | 镜像 | 说明 |
|---|---|---|
| Postgres | `postgres:16-alpine` | HMS 元数据库 |
| MinIO | `bitnamilegacy/minio:2025.7.23` | **MinIO 社区镜像已从 Docker Hub / Quay 下线**，这是 Bitnami 归档镜像，仅限本地开发 |
| HMS | `lakehouse-demo/hive-metastore:3.1.3`（本地构建） | 官方镜像 + Postgres JDBC 驱动 + 幂等 schema 初始化 |
| Presto | `prestodb/presto:0.290` | 官方 server 镜像（JDK 11），与项目版本对齐 |
| Adminer | `adminer:4` | 可选，Postgres 网页客户端 |

MinIO 替代方案（要长期用、不想依赖归档镜像时）：

- 企业对象存储（S3 / OSS / COS），只改 `platform.s3.*`；
- 自建 MinIO：从源码构建镜像（社区版已不再发布二进制镜像）；
- S3 兼容网关：SeaweedFS、Garage、VersityGW（`hive.s3.path-style-access=true` 都能用）。

---

## 6. 排错

| 现象 | 原因 / 处理 |
|---|---|
| `permission denied ... docker.sock` | `newgrp docker` 或重新登录；确认 `groups` 里有 docker |
| `failed to resolve reference ... 404` 拉 MinIO | `minio/minio` 已下线，用 compose 里的 `bitnamilegacy/minio` |
| 拉镜像超时 / 403 / 429 | 镜像加速器失效：见 1.3，换源或走代理 |
| 端口被占用 | `ss -ltnp \| grep -E '8080\|9000\|9083\|5432'`，改 compose 左侧端口 |
| HMS 反复重启 | `bin/infra.sh logs hive-metastore`；多半是 Postgres 没就绪或密码不对。entrypoint 会重试 10 次 schema 初始化 |
| `bin/infra.sh doctor` 里 Presto 失败 | `bin/infra.sh logs presto`；看是不是 `iceberg.properties` 连不上 HMS / MinIO |
| 查询报 `Schema lake not found` | 还没跑批处理。先 `MVN_PROFILE=s3 bin/dev-batch.sh --job all` |
| 容器 exit 137 | 被 OOM kill。降 Presto 堆或停掉不用的服务 |
| MinIO 报 `permission denied` 写 `/bitnami/minio/data` | 用了 bind mount 时执行 `sudo chown -R 1001:1001 <目录>`；named volume 不需要 |
| 宿主机 VPN 导致容器网络不通 | 在 `daemon.json` 加 `"default-address-pools": [{"base":"172.30.0.0/16","size":24}]` 后重启 docker |
| S3 签名错误 `SignatureDoesNotMatch` | 检查宿主机时间：`timedatectl`，偏差超过 5 分钟会签名失败 |

---

## 7. 已验证 / 未验证

写这份文档时的核查范围：

- 已核对：各镜像 tag 真实存在、Presto 0.290 镜像包含 server、HMS 官方 entrypoint 的
  `-initSchema` 行为、Presto 0.290 注册了 `fs.s3a.impl`、本机到 apt / Docker Hub / 各加速器的连通性。
- **未实测**：本机当时还没装 Docker（`sudo` 需要密码），所以 `docker compose up` 没跑过。
  装完后先执行 `bin/infra.sh doctor`；有失败项把 `bin/infra.sh logs <服务>` 的尾部贴出来再调。
