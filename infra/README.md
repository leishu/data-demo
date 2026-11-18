# 本地湖仓基础设施（Docker）

这份文档解决四件事：

1. 在这台机器上装好 **Docker Engine + Compose v2**（含镜像加速，否则拉不下镜像）；
2. 一条命令起齐 **Postgres + MinIO + Hive Metastore + Presto**，并验证「Spark 写、Presto 读」；
3. 日常运维：子命令、裸 docker 速查、数据卷与清理、固定排查顺序；
4. 说明**怎么加新依赖**（Kafka、ClickHouse、Redis……），不用每次重造环境。

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
```

首次验收就这四条。`logs` / `restart` / `psql` / `down` / `clean` 的完整语义和日常运维见第 3 章。

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

## 3. 日常运维（Docker 速查）

### 3.1 `bin/infra.sh` 子命令

日常只走这一个入口：它会自己 `cd infra/`、等容器 healthy、打印端点，省得记 compose 项目名和 profile。

| 命令 | 作用 | 说明 |
|---|---|---|
| `bin/infra.sh up` | 起核心四件套 | 自带 `--build`，改过 `infra/hive/Dockerfile` 会自动重建 HMS 镜像 |
| `bin/infra.sh up tools` | 额外起 Adminer（:8081） | 走 `--profile tools` |
| `bin/infra.sh ps` | 容器 + 健康状态 | 出问题时的第一步 |
| `bin/infra.sh logs [服务...]` | 跟日志（`-f --tail=200`） | 参数是**服务名**：`postgres` / `minio` / `hive-metastore` / `presto`，不是容器名 |
| `bin/infra.sh restart [服务...]` | 重启容器 | 只改了挂载文件或环境变量时够用，不必重建镜像 |
| `bin/infra.sh psql [参数]` | 进 HMS 的 Postgres | 等价于 `docker exec -it lake-postgres psql -U hive -d metastore` |
| `bin/infra.sh doctor` | 端口 / HTTP / Presto 端到端检查 | 检查清单见第 2 章 |
| `bin/infra.sh down` | 停容器，**保留数据卷** | 第二天 `up` 接着用 |
| `bin/infra.sh clean` | 停容器 + **删数据卷** | 不可恢复，见 3.3 |

> 命名对照：容器名固定是 `lake-postgres` / `lake-minio` / `lake-hms` / `lake-presto`；
> 而 `logs`、`restart` 要的是 compose **服务名**（`postgres` / `minio` / `hive-metastore` / `presto`）。
> 卷名前缀是 `infra_`（compose 项目名取目录名）。要直接用 `docker compose`，先 `cd infra/`。

### 3.2 裸 docker 命令速查

| 场景 | 命令 |
|---|---|
| 看容器 / 镜像 / 卷 / 磁盘占用 | `docker ps -a`、`docker images`、`docker volume ls`、`docker system df` |
| 进容器 | `docker exec -it lake-hms bash`、`docker exec -it lake-presto ls /opt/presto-server/etc/catalog` |
| 单个容器起停 | `docker stop lake-presto`、`docker start lake-presto` |
| 实时资源占用 | `docker stats`（`Ctrl-C` 只退查看，不停容器） |
| 看端口映射 | `docker port lake-presto` |
| 看 compose 展开后的最终配置 | `docker compose -f infra/docker-compose.yml config` |

### 3.3 数据卷、`prune` 与 `clean`

数据不在代码目录里，而在三个 named volume：

| 卷 | 内容 | 丢了会怎样 |
|---|---|---|
| `infra_pgdata` | HMS 元数据库（库表 / 分区 / 快照指针） | `show tables` 变空 |
| `infra_miniodata` | MinIO 里的 parquet + Iceberg metadata | 元数据还在也读不出数据 |
| `infra_prestodata` | Presto 本地暂存 | 影响最小，重启自动重建 |

| 想做的事 | 用哪个 | 注意 |
|---|---|---|
| 下班关环境 | `bin/infra.sh down` | 数据全在 |
| 改了 Dockerfile 想重来 | `bin/infra.sh up` | 只重建镜像，卷不动 |
| 元数据 / 对象存储要归零 | `bin/infra.sh clean` | 删完必须重跑 `MVN_PROFILE=s3 bin/dev-batch.sh --job all` |
| 只清构建缓存 | `docker builder prune` | 不动镜像和卷 |

> **不要随手 `docker system prune -a`。** 它会把 `prestodb/presto:0.290`（最大的一块）、
> `bitnamilegacy/minio`、`apache/hive` 一起删掉，下次 `up` 要重拉 1 GB+ 并重建 HMS 镜像
> （镜像里还带 86 MB 的 AWS SDK bundle）；`docker volume prune` 更狠，会把上面三个数据卷当
> “未使用”删掉。真要腾空间：先 `docker system df` 看清，再 `docker image prune`（只删悬空镜像）。

### 3.4 改了什么，要做什么

| 改动 | 需要执行 |
|---|---|
| Java / SQL / `conf/platform.properties` | 什么都不用，重跑作业或查询 |
| `infra/hive/core-site.xml`（挂载）或 compose 里的环境变量 | `bin/infra.sh restart hive-metastore` |
| `infra/hive/Dockerfile`、`infra/hive/entrypoint.sh` | `bin/infra.sh up`（自带 `--build`） |
| `infra/presto/etc/**` | `bin/infra.sh restart presto` |
| 环境彻底归零 | `bin/infra.sh clean`，再重跑批处理 |

### 3.5 排查顺序

固定三步，别跳：

```bash
bin/infra.sh ps                  # 谁没起来 / 谁 unhealthy
bin/infra.sh logs hive-metastore # 它自己说了什么（换成对应服务名）
bin/infra.sh doctor              # 端口 / HTTP / JDBC 全链路
```

> 规律：`unhealthy` 只是表象，真因都在容器日志里，而且大多是配置问题不是资源问题——
> HMS 报 schematool 参数不被识别、Presto 报 `Configuration property 'xxx' was not used`。
> 对照表见第 7 章。

---

## 4. 接 Spark 跑一遍

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

## 5. 加新依赖（可扩展性）

### 5.1 临时用：独立容器接进同一网络

网络名固定是 `lakehouse-net`，任何容器都能加入：

```bash
docker run --rm -it --network lakehouse-net \
  bitnamilegacy/minio-client:2025.7.21 --version
```

### 5.2 长期用：写 `docker-compose.override.yml`

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

### 5.3 团队共用：直接加进 `docker-compose.yml`

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

### 5.4 想装命令行工具（aws-cli、kcat、psql 客户端……）

不用污染宿主机，起一个一次性 toolbox：

```bash
docker run --rm -it --network lakehouse-net \
  -v "$PWD:/work" -w /work alpine:3.20 sh
```

或者做成 `tools` profile 里的常驻容器。宿主机只保留 JDK / Maven / Node 这些
**IDE 直接依赖**的运行时。

---

## 6. 版本与镜像现状

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

## 7. 排错

| 现象 | 原因 / 处理 |
|---|---|
| `permission denied ... docker.sock` | `newgrp docker` 或重新登录；确认 `groups` 里有 docker |
| `failed to resolve reference ... 404` 拉 MinIO | `minio/minio` 已下线，用 compose 里的 `bitnamilegacy/minio` |
| 拉镜像超时 / 403 / 429 | 镜像加速器失效：见 1.3，换源或走代理 |
| 端口被占用 | `ss -ltnp \| grep -E '8080\|9000\|9083\|5432'`，改 compose 左侧端口 |
| HMS 反复重启 | `bin/infra.sh logs hive-metastore`；多半是 Postgres 没就绪或密码不对。entrypoint 会重试 10 次 schema 初始化 |
| HMS 日志报 `Parsing failed. Reason: Missing required option: [-initSchema ...]` | HMS 3.1.3 的 schematool 没有 `-initOrUpgradeSchema`（4.x 才有）。entrypoint 用 `-info` 探状态再选 `-initSchema` / `-upgradeSchema` |
| 建库时报 `MetaException ... ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` | 这是 HMS 侧：建 database 要把库目录 mkdirs 到 `s3a://`，镜像需 `hadoop-aws`；S3A 参数必须写在挂载的 `infra/hive/core-site.xml`（JVM 的 `-Dfs.s3a.*` 对 Hadoop Configuration 无效） |
| Spark 报 `NoClassDefFoundError: org/apache/hadoop/hive/metastore/api/UnknownDBException` | 本地 `exec:java` 缺 HMS 客户端类（只在 Spark 发行版 `$SPARK_HOME/jars` 里）。`spark-batch` 已声明 `hive-metastore`（provided） |
| Spark 报 `Unable to load region from any of the providers` | Iceberg 1.5.x 的 S3FileIO 不认 `s3.region`，region 走 `client.region`（`PlatformConfig` 已按此输出） |
| Presto 反复重启，日志 `Configuration property 'xxx' was not used` | 该属性在 0.290 的 connector 里不存在（例如 `iceberg.register-table-procedure.enabled`），从 `iceberg.properties` 删掉即可 |
| `bin/infra.sh doctor` 里 Presto 失败 | `bin/infra.sh logs presto`；看是不是 `iceberg.properties` 连不上 HMS / MinIO |
| 查询报 `Schema lake not found` | 还没跑批处理。先 `MVN_PROFILE=s3 bin/dev-batch.sh --job all` |
| 容器 exit 137 | 被 OOM kill。降 Presto 堆或停掉不用的服务 |
| MinIO 报 `permission denied` 写 `/bitnami/minio/data` | 用了 bind mount 时执行 `sudo chown -R 1001:1001 <目录>`；named volume 不需要 |
| 宿主机 VPN 导致容器网络不通 | 在 `daemon.json` 加 `"default-address-pools": [{"base":"172.30.0.0/16","size":24}]` 后重启 docker |
| S3 签名错误 `SignatureDoesNotMatch` | 检查宿主机时间：`timedatectl`，偏差超过 5 分钟会签名失败 |

---

## 8. 已验证 / 未验证

写这份文档时的核查范围：

- 已核对：各镜像 tag 真实存在、Presto 0.290 镜像包含 server、HMS 官方 entrypoint 的
  `-initSchema` 行为、Presto 0.290 注册了 `fs.s3a.impl`、本机到 apt / Docker Hub / 各加速器的连通性。
- 已实测（Docker 29.10.0 + compose 5.8.0）：`bin/infra.sh up` 四件套全部 healthy；
  `MVN_PROFILE=s3 bin/dev-batch.sh --job all` 的 init / ingest / transform / maintain / show 全部通过；
  `bin/presto-query.sh --sql "select count(*) from iceberg.lake.silver_orders"` 返回 14000 行。
  期间踩到的坑已整理进第 7 章排错表。
