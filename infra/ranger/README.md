# HMS 接 Ranger（生产模板）

本地 demo 的 Hive Metastore 不做授权，谁连上 `thrift://localhost:9083` 都能建库改表。
生产环境要给 HMS 装 Ranger Hive 插件，让元数据操作按 Ranger 策略鉴权。

## 步骤（在 HMS 镜像/节点上做）

1. 从 Ranger Admin 下载 `ranger-2.x-hive-plugin.tar.gz` 并解压。
2. 用 `hive-ranger-plugin.properties` 里的值初始化 `install.properties`
   （仓库名、POLICY_MGR_URL、管理员凭据、Kerberos principal/keytab 按实际环境改）。
3. 执行 `./enable-hive-plugin.sh`，脚本会把插件 jar、配置与 `ranger-hive-audit.log4j.xml`
   放进 HMS 的 classpath，并改写启动脚本以加载 Ranger policy client。
4. 在 Ranger Admin 建名为 `lakehouse_hive` 的 Hive service，至少配三条策略：
   - `etl` 组：`iceberg.lake` 库 `create / alter / drop / select / update`；
   - `analyst` 组：`iceberg.lake` 库仅 `select`；
   - `admin` 组：全部权限。
5. 重启 HMS，确认日志里出现 `RangerHiveAuthorizer` 初始化成功、策略拉取成功。
6. 用 `beeline`/Trino 分别以 `etl` 和 `analyst` 身份试一次 `CREATE TABLE`：
   前者成功、后者应被拒绝（Ranger 审计里能看到 deny）。

## 与 Iceberg 表级授权的关系

- Ranger 管的是 **HMS 元数据操作**（建表、改表、删分区）；Iceberg 的数据文件读写走 S3/HDFS，
  还要靠对象存储策略（bucket policy / IAM）或 Iceberg REST Catalog 的授权层。
- Trino 侧的表级 SELECT/INSERT 由 `infra/trino/auth/rules.json` 或 Ranger Trino 插件控制，
  两边策略要对齐（同一批用户、同一组角色），否则会出现「HMS 放行但 Trino 拒绝」。

> 本目录只有配置模板和步骤，不包含 Ranger 服务端。不要在没有 Ranger Admin 的本地环境启用。
