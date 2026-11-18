# Hive Metastore：官方镜像 + Postgres JDBC 驱动 + 幂等的 schema 初始化
#
# 官方 apache/hive 镜像有两个坑：
#   1) 不带 Postgres 驱动，连 Postgres 会报 No suitable driver；
#   2) entrypoint 只会 `schematool -initSchema`，容器第二次启动时表已存在会直接退出。
# 这里补上驱动，并用自己的 entrypoint 做 -initOrUpgradeSchema。
FROM apache/hive:3.1.3

ARG PG_JDBC_VERSION=42.7.3

USER root
RUN curl -fsSL --retry 3 -o "/opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
    && chmod 644 "/opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar"

COPY entrypoint.sh /entrypoint-wrapper.sh
RUN chmod 755 /entrypoint-wrapper.sh

USER hive
ENTRYPOINT ["/entrypoint-wrapper.sh"]
