# Hive Metastore 只差一个 Postgres JDBC 驱动（官方镜像不带）
# 元数据走 Postgres；数据文件由 Spark / Presto 直接写 MinIO，HMS 不碰数据
FROM apache/hive:3.1.3

ARG PG_JDBC_VERSION=42.7.3

USER root
RUN curl -fsSL -o "/opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
    && chmod 644 "/opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar"
USER hive
