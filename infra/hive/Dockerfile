# Hive Metastore：官方镜像 + Postgres JDBC 驱动 + 幂等的 schema 初始化
#
# 官方 apache/hive 镜像有三个坑：
#   1) 不带 Postgres 驱动，连 Postgres 会报 No suitable driver；
#   2) 基于 debian:bullseye-slim，里面没有 curl/wget（只有 apt-get），RUN curl 会 127；
#   3) entrypoint 只会 `schematool -initSchema`，容器第二次启动时表已存在会直接退出。
# 这里用 buildkit 的远程 ADD 拉驱动（不依赖镜像内工具），并用自己的 entrypoint 做
# -initOrUpgradeSchema。
FROM apache/hive:3.1.3

# 换版本时把 PG_JDBC_SHA256 一起改，否则构建会因为 checksum 不匹配直接失败（宁可失败，
# 也不要让代理/镜像站返回的错误页被当成 jar 打进去）。
# MAVEN_REPO 可以在构建时换成国内镜像：
#   docker compose build --build-arg MAVEN_REPO=https://maven.aliyun.com/repository/central
ARG PG_JDBC_VERSION=42.7.3
ARG PG_JDBC_SHA256=a2644cbfba1baa145ff7e8c8ef582a6eed7a7ec4ca792f7f054122bdec756268
ARG MAVEN_REPO=https://repo1.maven.org/maven2

USER root
ADD --checksum=sha256:${PG_JDBC_SHA256} \
    ${MAVEN_REPO}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar \
    /opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar
# 远程 ADD 落盘权限固定 0600 root:root，而 HMS 以 hive 用户运行，必须放开读权限
RUN chmod 644 "/opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar"

# HMS 建 database 时要把库目录 mkdirs 到 s3a://lake/warehouse/<db>（Iceberg 建
# namespace 传的就是 s3a 位置），但官方镜像只带 hadoop-common，没有 S3A 实现，
# 会报 MetaException: ClassNotFoundException org.apache.hadoop.fs.s3a.S3AFileSystem。
# 版本必须对齐镜像里的 Hadoop 3.1.0；连接 MinIO 的参数在 core-site.xml（compose 挂载）。
ARG HADOOP_AWS_VERSION=3.1.0
ARG AWS_SDK_BUNDLE_VERSION=1.11.271
ADD --checksum=sha256:a18508b9348af095ea41301e439354dbd449e304ac44c6885b2b4fe78de88126 \
    ${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    /opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar
ADD --checksum=sha256:faf78ac4880f56cf52791d84ec1068ce7c66acc4295d580a726104b734c01fcd \
    ${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
    /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
RUN chmod 644 /opt/hive/lib/hadoop-aws-*.jar /opt/hive/lib/aws-java-sdk-bundle-*.jar

COPY entrypoint.sh /entrypoint-wrapper.sh
RUN chmod 755 /entrypoint-wrapper.sh

USER hive
ENTRYPOINT ["/entrypoint-wrapper.sh"]
