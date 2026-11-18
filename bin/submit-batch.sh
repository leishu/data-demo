#!/usr/bin/env bash
# 提交到已有 Spark 集群（YARN / K8s / standalone）
# 前置：spark-submit 在 PATH 中；集群能访问 Maven 仓库拉 iceberg runtime 包
#   SPARK_MASTER=yarn bin/submit-batch.sh --job all --catalog-type hive \
#       --hive-uri thrift://hms:9083 --warehouse s3a://lake/warehouse
set -euo pipefail
cd "$(dirname "$0")/.."

MVN_BIN="${MVN_BIN:-mvn}"
REPO_ARGS=()
if [[ -d "${HOME}/.m2/repository" ]]; then
  REPO_ARGS+=("-Dmaven.repo.local=${HOME}/.m2/repository")
fi

ICEBERG_VERSION="${ICEBERG_VERSION:-1.5.2}"
SCALA_BIN="${SCALA_BIN:-2.12}"
HADOOP_VERSION="${HADOOP_VERSION:-3.3.4}"
PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_${SCALA_BIN}:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"

# 仓库在 s3a:// 上时还得有 hadoop-aws（vanilla Spark 发行版不带 s3a，
# 集群自带的发行版如 EMR/CDP 一般已内置，重复引入会被 --packages 去重）。
if [[ " $* " == *"--warehouse s3"* ]]; then
  PACKAGES="${PACKAGES},org.apache.hadoop:hadoop-aws:${HADOOP_VERSION}"
fi

# 只构建提交需要的 spark-batch（及依赖模块），避免在 JDK 11 环境里连带编译要求 JDK 21 的 lake-service
"${MVN_BIN}" -q -DskipTests "${REPO_ARGS[@]}" -pl spark-batch -am package

JAR="$(ls -1 spark-batch/target/spark-batch-*.jar | grep -v sources | head -1)"
echo "jar: ${JAR}"
echo "packages: ${PACKAGES}"

exec spark-submit \
  --master "${SPARK_MASTER:-yarn}" \
  --deploy-mode "${DEPLOY_MODE:-client}" \
  --class com.demo.lake.batch.BatchApp \
  --packages "${PACKAGES}" \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-200}" \
  --conf spark.driver.memory="${DRIVER_MEMORY:-2g}" \
  --conf spark.executor.memory="${EXECUTOR_MEMORY:-4g}" \
  --conf spark.executor.cores="${EXECUTOR_CORES:-2}" \
  "${JAR}" "$@"
