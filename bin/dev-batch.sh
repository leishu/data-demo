#!/usr/bin/env bash
# 本地跑批处理：用 mvn exec:java 起一个 Spark 子进程跑 BatchApp。
# 数据写到哪不在这里定，由 conf/platform.properties 决定：
#   catalog.type=hive + warehouse=s3a://lake/warehouse -> 元数据进 HMS，数据文件进 MinIO
#   没有 conf/platform.properties 时会静默退化到 hadoop catalog + 本地 ./warehouse
#
#   bin/dev-batch.sh --job all
#   bin/dev-batch.sh --job ingest --days 3 --orders-per-day 1000
#   bin/dev-batch.sh --job all --evolve
set -euo pipefail
cd "$(dirname "$0")/.."

MVN_BIN="${MVN_BIN:-mvn}"
REPO_ARGS=()
# 某些 root 环境下 ${user.home} 会解析到 /root，显式指定仓库更稳
if [[ -d "${HOME}/.m2/repository" ]]; then
  REPO_ARGS+=("-Dmaven.repo.local=${HOME}/.m2/repository")
fi

# 仓库是 s3a:// 时必须激活 s3 profile（iceberg-aws-bundle + hadoop-aws 只在这个 profile 里，
# 不激活会报找不到 S3FileIO / s3a）：
#   MVN_PROFILE=s3 bin/dev-batch.sh --job all
# 被 lake-service 的作业编排拉起时，子进程继承父进程环境变量，所以要么给服务进程挂上
# MVN_PROFILE=s3，要么把 command-template 写成 env MVN_PROFILE=s3 bin/dev-batch.sh --job {job}
PROFILE_ARGS=()
if [[ -n "${MVN_PROFILE:-}" ]]; then
  PROFILE_ARGS+=("-P${MVN_PROFILE}")
fi

# 先 package 再 exec：同一个 Maven 会话里 platform-common 才能被解析到
exec "${MVN_BIN}" -q -pl spark-batch -am -DskipTests package exec:java \
  "${REPO_ARGS[@]}" "${PROFILE_ARGS[@]}" \
  -Dexec.mainClass=com.demo.lake.batch.BatchApp \
  -Dexec.args="$*"
