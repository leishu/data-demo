#!/usr/bin/env bash
# 本地跑批处理（免 Docker，Hadoop catalog + 本地文件仓库）
#   bin/dev-batch.sh --job all
#   bin/dev-batch.sh --job ingest --days 3 --orders-per-day 1000
#   bin/dev-batch.sh --job transform --evolve
set -euo pipefail
cd "$(dirname "$0")/.."

MVN_BIN="${MVN_BIN:-mvn}"
REPO_ARGS=()
# 某些 root 环境下 ${user.home} 会解析到 /root，显式指定仓库更稳
if [[ -d "${HOME}/.m2/repository" ]]; then
  REPO_ARGS+=("-Dmaven.repo.local=${HOME}/.m2/repository")
fi

# 读写 S3/MinIO 时：MVN_PROFILE=s3 bin/dev-batch.sh ...
PROFILE_ARGS=()
if [[ -n "${MVN_PROFILE:-}" ]]; then
  PROFILE_ARGS+=("-P${MVN_PROFILE}")
fi

# 先 package 再 exec：同一个 Maven 会话里 platform-common 才能被解析到
exec "${MVN_BIN}" -q -pl spark-batch -am package exec:java \
  "${REPO_ARGS[@]}" "${PROFILE_ARGS[@]}" \
  -Dexec.mainClass=com.demo.lake.batch.BatchApp \
  -Dexec.args="$*"
