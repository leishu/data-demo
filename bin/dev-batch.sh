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

# 仓库是 s3a:// / s3:// 时必须激活 s3 profile（iceberg-aws-bundle + hadoop-aws 只在这个 profile 里，
# 不激活会报 NoClassDefFoundError: software/amazon/awssdk/services/s3/model/S3Exception）。
# 这里按「生效的 warehouse」自动判断，优先级跟 PlatformConfig 一致：
#   命令行 --warehouse > LAKE_WAREHOUSE 环境变量 > conf/platform.properties
# 显式给了 MVN_PROFILE 则以它为准，不再自动猜。
# 被 lake-service 的作业编排拉起时走的就是自动判断，不需要给服务进程挂环境变量。
effective_warehouse() {
  local w="" arg expect=0
  for arg in "$@"; do
    if [[ "$expect" == "1" ]]; then
      expect=0
      # 值不能是另一个选项（防止 --warehouse 漏写值时把 --xxx 当成仓库路径）
      [[ "$arg" != --* ]] && w="$arg"
      continue
    fi
    case "$arg" in
      --warehouse) expect=1 ;;
      --warehouse=*) w="${arg#--warehouse=}" ;;
    esac
  done
  if [[ -z "$w" ]]; then
    w="${LAKE_WAREHOUSE:-}"
  fi
  if [[ -z "$w" && -r conf/platform.properties ]]; then
    w="$(sed -n 's/^[[:space:]]*platform\.warehouse[[:space:]]*=[[:space:]]*//p' conf/platform.properties \
      | tail -n 1 | tr -d '\r' | sed -e 's/[[:space:]]*$//')"
  fi
  printf '%s' "$w"
}

PROFILE_ARGS=()
if [[ -n "${MVN_PROFILE:-}" ]]; then
  PROFILE_ARGS+=("-P${MVN_PROFILE}")
else
  WAREHOUSE="$(effective_warehouse "$@")"
  case "$WAREHOUSE" in
    s3://*|s3a://*)
      PROFILE_ARGS+=("-Ps3")
      echo "warehouse=${WAREHOUSE} 是对象存储，自动激活 s3 profile"
      ;;
  esac
fi

# 先 package 再 exec：同一个 Maven 会话里 platform-common 才能被解析到
exec "${MVN_BIN}" -q -pl spark-batch -am -DskipTests package exec:java \
  "${REPO_ARGS[@]}" "${PROFILE_ARGS[@]}" \
  -Dexec.mainClass=com.demo.lake.batch.BatchApp \
  -Dexec.args="$*"
