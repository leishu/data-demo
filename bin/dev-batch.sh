#!/usr/bin/env bash
# 本地跑批处理：用预构建的 spark-batch jar 直接起 JVM（mvn/package 只在 bin/build.sh 里做一次）
#   先构建：bin/build.sh
#   再运行：bin/dev-batch.sh --job all
#           bin/dev-batch.sh --job ingest --days 3 --orders-per-day 1000
#           bin/dev-batch.sh --job all --evolve
#
# 数据写到哪不在这里定，由 conf/platform.properties（或 LAKE_* / --warehouse）决定：
#   catalog.type=hive + warehouse=s3a://lake/warehouse -> 元数据进 HMS，数据文件进 MinIO
#   没有 conf/platform.properties 时会静默退化到 hadoop catalog + 本地 ./warehouse
set -euo pipefail
cd "$(dirname "$0")/.."

find_jdk21() {
  if [[ -n "${JAVA21_HOME:-}" && -x "${JAVA21_HOME}/bin/java" ]]; then
    echo "${JAVA21_HOME}"; return 0
  fi
  local cand
  for cand in "${HOME}"/soft/jdk-21* "${HOME}"/soft/jdk21* /usr/lib/jvm/java-21-openjdk-*; do
    if [[ -x "${cand}/bin/java" ]] && "${cand}/bin/java" -version 2>&1 | grep -q '"21'; then
      echo "${cand}"; return 0
    fi
  done
  return 1
}

JAVA21="$(find_jdk21)" || { echo "需要 JDK 21：设 JAVA21_HOME 指到 jdk-21 目录" >&2; exit 1; }

CLASSPATH_FILE="spark-batch/target/classpath.txt"
JAR="$(ls -1 spark-batch/target/spark-batch-*.jar 2>/dev/null | grep -v sources | head -1 || true)"
if [[ -z "${JAR}" || ! -s "${CLASSPATH_FILE}" ]]; then
  echo "缺少预构建产物（spark-batch jar 或 ${CLASSPATH_FILE}）" >&2
  echo "请先执行一次 bin/build.sh，之后每次跑作业都只是 java -cp 启动" >&2
  exit 2
fi

ROOT="$(pwd)"
ABS_JAR="$(cd "$(dirname "${JAR}")" && printf '%s/%s' "$(pwd)" "$(basename "${JAR}")")"
# target/classes 放在最前：本地改动不用重新打包也能生效，且优先于本地仓库里可能过期的 jar
CLASSES="${ROOT}/spark-batch/target/classes:${ROOT}/platform-common/target/classes"
DEPS="$(cat "${CLASSPATH_FILE}")"

# 复用 .mvn/jvm.config 里的 --add-opens：Spark 3.5 在 JDK 21 上靠这些参数访问 JDK 内部类
JAVA_OPTS=()
while IFS= read -r opt || [[ -n "${opt}" ]]; do
  [[ -n "${opt}" ]] && JAVA_OPTS+=("${opt}")
done < .mvn/jvm.config

exec "${JAVA21}/bin/java" "${JAVA_OPTS[@]}" \
  -cp "${CLASSES}:${ABS_JAR}:${DEPS}" \
  com.demo.lake.batch.BatchApp "$@"
