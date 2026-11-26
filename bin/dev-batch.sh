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
  # CI / asdf / SDKMAN 通常只设 JAVA_HOME；只有确实是 21 才采用，避免把 JDK 11 当 21 用
  if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]] \
      && "${JAVA_HOME}/bin/java" -version 2>&1 | grep -q '"21'; then
    echo "${JAVA_HOME}"; return 0
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

# 复用 .mvn/jvm.config 里的 --add-opens：Spark 4.1 在 JDK 21 上靠这些参数访问 JDK 内部类
JAVA_OPTS=()
while IFS= read -r opt || [[ -n "${opt}" ]]; do
  [[ -n "${opt}" ]] && JAVA_OPTS+=("${opt}")
done < .mvn/jvm.config

RUN_CMD=("${JAVA21}/bin/java" "${JAVA_OPTS[@]}" \
  -cp "${CLASSES}:${ABS_JAR}:${DEPS}" \
  com.demo.lake.batch.BatchApp "$@")

# 日志归属：lake-service 拉起时已注入 BATCH_RUN_LOG，且 stdout 被执行器重定向到同一文件。
# 这条路径不能再接管，否则两个写入者打架，run_id 也会和状态库对不上。
if [[ -n "${BATCH_RUN_LOG:-}" ]]; then
  [[ -n "${BATCH_RUN_ID:-}" ]] || export BATCH_RUN_ID="$(basename "${BATCH_RUN_LOG%.log}")"
  exec "${RUN_CMD[@]}"
fi

# 命令行直跑：自己编 run_id + 日志文件，并通过环境变量交给 BatchApp。
# RunIds.resolve 认 BATCH_RUN_ID，RunRecordWriter.env() 认 BATCH_RUN_LOG，
# 于是 ops.batch_runs.log_file 有值；控制台的日志尾部也能从该文件回放（BatchJobService.tail 的文件回落）。
arg_run_id() {
  local prev="" a
  for a in "$@"; do
    if [[ "${prev}" == "--run-id" ]]; then printf '%s' "${a}"; return 0; fi
    case "${a}" in
      --run-id=*) printf '%s' "${a#--run-id=}"; return 0 ;;
    esac
    prev="${a}"
  done
  return 0
}

new_run_id() {
  local suffix
  suffix="$(cat /proc/sys/kernel/random/uuid 2>/dev/null | tr -d '-' | cut -c1-6)"
  [[ -n "${suffix}" ]] || suffix="$(uuidgen 2>/dev/null | tr -d '-' | cut -c1-6 | tr 'A-Z' 'a-z')"
  [[ -n "${suffix}" ]] || suffix="$(printf '%06x' $(( (RANDOM * 32768 + RANDOM) % 16777216 )))"
  printf 'run-%s-%s' "$(date +%Y-%m-%d-%H-%M)" "${suffix}"
}

RUN_ID="$(arg_run_id "$@")"
[[ -n "${RUN_ID}" ]] || RUN_ID="${BATCH_RUN_ID:-}"
[[ -n "${RUN_ID}" ]] || RUN_ID="$(new_run_id)"
export BATCH_RUN_ID="${RUN_ID}"

LOG_DIR="${LAKE_BATCH_LOG_DIR:-logs/batch}"
mkdir -p "${LOG_DIR}"
export BATCH_RUN_LOG="${ROOT}/${LOG_DIR}/${RUN_ID}.log"

# tee 让日志既落文件又留在终端；pipefail 保留，但要临时关掉 errexit 才能拿到 java 的真实退出码。
set +e
{
  printf '[dev-batch] run_id=%s 日志=%s\n' "${RUN_ID}" "${BATCH_RUN_LOG}"
  "${RUN_CMD[@]}"
} 2>&1 | tee "${BATCH_RUN_LOG}"
status=${PIPESTATUS[0]}
set -e
exit "${status}"
