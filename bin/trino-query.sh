#!/usr/bin/env bash
# Trino 即席查询（lake-adhoc）：java -cp 直起，不再每次走 Maven
#
#   先构建一次：bin/build.sh（会生成 lake-adhoc/target/classpath.txt）
#   bin/trino-query.sh --list
#   bin/trino-query.sh --sql-file sql/adhoc/01_daily_revenue.sql
#   bin/trino-query.sh --sql "select count(*) from iceberg.lake.silver_orders" --stats
#   bin/trino-query.sh --sql-file sql/adhoc/04_time_travel.sql --params snapshot_id=123
#
# 参数原样交给 TrinoQuery：`--sql "..."` 里的空格/引号不会被二次分词，
# 所以不需要再像 mvn exec:java 那样把 SQL 落到临时文件。
set -euo pipefail
cd "$(dirname "$0")/.."

CLASSES="lake-adhoc/target/classes"
COMMON_CLASSES="platform-common/target/classes"
CLASSPATH_FILE="lake-adhoc/target/classpath.txt"

if [[ ! -d "${CLASSES}" || ! -d "${COMMON_CLASSES}" || ! -s "${CLASSPATH_FILE}" ]]; then
  echo "缺少预构建产物（${CLASSES} / ${COMMON_CLASSES} / ${CLASSPATH_FILE}）" >&2
  echo "请先执行一次 bin/build.sh，之后每次查询都只是 java -cp 启动" >&2
  exit 2
fi

JAVA_BIN=""
if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  JAVA_BIN="${JAVA_HOME}/bin/java"
elif command -v java >/dev/null 2>&1; then
  JAVA_BIN="$(command -v java)"
fi
if [[ -z "${JAVA_BIN}" ]]; then
  echo "找不到 java（需要 JDK 11+）：设置 JAVA_HOME 或把 java 放进 PATH" >&2
  exit 2
fi

# 兼容 `--sql=...`：Args 只认 `--key value`，在这里拆成两个参数（不落临时文件）
ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --sql=*) ARGS+=("--sql" "${arg#--sql=}") ;;
    *)       ARGS+=("${arg}") ;;
  esac
done

# target/classes 放最前：本地改动不重新打包也能生效，且优先于本地仓库里可能过期的 jar
exec "${JAVA_BIN}" -cp "${CLASSES}:${COMMON_CLASSES}:$(cat "${CLASSPATH_FILE}")" \
  com.demo.lake.adhoc.TrinoQuery "${ARGS[@]}"
