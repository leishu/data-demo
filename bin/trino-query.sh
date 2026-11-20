#!/usr/bin/env bash
# Trino 即席查询
#   一次 mvn exec:java 完成：SqlParams 预处理（${var} 替换、缺参跳过）+ 官方 trino-jdbc 执行 + 表格输出
#   bin/trino-query.sh --list
#   bin/trino-query.sh --sql-file sql/adhoc/01_daily_revenue.sql
#   bin/trino-query.sh --sql "select count(*) from iceberg.lake.silver_orders" --stats
#   bin/trino-query.sh --sql-file sql/adhoc/04_time_travel.sql --params snapshot_id=123
set -euo pipefail
cd "$(dirname "$0")/.."

MVN_BIN="${MVN_BIN:-mvn}"
REPO_ARGS=()
if [[ -d "${HOME}/.m2/repository" ]]; then
  REPO_ARGS+=("-Dmaven.repo.local=${HOME}/.m2/repository}")
fi

TMP_SQL=""
cleanup() {
  [[ -n "${TMP_SQL}" ]] && rm -f "${TMP_SQL}"
  return 0
}
trap cleanup EXIT

# --sql 的 SQL 落到临时文件再走 --sql-file：避开 exec.args 的二次分词（空格/引号/分号）
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sql)
      if [[ $# -lt 2 ]]; then echo "--sql 缺少参数" >&2; exit 2; fi
      TMP_SQL="$(mktemp "${TMPDIR:-/tmp}/lake-sql-XXXXXX.sql")"
      printf '%s\n' "$2" >"${TMP_SQL}"
      ARGS+=("--sql-file" "${TMP_SQL}")
      shift 2 ;;
    --sql=*)
      TMP_SQL="$(mktemp "${TMPDIR:-/tmp}/lake-sql-XXXXXX.sql")"
      printf '%s\n' "${1#--sql=}" >"${TMP_SQL}"
      ARGS+=("--sql-file" "${TMP_SQL}")
      shift ;;
    *)
      ARGS+=("$1")
      shift ;;
  esac
done

# 含空格/引号的值加引号，保证 exec 插件分词后仍是一个参数
quote_arg() {
  local s="$1"
  if [[ "$s" == *[[:space:]\"]* || -z "$s" ]]; then
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    printf '"%s"' "$s"
  else
    printf '%s' "$s"
  fi
}
EXEC_ARGS=""
for arg in "${ARGS[@]}"; do
  EXEC_ARGS+="$(quote_arg "$arg") "
done
EXEC_ARGS="${EXEC_ARGS% }"

exec "${MVN_BIN}" -q -pl lake-adhoc -am -DskipTests package exec:java \
  "${REPO_ARGS[@]}" \
  -Dexec.mainClass=com.demo.lake.adhoc.TrinoQuery \
  -Dexec.args="${EXEC_ARGS}"
