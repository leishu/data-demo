#!/usr/bin/env bash
# Presto 即席查询
#   bin/presto-query.sh --list
#   bin/presto-query.sh --sql-file sql/adhoc/01_daily_revenue.sql
#   bin/presto-query.sh --sql "select count(*) from iceberg.lake.silver_orders" --stats
set -euo pipefail
cd "$(dirname "$0")/.."

MVN_BIN="${MVN_BIN:-mvn}"
REPO_ARGS=()
if [[ -d "${HOME}/.m2/repository" ]]; then
  REPO_ARGS+=("-Dmaven.repo.local=${HOME}/.m2/repository")
fi

# -Dexec.args 是一个字符串，exec 插件会再分一次词；直接把 "$*" 拼进去会把
# --sql "select ..." 拆成多个参数（实际只执行了 select）。
#
# 这里把 inline SQL 落到临时文件、改走 --sql-file：既保留空格/引号/分号，
# 也不依赖两个 shell/插件之间的转义规则。其余参数逐个加引号即可。
TMP_SQL=""
cleanup() {
  [[ -n "${TMP_SQL}" ]] && rm -f "${TMP_SQL}"
}
trap cleanup EXIT

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sql)
      if [[ $# -lt 2 ]]; then
        echo "--sql 缺少参数" >&2
        exit 2
      fi
      TMP_SQL="$(mktemp "${TMPDIR:-/tmp}/lake-sql-XXXXXX.sql")"
      printf '%s\n' "$2" >"${TMP_SQL}"
      ARGS+=("--sql-file" "${TMP_SQL}")
      shift 2
      ;;
    --sql=*)
      TMP_SQL="$(mktemp "${TMPDIR:-/tmp}/lake-sql-XXXXXX.sql")"
      printf '%s\n' "${1#--sql=}" >"${TMP_SQL}"
      ARGS+=("--sql-file" "${TMP_SQL}")
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

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

"${MVN_BIN}" -q -pl presto-adhoc -am -DskipTests package exec:java \
  "${REPO_ARGS[@]}" \
  -Dexec.mainClass=com.demo.lake.presto.PrestoAdhocRunner \
  -Dexec.args="${EXEC_ARGS}"
