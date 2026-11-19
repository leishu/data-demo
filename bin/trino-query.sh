#!/usr/bin/env bash
# Trino 即席查询
#   bin/trino-query.sh --list
#   bin/trino-query.sh --sql-file sql/adhoc/01_daily_revenue.sql
#   bin/trino-query.sh --sql "select count(*) from iceberg.lake.silver_orders" --stats
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
  # 注意：--sql-file 时 TMP_SQL 为空，末尾不能留一个恒假的条件表达式——
  # EXIT trap 的返回码会变成脚本退出码，查出结果也会“失败”
  if [[ -n "${TMP_SQL}" ]]; then
    rm -f "${TMP_SQL}"
  fi
  return 0
}
trap cleanup EXIT

ARGS=()
NO_TRINO=0   # --list / --print-only 不需要连服务
CUSTOM_URL=0 # 指定了 --url 就不知道目标在哪儿，不做本地探测
while [[ $# -gt 0 ]]; do
  case "$1" in
    --list|--print-only)
      NO_TRINO=1
      ARGS+=("$1")
      shift
      ;;
    --url)
      CUSTOM_URL=1
      ARGS+=("$1" "${2:-}")
      shift 2 || shift
      ;;
    --url=*)
      CUSTOM_URL=1
      ARGS+=("$1")
      shift
      ;;
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

# Trino 默认不随基础设施启动（常驻 ~0.8G），先探一下 8080：
# 不然要等到 mvn 跑起来才报 JDBC 连接超时，看不出是服务没起
if [[ "${NO_TRINO}" == "0" && "${CUSTOM_URL}" == "0" ]] \
   && ! curl -fsS --max-time 3 http://localhost:8080/v1/info >/dev/null 2>&1; then
  echo "[!] Trino 没在跑（localhost:8080 不可达）。先执行：bin/infra.sh up trino" >&2
  exit 1
fi

"${MVN_BIN}" -q -pl trino-adhoc -am -DskipTests package exec:java \
  "${REPO_ARGS[@]}" \
  -Dexec.mainClass=com.demo.lake.trino.TrinoAdhocRunner \
  -Dexec.args="${EXEC_ARGS}"
