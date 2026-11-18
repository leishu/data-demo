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

exec "${MVN_BIN}" -q -pl presto-adhoc -am package exec:java \
  "${REPO_ARGS[@]}" \
  -Dexec.mainClass=com.demo.lake.presto.PrestoAdhocRunner \
  -Dexec.args="$*"
