#!/usr/bin/env bash
# Hive Metastore 启动包装：
#   1) 用 schematool -initOrUpgradeSchema 幂等初始化 / 升级 Postgres 里的元数据表
#      （官方镜像只会 -initSchema，第二次启动会因为表已存在直接退出）
#   2) 初始化完把 IS_RESUME 置 true，再交给官方 entrypoint 正常启动 HMS
set -euo pipefail

: "${DB_DRIVER:=postgres}"

if [[ "${IS_RESUME:-false}" != "true" ]]; then
  echo "[hms] ensure metastore schema in ${DB_DRIVER} ..."
  ok=0
  for attempt in $(seq 1 10); do
    if HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G ${SERVICE_OPTS:-}" \
         "${HIVE_HOME}/bin/schematool" -dbType "${DB_DRIVER}" -initOrUpgradeSchema; then
      ok=1
      break
    fi
    echo "[hms] schema init failed (attempt ${attempt}/10), retrying in 3s ..." >&2
    sleep 3
  done
  if [[ "${ok}" != "1" ]]; then
    echo "[hms] could not initialize schema, check Postgres connectivity / credentials" >&2
    exit 1
  fi
fi

export IS_RESUME=true
exec /entrypoint.sh
