#!/usr/bin/env bash
# Hive Metastore 启动包装：
#   1) 幂等初始化 / 升级 Postgres 里的元数据表
#      （官方镜像只会 -initSchema，第二次启动会因为表已存在直接退出）
#   2) 初始化完把 IS_RESUME 置 true，再交给官方 entrypoint 正常启动 HMS
set -euo pipefail

: "${DB_DRIVER:=postgres}"

# Hive 3.1.3 的 schematool 没有 -initOrUpgradeSchema（那是 4.x 才加的），
# 传进去会被当成参数解析失败，表现为 usage 打印 + 永远等不到 9083。
# 3.1.3 上只能自己探状态（实测语义）：
#   空库：-info exit 1，-initSchema exit 0
#   已建表：-info exit 0，-initSchema exit 1，-upgradeSchema exit 0（已最新时是 no-op）
# schematool 是跑完就退的一次性进程，不需要 1G 堆（跟 SERVICE_OPTS 里的 -Xmx512m 对齐）
HMS_TOOL_HEAP="${HMS_TOOL_HEAP:-512m}"

hms_schematool() {
  HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx${HMS_TOOL_HEAP} ${SERVICE_OPTS:-}" \
    "${HIVE_HOME}/bin/schematool" -dbType "${DB_DRIVER}" "$@"
}

if [[ "${IS_RESUME:-false}" != "true" ]]; then
  echo "[hms] ensure metastore schema in ${DB_DRIVER} ..."
  ok=0
  for attempt in $(seq 1 10); do
    # 每轮都重新探状态：首轮 Postgres 可能还没就绪，也可能是并发启动时别人先建了表
    if hms_schematool -info >/dev/null 2>&1; then
      action=-upgradeSchema
    else
      action=-initSchema
    fi
    if hms_schematool "${action}"; then
      ok=1
      break
    fi
    echo "[hms] schema ${action#-} failed (attempt ${attempt}/10), retrying in 3s ..." >&2
    sleep 3
  done
  if [[ "${ok}" != "1" ]]; then
    echo "[hms] could not initialize schema, check Postgres connectivity / credentials" >&2
    exit 1
  fi
fi

export IS_RESUME=true
exec /entrypoint.sh
