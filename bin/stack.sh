#!/usr/bin/env bash
# 一键全栈：Docker infra（Postgres/MinIO/HMS/Trino）+ 宿主机批处理 + 容器化 lake-service
#
#   bin/stack.sh up [--build] [--no-data]
#       构建 jar -> 起 infra -> 用集群配置跑一遍 all -> 起 lake-service 容器 -> 等健康
#   bin/stack.sh down            停服务与 infra（保留数据卷）
#   bin/stack.sh clean           停服务与 infra 并删除数据卷
#   bin/stack.sh smoke           对已启动的栈跑端到端冒烟（容器模式批作业关闭）
#   bin/stack.sh batch --job all 在宿主机跑批（容器内没有 Spark/Maven 运行时）
#   bin/stack.sh logs [服务]     跟日志
#   bin/stack.sh ps              容器状态
#   bin/stack.sh build           只重新构建 lake-service jar
#
# 容器里的 lake-service 关闭了批作业（LAKE_BATCH_ENABLED=false），
# 所以 /api/batch/** 会返回 403；批作业统一走 bin/stack.sh batch。
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml)
STACK_CONF="conf/platform.stack.properties"
BASE_URL="${BASE_URL:-http://localhost:8088}"

log()  { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; }

usage() {
  cat <<'EOF'
用法: bin/stack.sh <命令> [参数]

  up [--build] [--no-data]  起全栈；--build 强制重建 jar，--no-data 跳过初始批处理
  down                      停服务与 infra，保留数据卷
  clean                     停服务与 infra 并删除数据卷（不可恢复）
  smoke                     端到端冒烟（SMOKE_BATCH=0）
  batch --job ...           在宿主机跑批（默认追加 --conf conf/platform.stack.properties）
  build                     重新构建 lake-service jar
  logs [服务]               跟日志（默认全部）
  ps                        容器状态
EOF
  exit 2
}

need_docker() {
  command -v docker >/dev/null 2>&1 || {
    err "没有 docker。先跑：bash bin/install-docker.sh"
    exit 1
  }
  docker info >/dev/null 2>&1 || {
    err "docker 守护进程不可用。可能是没启动，或者当前用户不在 docker 组。"
    exit 1
  }
}

ensure_stack_conf() {
  if [[ ! -f "${STACK_CONF}" ]]; then
    cp conf/platform.properties.example "${STACK_CONF}"
    log "已生成 ${STACK_CONF}（集群模式：HMS + MinIO）"
  fi
}

wait_service() {
  local tries=90 i code
  log "等待 lake-service 健康（最多 $((tries * 2)) 秒）"
  for ((i = 1; i <= tries; i++)); do
    code="$(curl -sS -o /dev/null -w '%{http_code}' "${BASE_URL}/actuator/health" 2>/dev/null || true)"
    [[ "${code}" == "200" ]] && return 0
    sleep 2
  done
  err "lake-service 没有在预期时间内就绪，看：bin/stack.sh logs lake-service"
  return 1
}

cmd_up() {
  need_docker
  local force_build=0 skip_data=0 arg
  for arg in "$@"; do
    case "${arg}" in
      --build) force_build=1 ;;
      --no-data) skip_data=1 ;;
      *) warn "忽略未知参数：${arg}" ;;
    esac
  done

  ensure_stack_conf

  local jar
  jar="$(ls -1 lake-service/target/lake-service-*.jar 2>/dev/null | grep -v sources | head -1 || true)"
  if [[ -z "${jar}" || "${force_build}" == "1" ]]; then
    log "构建 lake-service jar（含控制台）"
    bin/lake-service.sh build
  else
    log "复用已有 jar：${jar}（要重建加 --build）"
  fi

  log "启动 Postgres + MinIO + HMS + Trino"
  bin/infra.sh up trino

  if [[ "${skip_data}" != "1" ]]; then
    log "用集群配置跑初始批处理（${STACK_CONF}）"
    bin/dev-batch.sh --job all --conf "${STACK_CONF}"
  else
    warn "跳过初始批处理（--no-data），查询可能没有数据"
  fi

  log "启动 lake-service 容器"
  "${COMPOSE[@]}" up -d --build lake-service
  wait_service

  cat <<EOF

  控制台         ${BASE_URL}/
  接口文档       ${BASE_URL}/swagger-ui.html
  健康检查       ${BASE_URL}/actuator/health
  批作业         bin/stack.sh batch --job all   （容器内关闭了 /api/batch）
  冒烟           bin/stack.sh smoke

  账号           viewer/viewer（只读）、operator/operator、auditor/auditor

EOF
}

cmd_down() {
  need_docker
  bin/lake-service.sh stop >/dev/null 2>&1 || true
  "${COMPOSE[@]}" down
}

cmd_clean() {
  need_docker
  warn "将删除 Postgres / MinIO / Trino 数据卷"
  bin/lake-service.sh stop >/dev/null 2>&1 || true
  "${COMPOSE[@]}" down -v
}

cmd_smoke() {
  ensure_stack_conf
  SMOKE_BATCH=0 BASE_URL="${BASE_URL}" bin/smoke.sh
}

cmd_batch() {
  ensure_stack_conf
  # 用户显式给了 --conf 就用用户的（兼容 --conf x 和 --conf=x）
  if [[ " $* " == *" --conf "* || " $* " == *" --conf="* ]]; then
    bin/dev-batch.sh "$@"
  else
    bin/dev-batch.sh "$@" --conf "${STACK_CONF}"
  fi
}

cmd="${1:-up}"
shift || true

case "${cmd}" in
  up)     cmd_up "$@" ;;
  down)   cmd_down ;;
  clean)  cmd_clean ;;
  smoke)  cmd_smoke ;;
  batch)  cmd_batch "$@" ;;
  build)  bin/lake-service.sh build ;;
  logs)   need_docker; "${COMPOSE[@]}" logs -f --tail=200 "$@" ;;
  ps)     need_docker; "${COMPOSE[@]}" ps ;;
  -h|--help|help) usage ;;
  *) usage ;;
esac
