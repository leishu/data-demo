#!/usr/bin/env bash
# 本地湖仓基础设施：起停 / 巡检 / 调试
#
#   bin/infra.sh up            # 起核心四件套（Postgres + MinIO + HMS + Presto）
#   bin/infra.sh up tools      # 额外起 Adminer（:8081）
#   bin/infra.sh down          # 停容器，保留数据卷
#   bin/infra.sh clean         # 停容器并删数据卷（元数据、对象存储全清）
#   bin/infra.sh ps            # 容器与健康状态
#   bin/infra.sh logs [svc]    # 跟日志，默认全部
#   bin/infra.sh doctor        # 端口 / HTTP / Presto 端到端检查
#   bin/infra.sh psql          # 进 Postgres（HMS 元数据库）
#   bin/infra.sh restart [svc]
#
# 依赖安装见 infra/README.md
set -euo pipefail
SCRIPT="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$SCRIPT")/../infra"

COMPOSE=(docker compose)
PROFILE_TOOLS=(--profile tools)

log()  { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; }
ok()   { printf '  \033[1;32mOK\033[0m   %s\n' "$*"; }
bad()  { printf '  \033[1;31mFAIL\033[0m %s\n' "$*"; }

usage() {
  cat <<'EOF'
用法: bin/infra.sh <命令> [参数]

  up [tools]        起核心四件套；带 tools 时额外起 Adminer
  down              停容器，保留数据卷
  clean             停容器并删除数据卷（不可恢复）
  ps                容器与健康状态
  logs [服务...]    跟日志（默认全部）
  doctor            端口 / HTTP / Presto 端到端检查
  psql [参数]       进 Postgres（HMS 元数据库）
  restart [服务...] 重启容器
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
    err "试试：newgrp docker   （或重新登录）"
    exit 1
  }
}

# shellcheck disable=SC2120,SC2119  # 可选参数：$1 是重试次数，调用方一般不传
wait_healthy() {
  local tries="${1:-150}" i state
  log "等待容器 healthy（最多 $((tries * 2)) 秒）"
  for ((i = 1; i <= tries; i++)); do
    state="$(docker compose ps --format '{{.Name}}|{{.State}}|{{.Health}}' 2>/dev/null || true)"
    # 所有容器都 running，且带健康检查的容器都是 healthy（没有健康检查的 Health 为空）
    if [[ -n "${state}" ]] && ! awk -F'|' '
        ($2 != "running") || ($3 != "" && $3 != "healthy") { bad = 1 }
        END { exit(bad ? 0 : 1) }
      ' <<<"${state}"; then
      return 0
    fi
    sleep 2
  done
  warn "还有容器没到 healthy，用 bin/infra.sh ps / logs 看原因"
  return 1
}

print_endpoints() {
  cat <<'EOF'

  Presto          http://localhost:8080          (SQL 即席查询)
  MinIO API       http://localhost:9000          (admin / password)
  MinIO 控制台    http://localhost:9001
  Hive Metastore  thrift://localhost:9083
  Postgres        localhost:5432                 (hive / hive, db=metastore)
  Adminer         http://localhost:8081          (仅在 up tools 时启动)

  下一步：
    cp conf/platform.properties.example conf/platform.properties
    MVN_PROFILE=s3 bin/dev-batch.sh --job all
    bin/presto-query.sh --sql "show tables from iceberg.lake"
    bin/lake-service.sh build && bin/lake-service.sh start

EOF
}

cmd_up() {
  need_docker
  local args=()
  if [[ "${1:-}" == "tools" || "${1:-}" == "--tools" ]]; then
    args=("${PROFILE_TOOLS[@]}")
    log "启动核心服务 + tools profile"
  else
    log "启动核心服务"
  fi
  "${COMPOSE[@]}" "${args[@]}" up -d --build
  wait_healthy || true
  "${COMPOSE[@]}" ps
  print_endpoints
}

cmd_doctor() {
  need_docker
  local failed=0

  echo
  log "1. Docker"
  if docker info >/dev/null 2>&1; then
    ok "daemon $(docker version --format '{{.Server.Version}}')"
  else
    bad "daemon 不可用"; return 1
  fi

  echo
  log "2. 容器状态"
  "${COMPOSE[@]}" ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'

  echo
  log "3. 端口 / HTTP"
  check_http() {
    local name="$1" url="$2"
    if curl -fsS --max-time 5 "${url}" >/dev/null 2>&1; then
      ok "${name}  ${url}"
    else
      bad "${name}  ${url}"
      failed=1
    fi
  }
  check_http "MinIO 健康"  "http://localhost:9000/minio/health/live"
  check_http "Presto 信息" "http://localhost:8080/v1/info"

  if timeout 3 bash -c 'exec 3<>/dev/tcp/127.0.0.1/9083' 2>/dev/null; then
    ok "HMS    127.0.0.1:9083"
  else
    bad "HMS    127.0.0.1:9083"; failed=1
  fi
  if "${COMPOSE[@]}" exec -T postgres pg_isready -U hive -d metastore >/dev/null 2>&1; then
    ok "Postgres metastore"
  else
    bad "Postgres metastore"; failed=1
  fi

  echo
  log "4. Presto 端到端"
  if "${COMPOSE[@]}" exec -T presto presto-cli \
      --server localhost:8080 --execute 'select 1' >/dev/null 2>&1; then
    ok "Presto 能执行查询"

    local tables
    if tables="$("${COMPOSE[@]}" exec -T presto presto-cli \
        --server localhost:8080 --catalog iceberg --schema lake \
        --execute 'show tables' 2>/dev/null)"; then
      if [[ -z "${tables//[[:space:]]/}" ]]; then
        warn "iceberg.lake 还是空的：先跑 MVN_PROFILE=s3 bin/dev-batch.sh --job all"
      else
        ok "iceberg.lake 表：$(tr '\n' ' ' <<<"${tables}")"
        if "${COMPOSE[@]}" exec -T presto presto-cli \
            --server localhost:8080 --execute \
            'select count(*) from iceberg.lake.silver_orders' >/dev/null 2>&1; then
          ok "Iceberg 表可读（silver_orders 查询通过）"
        else
          warn "Presto 能连 Iceberg，但 silver_orders 读不出来，看 logs presto"
        fi
      fi
    else
      warn "iceberg.lake schema 还不存在（正常，跑完 init 之后才会有）"
    fi
  else
    bad "Presto 执行查询失败"; failed=1
  fi

  echo
  if [[ "${failed}" == "0" ]]; then
    log "检查通过"
  else
    err "有检查项失败，先看：bin/infra.sh logs <服务名>"
    return 1
  fi
}

cmd="${1:-up}"
shift || true

case "${cmd}" in
  up)       cmd_up "$@" ;;
  down)     need_docker; "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" down ;;
  clean)
    need_docker
    warn "将删除 pgdata / miniodata / prestodata 三个数据卷"
    "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" down -v
    ;;
  ps|status) need_docker; "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" ps ;;
  logs)     need_docker; "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" logs -f --tail=200 "$@" ;;
  restart)  need_docker; "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" restart "$@" ;;
  psql)     need_docker; "${COMPOSE[@]}" exec postgres psql -U hive -d metastore "$@" ;;
  doctor)   cmd_doctor ;;
  -h|--help|help) usage ;;
  *) usage ;;
esac
