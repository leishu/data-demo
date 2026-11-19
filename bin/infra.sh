#!/usr/bin/env bash
# 本地湖仓基础设施：起停 / 巡检 / 调试
#
#   bin/infra.sh up            # 起 Postgres + MinIO + HMS（默认，跑批够用）
#   bin/infra.sh up trino      # 额外起 Trino（即席查询才需要，常驻 ~0.8G）
#   bin/infra.sh up tools      # 额外起 Adminer（:8081），可和 trino 组合
#   bin/infra.sh stop [svc]    # 停指定服务（如 stop trino），不带参数停全部
#                                 # svc 取值：postgres / minio / hive-metastore / trino / adminer（脚本已自动带 --profile tools）
#   bin/infra.sh down          # 停容器，保留数据卷
#   bin/infra.sh clean         # 停容器并删数据卷（元数据、对象存储全清）
#   bin/infra.sh ps            # 容器与健康状态
#   bin/infra.sh logs [svc]    # 跟日志，默认全部
#   bin/infra.sh doctor        # 端口 / HTTP / Trino 端到端检查
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

  up [trino|tools|all]  起 Postgres + MinIO + HMS；带 trino（或 all）额外起 Trino，带 tools 额外起 Adminer
  core              同 up（保留兼容：默认就不起 Trino）
  stop [服务...]    停容器（默认全部，与 up 对称），容器和数据卷都保留
  down              停容器，保留数据卷
  clean             停容器并删除数据卷（不可恢复）
  ps                容器与健康状态
  logs [服务...]    跟日志（默认全部）
  doctor            端口 / HTTP / 端到端检查（Trino 未起时自动跳过它那几项）
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

# 可选参数：要检查的 compose 服务名（不传 = compose ps 看到的全部服务）
wait_healthy() {
  local tries=150 i state
  local -a svcs=("$@")
  log "等待容器 healthy（最多 $((tries * 2)) 秒）"
  for ((i = 1; i <= tries; i++)); do
    state="$(docker compose ps --format '{{.Name}}|{{.State}}|{{.Health}}' "${svcs[@]}" 2>/dev/null || true)"
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
  # 参数：1 = 已起 Trino；不传/0 = 三件套模式
  if [[ "${1:-0}" == "1" ]]; then
    cat <<'EOF'

  Trino           http://localhost:8080          (SQL 即席查询)
  MinIO API       http://localhost:9000          (admin / password)
  MinIO 控制台    http://localhost:9001
  Hive Metastore  thrift://localhost:9083
  Postgres        localhost:5432                 (hive / hive, db=metastore)
  Adminer         http://localhost:8081          (仅在带 tools 时启动)

  下一步：
    cp conf/platform.properties.example conf/platform.properties
    MVN_PROFILE=s3 bin/dev-batch.sh --job all
    bin/trino-query.sh --sql "show tables from iceberg.lake"
    bin/lake-service.sh build && bin/lake-service.sh start

EOF
    return
  fi
  cat <<'EOF'

  MinIO API       http://localhost:9000          (admin / password)
  MinIO 控制台    http://localhost:9001
  Hive Metastore  thrift://localhost:9083
  Postgres        localhost:5432                 (hive / hive, db=metastore)
  Adminer         http://localhost:8081          (仅在带 tools 时启动)

  下一步：
    cp conf/platform.properties.example conf/platform.properties
    MVN_PROFILE=s3 bin/dev-batch.sh --job all
    bin/lake-service.sh build && bin/lake-service.sh start

  要查 SQL 时再补起 Trino（常驻 ~0.8G，不用时 bin/infra.sh stop trino）：
    bin/infra.sh up trino

EOF
}

# 默认只起 Postgres + MinIO + HMS：日常跑批够用。Trino 只在做 SQL 查询时需要，
# 常驻要 0.8G，所以改成按需拉起。
#   bin/infra.sh up              三件套
#   bin/infra.sh up trino        三件套 + Trino（all 等价）
#   bin/infra.sh up tools        三件套 + Adminer
#   bin/infra.sh up tools trino  三件套 + Adminer + Trino
cmd_up() {
  need_docker
  local with_tools=0 with_trino=0 arg label
  local -a args=() svcs=(postgres minio hive-metastore)
  for arg in "$@"; do
    case "${arg}" in
      tools|--tools) with_tools=1 ;;
      trino|query|all) with_trino=1 ;;
      *) warn "忽略未知参数：${arg}" ;;
    esac
  done
  if [[ "${with_tools}" == "1" ]]; then
    args=("${PROFILE_TOOLS[@]}")
  fi
  label="Postgres + MinIO + HMS"
  if [[ "${with_tools}" == "1" ]]; then label+=" + Adminer"; fi
  if [[ "${with_trino}" == "1" ]]; then
    svcs+=(trino)
    label+=" + Trino"
  fi
  log "启动 ${label}"
  "${COMPOSE[@]}" "${args[@]}" up -d --build "${svcs[@]}"
  wait_healthy "${svcs[@]}" || true
  "${COMPOSE[@]}" ps
  print_endpoints "${with_trino}"
}

# 保留旧命令名：up 默认行为已经就是 core
cmd_core() {
  cmd_up
}

# 只停不停删：和 up 对称，日常关 Trino 省 ~0.8G 用它（容器还在，docker start 秒起）
cmd_stop() {
  need_docker
  "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" stop "$@"
  # 用 -a：停掉的服务仍在列表里，能直接看出是“停了”而不是“没了”
  "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" ps -a
}

cmd_doctor() {
  need_docker
  local failed=0 trino_up=0

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
  # 默认模式故意不起 Trino，这里不能算失败
  if docker ps --format '{{.Names}}' | grep -qx lake-trino; then
    trino_up=1
    check_http "Trino 信息" "http://localhost:8080/v1/info"
  else
    warn "Trino 未启动（默认模式），跳过 Trino 相关检查"
  fi

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
  log "4. Trino 端到端"
  if [[ "${trino_up}" != "1" ]]; then
    warn "Trino 未启动，跳过端到端查询（bin/infra.sh up trino 可补起）"
  elif "${COMPOSE[@]}" exec -T trino trino \
      --server localhost:8080 --execute 'select 1' >/dev/null 2>&1; then
    ok "Trino 能执行查询"

    local tables
    if tables="$("${COMPOSE[@]}" exec -T trino trino \
        --server localhost:8080 --catalog iceberg --schema lake \
        --execute 'show tables' 2>/dev/null)"; then
      if [[ -z "${tables//[[:space:]]/}" ]]; then
        warn "iceberg.lake 还是空的：先跑 MVN_PROFILE=s3 bin/dev-batch.sh --job all"
      else
        ok "iceberg.lake 表：$(tr '\n' ' ' <<<"${tables}")"
        if "${COMPOSE[@]}" exec -T trino trino \
            --server localhost:8080 --execute \
            'select count(*) from iceberg.lake.silver_orders' >/dev/null 2>&1; then
          ok "Iceberg 表可读（silver_orders 查询通过）"
        else
          warn "Trino 能连 Iceberg，但 silver_orders 读不出来，看 logs trino"
        fi
      fi
    else
      warn "iceberg.lake schema 还不存在（正常，跑完 init 之后才会有）"
    fi
  else
    bad "Trino 执行查询失败"; failed=1
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
  core)     cmd_core ;;
  stop)     cmd_stop "$@" ;;
  down)     need_docker; "${COMPOSE[@]}" "${PROFILE_TOOLS[@]}" down ;;
  clean)
    need_docker
    warn "将删除 pgdata / miniodata / trinodata 三个数据卷"
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
