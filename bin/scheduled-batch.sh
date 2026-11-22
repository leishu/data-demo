#!/usr/bin/env bash
# 外部调度器（Airflow / DolphinScheduler / crontab）触发 lake-service 批作业。
#
# 约定：一次逻辑运行 = (job, 逻辑日期)，用确定性 Idempotency-Key 去重；
# 默认等到终态再退出，退出码可直接被调度器当任务状态：0 成功 / 1 作业失败 / 2 参数或配置错误。
#
#   bin/scheduled-batch.sh --job ingest --date 2026-11-22 --wait --timeout 3600
#   bin/scheduled-batch.sh --job maintain --date '{{ ds }}' --wait   # Airflow 传入逻辑日期
#   bin/scheduled-batch.sh --job show --dry-run                      # 只打印将发出的请求，不联网
#
# 环境变量：LAKE_BASE_URL / LAKE_SCHED_USER / LAKE_SCHED_PASSWORD / LAKE_SCHED_POLL
set -euo pipefail
cd "$(dirname "$0")/.."

BASE_URL="${LAKE_BASE_URL:-http://localhost:8088}"
SCHED_USER="${LAKE_SCHED_USER:-operator}"
SCHED_PASS="${LAKE_SCHED_PASSWORD:-operator}"
POLL_SECONDS="${LAKE_SCHED_POLL:-5}"

JOB=""
LOGICAL_DATE="$(date -u +%F)"
WAIT=1
TIMEOUT=1800
DRY_RUN=0

usage() {
  cat <<'EOF'
用法: bin/scheduled-batch.sh --job <job> [选项]

  --job <job>          作业名（白名单：init / ingest / transform / maintain / show ...）
  --date <YYYY-MM-DD>  逻辑日期，用于生成幂等键；默认今天（UTC）
  --wait               等到终态再退出（默认）
  --no-wait            提交后立即返回 0，不等待
  --timeout <秒>       等终态的最长秒数（默认 1800）
  --poll <秒>          查询间隔（默认 5，也可用 LAKE_SCHED_POLL）
  --dry-run            只打印请求摘要，不联网

环境变量：LAKE_BASE_URL / LAKE_SCHED_USER / LAKE_SCHED_PASSWORD

退出码：0 成功；1 提交失败或作业 FAILED/TIMEOUT；2 参数错误
EOF
  exit 2
}

die() { echo "错误: $*" >&2; exit 2; }

while (( $# )); do
  case "$1" in
    --job)      JOB="${2:-}"; shift 2 ;;
    --date)     LOGICAL_DATE="${2:-}"; shift 2 ;;
    --wait)     WAIT=1; shift ;;
    --no-wait)  WAIT=0; shift ;;
    --timeout)  TIMEOUT="${2:-}"; shift 2 ;;
    --poll)     POLL_SECONDS="${2:-}"; shift 2 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    -h|--help)  usage ;;
    *) die "未知参数: $1" ;;
  esac
done

[[ -n "${JOB}" ]] || die "缺少 --job"
[[ "${JOB}" =~ ^[a-zA-Z0-9_-]+$ ]] || die "非法作业名: ${JOB}"
[[ "${LOGICAL_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "非法逻辑日期（要 YYYY-MM-DD）: ${LOGICAL_DATE}"
[[ "${TIMEOUT}" =~ ^[1-9][0-9]*$ ]] || die "非法超时: ${TIMEOUT}"
[[ "${POLL_SECONDS}" =~ ^[1-9][0-9]*$ ]] || die "非法轮询间隔: ${POLL_SECONDS}"

IDEM_KEY="lake-${JOB}-${LOGICAL_DATE}"
URL="${BASE_URL%/}/api/batch/${JOB}"

if [[ "${DRY_RUN}" == "1" ]]; then
  cat <<EOF
job=${JOB}
logical_date=${LOGICAL_DATE}
idempotency_key=${IDEM_KEY}
url=${URL}
wait=${WAIT} timeout=${TIMEOUT}s poll=${POLL_SECONDS}s
EOF
  exit 0
fi

command -v curl >/dev/null 2>&1 || die "需要 curl"
command -v python3 >/dev/null 2>&1 || die "需要 python3（解析 JSON）"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

# 提交：带 Idempotency-Key，同一 (job, date) 重试不会重复跑
code="$(curl -sS -o "${TMP}/submit.json" -w '%{http_code}' \
    -u "${SCHED_USER}:${SCHED_PASS}" -X POST \
    -H 'Accept: application/json' \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${IDEM_KEY}" \
    "${URL}" 2>"${TMP}/submit.err" || true)"
if [[ "${code}" != "202" ]]; then
  echo "提交失败（HTTP ${code}，key=${IDEM_KEY}）: $(cat "${TMP}/submit.json" "${TMP}/submit.err" 2>/dev/null | head -c 500)" >&2
  exit 1
fi

run_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("id", ""))' <"${TMP}/submit.json" 2>/dev/null || true)"
if [[ -z "${run_id}" ]]; then
  echo "提交成功但响应里没有 runId: $(head -c 500 "${TMP}/submit.json")" >&2
  exit 1
fi
echo "已提交 job=${JOB} date=${LOGICAL_DATE} runId=${run_id} key=${IDEM_KEY}"

if [[ "${WAIT}" == "0" ]]; then
  exit 0
fi

deadline=$(( $(date +%s) + TIMEOUT ))
while :; do
  code="$(curl -sS -o "${TMP}/run.json" -w '%{http_code}' \
      -u "${SCHED_USER}:${SCHED_PASS}" -H 'Accept: application/json' \
      "${BASE_URL%/}/api/batch/runs/${run_id}?tail=20" 2>"${TMP}/run.err" || true)"
  if [[ "${code}" != "200" ]]; then
    echo "查询运行状态失败（HTTP ${code}）: $(head -c 500 "${TMP}/run.json" "${TMP}/run.err" 2>/dev/null)" >&2
    exit 1
  fi
  status="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("run", {}).get("status", ""))' <"${TMP}/run.json" 2>/dev/null || true)"
  case "${status}" in
    SUCCEEDED)
      echo "作业成功 runId=${run_id}"
      exit 0
      ;;
    FAILED|TIMEOUT)
      echo "作业${status} runId=${run_id}" >&2
      python3 -c 'import json,sys; d=json.load(sys.stdin); print("message:", d.get("run",{}).get("message","")); print("--- 日志尾部 ---"); print("\n".join(d.get("tail", [])))' <"${TMP}/run.json" >&2 || true
      exit 1
      ;;
  esac
  if (( $(date +%s) >= deadline )); then
    echo "等待终态超时（${TIMEOUT}s）runId=${run_id} status=${status}" >&2
    exit 1
  fi
  sleep "${POLL_SECONDS}"
done
