#!/usr/bin/env bash
# lake-service 端到端冒烟：健康检查 / 鉴权 / 查询 / SSE / 语义层 / 资产目录 / 质量结果 / 血缘 / 保留审计 / 批作业 / 审计 / 只读守卫 / 指标抓取
#
#   bin/smoke.sh                         # 默认 http://localhost:8088，operator/operator
#   BASE_URL=http://host:8088 bin/smoke.sh
#   SMOKE_BATCH=0 bin/smoke.sh           # 服务关闭了批作业（容器模式）
#
# 依赖：curl、python3（只用来解析 JSON）。失败时非零退出，适合直接放进 CI。
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8088}"
SMOKE_USER="${SMOKE_USER:-operator}"
SMOKE_PASS="${SMOKE_PASS:-operator}"
SMOKE_BATCH="${SMOKE_BATCH:-1}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"

command -v curl >/dev/null 2>&1 || { echo "需要 curl" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "需要 python3（解析 JSON）" >&2; exit 2; }

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

PASS=0
FAIL=0

step() { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '  \033[1;32mOK\033[0m   %s\n' "$*"; PASS=$((PASS + 1)); }
bad()  { printf '  \033[1;31mFAIL\033[0m %s\n' "$*"; FAIL=$((FAIL + 1)); }

# 带 Basic 认证的请求；结果体写 ${TMP}/body，stdout 只输出 HTTP 状态码
req() {
  local method="$1" path="$2" body="${3:-}"
  shift 3
  local -a args=(
    -sS -o "${TMP}/body" -w '%{http_code}'
    -X "${method}"
    -u "${SMOKE_USER}:${SMOKE_PASS}"
    -H 'Accept: application/json'
    "$@"
  )
  if [[ -n "${body}" ]]; then
    args+=(-H 'Content-Type: application/json' -d "${body}")
  fi
  curl "${args[@]}" "${BASE_URL}${path}" || true
}

check_code() { # 描述 期望 实际
  if [[ "$2" == "$3" ]]; then
    ok "$1（HTTP $3）"
  else
    bad "$1（期望 HTTP $2，实际 $3）"
  fi
}

json_field() { # 表达式（如 id / run.status），从 stdin 读 JSON
  python3 -c '
import json, sys
value = json.load(sys.stdin)
for part in sys.argv[1].split("."):
    value = value.get(part, {}) if isinstance(value, dict) else {}
print(value if isinstance(value, (str, int, float)) else "")
' "$1" 2>/dev/null || true
}

step "0. 等待服务（${BASE_URL}）"
code=""
for ((i = 1; i <= WAIT_SECONDS; i++)); do
  code="$(curl -sS -o "${TMP}/health" -w '%{http_code}' "${BASE_URL}/actuator/health" 2>/dev/null || true)"
  [[ "${code}" == "200" ]] && break
  sleep 1
done
check_code "健康检查可访问" 200 "${code}"
if grep -q '"status":"UP"' "${TMP}/health" 2>/dev/null; then
  ok "健康状态 UP"
else
  bad "健康状态不是 UP：$(head -c 300 "${TMP}/health" 2>/dev/null || true)"
fi

step "1. 鉴权"
code="$(curl -sS -o "${TMP}/unauth" -w '%{http_code}' "${BASE_URL}/api/config" 2>/dev/null || true)"
check_code "未认证访问 /api/config 被拒" 401 "${code}"

step "2. 配置与表清单"
code="$(req GET /api/config "")"
check_code "GET /api/config" 200 "${code}"
if grep -q '"trinoUrl"' "${TMP}/body" && grep -q '"catalog"' "${TMP}/body"; then
  ok "配置回显包含 trinoUrl / catalog"
else
  bad "配置回显字段不完整"
fi

code="$(req GET /api/tables "")"
check_code "GET /api/tables" 200 "${code}"
if grep -q 'silver_orders' "${TMP}/body" && grep -q 'gold_daily_revenue' "${TMP}/body"; then
  ok "表清单包含 silver_orders / gold_daily_revenue"
else
  bad "表清单缺少业务表"
fi

code="$(req GET /api/governance/assets "")"
check_code "GET /api/governance/assets" 200 "${code}"
if grep -q '"owner"' "${TMP}/body" && grep -q '"sensitivityLabel"' "${TMP}/body" \
    && grep -q '"freshness"' "${TMP}/body" && grep -q 'silver_orders' "${TMP}/body"; then
  ok "治理资产含 owner / 敏感级别 / 新鲜度"
else
  bad "治理资产字段不完整：$(head -c 300 "${TMP}/body" 2>/dev/null || true)"
fi

code="$(req GET "/api/governance/dq?limit=5" "")"
check_code "GET /api/governance/dq" 200 "${code}"

code="$(req GET "/api/governance/lineage?table=gold_daily_revenue" "")"
check_code "GET /api/governance/lineage" 200 "${code}"
if grep -q '"upstream"' "${TMP}/body" && grep -q '"columns"' "${TMP}/body"; then
  ok "血缘接口含上游与列级声明"
else
  bad "血缘接口字段不完整：$(head -c 300 "${TMP}/body" 2>/dev/null || true)"
fi

code="$(req GET "/api/governance/retention?limit=5" "")"
check_code "GET /api/governance/retention" 200 "${code}"

step "3. 同步查询"
code="$(req POST /api/query '{"sql":"select count(*) as c from silver_orders","maxRows":10}')"
check_code "POST /api/query" 200 "${code}"
if grep -Eq '"rowCount":[1-9][0-9]*' "${TMP}/body"; then
  ok "查询返回了结果行"
else
  bad "查询结果 rowCount 为 0：$(head -c 300 "${TMP}/body" 2>/dev/null || true)"
fi

step "4. 流式查询（SSE）"
if curl -sS -N --max-time 20 -u "${SMOKE_USER}:${SMOKE_PASS}" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"select 1 as one","maxRows":10}' \
    "${BASE_URL}/api/query/stream" >"${TMP}/sse" 2>"${TMP}/sse.err"; then
  if grep -Eq '^event: ?done' "${TMP}/sse"; then
    ok "SSE 收到 done 事件"
  else
    bad "SSE 没有 done 事件：$(head -c 300 "${TMP}/sse" 2>/dev/null || true)"
  fi
else
  bad "SSE 请求失败：$(head -c 300 "${TMP}/sse.err" 2>/dev/null || true)"
fi

step "5. 语义层"
code="$(req POST /api/semantics/render '{"metric":"gmv","dimensions":["dt"],"limit":5}')"
check_code "POST /api/semantics/render" 200 "${code}"
if grep -q '"sql"' "${TMP}/body"; then
  ok "语义层生成了 SQL"
else
  bad "语义层没有返回 SQL"
fi

code="$(req POST /api/semantics/query '{"metric":"gmv","dimensions":["dt"],"limit":5}')"
check_code "POST /api/semantics/query" 200 "${code}"
if grep -Eq '"rowCount":[1-9][0-9]*' "${TMP}/body"; then
  ok "语义查询返回了结果行"
else
  bad "语义查询 rowCount 为 0"
fi

if [[ "${SMOKE_BATCH}" == "1" ]]; then
  step "6. 批作业（show）"
  code="$(req POST /api/batch/show "" -H "Idempotency-Key: smoke-$(date +%s)-$$")"
  check_code "POST /api/batch/show" 202 "${code}"
  run_id="$(json_field id <"${TMP}/body")"
  if [[ -n "${run_id}" ]]; then
    ok "已排队 runId=${run_id}"
    status=""
    for ((i = 1; i <= 90; i++)); do
      code="$(req GET "/api/batch/runs/${run_id}?tail=5" "")"
      status="$(json_field run.status <"${TMP}/body")"
      [[ "${status}" == "SUCCEEDED" || "${status}" == "FAILED" || "${status}" == "TIMEOUT" ]] && break
      sleep 2
    done
    if [[ "${status}" == "SUCCEEDED" ]]; then
      ok "show 作业执行成功"
    else
      bad "show 作业终态=${status:-未知}（runId=${run_id}）"
    fi
  else
    bad "提交响应里没有 runId"
  fi
else
  step "6. 批作业（跳过：SMOKE_BATCH=0）"
fi

step "7. 审计与只读守卫"
code="$(req GET "/api/audit?limit=20" "")"
check_code "GET /api/audit" 200 "${code}"
if grep -q 'query.execute' "${TMP}/body"; then
  ok "审计缓冲里能看到 query.execute"
else
  bad "审计缓冲里没有 query.execute"
fi

code="$(req POST /api/query '{"sql":"delete from silver_orders","maxRows":1}')"
check_code "写语句被只读守卫拒绝" 400 "${code}"

step "8. Prometheus 指标抓取"
code="$(curl -sS -o "${TMP}/prom" -w '%{http_code}' -u "${SMOKE_USER}:${SMOKE_PASS}" \
    "${BASE_URL}/actuator/prometheus" 2>/dev/null || true)"
check_code "GET /actuator/prometheus" 200 "${code}"
if grep -q '^lake_query_executed_total' "${TMP}/prom" 2>/dev/null; then
  ok "Prometheus 文本里包含业务指标 lake_query_executed_total"
else
  bad "Prometheus 文本里没有业务指标：$(head -c 300 "${TMP}/prom" 2>/dev/null || true)"
fi

echo
if ((FAIL == 0)); then
  printf '\033[1;32m冒烟通过：%d 项检查\033[0m\n' "${PASS}"
else
  printf '\033[1;31m冒烟失败：%d 通过 / %d 失败\033[0m\n' "${PASS}" "${FAIL}"
  exit 1
fi
