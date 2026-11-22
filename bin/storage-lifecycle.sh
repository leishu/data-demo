#!/usr/bin/env bash
# 把 infra/storage/lifecycle.json 应用到对象存储 bucket（S3 / MinIO）。
#
# 前置：AWS CLI v2（支持 --endpoint-url）；凭据走 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY。
#   bin/storage-lifecycle.sh --dry-run
#   LAKE_S3_ENDPOINT=http://localhost:9000 LAKE_S3_BUCKET=lake \
#     AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=password \
#     bin/storage-lifecycle.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BUCKET="${LAKE_S3_BUCKET:-lake}"
ENDPOINT="${LAKE_S3_ENDPOINT:-}"
CONFIG="infra/storage/lifecycle.json"
DRY_RUN=0

usage() {
  cat <<'EOF'
用法: bin/storage-lifecycle.sh [选项]

  --bucket <name>    目标 bucket（默认 LAKE_S3_BUCKET=lake）
  --endpoint <url>   S3 兼容端点（默认 LAKE_S3_ENDPOINT，MinIO 如 http://localhost:9000）
  --config <path>    生命周期配置 JSON（默认 infra/storage/lifecycle.json）
  --dry-run          只打印要执行的命令与配置，不联网
  -h|--help          帮助

凭据从 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY 读取，不写进脚本。
EOF
  exit 2
}

die() { echo "错误: $*" >&2; exit 2; }

while (( $# )); do
  case "$1" in
    --bucket)   BUCKET="${2:-}"; shift 2 ;;
    --endpoint) ENDPOINT="${2:-}"; shift 2 ;;
    --config)   CONFIG="${2:-}"; shift 2 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    -h|--help)  usage ;;
    *) die "未知参数: $1" ;;
  esac
done

[[ -f "${CONFIG}" ]] || die "找不到生命周期配置: ${CONFIG}"
python3 -m json.tool "${CONFIG}" >/dev/null 2>&1 || die "配置不是合法 JSON: ${CONFIG}"
[[ -n "${BUCKET}" ]] || die "bucket 不能为空"

ENDPOINT_ARGS=()
if [[ -n "${ENDPOINT}" ]]; then
  ENDPOINT_ARGS=(--endpoint-url "${ENDPOINT}")
fi

if [[ "${DRY_RUN}" == "1" ]]; then
  echo "=== 生命周期配置（${CONFIG}）==="
  cat "${CONFIG}"
  echo
  echo "=== 将执行的命令（凭据已省略）==="
  printf 'aws %s s3api put-bucket-lifecycle-configuration --bucket %s --lifecycle-configuration file://%s\n' \
    "${ENDPOINT_ARGS[*]:-}" "${BUCKET}" "${CONFIG}"
  exit 0
fi

command -v aws >/dev/null 2>&1 || die "需要 AWS CLI v2"
[[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]] \
  || die "请设置 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY"

aws "${ENDPOINT_ARGS[@]}" s3api put-bucket-lifecycle-configuration \
  --bucket "${BUCKET}" \
  --lifecycle-configuration "file://${CONFIG}"
echo "已应用生命周期规则到 bucket=${BUCKET} endpoint=${ENDPOINT:-AWS 默认}"
echo "验证：aws ${ENDPOINT_ARGS[*]:-} s3api get-bucket-lifecycle-configuration --bucket ${BUCKET}"
