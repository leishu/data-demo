#!/usr/bin/env bash
# DolphinScheduler 的 SHELL 任务入口：参数 = 作业名 + 业务日期。
#
# 在 DS 的任务定义里可以这样传参（两种都可）：
#   ./scheduler/dolphinscheduler/run_lake_job.sh ingest ${bizdate}
#   ./scheduler/dolphinscheduler/run_lake_job.sh maintain $(date -u +%F)
#
# 工作目录由脚本自己切到仓库根目录，不必在 DS 里额外配置。
set -euo pipefail

JOB="${1:-}"
DATE="${2:-$(date -u +%F)}"
if [[ -z "${JOB}" ]]; then
  echo "用法: run_lake_job.sh <job> [YYYY-MM-DD]" >&2
  exit 2
fi

cd "$(dirname "$0")/../.."
exec bin/scheduled-batch.sh --job "${JOB}" --date "${DATE}" --wait
