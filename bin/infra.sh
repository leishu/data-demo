#!/usr/bin/env bash
# 起/停本地湖仓基础设施：Postgres + MinIO + Hive Metastore + Presto
set -euo pipefail
cd "$(dirname "$0")/../infra"

case "${1:-up}" in
  up)
    docker compose up -d --build
    echo
    echo "Presto UI : http://localhost:8080"
    echo "MinIO UI  : http://localhost:9001  (admin/password)"
    echo "HMS       : thrift://localhost:9083"
    echo
    echo "下一步: bin/dev-batch.sh --job all --catalog-type hive --warehouse s3a://lake/warehouse"
    ;;
  down)
    docker compose down
    ;;
  clean)
    docker compose down -v
    ;;
  *)
    echo "用法: $0 [up|down|clean]" >&2
    exit 2
    ;;
esac
