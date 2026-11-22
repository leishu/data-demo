"""Iceberg 维护作业：rewrite_data_files / rewrite_manifests / expire_snapshots。

刻意和每日 ingest/transform 分开调度（默认每周日凌晨 4 点）：
维护会重写数据文件并清理快照，不适合跟写入作业串在同一条链上抢资源。
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="lakehouse_maintain",
    description="Iceberg 表维护（单独调度，不和 ingest/transform 串跑）",
    schedule="0 4 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=90),
    },
    tags=["lakehouse", "maintenance"],
) as dag:
    BashOperator(
        task_id="maintain",
        bash_command=(
            'cd "$LAKEHOUSE_HOME" && '
            'bin/scheduled-batch.sh --job maintain --date "{{ ds }}" --wait --timeout 5400'
        ),
    )
