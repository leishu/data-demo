"""每日湖仓任务：ingest -> transform。

作业不在 DAG 里直接跑 Spark，而是交给 lake-service 编排：
bin/scheduled-batch.sh 用 (job, 逻辑日期) 生成幂等键，同一天重试不会产生重复运行。
需要维护作业单独走 dag_lakehouse_maintain.py，不挂在这条链上。

Worker 需要这些环境变量（也可用 Airflow Connections / Variables 注入）：
  LAKEHOUSE_HOME      仓库根目录
  LAKE_BASE_URL       lake-service 地址，默认 http://localhost:8088
  LAKE_SCHED_USER     默认 operator
  LAKE_SCHED_PASSWORD 默认 operator
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

LAKE_CMD = (
    'cd "$LAKEHOUSE_HOME" && '
    'bin/scheduled-batch.sh --job %s --date "{{ ds }}" --wait --timeout 3600'
)

with DAG(
    dag_id="lakehouse_daily",
    description="湖仓每日 ingest -> transform（经 lake-service 编排）",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "lakehouse",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=70),
    },
    tags=["lakehouse", "batch"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command=LAKE_CMD % "ingest",
    )
    transform = BashOperator(
        task_id="transform",
        bash_command=LAKE_CMD % "transform",
    )
    ingest >> transform
