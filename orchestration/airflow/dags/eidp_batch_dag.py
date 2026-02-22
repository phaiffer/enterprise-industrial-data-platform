from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="eidp_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["eidp", "batch"],
) as dag:
    silver = BashOperator(
        task_id="silver_transform",
        bash_command="""
        docker compose exec -T spark-submit /opt/bitnami/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
          /opt/eidp/src/batch/transform_to_silver.py
        """.strip(),
    )

    gold = BashOperator(
        task_id="gold_aggregate",
        bash_command="""
        docker compose exec -T spark-submit /opt/bitnami/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
          /opt/eidp/src/batch/aggregate_to_gold.py
        """.strip(),
    )

    silver >> gold
