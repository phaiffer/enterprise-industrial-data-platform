from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


def write_infra_smoke_artifact(**context) -> None:
    output_dir = Path("/opt/eidp/reports/infra_smoke")
    output_dir.mkdir(parents=True, exist_ok=True)

    run_id = context["run_id"]
    executed_at = datetime.now(timezone.utc).isoformat()

    output_path = output_dir / "ok.txt"
    output_path.write_text(
        (
            "infra_smoke_dag=success\n"
            f"run_id={run_id}\n"
            f"executed_at={executed_at}\n"
        ),
        encoding="utf-8",
    )


with DAG(
    dag_id="infra_smoke_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["eidp", "smoke", "infra"],
) as dag:
    write_artifact = PythonOperator(
        task_id="write_infra_smoke_artifact",
        python_callable=write_infra_smoke_artifact,
    )

    write_artifact
