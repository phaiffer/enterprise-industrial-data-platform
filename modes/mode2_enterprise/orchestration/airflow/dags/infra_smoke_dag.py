from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

ARTIFACT_PATH = Path("/opt/airflow/reports/infra_smoke/ok.txt")


def write_infra_smoke_artifact(**context) -> None:
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)

    run_id = context["run_id"]
    executed_at = datetime.now(timezone.utc).isoformat()

    ARTIFACT_PATH.write_text(
        (
            "infra_smoke_dag=success\n"
            f"run_id={run_id}\n"
            f"timestamp_utc={executed_at}\n"
            "message=Mode 2 enterprise stack is alive.\n"
        ),
        encoding="utf-8",
    )


def print_infra_smoke_artifact() -> None:
    if not ARTIFACT_PATH.exists():
        raise FileNotFoundError(f"Smoke artifact not found: {ARTIFACT_PATH}")
    print(f"Artifact path: {ARTIFACT_PATH}")
    print(ARTIFACT_PATH.read_text(encoding='utf-8').strip())


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

    print_artifact = PythonOperator(
        task_id="print_infra_smoke_artifact",
        python_callable=print_infra_smoke_artifact,
    )

    write_artifact >> print_artifact
