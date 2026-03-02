#!/usr/bin/env python3
"""Trigger and validate enterprise-mode infrastructure smoke checks."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True)


def compose_exec(compose_file: str, service: str, args: list[str]) -> subprocess.CompletedProcess[str]:
    return run_command(["docker", "compose", "-f", compose_file, "exec", "-T", service, *args])


def assert_scheduler_running(compose_file: str) -> None:
    command = [
        "docker",
        "compose",
        "-f",
        compose_file,
        "ps",
        "--services",
        "--status",
        "running",
        "airflow-scheduler",
    ]
    result = run_command(command)
    if result.returncode != 0 or "airflow-scheduler" not in result.stdout:
        raise RuntimeError(
            "airflow-scheduler is not running. Start the stack first with `make infra-up`."
        )


def ensure_kafka_topic_exists(compose_file: str, topic: str) -> dict[str, Any]:
    create_result = compose_exec(
        compose_file,
        "kafka",
        [
            "kafka-topics",
            "--bootstrap-server",
            "kafka:29092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
    )
    if create_result.returncode != 0:
        raise RuntimeError(
            "Failed to create/validate Kafka topic. "
            f"Error: {create_result.stderr.strip() or create_result.stdout.strip()}"
        )

    list_result = compose_exec(
        compose_file,
        "kafka",
        ["kafka-topics", "--bootstrap-server", "kafka:29092", "--list"],
    )
    if list_result.returncode != 0:
        raise RuntimeError(
            "Failed to list Kafka topics. "
            f"Error: {list_result.stderr.strip() or list_result.stdout.strip()}"
        )

    topics = {line.strip() for line in list_result.stdout.splitlines() if line.strip()}
    if topic not in topics:
        raise RuntimeError(f"Kafka topic '{topic}' was not found after creation step.")

    return {
        "topic": topic,
        "topic_count": len(topics),
        "list_stdout": list_result.stdout.strip(),
    }


def assert_minio_bucket_accessible(compose_file: str, bucket: str) -> dict[str, Any]:
    result = compose_exec(
        compose_file,
        "minio",
        ["/bin/sh", "-lc", f"test -d /data/{bucket} && ls -ld /data/{bucket}"],
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"MinIO bucket '{bucket}' is not accessible from the MinIO container. "
            f"Error: {result.stderr.strip() or result.stdout.strip()}"
        )

    return {
        "bucket": bucket,
        "access_check": result.stdout.strip(),
    }


def run_spark_smoke_job(compose_file: str) -> dict[str, Any]:
    spark_script = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://spark-master:7077").appName("infra_smoke").getOrCreate()
count = spark.range(0, 10).count()
print(f"SPARK_SMOKE_COUNT={count}")
spark.stop()
""".strip()

    result = compose_exec(
        compose_file,
        "spark-submit",
        ["/bin/bash", "-lc", f"python - <<'PY'\n{spark_script}\nPY"],
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Spark smoke job failed. "
            f"Error: {result.stderr.strip() or result.stdout.strip()}"
        )

    combined_output = "\n".join([result.stdout, result.stderr])
    match = re.search(r"SPARK_SMOKE_COUNT=(\d+)", combined_output)
    if not match:
        raise RuntimeError("Spark smoke job completed but did not emit SPARK_SMOKE_COUNT marker.")

    return {
        "count": int(match.group(1)),
        "stdout_tail": "\n".join(result.stdout.splitlines()[-10:]).strip(),
    }


def unpause_dag(compose_file: str, dag_id: str) -> None:
    result = compose_exec(compose_file, "airflow-scheduler", ["airflow", "dags", "unpause", dag_id])
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to unpause infra smoke DAG. "
            f"Error: {result.stderr.strip() or result.stdout.strip()}"
        )


def trigger_dag(
    compose_file: str,
    dag_id: str,
    run_id: str,
    retries: int = 15,
    retry_wait: int = 4,
) -> None:
    command = [
        "airflow-scheduler",
        "airflow",
        "dags",
        "trigger",
        "-r",
        run_id,
        dag_id,
    ]

    for attempt in range(1, retries + 1):
        result = compose_exec(compose_file, command[0], command[1:])
        if result.returncode == 0:
            return
        if attempt == retries:
            raise RuntimeError(
                "Failed to trigger infra smoke DAG after retries. "
                f"Last error: {result.stderr.strip() or result.stdout.strip()}"
            )
        time.sleep(retry_wait)


def parse_key_value_block(content: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in content.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            parsed[key.strip()] = value.strip()
    return parsed


def wait_for_artifact(
    artifact_path: Path,
    expected_run_id: str,
    started_at: float,
    timeout_seconds: int = 180,
) -> dict[str, str]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        if artifact_path.exists() and artifact_path.stat().st_mtime >= started_at:
            content = artifact_path.read_text(encoding="utf-8")
            parsed = parse_key_value_block(content)
            if (
                parsed.get("infra_smoke_dag") == "success"
                and parsed.get("run_id") == expected_run_id
            ):
                return parsed
        time.sleep(3)

    raise RuntimeError(
        f"Timed out waiting for smoke artifact at {artifact_path}. "
        "Check Airflow scheduler logs with `make infra-logs`."
    )


def fetch_dag_run_state(compose_file: str, dag_id: str, run_id: str) -> str:
    sql = (
        "select state "
        "from dag_run "
        f"where dag_id = '{dag_id}' and run_id = '{run_id}' "
        "order by execution_date desc limit 1;"
    )
    result = compose_exec(
        compose_file,
        "airflow-db",
        ["/bin/bash", "-lc", f'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "{sql}"'],
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to query Airflow DAG run state from airflow-db. "
            f"Error: {result.stderr.strip() or result.stdout.strip()}"
        )

    states = [line.strip().lower() for line in result.stdout.splitlines() if line.strip()]
    return states[-1] if states else ""


def wait_for_dag_success(
    compose_file: str, dag_id: str, run_id: str, timeout_seconds: int = 180
) -> dict[str, str]:
    deadline = time.time() + timeout_seconds
    last_state = ""

    while time.time() < deadline:
        state = fetch_dag_run_state(compose_file, dag_id, run_id)
        if state:
            last_state = state
        if state == "success":
            return {"state": state, "run_id": run_id}
        if state == "failed":
            raise RuntimeError(f"Airflow DAG run failed for run_id='{run_id}'.")
        time.sleep(3)

    raise RuntimeError(
        "Timed out waiting for Airflow DAG run to succeed. "
        f"dag_id='{dag_id}', run_id='{run_id}', last_state='{last_state or 'not_found'}'."
    )


def write_result(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run enterprise mode smoke test")
    parser.add_argument(
        "--compose-file",
        required=True,
        help="Path to the enterprise docker compose file",
    )
    parser.add_argument(
        "--artifact-path",
        default="reports/infra_smoke/ok.txt",
        help="Host path where smoke artifact should be written",
    )
    parser.add_argument(
        "--dag-id",
        default="infra_smoke_dag",
        help="Airflow DAG id to trigger",
    )
    parser.add_argument(
        "--kafka-topic",
        default="sensor-data",
        help="Kafka topic that must exist for smoke validation",
    )
    parser.add_argument(
        "--minio-bucket",
        default="eidp",
        help="MinIO bucket that must be accessible for smoke validation",
    )
    parser.add_argument(
        "--result-path",
        default="reports/infra_smoke_result.json",
        help="JSON file path where structured smoke results will be written",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    artifact_path = Path(args.artifact_path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    result_path = Path(args.result_path)
    started_at = time.time()
    run_id = f"manual__infra_smoke__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    result_payload: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "compose_file": args.compose_file,
        "dag_id": args.dag_id,
        "kafka_topic": args.kafka_topic,
        "minio_bucket": args.minio_bucket,
        "artifact_path": str(artifact_path),
        "checks": {},
    }

    try:
        assert_scheduler_running(args.compose_file)
        result_payload["checks"]["airflow_scheduler_running"] = {"success": True}

        kafka_details = ensure_kafka_topic_exists(args.compose_file, args.kafka_topic)
        result_payload["checks"]["kafka_topic_exists"] = {"success": True, **kafka_details}

        minio_details = assert_minio_bucket_accessible(args.compose_file, args.minio_bucket)
        result_payload["checks"]["minio_bucket_accessible"] = {"success": True, **minio_details}

        spark_details = run_spark_smoke_job(args.compose_file)
        result_payload["checks"]["spark_job_executed"] = {"success": True, **spark_details}

        unpause_dag(args.compose_file, args.dag_id)
        trigger_dag(args.compose_file, args.dag_id, run_id)

        artifact_details = wait_for_artifact(artifact_path, run_id, started_at)
        result_payload["checks"]["airflow_artifact_written"] = {
            "success": True,
            **artifact_details,
        }

        dag_details = wait_for_dag_success(args.compose_file, args.dag_id, run_id)
        result_payload["checks"]["airflow_dag_succeeded"] = {"success": True, **dag_details}

        result_payload["success"] = True
        write_result(result_path, result_payload)

        print("Infra smoke succeeded.")
        print(f"Artifact: {artifact_path}")
        print(f"Structured result: {result_path}")
        return 0
    except Exception as exc:
        result_payload["success"] = False
        result_payload["error"] = str(exc)
        write_result(result_path, result_payload)
        print(f"Infra smoke failed. Details written to: {result_path}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
