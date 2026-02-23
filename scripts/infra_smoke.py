#!/usr/bin/env python3
"""Trigger and validate the enterprise-mode Airflow smoke DAG."""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True)


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


def unpause_dag(compose_file: str, dag_id: str) -> None:
    command = [
        "docker",
        "compose",
        "-f",
        compose_file,
        "exec",
        "-T",
        "airflow-scheduler",
        "airflow",
        "dags",
        "unpause",
        dag_id,
    ]
    result = run_command(command)
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to unpause infra smoke DAG. "
            f"Error: {result.stderr.strip() or result.stdout.strip()}"
        )


def trigger_dag(compose_file: str, dag_id: str, retries: int = 15, retry_wait: int = 4) -> None:
    command = [
        "docker",
        "compose",
        "-f",
        compose_file,
        "exec",
        "-T",
        "airflow-scheduler",
        "airflow",
        "dags",
        "trigger",
        dag_id,
    ]

    for attempt in range(1, retries + 1):
        result = run_command(command)
        if result.returncode == 0:
            return
        if attempt == retries:
            raise RuntimeError(
                "Failed to trigger infra smoke DAG after retries. "
                f"Last error: {result.stderr.strip() or result.stdout.strip()}"
            )
        time.sleep(retry_wait)


def wait_for_artifact(artifact_path: Path, started_at: float, timeout_seconds: int = 180) -> str:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        if artifact_path.exists() and artifact_path.stat().st_mtime >= started_at:
            content = artifact_path.read_text(encoding="utf-8")
            if "infra_smoke_dag=success" in content:
                return content
        time.sleep(3)

    raise RuntimeError(
        f"Timed out waiting for smoke artifact at {artifact_path}. "
        "Check Airflow scheduler logs with `make infra-logs`."
    )


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
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    artifact_path = Path(args.artifact_path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = time.time()

    assert_scheduler_running(args.compose_file)
    unpause_dag(args.compose_file, args.dag_id)
    trigger_dag(args.compose_file, args.dag_id)
    content = wait_for_artifact(artifact_path, started_at)

    print("Infra smoke succeeded.")
    print(f"Artifact: {artifact_path}")
    print(content.strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
