#!/usr/bin/env python3
"""Execute the notebook pipeline in a deterministic order with Papermill."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

import papermill as pm

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.io import update_pipeline_metrics, update_stage_metrics
from src.common.paths import EXECUTED_NOTEBOOKS_DIR, ensure_project_dirs

NOTEBOOK_ORDER = [
    "00_project_overview.ipynb",
    "01_data_sources.ipynb",
    "02_bronze_ingestion.ipynb",
    "03_silver_cleaning.ipynb",
    "04_dbt_marts_gold.ipynb",
    "05_data_quality.ipynb",
    "06_observability.ipynb",
    "07_portfolio_story.ipynb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all pipeline notebooks with Papermill")
    parser.add_argument("--source", default="fivethirtyeight")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["recent_grads", "bechdel_movies"],
        help="Dataset keys to process",
    )
    parser.add_argument("--run-date", default=date.today().isoformat())
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--kernel-name", default="python3")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_project_dirs()

    dataset_argument = ",".join(args.datasets)
    common_parameters = {
        "source": args.source,
        "dataset": dataset_argument,
        "run_date": args.run_date,
        "force_refresh": args.force_refresh,
    }

    update_pipeline_metrics(
        {
            "source": args.source,
            "datasets": args.datasets,
            "run_date": args.run_date,
        }
    )

    for notebook_name in NOTEBOOK_ORDER:
        input_path = ROOT_DIR / "notebooks" / notebook_name
        output_path = EXECUTED_NOTEBOOKS_DIR / notebook_name

        start = time.perf_counter()
        pm.execute_notebook(
            input_path=str(input_path),
            output_path=str(output_path),
            parameters=common_parameters,
            kernel_name=args.kernel_name,
            log_output=True,
        )
        runtime_seconds = round(time.perf_counter() - start, 2)

        update_stage_metrics(
            f"notebook::{notebook_name}",
            {"runtime_seconds": runtime_seconds, "status": "completed"},
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
