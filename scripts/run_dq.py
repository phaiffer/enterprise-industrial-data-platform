#!/usr/bin/env python3
"""Run Great Expectations checkpoint for Silver-layer data quality gates."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import great_expectations as gx

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.io import update_pipeline_metrics, update_stage_metrics, write_json
from src.common.paths import GE_DIR, REPORTS_DIR

CHECKPOINT_NAME = "silver_bechdel_checkpoint"


def run_checkpoint(raise_on_failure: bool = True) -> dict:
    context = gx.get_context(context_root_dir=str(GE_DIR))
    result = context.run_checkpoint(checkpoint_name=CHECKPOINT_NAME)
    context.build_data_docs()

    result_dict = result.to_json_dict()
    write_json(REPORTS_DIR / "dq_result_full.json", result_dict)

    unsuccessful_expectations = 0
    evaluated_expectations = 0

    for run_result in result_dict.get("run_results", {}).values():
        statistics = run_result.get("validation_result", {}).get("statistics", {})
        unsuccessful_expectations += int(statistics.get("unsuccessful_expectations", 0))
        evaluated_expectations += int(statistics.get("evaluated_expectations", 0))

    summary = {
        "checkpoint_name": CHECKPOINT_NAME,
        "success": bool(result_dict.get("success", False)),
        "evaluated_expectations": evaluated_expectations,
        "unsuccessful_expectations": unsuccessful_expectations,
    }

    write_json(REPORTS_DIR / "dq_result_summary.json", summary)
    update_stage_metrics("dq", summary)
    update_pipeline_metrics({"dq_pass": summary["success"]})

    if raise_on_failure and not summary["success"]:
        raise RuntimeError(json.dumps(summary, indent=2))

    return summary


def main() -> int:
    run_checkpoint(raise_on_failure=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
