#!/usr/bin/env python3
"""Publish a small, verified Mode 1 snapshot for the local showcase."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
EXPORTS = ROOT / "docs" / "portfolio" / "exports"
EVIDENCE = ROOT / "showcase" / "static" / "evidence"
RUN_RESULTS = ROOT / "dbt" / "lakehouse_dbt" / "target" / "run_results.json"
DBT_MANIFEST = ROOT / "dbt" / "lakehouse_dbt" / "target" / "manifest.json"
PIPELINE_METRICS = ROOT / "reports" / "metrics" / "pipeline_metrics.json"
DQ_FULL = ROOT / "reports" / "dq_result_full.json"
DQ_SUMMARY = ROOT / "reports" / "dq_result_summary.json"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def main() -> int:
    run_id = (EXPORTS / "latest.txt").read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid portfolio run ID")

    export_dir = EXPORTS / run_id
    source_manifest = read_json(export_dir / "manifests" / "run.json")
    dbt_results = read_json(RUN_RESULTS)
    dbt_manifest = read_json(DBT_MANIFEST)
    pipeline_metrics = read_json(PIPELINE_METRICS)
    dq_full = read_json(DQ_FULL)
    dq_summary = read_json(DQ_SUMMARY)

    pipeline_started = parse_time(pipeline_metrics["pipeline_started_at"])
    dbt_finished = parse_time(dbt_results["metadata"]["generated_at"])
    dq_finished = parse_time(dq_full["run_id"]["run_time"])
    export_finished = parse_time(source_manifest["finished_at"])
    if not (
        pipeline_started <= dbt_finished <= export_finished
        and pipeline_started <= dq_finished <= export_finished
    ):
        raise ValueError("Pipeline, dbt test, DQ and export artifacts belong to different runs")

    for layer in ("bronze", "silver", "gold"):
        expected = pipeline_metrics[f"rows_{layer}"]
        observed = source_manifest["row_counts"][layer]
        if expected != observed:
            raise ValueError(f"Row counts disagree for {layer}: {expected} != {observed}")

    tests = [
        result
        for result in dbt_results["results"]
        if str(result.get("unique_id", "")).startswith("test.")
    ]
    passed = sum(str(result.get("status", "")).lower() == "pass" for result in tests)
    quality = source_manifest["dq_summary"]
    if not tests or passed != len(tests) or quality["rules_checked"] < 1:
        raise ValueError("Validated dbt and Great Expectations results are required")
    if quality["violations_count"]:
        raise ValueError("Great Expectations reported violations")
    if not dq_summary.get("success") or (
        dq_summary["evaluated_expectations"] != quality["rules_checked"]
        or dq_summary["unsuccessful_expectations"] != quality["violations_count"]
    ):
        raise ValueError("Great Expectations summary disagrees with portfolio export")

    models_by_layer: dict[str, list[str]] = {"Silver": [], "Gold": []}
    for node in dbt_manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        path = str(node.get("original_file_path", ""))
        layer = "Silver" if "/staging/" in f"/{path}" else "Gold"
        models_by_layer[layer].append(str(node["name"]))
    for names in models_by_layer.values():
        names.sort()
    if sum(map(len, models_by_layer.values())) < 1:
        raise ValueError("dbt model manifest is empty")

    charts = {
        "bechdel_pass_rate_by_year.png": "business_kpi_pass_rate_trend.png",
        "quality_checks.png": "quality_violations_by_category.png",
    }
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    image_hashes: dict[str, str] = {}
    for published_name, source_name in charts.items():
        source = export_dir / "charts" / source_name
        destination = EVIDENCE / published_name
        shutil.copyfile(source, destination)
        image_hashes[published_name] = hashlib.sha256(destination.read_bytes()).hexdigest()

    payload = {
        "kind": "technical_demo_snapshot",
        "run_id": run_id,
        "generated_at_utc": source_manifest["finished_at"],
        "git_sha": source_manifest["git_sha"],
        "input_sources": source_manifest["input_sources_used"],
        "input_datasets": source_manifest["input_datasets_used"],
        "row_counts": source_manifest["row_counts"],
        "dbt_tests": {"passed": passed, "total": len(tests)},
        "dbt_models_by_layer": models_by_layer,
        "great_expectations": {
            "rules_checked": quality["rules_checked"],
            "violations": quality["violations_count"],
        },
        "images_sha256": image_hashes,
        "note": "Historical public datasets; local demonstration only, not a client or production result.",
    }
    (EVIDENCE / "manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Showcase evidence prepared from {run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
