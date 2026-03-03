"""Utility helpers for the showcase visualization layer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "dbt" / "lakehouse_dbt" / "target" / "manifest.json"
RUN_RESULTS_PATH = PROJECT_ROOT / "dbt" / "lakehouse_dbt" / "target" / "run_results.json"
REPORTS_DIR = PROJECT_ROOT / "reports"
GE_DIR = PROJECT_ROOT / "great_expectations"
PIPELINE_METRICS_PATH = REPORTS_DIR / "metrics" / "pipeline_metrics.json"
DQ_SUMMARY_PATH = REPORTS_DIR / "dq_result_summary.json"
DQ_FULL_PATH = REPORTS_DIR / "dq_result_full.json"

LAYER_ORDER = ["Raw", "Bronze", "Silver", "Gold", "Other"]


def load_json(path: Path) -> dict[str, Any] | None:
    """Load JSON file safely and return None when unavailable or invalid."""
    if not path.exists() or not path.is_file():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def artifact_notice(path: Path) -> str | None:
    """Return placeholder message when an artifact is missing."""
    if path.exists():
        return None
    return f"Artifact not generated yet: {path.relative_to(PROJECT_ROOT)}"


def format_timestamp(value: str | None) -> str:
    """Convert an ISO-8601 timestamp to a UI friendly UTC label."""
    if not value:
        return "Artifact not generated yet"

    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return value


def _safe_int(value: Any) -> int | None:
    """Parse an integer-like value and return None when impossible."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def classify_layer(model_node: dict[str, Any]) -> str:
    """Map a dbt model to medallion layers using path and naming conventions."""
    searchable_parts = [
        model_node.get("path", ""),
        model_node.get("original_file_path", ""),
        model_node.get("schema", ""),
        model_node.get("name", ""),
        " ".join(model_node.get("fqn", [])),
        " ".join(model_node.get("tags", [])),
    ]
    blob = " ".join(str(part).lower() for part in searchable_parts if part)

    if "raw" in blob:
        return "Raw"
    if "bronze" in blob:
        return "Bronze"
    if any(token in blob for token in ("silver", "staging", "stg_")):
        return "Silver"
    if any(token in blob for token in ("gold", "mart", "marts", "fact_", "dim_", "kpi_")):
        return "Gold"
    return "Other"


def parse_test_results(run_results: dict[str, Any] | None) -> dict[str, Any]:
    """Aggregate dbt test execution metrics from run_results.json."""
    total = 0
    passed = 0
    failed = 0

    if run_results:
        for result in run_results.get("results", []):
            unique_id = str(result.get("unique_id", ""))
            if not unique_id.startswith("test."):
                continue

            total += 1
            status = str(result.get("status", "")).lower()
            if status in {"pass", "success"}:
                passed += 1
            else:
                failed += 1

    ratio = round((passed / total) * 100, 2) if total else 0.0
    last_execution = None
    if run_results:
        metadata = run_results.get("metadata", {})
        last_execution = metadata.get("generated_at") or metadata.get("invocation_started_at")

    return {
        "total_tests": total,
        "passed_tests": passed,
        "failed_tests": failed,
        "success_ratio": ratio,
        "last_execution": last_execution,
    }


def _extract_row_counts(pipeline_metrics: dict[str, Any] | None) -> tuple[dict[str, Any], str | None]:
    """Return row counts by medallion layer from pipeline metrics when present."""
    if not pipeline_metrics:
        return {}, None

    row_counts: dict[str, Any] = {}
    raw_count = _safe_int(pipeline_metrics.get("rows_ingested"))
    bronze_count = _safe_int(pipeline_metrics.get("rows_bronze"))
    silver_count = _safe_int(pipeline_metrics.get("rows_silver"))
    gold_count = _safe_int(pipeline_metrics.get("rows_gold"))

    if raw_count is not None:
        row_counts["Raw"] = raw_count
    if bronze_count is not None:
        row_counts["Bronze"] = bronze_count
    if silver_count is not None:
        row_counts["Silver"] = silver_count
    if gold_count is not None:
        row_counts["Gold"] = gold_count

    updated_at = pipeline_metrics.get("last_updated_at")
    return row_counts, updated_at


def get_overview_context() -> dict[str, Any]:
    """Build UI context for the overview page."""
    pipeline_metrics = load_json(PIPELINE_METRICS_PATH)
    run_results = load_json(RUN_RESULTS_PATH)
    row_counts, metrics_timestamp = _extract_row_counts(pipeline_metrics)
    test_stats = parse_test_results(run_results)

    run_results_notice = artifact_notice(RUN_RESULTS_PATH)
    metrics_notice = artifact_notice(PIPELINE_METRICS_PATH)
    last_execution = test_stats["last_execution"] or metrics_timestamp

    return {
        "row_counts": row_counts,
        "last_execution": format_timestamp(last_execution),
        "run_results_notice": run_results_notice,
        "metrics_notice": metrics_notice,
    }


def get_lineage_context() -> dict[str, Any]:
    """Build UI context for lineage page from dbt artifacts."""
    manifest = load_json(MANIFEST_PATH)
    run_results = load_json(RUN_RESULTS_PATH)
    test_stats = parse_test_results(run_results)

    if not manifest:
        return {
            "manifest_notice": artifact_notice(MANIFEST_PATH) or "Artifact not generated yet",
            "run_results_notice": artifact_notice(RUN_RESULTS_PATH),
            "total_models": 0,
            "layer_counts": {},
            "models_by_layer": {},
            "tests_executed": test_stats["total_tests"],
            "failed_tests": test_stats["failed_tests"],
        }

    models_by_layer: dict[str, list[str]] = {layer: [] for layer in LAYER_ORDER}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        layer = classify_layer(node)
        models_by_layer.setdefault(layer, []).append(node.get("name", "unnamed_model"))

    for layer in models_by_layer:
        models_by_layer[layer] = sorted(models_by_layer[layer])

    layer_counts = {layer: len(names) for layer, names in models_by_layer.items() if names}
    filtered_models = {layer: names for layer, names in models_by_layer.items() if names}
    total_models = sum(layer_counts.values())

    return {
        "manifest_notice": artifact_notice(MANIFEST_PATH),
        "run_results_notice": artifact_notice(RUN_RESULTS_PATH),
        "total_models": total_models,
        "layer_counts": layer_counts,
        "models_by_layer": filtered_models,
        "tests_executed": test_stats["total_tests"],
        "failed_tests": test_stats["failed_tests"],
    }


def _dq_from_ge_summary() -> dict[str, Any] | None:
    """Parse compact Great Expectations summary report."""
    summary = load_json(DQ_SUMMARY_PATH)
    if not summary:
        return None

    total = _safe_int(summary.get("evaluated_expectations")) or 0
    failed = _safe_int(summary.get("unsuccessful_expectations")) or 0
    passed = max(total - failed, 0)
    ratio = round((passed / total) * 100, 2) if total else 0.0

    return {
        "source": "Great Expectations summary report",
        "total_tests": total,
        "passed_tests": passed,
        "failed_tests": failed,
        "success_ratio": ratio,
    }


def _dq_from_ge_full() -> dict[str, Any] | None:
    """Parse full Great Expectations output as fallback."""
    full = load_json(DQ_FULL_PATH)
    if not full:
        return None

    total = 0
    passed = 0
    failed = 0

    for payload in full.get("run_results", {}).values():
        stats = payload.get("validation_result", {}).get("statistics", {})
        evaluated = _safe_int(stats.get("evaluated_expectations"))
        successful = _safe_int(stats.get("successful_expectations"))
        unsuccessful = _safe_int(stats.get("unsuccessful_expectations"))

        if evaluated is None and successful is None and unsuccessful is None:
            continue

        if evaluated is None:
            evaluated = 0
            if successful is not None:
                evaluated += successful
            if unsuccessful is not None:
                evaluated += unsuccessful

        successful = successful if successful is not None else max(evaluated - (unsuccessful or 0), 0)
        unsuccessful = unsuccessful if unsuccessful is not None else max(evaluated - successful, 0)

        total += evaluated
        passed += successful
        failed += unsuccessful

    if total == 0:
        return None

    ratio = round((passed / total) * 100, 2)
    run_id = full.get("run_id", {})
    run_time = run_id.get("run_time") if isinstance(run_id, dict) else None

    return {
        "source": "Great Expectations full report",
        "total_tests": total,
        "passed_tests": passed,
        "failed_tests": failed,
        "success_ratio": ratio,
        "last_execution": run_time,
    }


def get_dq_context() -> dict[str, Any]:
    """Build UI context for the Data Quality page."""
    run_results = load_json(RUN_RESULTS_PATH)
    run_result_stats = parse_test_results(run_results)

    if run_result_stats["total_tests"] > 0:
        return {
            "source": "dbt run_results.json",
            "total_tests": run_result_stats["total_tests"],
            "passed_tests": run_result_stats["passed_tests"],
            "failed_tests": run_result_stats["failed_tests"],
            "success_ratio": run_result_stats["success_ratio"],
            "last_execution": format_timestamp(run_result_stats["last_execution"]),
            "artifact_notice": artifact_notice(RUN_RESULTS_PATH),
            "placeholder_notice": None,
        }

    ge_summary = _dq_from_ge_summary()
    if ge_summary:
        return {
            "source": ge_summary["source"],
            "total_tests": ge_summary["total_tests"],
            "passed_tests": ge_summary["passed_tests"],
            "failed_tests": ge_summary["failed_tests"],
            "success_ratio": ge_summary["success_ratio"],
            "last_execution": format_timestamp(None),
            "artifact_notice": artifact_notice(DQ_SUMMARY_PATH),
            "placeholder_notice": None,
        }

    ge_full = _dq_from_ge_full()
    if ge_full:
        return {
            "source": ge_full["source"],
            "total_tests": ge_full["total_tests"],
            "passed_tests": ge_full["passed_tests"],
            "failed_tests": ge_full["failed_tests"],
            "success_ratio": ge_full["success_ratio"],
            "last_execution": format_timestamp(ge_full.get("last_execution")),
            "artifact_notice": artifact_notice(DQ_FULL_PATH),
            "placeholder_notice": None,
        }

    ge_notice = artifact_notice(GE_DIR)
    reports_notice = artifact_notice(REPORTS_DIR)
    placeholder = "Artifact not generated yet"
    if ge_notice and reports_notice:
        placeholder = "Artifact not generated yet: no dbt run results or Great Expectations outputs found."

    return {
        "source": "No execution artifact",
        "total_tests": 0,
        "passed_tests": 0,
        "failed_tests": 0,
        "success_ratio": 0.0,
        "last_execution": "Artifact not generated yet",
        "artifact_notice": ge_notice or reports_notice,
        "placeholder_notice": placeholder,
    }
