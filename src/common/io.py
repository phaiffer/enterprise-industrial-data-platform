import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.paths import PIPELINE_METRICS_PATH, ensure_project_dirs


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_json(path: Path, fallback: Any | None = None) -> Any:
    if not path.exists():
        return {} if fallback is None else fallback
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def load_pipeline_metrics() -> dict[str, Any]:
    ensure_project_dirs()
    return read_json(
        PIPELINE_METRICS_PATH,
        fallback={
            "stages": {},
            "rows_ingested": 0,
            "rows_bronze": 0,
            "rows_silver": 0,
            "rows_gold": 0,
            "dq_pass": None,
            "data_freshness": None,
        },
    )


def save_pipeline_metrics(metrics: dict[str, Any]) -> None:
    metrics["last_updated_at"] = _utc_now_iso()
    write_json(PIPELINE_METRICS_PATH, metrics)


def update_pipeline_metrics(updates: dict[str, Any]) -> dict[str, Any]:
    metrics = load_pipeline_metrics()
    metrics.update(updates)
    save_pipeline_metrics(metrics)
    return metrics


def update_stage_metrics(stage: str, updates: dict[str, Any]) -> dict[str, Any]:
    metrics = load_pipeline_metrics()
    metrics.setdefault("stages", {})
    metrics["stages"].setdefault(stage, {})
    metrics["stages"][stage].update(updates)
    save_pipeline_metrics(metrics)
    return metrics
