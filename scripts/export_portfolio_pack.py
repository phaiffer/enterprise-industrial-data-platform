#!/usr/bin/env python3
"""Generate deterministic portfolio pack exports for sales enablement."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.io import read_json, write_json
from src.common.paths import (
    BRONZE_DIR,
    GOLD_DIR,
    PIPELINE_METRICS_PATH,
    REPORTS_DIR,
    SILVER_DIR,
    ensure_project_dirs,
)

PORTFOLIO_EXPORTS_DIR = ROOT_DIR / "docs" / "portfolio" / "exports"
LATEST_POINTER_PATH = PORTFOLIO_EXPORTS_DIR / "latest.txt"
MAX_IMAGE_SIZE_BYTES = 500 * 1024
PIPELINE_STAGE_SLA_SECONDS = 20.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export deterministic portfolio artifacts")
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id for export folder naming; defaults to UTC timestamp",
    )
    parser.add_argument(
        "--skip-latest-pointer",
        action="store_true",
        help="Do not update docs/portfolio/exports/latest.txt",
    )
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def resolve_run_id(explicit_run_id: str) -> str:
    if explicit_run_id.strip():
        return explicit_run_id.strip()
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def to_repo_relative(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT_DIR.resolve()))


def deterministic_day_of_year(raw_value: Any) -> int:
    hashed = hashlib.md5(str(raw_value).encode("utf-8")).hexdigest()
    return (int(hashed[:8], 16) % 365) + 1


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT_DIR,
            text=True,
        ).strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        return "unknown"


def read_required_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required dataset: {to_repo_relative(path)}")
    return pd.read_parquet(path)


def read_layer_row_counts(layer_dir: Path) -> tuple[int, dict[str, int]]:
    counts: dict[str, int] = {}
    total = 0

    for parquet_path in sorted(layer_dir.glob("*/data.parquet")):
        row_count = int(len(pd.read_parquet(parquet_path)))
        dataset_name = parquet_path.parent.name
        counts[dataset_name] = row_count
        total += row_count

    return total, counts


def load_input_sources() -> tuple[list[str], list[str]]:
    dataset_manifest_path = REPORTS_DIR / "dataset_manifest.json"
    payload = read_json(dataset_manifest_path, fallback={})
    datasets = payload.get("datasets", [])

    source_names = sorted(
        {
            str(item.get("source", "")).strip()
            for item in datasets
            if str(item.get("source", "")).strip()
        }
    )
    dataset_names = sorted(
        {
            str(item.get("dataset", "")).strip()
            for item in datasets
            if str(item.get("dataset", "")).strip()
        }
    )

    if not source_names:
        metrics = read_json(PIPELINE_METRICS_PATH, fallback={})
        source_name = str(metrics.get("source", "")).strip()
        if source_name:
            source_names = [source_name]

    return source_names, dataset_names


def build_throughput_daily(bechdel_silver: pd.DataFrame) -> pd.DataFrame:
    events = bechdel_silver.copy()
    if "year" not in events.columns:
        raise ValueError("silver/bechdel_movies is missing required column: year")

    events["year"] = pd.to_numeric(events["year"], errors="coerce")
    events = events.dropna(subset=["year"])
    events["year"] = events["year"].astype(int).clip(lower=1900, upper=2100)

    if "imdb" in events.columns:
        day_source = events["imdb"]
    elif "title" in events.columns:
        day_source = events["title"]
    else:
        day_source = pd.Series(range(len(events)), index=events.index)

    events["day_of_year"] = day_source.map(deterministic_day_of_year)
    events["event_date"] = pd.to_datetime(
        events["year"].astype(str), format="%Y"
    ) + pd.to_timedelta(
        events["day_of_year"] - 1,
        unit="D",
    )

    daily = (
        events.groupby("event_date", as_index=False)
        .agg(records_processed=("event_date", "size"))
        .sort_values("event_date")
    )
    daily["records_processed_7d_avg"] = daily["records_processed"].rolling(7, min_periods=1).mean()
    return daily


def build_quality_summary() -> tuple[pd.DataFrame, dict[str, Any]]:
    dq_full = read_json(REPORTS_DIR / "dq_result_full.json", fallback={})

    rows: list[dict[str, Any]] = []
    for run_result in dq_full.get("run_results", {}).values():
        validation_result = run_result.get("validation_result", {})
        for result in validation_result.get("results", []):
            expectation_type = str(
                result.get("expectation_config", {}).get("expectation_type", "unknown_rule")
            )
            rows.append(
                {
                    "expectation_type": expectation_type,
                    "success": bool(result.get("success", False)),
                }
            )

    if not rows:
        dq_summary = read_json(REPORTS_DIR / "dq_result_summary.json", fallback={})
        evaluated = int(dq_summary.get("evaluated_expectations", 0))
        violations = int(dq_summary.get("unsuccessful_expectations", 0))
        successful = max(evaluated - violations, 0)
        rows.extend([{"expectation_type": "all_rules", "success": True}] * successful)
        rows.extend([{"expectation_type": "all_rules", "success": False}] * violations)

    if not rows:
        raise RuntimeError(
            "No data quality results found. Run `make dq` before exporting the portfolio pack."
        )

    frame = pd.DataFrame(rows)
    grouped = (
        frame.groupby("expectation_type", as_index=False)
        .agg(
            rules_checked=("expectation_type", "size"),
            violations=("success", lambda values: int((~values).sum())),
        )
        .sort_values("expectation_type")
    )

    dq_summary_payload = {
        "rules_checked": int(len(frame)),
        "violations_count": int((~frame["success"]).sum()),
        "violations_by_category": {
            row["expectation_type"]: int(row["violations"])
            for row in grouped.to_dict(orient="records")
        },
    }
    return grouped, dq_summary_payload


def build_latency_sla_summary() -> tuple[pd.DataFrame, dict[str, Any]]:
    pipeline_metrics = read_json(PIPELINE_METRICS_PATH, fallback={})
    stage_metrics = pipeline_metrics.get("stages", {})

    rows: list[dict[str, Any]] = []
    for stage_name, stage_payload in sorted(stage_metrics.items()):
        runtime_seconds = stage_payload.get("runtime_seconds")
        if runtime_seconds is None:
            continue
        rows.append(
            {
                "stage": str(stage_name),
                "runtime_seconds": float(runtime_seconds),
            }
        )

    if not rows:
        raise RuntimeError(
            "Missing pipeline stage runtimes in reports/metrics/pipeline_metrics.json. "
            "Run `make run-all` first."
        )

    frame = pd.DataFrame(rows).sort_values("stage").reset_index(drop=True)
    frame["within_sla"] = frame["runtime_seconds"] <= PIPELINE_STAGE_SLA_SECONDS
    compliance_ratio = float(frame["within_sla"].mean()) if len(frame) else 0.0

    payload = {
        "stage_sla_seconds": PIPELINE_STAGE_SLA_SECONDS,
        "stages_checked": int(len(frame)),
        "stages_within_sla": int(frame["within_sla"].sum()),
        "compliance_ratio": round(compliance_ratio, 4),
    }
    return frame, payload


def build_business_kpi(bechdel_silver: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = bechdel_silver.copy()
    if "year" not in frame.columns or "binary" not in frame.columns:
        raise ValueError("silver/bechdel_movies must include year and binary columns.")

    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["binary"] = frame["binary"].astype(str).str.upper().str.strip()
    frame = frame.dropna(subset=["year"])
    frame["year"] = frame["year"].astype(int)
    frame = frame[frame["binary"].isin(["PASS", "FAIL"])]

    if frame.empty:
        raise RuntimeError("No valid rows available for the business KPI chart.")

    frame["pass_flag"] = (frame["binary"] == "PASS").astype(int)
    yearly = (
        frame.groupby("year", as_index=False)
        .agg(
            pass_rate=("pass_flag", "mean"),
            movie_count=("pass_flag", "size"),
        )
        .sort_values("year")
    )
    yearly["pass_rate_5y_avg"] = yearly["pass_rate"].rolling(5, min_periods=1).mean()

    payload = {
        "pass_rate_latest": round(float(yearly["pass_rate"].iloc[-1]), 4),
        "pass_rate_overall": round(float(frame["pass_flag"].mean()), 4),
        "movies_total": int(frame["pass_flag"].size),
    }
    return yearly, payload


def save_chart(path: Path, figure: plt.Figure) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=110, bbox_inches="tight")
    plt.close(figure)

    if path.stat().st_size > MAX_IMAGE_SIZE_BYTES:
        raise RuntimeError(
            f"Generated image exceeds size limit ({MAX_IMAGE_SIZE_BYTES} bytes): {to_repo_relative(path)}"
        )


def export_throughput_chart(daily: pd.DataFrame, output_path: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 4.5))
    axis.plot(daily["event_date"], daily["records_processed"], label="Daily records")
    axis.plot(daily["event_date"], daily["records_processed_7d_avg"], label="7-day moving average")
    axis.set_title("Throughput Trend (Daily Records Processed)")
    axis.set_xlabel("Date")
    axis.set_ylabel("Records")
    axis.legend(loc="upper left")
    figure.autofmt_xdate()
    save_chart(output_path, figure)


def export_quality_chart(grouped_quality: pd.DataFrame, output_path: Path) -> None:
    labels = grouped_quality["expectation_type"].tolist()
    positions = list(range(len(labels)))
    width = 0.4

    figure, axis = plt.subplots(figsize=(11, 5))
    axis.bar(
        [position - width / 2 for position in positions],
        grouped_quality["rules_checked"],
        width=width,
        label="Rules checked",
    )
    axis.bar(
        [position + width / 2 for position in positions],
        grouped_quality["violations"],
        width=width,
        label="Violations",
    )
    axis.set_title("Quality Summary (Rule Violations by Category)")
    axis.set_xlabel("Rule category")
    axis.set_ylabel("Count")
    axis.set_xticks(positions, labels, rotation=25, ha="right")
    axis.legend(loc="upper right")
    figure.tight_layout()
    save_chart(output_path, figure)


def export_latency_sla_chart(stage_latency: pd.DataFrame, output_path: Path) -> None:
    labels = [
        stage_name.replace("notebook::", "nb::").replace(".ipynb", "")
        for stage_name in stage_latency["stage"].tolist()
    ]
    positions = list(range(len(labels)))

    figure, axis = plt.subplots(figsize=(11, 5))
    axis.bar(positions, stage_latency["runtime_seconds"])
    axis.axhline(
        y=PIPELINE_STAGE_SLA_SECONDS,
        linestyle="--",
        label=f"SLA threshold ({PIPELINE_STAGE_SLA_SECONDS:.0f}s)",
    )
    axis.set_title("Freshness/Latency View (Pipeline Stage SLA Compliance)")
    axis.set_xlabel("Pipeline stage")
    axis.set_ylabel("Runtime (seconds)")
    axis.set_xticks(positions, labels, rotation=25, ha="right")
    axis.legend(loc="upper right")
    figure.tight_layout()
    save_chart(output_path, figure)


def export_business_kpi_chart(kpi_yearly: pd.DataFrame, output_path: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 4.5))
    axis.plot(kpi_yearly["year"], kpi_yearly["pass_rate"] * 100.0, label="Annual pass rate")
    axis.plot(
        kpi_yearly["year"], kpi_yearly["pass_rate_5y_avg"] * 100.0, label="5-year moving average"
    )
    axis.set_title("Business KPI: Bechdel Pass Rate Trend")
    axis.set_xlabel("Release year")
    axis.set_ylabel("Pass rate (%)")
    axis.legend(loc="upper left")
    save_chart(output_path, figure)


def collect_exported_files(export_dir: Path) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for path in sorted(export_dir.rglob("*")):
        if not path.is_file():
            continue
        files.append(
            {
                "path": to_repo_relative(path),
                "size_bytes": int(path.stat().st_size),
            }
        )
    return files


def write_run_report(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"# Portfolio Run Report ({payload['run_id']})",
        "",
        f"- Started at (UTC): {payload['started_at']}",
        f"- Finished at (UTC): {payload['finished_at']}",
        f"- Git SHA: {payload['git_sha']}",
        f"- Input sources: {', '.join(payload['input_sources_used']) or 'n/a'}",
        f"- Input datasets: {', '.join(payload['input_datasets_used']) or 'n/a'}",
        (
            f"- Dataset time range: {payload['dataset_time_range']['start']} -> "
            f"{payload['dataset_time_range']['end']}"
        ),
        (
            f"- Row counts (bronze/silver/gold): {payload['row_counts']['bronze']} / "
            f"{payload['row_counts']['silver']} / {payload['row_counts']['gold']}"
        ),
        (
            f"- DQ rules checked: {payload['dq_summary']['rules_checked']} "
            f"(violations: {payload['dq_summary']['violations_count']})"
        ),
        (
            f"- SLA compliance: {payload['latency_summary']['stages_within_sla']} / "
            f"{payload['latency_summary']['stages_checked']} stages"
        ),
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    started_at = utc_now_iso()
    ensure_project_dirs()

    run_id = resolve_run_id(args.run_id)
    export_dir = PORTFOLIO_EXPORTS_DIR / run_id
    charts_dir = export_dir / "charts"
    reports_dir = export_dir / "reports"
    manifests_dir = export_dir / "manifests"
    manifest_path = manifests_dir / "run.json"
    report_path = reports_dir / "portfolio_run_report.md"

    charts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    bechdel_silver = read_required_parquet(SILVER_DIR / "bechdel_movies" / "data.parquet")

    throughput_daily = build_throughput_daily(bechdel_silver)
    quality_frame, dq_summary = build_quality_summary()
    stage_latency, latency_summary = build_latency_sla_summary()
    business_kpi_yearly, business_kpi_summary = build_business_kpi(bechdel_silver)

    throughput_chart_path = charts_dir / "throughput_trend_daily.png"
    quality_chart_path = charts_dir / "quality_violations_by_category.png"
    freshness_chart_path = charts_dir / "freshness_latency_sla_compliance.png"
    kpi_chart_path = charts_dir / "business_kpi_pass_rate_trend.png"

    export_throughput_chart(throughput_daily, throughput_chart_path)
    export_quality_chart(quality_frame, quality_chart_path)
    export_latency_sla_chart(stage_latency, freshness_chart_path)
    export_business_kpi_chart(business_kpi_yearly, kpi_chart_path)

    bronze_rows, bronze_by_dataset = read_layer_row_counts(BRONZE_DIR)
    silver_rows, silver_by_dataset = read_layer_row_counts(SILVER_DIR)
    gold_rows, gold_by_dataset = read_layer_row_counts(GOLD_DIR)
    input_sources, input_datasets = load_input_sources()

    finished_at = utc_now_iso()
    payload: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "git_sha": git_sha(),
        "input_sources_used": input_sources,
        "input_datasets_used": input_datasets,
        "dataset_time_range": {
            "start": str(throughput_daily["event_date"].min().date()),
            "end": str(throughput_daily["event_date"].max().date()),
        },
        "row_counts": {
            "bronze": bronze_rows,
            "silver": silver_rows,
            "gold": gold_rows,
            "bronze_by_dataset": bronze_by_dataset,
            "silver_by_dataset": silver_by_dataset,
            "gold_by_dataset": gold_by_dataset,
        },
        "dq_summary": dq_summary,
        "latency_summary": latency_summary,
        "business_kpi_summary": business_kpi_summary,
        "exported_files": [],
    }

    write_run_report(report_path, payload)

    for _ in range(2):
        payload["exported_files"] = collect_exported_files(export_dir)
        write_json(manifest_path, payload)

    if not args.skip_latest_pointer:
        PORTFOLIO_EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        LATEST_POINTER_PATH.write_text(f"{run_id}\n", encoding="utf-8")

    print(f"Portfolio pack exported: {to_repo_relative(export_dir)}")
    print(f"Run manifest: {to_repo_relative(manifest_path)}")
    if not args.skip_latest_pointer:
        print(f"Latest pointer: {to_repo_relative(LATEST_POINTER_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
