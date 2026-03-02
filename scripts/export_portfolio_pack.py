#!/usr/bin/env python3
"""Generate lightweight portfolio artifacts for website embedding."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.io import read_json, write_json
from src.common.paths import REPORTS_DIR, SILVER_DIR, ensure_project_dirs

matplotlib.use("Agg")

PORTFOLIO_EXPORTS_DIR = ROOT_DIR / "docs" / "portfolio" / "exports"
LATEST_POINTER_PATH = PORTFOLIO_EXPORTS_DIR / "latest.txt"
MAX_IMAGE_SIZE_BYTES = 500 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export portfolio pack artifacts")
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id for export folder naming; defaults to UTC timestamp",
    )
    parser.add_argument(
        "--skip-lineage-proof",
        action="store_true",
        help="Skip dbt lineage proof chart generation",
    )
    return parser.parse_args()


def resolve_run_id(explicit_run_id: str) -> str:
    if explicit_run_id.strip():
        return explicit_run_id.strip()
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def deterministic_day_of_year(raw_value: Any) -> int:
    hashed = hashlib.md5(str(raw_value).encode("utf-8")).hexdigest()
    return (int(hashed[:8], 16) % 365) + 1


def choose_fare_series(frame: pd.DataFrame) -> pd.Series:
    candidate_columns = [
        "fare_amount",
        "fare",
        "trip_fare",
        "domgross_2013",
        "intgross_2013",
        "budget_2013",
        "domgross",
        "intgross",
        "budget",
    ]

    for column_name in candidate_columns:
        if column_name in frame.columns:
            series = pd.to_numeric(frame[column_name], errors="coerce")
            if series.notna().any():
                return series

    fallback = pd.Series(range(1, len(frame) + 1), dtype="float64")
    return fallback


def load_trip_events() -> tuple[pd.DataFrame, str]:
    trip_path = SILVER_DIR / "trips" / "data.parquet"
    if trip_path.exists():
        trips = pd.read_parquet(trip_path)
        date_columns = ["trip_date", "pickup_date", "pickup_datetime", "date", "event_date"]
        fare_columns = ["fare_amount", "fare", "trip_fare"]

        date_column = next((column for column in date_columns if column in trips.columns), "")
        fare_column = next((column for column in fare_columns if column in trips.columns), "")

        if date_column and fare_column:
            events = pd.DataFrame(
                {
                    "trip_date": pd.to_datetime(trips[date_column], errors="coerce").dt.floor("D"),
                    "fare_amount": pd.to_numeric(trips[fare_column], errors="coerce"),
                }
            )
            events = events.dropna(subset=["trip_date", "fare_amount"])
            if not events.empty:
                return events, "silver/trips"

    bechdel_path = SILVER_DIR / "bechdel_movies" / "data.parquet"
    if not bechdel_path.exists():
        raise FileNotFoundError(
            "No trips dataset found and fallback dataset missing at "
            f"{bechdel_path}. Run `make run-all` first."
        )

    movies = pd.read_parquet(bechdel_path).copy()
    if "year" not in movies.columns:
        raise ValueError("Fallback dataset is missing 'year' column required for daily proxy charting.")

    movies["year"] = pd.to_numeric(movies["year"], errors="coerce")
    movies = movies.dropna(subset=["year"])
    movies["year"] = movies["year"].astype(int).clip(lower=1900, upper=2100)

    if "imdb" in movies.columns:
        day_source = movies["imdb"]
    elif "title" in movies.columns:
        day_source = movies["title"]
    else:
        day_source = pd.Series(range(len(movies)), index=movies.index)

    movies["day_of_year"] = day_source.map(deterministic_day_of_year)
    movies["trip_date"] = pd.to_datetime(movies["year"].astype(str), format="%Y") + pd.to_timedelta(
        movies["day_of_year"] - 1,
        unit="D",
    )

    fare_proxy = choose_fare_series(movies).abs()
    fare_proxy = fare_proxy.fillna(fare_proxy.median() if fare_proxy.notna().any() else 25_000_000.0)

    if float(fare_proxy.max()) > 1000:
        fare_amount = (fare_proxy / 1_000_000).clip(lower=6.0, upper=120.0)
    else:
        fare_amount = fare_proxy.clip(lower=6.0, upper=120.0)

    events = pd.DataFrame({"trip_date": movies["trip_date"], "fare_amount": fare_amount})
    events = events.dropna(subset=["trip_date", "fare_amount"]).sort_values("trip_date")
    return events, "silver/bechdel_movies (deterministic proxy)"


def build_daily_metrics(events: pd.DataFrame) -> pd.DataFrame:
    daily = (
        events.groupby("trip_date", as_index=False)
        .agg(trips_volume=("trip_date", "size"), avg_fare_per_trip=("fare_amount", "mean"))
        .sort_values("trip_date")
    )
    daily["trips_volume_7d_avg"] = daily["trips_volume"].rolling(7, min_periods=1).mean()
    daily["avg_fare_7d_avg"] = daily["avg_fare_per_trip"].rolling(7, min_periods=1).mean()
    return daily


def save_chart(path: Path, fig: plt.Figure) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)

    if path.stat().st_size > MAX_IMAGE_SIZE_BYTES:
        raise RuntimeError(
            f"Generated image exceeds size limit ({MAX_IMAGE_SIZE_BYTES} bytes): {path}"
        )


def export_trips_volume_chart(daily: pd.DataFrame, output_path: Path) -> None:
    fig, axis = plt.subplots(figsize=(10, 4.5))
    axis.plot(
        daily["trip_date"],
        daily["trips_volume"],
        color="#4e79a7",
        linewidth=1.2,
        alpha=0.7,
        label="Daily trips",
    )
    axis.plot(
        daily["trip_date"],
        daily["trips_volume_7d_avg"],
        color="#f28e2b",
        linewidth=2.0,
        label="7-day moving average",
    )
    axis.set_title("Trips Volume Trend (Daily)")
    axis.set_xlabel("Date")
    axis.set_ylabel("Trips")
    axis.grid(alpha=0.25, linestyle="--", linewidth=0.6)
    axis.legend(loc="upper left")
    fig.autofmt_xdate()
    save_chart(output_path, fig)


def export_avg_fare_chart(daily: pd.DataFrame, output_path: Path) -> None:
    fig, axis = plt.subplots(figsize=(10, 4.5))
    axis.plot(
        daily["trip_date"],
        daily["avg_fare_per_trip"],
        color="#59a14f",
        linewidth=1.2,
        alpha=0.7,
        label="Daily avg fare",
    )
    axis.plot(
        daily["trip_date"],
        daily["avg_fare_7d_avg"],
        color="#e15759",
        linewidth=2.0,
        label="7-day moving average",
    )
    axis.set_title("Avg Fare per Trip (Daily)")
    axis.set_xlabel("Date")
    axis.set_ylabel("Fare")
    axis.grid(alpha=0.25, linestyle="--", linewidth=0.6)
    axis.legend(loc="upper left")
    fig.autofmt_xdate()
    save_chart(output_path, fig)


def export_dq_summary_chart(output_path: Path) -> dict[str, int]:
    dq_summary_path = REPORTS_DIR / "dq_result_summary.json"
    if not dq_summary_path.exists():
        raise FileNotFoundError(f"Missing data quality summary at {dq_summary_path}. Run `make dq` first.")

    dq_summary = read_json(dq_summary_path, fallback={})
    evaluated = int(dq_summary.get("evaluated_expectations", 0))
    failures = int(dq_summary.get("unsuccessful_expectations", 0))
    successful = max(evaluated - failures, 0)

    fig, axis = plt.subplots(figsize=(8, 4.5))
    labels = ["Rules Evaluated", "Rules Failed", "Rules Passed"]
    values = [evaluated, failures, successful]
    colors = ["#4e79a7", "#e15759", "#59a14f"]

    bars = axis.bar(labels, values, color=colors)
    axis.set_title("Data Quality Summary (Violations)")
    axis.set_ylabel("Rule Count")
    axis.grid(axis="y", alpha=0.25, linestyle="--", linewidth=0.6)
    axis.set_axisbelow(True)

    for bar, value in zip(bars, values):
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            value + max(1, int(0.02 * max(values or [1]))),
            str(value),
            ha="center",
            va="bottom",
            fontsize=10,
        )

    save_chart(output_path, fig)
    return {
        "evaluated_expectations": evaluated,
        "unsuccessful_expectations": failures,
        "successful_expectations": successful,
    }


def model_stage(node: dict[str, Any]) -> str:
    original_file_path = str(node.get("original_file_path", ""))
    if "/staging/" in original_file_path:
        return "staging"
    if "/marts/" in original_file_path:
        return "marts"
    return "other"


def export_lineage_proof(output_path: Path) -> bool:
    manifest_path = ROOT_DIR / "dbt" / "lakehouse_dbt" / "target" / "manifest.json"
    if not manifest_path.exists():
        return False

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_nodes = manifest.get("nodes", {})

    model_nodes = {
        node_id: node
        for node_id, node in raw_nodes.items()
        if node.get("resource_type") == "model"
        and node.get("package_name") == "lakehouse_dbt"
    }

    if not model_nodes:
        return False

    stages = ["staging", "marts", "other"]
    stage_x = {stage: index for index, stage in enumerate(stages)}

    grouped: dict[str, list[tuple[str, dict[str, Any]]]] = {stage: [] for stage in stages}
    for node_id, node in model_nodes.items():
        grouped[model_stage(node)].append((node_id, node))

    for stage in grouped:
        grouped[stage] = sorted(grouped[stage], key=lambda item: item[1].get("name", ""))

    positions: dict[str, tuple[float, float]] = {}
    for stage in stages:
        items = grouped[stage]
        for index, (node_id, _node) in enumerate(items):
            y_position = -index
            positions[node_id] = (float(stage_x[stage]), float(y_position))

    fig, axis = plt.subplots(figsize=(10, 4.5))
    axis.set_title("dbt Lineage Proof (Mode 1)")
    axis.axis("off")

    for node_id, node in model_nodes.items():
        x_pos, y_pos = positions[node_id]
        axis.scatter([x_pos], [y_pos], s=800, color="#4e79a7", alpha=0.9)
        axis.text(
            x_pos,
            y_pos,
            str(node.get("name", "")),
            color="white",
            ha="center",
            va="center",
            fontsize=8,
            weight="bold",
        )

    for node_id, node in model_nodes.items():
        child_x, child_y = positions[node_id]
        dependencies = node.get("depends_on", {}).get("nodes", [])
        for parent_id in dependencies:
            if parent_id not in model_nodes:
                continue
            parent_x, parent_y = positions[parent_id]
            axis.annotate(
                "",
                xy=(child_x - 0.08, child_y),
                xytext=(parent_x + 0.08, parent_y),
                arrowprops={"arrowstyle": "->", "color": "#333333", "linewidth": 1.0},
            )

    for stage, x_pos in stage_x.items():
        axis.text(
            x_pos,
            1.0,
            stage.capitalize(),
            ha="center",
            va="bottom",
            fontsize=10,
            color="#333333",
            weight="bold",
        )

    axis.set_xlim(-0.5, max(stage_x.values()) + 0.5)
    y_values = [pos[1] for pos in positions.values()]
    axis.set_ylim(min(y_values) - 0.8, 1.5)
    save_chart(output_path, fig)
    return True


def to_repo_relative(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT_DIR.resolve()))


def write_run_report_text(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"run_id: {payload['run_id']}",
        f"generated_at_utc: {payload['generated_at_utc']}",
        f"trip_source: {payload['trip_source']}",
        f"events_processed: {payload['events_processed']}",
        f"date_range: {payload['date_range']['start']} -> {payload['date_range']['end']}",
        f"dq_rules_evaluated: {payload['dq_summary']['evaluated_expectations']}",
        f"dq_rules_failed: {payload['dq_summary']['unsuccessful_expectations']}",
        "artifacts:",
    ]

    for label, relative_path in payload["artifacts"].items():
        lines.append(f"  - {label}: {relative_path}")

    if payload.get("notes"):
        lines.append("notes:")
        for note in payload["notes"]:
            lines.append(f"  - {note}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    ensure_project_dirs()

    run_id = resolve_run_id(args.run_id)
    export_dir = PORTFOLIO_EXPORTS_DIR / run_id
    charts_dir = export_dir / "charts"
    lineage_dir = export_dir / "lineage"
    export_dir.mkdir(parents=True, exist_ok=True)

    events, trip_source = load_trip_events()
    if events.empty:
        raise RuntimeError("No trip-like events available to build portfolio charts.")

    daily = build_daily_metrics(events)

    trips_chart_path = charts_dir / "trips_volume_trend_daily.png"
    fare_chart_path = charts_dir / "avg_fare_per_trip_daily.png"
    dq_chart_path = charts_dir / "dq_violations_summary.png"
    run_report_json_path = export_dir / "run_report.json"
    run_report_text_path = export_dir / "run_report.txt"
    lineage_chart_path = lineage_dir / "dbt_lineage_proof.png"

    export_trips_volume_chart(daily, trips_chart_path)
    export_avg_fare_chart(daily, fare_chart_path)
    dq_summary = export_dq_summary_chart(dq_chart_path)

    lineage_generated = False
    if not args.skip_lineage_proof:
        lineage_generated = export_lineage_proof(lineage_chart_path)

    notes: list[str] = []
    if "proxy" in trip_source:
        notes.append(
            "Trips/fare charts are deterministically derived from the Bechdel dataset because no "
            "native trips fact table is present in Mode 1 outputs."
        )
    if not lineage_generated:
        notes.append(
            "Lineage proof chart skipped because dbt manifest was unavailable. Run `make run-all` first."
        )

    payload = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "trip_source": trip_source,
        "events_processed": int(len(events)),
        "date_range": {
            "start": str(daily["trip_date"].min().date()),
            "end": str(daily["trip_date"].max().date()),
        },
        "dq_summary": dq_summary,
        "artifacts": {
            "run_report_text": to_repo_relative(run_report_text_path),
            "trips_volume_trend_daily": to_repo_relative(trips_chart_path),
            "avg_fare_per_trip_daily": to_repo_relative(fare_chart_path),
            "dq_violations_summary": to_repo_relative(dq_chart_path),
            "dbt_lineage_proof": to_repo_relative(lineage_chart_path) if lineage_generated else "",
        },
        "notes": notes,
    }

    write_json(run_report_json_path, payload)
    write_run_report_text(run_report_text_path, payload)
    PORTFOLIO_EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_POINTER_PATH.write_text(f"{run_id}\n", encoding="utf-8")

    print(f"Portfolio pack exported: {to_repo_relative(export_dir)}")
    print(f"Latest pointer: {to_repo_relative(LATEST_POINTER_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
