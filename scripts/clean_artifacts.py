#!/usr/bin/env python3
"""Clean generated artifacts while keeping repository structure intact."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.paths import (
    BRONZE_DIR,
    EXECUTED_NOTEBOOKS_DIR,
    GOLD_DIR,
    METRICS_DIR,
    REPORTS_DIR,
    SILVER_DIR,
    WAREHOUSE_DIR,
)


def remove_files_recursively(root: Path) -> None:
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_file() and path.name != ".gitkeep":
            path.unlink()


def main() -> int:
    for directory in (BRONZE_DIR, SILVER_DIR, GOLD_DIR, EXECUTED_NOTEBOOKS_DIR, METRICS_DIR):
        remove_files_recursively(directory)

    for path in (
        REPORTS_DIR / "dq_result_full.json",
        REPORTS_DIR / "dq_result_summary.json",
        WAREHOUSE_DIR / "lakehouse.duckdb",
        WAREHOUSE_DIR / "lakehouse.duckdb.wal",
        WAREHOUSE_DIR / "lakehouse.duckdb.tmp",
    ):
        if path.exists() and path.is_file():
            path.unlink()

    ge_site = REPORTS_DIR / "great_expectations"
    if ge_site.exists() and ge_site.is_dir():
        shutil.rmtree(ge_site)

    dbt_target = Path("dbt/lakehouse_dbt/target")
    dbt_packages = Path("dbt/lakehouse_dbt/dbt_packages")
    if dbt_target.exists():
        shutil.rmtree(dbt_target)
    if dbt_packages.exists():
        shutil.rmtree(dbt_packages)

    # Remove legacy duplicate GE context if it appears at repository root.
    legacy_gx_dir = ROOT_DIR / "gx"
    if legacy_gx_dir.exists():
        shutil.rmtree(legacy_gx_dir)

    # Remove legacy root MySQL volume if it exists from older compose paths.
    legacy_mysql_volume = ROOT_DIR / "warehouse" / "mysql"
    if legacy_mysql_volume.exists():
        shutil.rmtree(legacy_mysql_volume)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
