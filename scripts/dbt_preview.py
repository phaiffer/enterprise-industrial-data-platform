#!/usr/bin/env python3
"""Preview dbt-duckdb objects without requiring the duckdb CLI."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import duckdb
import yaml

DBT_PROFILE_NAME = "lakehouse_dbt"
ENV_VAR_TEMPLATE = re.compile(
    r"\{\{\s*env_var\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*\}\}"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview dbt-duckdb schemas/tables and KPI rows")
    parser.add_argument("--profiles-path", required=True)
    parser.add_argument("--project-dir", required=True)
    return parser.parse_args()


def resolve_repo_root() -> Path:
    configured = os.getenv("EIDP_REPO_ROOT")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[1]


def render_env_template(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        env_key = match.group(1)
        default_value = match.group(2) or ""
        return os.getenv(env_key, default_value)

    return ENV_VAR_TEMPLATE.sub(replace, value).strip()


def load_profile_path(profiles_path: Path) -> str | None:
    env_override = os.getenv("DBT_DUCKDB_PATH") or os.getenv("DUCKDB_PATH")
    if env_override:
        return env_override

    if not profiles_path.exists():
        return None

    profile_data = yaml.safe_load(profiles_path.read_text(encoding="utf-8")) or {}
    profile_cfg = profile_data.get(DBT_PROFILE_NAME, {})
    target_name = os.getenv("DBT_TARGET", profile_cfg.get("target", "dev"))

    outputs = profile_cfg.get("outputs", {})
    target_cfg = outputs.get(target_name, {})
    configured_path = target_cfg.get("path")

    if isinstance(configured_path, str) and configured_path.strip():
        return render_env_template(configured_path)

    return None


def resolve_db_path(profiles_path: Path, project_dir: Path, repo_root: Path) -> Path:
    configured_path = load_profile_path(profiles_path)
    candidates: list[Path] = []

    if configured_path:
        configured = Path(configured_path)
        if configured.is_absolute():
            candidates.append(configured)
        else:
            candidates.append((repo_root / configured).resolve())
            candidates.append((profiles_path.parent / configured).resolve())
            candidates.append((project_dir / configured).resolve())

    for candidate in candidates:
        if candidate.exists():
            return candidate

    fallback_files = sorted(
        project_dir.rglob("*.duckdb"),
        key=lambda file_path: file_path.stat().st_mtime,
        reverse=True,
    )
    if fallback_files:
        return fallback_files[0]

    candidate_list = "\n".join(str(item) for item in candidates) or "(none)"
    raise FileNotFoundError(
        "Could not locate dbt duckdb database file.\n"
        f"Checked:\n{candidate_list}\n"
        "Run `make dbt-run` first or set DBT_DUCKDB_PATH."
    )


def table_exists(connection: duckdb.DuckDBPyConnection, schema_name: str, table_name: str) -> bool:
    result = connection.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ? and table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    return bool(result and result[0] > 0)


def print_schemas(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        select schema_name
        from information_schema.schemata
        order by schema_name
        """
    ).fetchall()

    print("Schemas:")
    for (schema_name,) in rows:
        print(f"  - {schema_name}")


def print_tables(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        select table_schema, table_name
        from information_schema.tables
        where table_schema not in ('information_schema', 'pg_catalog')
        order by table_schema, table_name
        """
    ).fetchall()

    print("\nTables:")
    for schema_name, table_name in rows:
        print(f"  - {schema_name}.{table_name}")


def print_preview(connection: duckdb.DuckDBPyConnection, schema_name: str, table_name: str, limit: int = 10) -> None:
    query = f"select * from {schema_name}.{table_name} limit {limit}"
    result = connection.execute(query)
    rows = result.fetchall()
    column_names = [column[0] for column in result.description]

    print(f"\nPreview: {schema_name}.{table_name} (top {limit})")
    print(" | ".join(column_names))
    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))


def main() -> int:
    args = parse_args()

    repo_root = resolve_repo_root()
    profiles_path = Path(args.profiles_path).resolve()
    project_dir = Path(args.project_dir).resolve()

    db_path = resolve_db_path(profiles_path, project_dir, repo_root)
    print(f"Using DuckDB database: {db_path}")

    connection = duckdb.connect(str(db_path), read_only=True)
    print_schemas(connection)
    print_tables(connection)

    preview_targets = [
        ("marts", "kpi_major_outcomes"),
        ("marts", "kpi_movie_representation"),
    ]

    previews_printed = 0
    for schema_name, table_name in preview_targets:
        if table_exists(connection, schema_name, table_name):
            print_preview(connection, schema_name, table_name)
            previews_printed += 1

    if previews_printed == 0:
        fallback_tables = connection.execute(
            """
            select table_schema, table_name
            from information_schema.tables
            where table_schema in ('marts', 'analytics')
            order by table_schema, table_name
            limit 2
            """
        ).fetchall()
        for schema_name, table_name in fallback_tables:
            print_preview(connection, schema_name, table_name)
            previews_printed += 1

    connection.close()

    if previews_printed == 0:
        raise RuntimeError("No marts/analytics tables found. Run `make dbt-run` first.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
