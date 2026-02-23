#!/usr/bin/env python3
"""Publish curated Gold marts from DuckDB into optional MySQL serving tables."""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import mysql.connector
import numpy as np
import pandas as pd
import yaml

DBT_PROFILE_NAME = "lakehouse_dbt"
DEFAULT_TABLES = [
    "marts.kpi_major_outcomes",
    "marts.kpi_movie_representation",
    "marts.dim_major_category",
    "marts.fact_major_employment",
]
ENV_VAR_TEMPLATE = re.compile(
    r"\{\{\s*env_var\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*\}\}"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish dbt Gold marts from DuckDB to MySQL")
    parser.add_argument("--profiles-path", default="dbt/lakehouse_dbt/profiles.yml")
    parser.add_argument("--project-dir", default="dbt/lakehouse_dbt")
    parser.add_argument("--duckdb-path", default="")
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLES))
    parser.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "localhost"))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", "yourpassword"))
    parser.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", "eidp_serving"))
    parser.add_argument("--source-run-id", default=os.getenv("SOURCE_RUN_ID", ""))
    return parser.parse_args()


def resolve_repo_root() -> Path:
    explicit = os.getenv("EIDP_REPO_ROOT")
    if explicit:
        return Path(explicit).resolve()
    return Path(__file__).resolve().parents[1]


def render_env_template(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        env_key = match.group(1)
        default_value = match.group(2) or ""
        return os.getenv(env_key, default_value)

    return ENV_VAR_TEMPLATE.sub(replace, value).strip()


def load_profile_path(profiles_path: Path) -> str | None:
    override = os.getenv("DBT_DUCKDB_PATH") or os.getenv("DUCKDB_PATH")
    if override:
        return override

    if not profiles_path.exists():
        return None

    profile_data = yaml.safe_load(profiles_path.read_text(encoding="utf-8")) or {}
    profile_cfg = profile_data.get(DBT_PROFILE_NAME, {})
    target_name = os.getenv("DBT_TARGET", profile_cfg.get("target", "dev"))
    target_cfg = profile_cfg.get("outputs", {}).get(target_name, {})
    configured_path = target_cfg.get("path")

    if isinstance(configured_path, str) and configured_path.strip():
        return render_env_template(configured_path)

    return None


def resolve_duckdb_path(args: argparse.Namespace, repo_root: Path) -> Path:
    if args.duckdb_path:
        candidate = Path(args.duckdb_path)
        return candidate.resolve() if candidate.is_absolute() else (repo_root / candidate).resolve()

    profiles_path = Path(args.profiles_path).resolve()
    project_dir = Path(args.project_dir).resolve()
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

    raise FileNotFoundError(
        "Could not locate DuckDB database file. Run `make dbt-run` first or provide --duckdb-path."
    )


def mysql_connect_with_retries(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    max_attempts: int = 15,
    retry_seconds: int = 4,
) -> mysql.connector.MySQLConnection:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            connection = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                autocommit=False,
            )
            return connection
        except mysql.connector.Error as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            time.sleep(retry_seconds)

    raise RuntimeError(f"Unable to connect to MySQL after {max_attempts} attempts: {last_error}")


def mysql_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series.dtype):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(series.dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return "DATETIME"
    return "TEXT"


def normalize_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def parse_table_identifiers(table_argument: str) -> list[str]:
    tables = [table.strip() for table in table_argument.split(",") if table.strip()]
    if not tables:
        raise ValueError("No tables provided to publish.")
    for table in tables:
        if "." not in table:
            raise ValueError(f"Table '{table}' must be fully qualified as <schema>.<table>.")
    return tables


def table_exists(cursor: mysql.connector.cursor.MySQLCursor, database: str, table: str) -> bool:
    cursor.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = %s and table_name = %s
        """,
        (database, table),
    )
    return int(cursor.fetchone()[0]) > 0


def existing_column_names(cursor: mysql.connector.cursor.MySQLCursor, database: str, table: str) -> list[str]:
    cursor.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
        order by ordinal_position
        """,
        (database, table),
    )
    return [row[0] for row in cursor.fetchall()]


def create_table_sql(table: str, frame: pd.DataFrame) -> str:
    columns_sql = []
    for column in frame.columns:
        mysql_type = mysql_type_for_series(frame[column])
        columns_sql.append(f"`{column}` {mysql_type} NULL")

    columns_sql.append("`_published_at` DATETIME NOT NULL")
    columns_sql.append("`_source_run_id` VARCHAR(255) NOT NULL")
    column_block = ",\n  ".join(columns_sql)

    return f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
      {column_block}
    )
    """.strip()


def publish_table(
    duckdb_connection: duckdb.DuckDBPyConnection,
    mysql_cursor: mysql.connector.cursor.MySQLCursor,
    database: str,
    full_table_name: str,
    published_at: datetime,
    source_run_id: str,
) -> int:
    schema_name, table_name = full_table_name.split(".", 1)
    frame = duckdb_connection.execute(f"select * from {schema_name}.{table_name}").df()

    expected_columns = list(frame.columns) + ["_published_at", "_source_run_id"]

    if table_exists(mysql_cursor, database, table_name):
        current_columns = existing_column_names(mysql_cursor, database, table_name)
        if current_columns != expected_columns:
            mysql_cursor.execute(f"DROP TABLE `{table_name}`")
            mysql_cursor.execute(create_table_sql(table_name, frame))
    else:
        mysql_cursor.execute(create_table_sql(table_name, frame))

    mysql_cursor.execute(f"TRUNCATE TABLE `{table_name}`")

    if frame.empty:
        return 0

    insert_columns = list(frame.columns) + ["_published_at", "_source_run_id"]
    placeholders = ", ".join(["%s"] * len(insert_columns))
    quoted_columns = ", ".join(f"`{column}`" for column in insert_columns)
    insert_sql = f"INSERT INTO `{table_name}` ({quoted_columns}) VALUES ({placeholders})"

    rows = []
    for values in frame.itertuples(index=False, name=None):
        normalized_values = tuple(normalize_value(value) for value in values)
        rows.append((*normalized_values, published_at, source_run_id))

    mysql_cursor.executemany(insert_sql, rows)
    return len(rows)


def main() -> int:
    args = parse_args()
    repo_root = resolve_repo_root()
    duckdb_path = resolve_duckdb_path(args, repo_root)
    source_run_id = args.source_run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    published_at = datetime.now(timezone.utc).replace(tzinfo=None)

    print(f"Using DuckDB source: {duckdb_path}")
    print(f"Publishing to MySQL: {args.mysql_host}:{args.mysql_port}/{args.mysql_database}")

    tables = parse_table_identifiers(args.tables)

    duckdb_connection = duckdb.connect(str(duckdb_path), read_only=True)

    bootstrap_connection = mysql_connect_with_retries(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database="mysql",
    )
    bootstrap_cursor = bootstrap_connection.cursor()
    bootstrap_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{args.mysql_database}`")
    bootstrap_connection.commit()
    bootstrap_cursor.close()
    bootstrap_connection.close()

    mysql_connection = mysql_connect_with_retries(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
    )
    mysql_cursor = mysql_connection.cursor()

    summary: dict[str, int] = {}
    start_time = time.perf_counter()

    try:
        for full_table_name in tables:
            row_count = publish_table(
                duckdb_connection=duckdb_connection,
                mysql_cursor=mysql_cursor,
                database=args.mysql_database,
                full_table_name=full_table_name,
                published_at=published_at,
                source_run_id=source_run_id,
            )
            summary[full_table_name] = row_count
            print(f"Published {row_count} rows: {full_table_name}")

        mysql_connection.commit()
    finally:
        mysql_cursor.close()
        mysql_connection.close()
        duckdb_connection.close()

    runtime_seconds = round(time.perf_counter() - start_time, 2)
    print(f"Publish completed in {runtime_seconds}s")

    for table_name, row_count in summary.items():
        print(f"  - {table_name}: {row_count} rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
