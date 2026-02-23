#!/usr/bin/env python3
"""Preview published MySQL serving tables for quick verification."""

from __future__ import annotations

import argparse
import os
import time
from typing import Sequence

import mysql.connector

DEFAULT_PREVIEW_TABLES = [
    "kpi_major_outcomes",
    "kpi_movie_representation",
    "dim_major_category",
    "fact_major_employment",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview MySQL serving layer tables")
    parser.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "localhost"))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", "yourpassword"))
    parser.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", "eidp_serving"))
    parser.add_argument("--limit", type=int, default=5)
    return parser.parse_args()


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
            return mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                autocommit=True,
            )
        except mysql.connector.Error as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            time.sleep(retry_seconds)

    raise RuntimeError(f"Unable to connect to MySQL after {max_attempts} attempts: {last_error}")


def list_tables(cursor: mysql.connector.cursor.MySQLCursor, database: str) -> list[str]:
    cursor.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = %s
        order by table_name
        """,
        (database,),
    )
    return [row[0] for row in cursor.fetchall()]


def print_row_counts(cursor: mysql.connector.cursor.MySQLCursor, tables: Sequence[str]) -> None:
    print("Row counts:")
    for table_name in tables:
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        row_count = int(cursor.fetchone()[0])
        print(f"  - {table_name}: {row_count}")


def print_preview(cursor: mysql.connector.cursor.MySQLCursor, table_name: str, limit: int) -> None:
    cursor.execute(f"SELECT * FROM `{table_name}` LIMIT {limit}")
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]

    print(f"\nPreview: {table_name} (top {limit})")
    print(" | ".join(column_names))

    if not rows:
        print("<no rows>")
        return

    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))


def main() -> int:
    args = parse_args()

    print(f"Connecting to MySQL: {args.mysql_host}:{args.mysql_port}/{args.mysql_database}")
    connection = mysql_connect_with_retries(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
    )
    cursor = connection.cursor()

    try:
        tables = list_tables(cursor, args.mysql_database)
        if not tables:
            raise RuntimeError("No tables found in MySQL serving database. Run `make mysql-publish` first.")

        print("Tables:")
        for table_name in tables:
            print(f"  - {table_name}")

        print()
        print_row_counts(cursor, tables)

        for table_name in DEFAULT_PREVIEW_TABLES:
            if table_name in tables:
                print_preview(cursor, table_name, args.limit)
    finally:
        cursor.close()
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
