# ADR 0003: Add Optional MySQL Serving Layer

## Status
Accepted

## Context
The platform already uses local Parquet + DuckDB + dbt-duckdb as the default Medallion workflow. We also need a relational serving option that mirrors how downstream BI tools and APIs often consume curated data.

## Decision
Introduce an **optional** MySQL serving layer in Docker and a publish step that loads curated Gold marts/KPIs from DuckDB into MySQL tables.

- Lakehouse storage remains Parquet under `lakehouse/`.
- dbt-duckdb remains the default transformation engine.
- MySQL is used only as a serving endpoint.

## Why MySQL For Serving
- Familiar RDBMS interface for analysts, BI tools, and API integration.
- Good fit for small curated marts and KPI tables.
- Demonstrates separation of compute/storage (Lakehouse) from serving.

## Tradeoffs
Benefits:
- Easier SQL consumption for tools expecting a relational database.
- Clear portfolio signal for data product serving patterns.

Costs:
- Additional optional runtime and operational surface area.
- Requires an explicit publish step to keep serving tables current.

## Medallion Fit
- Bronze/Silver/Gold lifecycle remains in the Lakehouse.
- MySQL sits downstream of Gold as a read-oriented serving layer.
- Serving tables are refreshed idempotently via truncate + insert and metadata tagging.
