# ADR 0001: Choose DuckDB + dbt-duckdb for Local Lakehouse

## Status
Accepted

## Context
The project must run locally on Linux with a single bootstrap command and no managed warehouse dependencies. It should still reflect modern Analytics Engineering workflows expected in senior-level roles.

## Decision
Use:
- DuckDB as the local analytical warehouse engine.
- dbt-duckdb for transformation, testing, and documentation.

## Rationale
- Zero external infrastructure and fast local iteration.
- SQL-first transformation workflow aligned with production dbt practices.
- Easy CI portability (GitHub Actions runner can execute full pipeline).
- Native Parquet interoperability for Bronze/Silver/Gold handoff.

## Consequences
Positive:
- Deterministic local development and reproducibility.
- Strong portfolio signal for modern ELT and analytics engineering patterns.

Tradeoffs:
- Not distributed compute; large-scale Spark semantics are out of scope.
- Runtime profile differs from cloud warehouses (though modeling patterns transfer).
