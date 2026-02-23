# Enterprise Industrial Data Platform

![CI](https://github.com/phaiffer/enterprise-industrial-data-platform/actions/workflows/ci.yml/badge.svg)

Two-mode portfolio repository with a default notebook-first local Lakehouse and an optional enterprise infrastructure stack.

## Two Execution Modes

### Mode 1 (Default): Notebook-First Local Lakehouse
Mode 1 is the default path for development, demos, and CI.

Stack:
- DuckDB + Parquet
- dbt (`dbt-duckdb`)
- Great Expectations
- Papermill + Jupyter notebooks

Quickstart:
```bash
make setup
make run-all
make dbt-run
make dbt-test
make dq
make dbt-preview
```

Expected outputs:
- `lakehouse/bronze/*`, `lakehouse/silver/*`, `lakehouse/gold/*` (runtime Parquet; gitignored)
- `dbt/lakehouse_dbt/target/` (dbt docs/artifacts; gitignored)
- `reports/great_expectations/` (Data Docs)
- `reports/metrics/pipeline_metrics.json`

### Mode 2 (Optional): Enterprise Infra (Docker Compose)
Mode 2 is optional and **not required** to validate Mode 1 quality, correctness, or CI readiness.

Commands:
```bash
make infra-up
make infra-status
make infra-logs
make infra-smoke
make infra-down
```

Primary endpoints:
- Airflow UI: `http://localhost:8089` (`admin` / `admin`)
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8088`
- MinIO Console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- Spark Master UI: `http://localhost:18080`
- Spark Worker UI: `http://localhost:18081`

Smoke demo (real artifact):
1. Start infra: `make infra-up`
2. Open Airflow UI (`http://localhost:8089`) and trigger `infra_smoke_dag`
3. Confirm host artifact exists: `reports/infra_smoke/ok.txt`
4. Optional automated check: `make infra-smoke`
5. Stop infra: `make infra-down`

## Architecture

### Mode 1 Diagram (Default)
```text
FiveThirtyEight CSV (HTTPS)
        |
        v
data/raw (runtime)
        |
        v
Bronze Parquet -> Silver Parquet -> DuckDB + dbt -> Gold Parquet
                                         |
                                         v
                          Great Expectations + metrics reports
```

### Mode 2 Diagram (Optional)
```text
Kafka <-> Spark <-> MinIO
   |          |        |
   +----------+--------+----> Airflow (8089)
                               |
                               v
                     reports/infra_smoke/ok.txt

Prometheus (9090) <- cAdvisor (8082) -> Grafana (3000)
```

## Repo Organization

- `notebooks/`: portfolio storyline notebooks (`00` to `07`)
- `src/`: ingestion, transforms, shared utilities
- `scripts/`: operational entrypoints (`run_notebooks`, `run_dq`, previews)
- `dbt/lakehouse_dbt/`: analytics models, tests, docs generation
- `great_expectations/`: canonical GE project (single source of truth)
- `docs/`: ADRs, runbooks, architecture notes, demo assets placeholders
- `modes/mode1_local/`: Mode 1 scoped documentation
- `modes/mode2_enterprise/`: enterprise compose/orchestration/observability/conf
- `lakehouse/`: runtime local medallion output root (tracked placeholders only)
- `reports/`: runtime reports and smoke artifacts (gitignored except placeholders)

## What Recruiters Should Look At

Notebook story:
- `notebooks/00_project_overview.ipynb`
- `notebooks/01_data_sources.ipynb`
- `notebooks/02_bronze_ingestion.ipynb`
- `notebooks/03_silver_cleaning.ipynb`
- `notebooks/04_dbt_marts_gold.ipynb`
- `notebooks/05_data_quality.ipynb`
- `notebooks/06_observability.ipynb`
- `notebooks/07_portfolio_story.ipynb`

Engineering quality signals:
- dbt model validation: `make dbt-test`
- Great Expectations quality gate: `make dq`
- Mode 1 CI pipeline (badge above): `.github/workflows/ci.yml`

## Demo Assets

Place screenshots in `docs/assets/` (placeholders already included):
- `mode1_notebook_execution.png`
- `mode1_dbt_docs.png`
- `mode2_airflow_smoke_dag.png`
- `mode2_grafana_overview.png`

## CI Scope and Data Policy

- CI is Mode 1 only (`make setup`, `make run-all`, `make dbt-test`, `make dq`).
- Docker/compose is intentionally excluded from CI.
- Runtime data outputs under `lakehouse/` and `reports/` are not committed.

## Mode-Specific Docs

- `modes/mode1_local/README.md`
- `modes/mode2_enterprise/README.md`
- `docs/runbook.md`
- `docs/runbook_mode2.md`
