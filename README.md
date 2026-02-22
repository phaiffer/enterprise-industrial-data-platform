# Enterprise Industrial Data Platform

A portfolio-grade **Two-Mode Repo** that balances fast local execution with optional enterprise platform breadth.

## Two Execution Modes

### Mode 1 (Default): Notebook-First Local Lakehouse
Use this for day-to-day development, demos, and CI.

- Stack: Jupyter + Papermill + DuckDB + dbt-duckdb + Great Expectations
- Storage: local Parquet under `lakehouse/` (gitignored)
- Runtime profile: fast, reproducible, zero external services

Quickstart:
```bash
make setup
make run-all
make dbt-test
make dq
```

Primary outputs:
- Bronze/Silver/Gold Parquet in `lakehouse/`
- dbt artifacts in `dbt/lakehouse_dbt/target/`
- Data quality artifacts in `reports/great_expectations/`
- Metrics in `reports/metrics/pipeline_metrics.json`

Typical runtime (laptop): ~1-3 minutes.

### Mode 2 (Optional): Enterprise Stack (Docker Compose)
Use this to demonstrate platform breadth (orchestration, streaming/storage infra, observability).

- Services: Kafka, MinIO, Spark, Airflow, Prometheus, Grafana, Postgres
- Compose file: `modes/mode2_enterprise/docker-compose.enterprise.yml`
- Independent from Mode 1; not required for CI

Commands:
```bash
make infra-up
make infra-status
make infra-smoke
make infra-logs
make infra-down
```

## Architecture

### Mode 1 Diagram (Default)
```text
FiveThirtyEight CSV (HTTPS)
        |
        v
/data/raw (runtime download)
        |
        v
Bronze Parquet (lakehouse/bronze)
  + metadata columns
        |
        v
Silver Parquet (lakehouse/silver)
  typed + deduped
        |
        v
DuckDB + dbt-duckdb (staging + marts)
        |
        v
Gold Parquet (lakehouse/gold) + GE reports + metrics
```

### Mode 2 Diagram (Optional)
```text
                   +-----------------------+
                   |      Airflow UI       |
                   |  localhost:8089       |
                   +-----------+-----------+
                               |
                               v
+---------+   +---------+   +---------+   +---------+
| Kafka   |   | Spark   |   | MinIO   |   | Postgres|
| 9092    |<->| 7077    |<->| 9000    |   | 5432    |
+---------+   +---------+   +---------+   +---------+
                               |
                               v
                        reports/infra_smoke

Observability sidecar:
Prometheus (9090) <- cAdvisor (8082) -> Grafana (3000)
```

## What To Show Recruiters
- Notebook storyline:
  - `notebooks/00_project_overview.ipynb`
  - `notebooks/01_data_sources.ipynb`
  - `notebooks/02_bronze_ingestion.ipynb`
  - `notebooks/03_silver_cleaning.ipynb`
  - `notebooks/04_dbt_marts_gold.ipynb`
  - `notebooks/05_data_quality.ipynb`
  - `notebooks/06_observability.ipynb`
  - `notebooks/07_portfolio_story.ipynb`
- dbt docs:
  - Generate: `make dbt-run`
  - Serve: `.venv/bin/dbt docs serve --project-dir dbt/lakehouse_dbt --profiles-dir dbt/lakehouse_dbt`
- Great Expectations Data Docs:
  - `reports/great_expectations/index.html`
- CI scope (Mode 1 only):
  - `make setup`
  - `make run-all`
  - `make dbt-test`
  - `make dq`

## Decision: Why Two-Mode
- Mode 1 gives deterministic local execution and clean portfolio signal.
- Mode 2 demonstrates enterprise platform capability without forcing heavy infra for every run.
- Separation keeps CI fast and onboarding friction low while still showing orchestration/infra maturity.

## Data Sources and Licensing
Runtime-ingested datasets (no committed raw data):
- `https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/recent-grads.csv`
- `https://raw.githubusercontent.com/fivethirtyeight/data/master/bechdel/movies.csv`

Source repository: `https://github.com/fivethirtyeight/data`  
License: CC BY 4.0.

## Mode-Specific Docs
- Local mode: `modes/mode1_local/README.md`
- Enterprise mode: `modes/mode2_enterprise/README.md`
- Mode 2 runbook: `docs/runbook_mode2.md`
- ADRs:
  - `docs/adr/0001_duckdb_dbt_choice.md`
  - `docs/adr/0002_two_mode_repo.md`

## Demo Assets
- Folder: `docs/assets/`
- Suggested screenshots to add:
  - `mode1_notebook_execution.png`
  - `mode1_dbt_docs.png`
  - `mode2_airflow_smoke_dag.png`
  - `mode2_grafana_overview.png`

## Suggested Repository Topics
`data-engineering`, `analytics-engineering`, `duckdb`, `dbt`, `great-expectations`, `jupyter-notebook`, `papermill`, `airflow`, `spark`, `kafka`, `minio`, `prometheus`, `grafana`, `lakehouse`, `medallion-architecture`
