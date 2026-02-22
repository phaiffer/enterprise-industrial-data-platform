# Notebook-First Lakehouse (DuckDB + dbt + Great Expectations)

Portfolio-grade local data platform project showing a modern Analytics Engineering workflow with notebook-led development, Medallion architecture, dbt marts, data quality gates, observability metrics, and CI automation.

## Problem Statement
Hiring teams want to see production-minded data engineering skills, not isolated notebooks. This repository demonstrates how to build a reproducible, notebook-first Lakehouse pipeline that:
- ingests real public datasets at runtime,
- applies Bronze/Silver/Gold contracts,
- curates analytics models in dbt,
- enforces quality with Great Expectations,
- and runs headlessly in CI with Papermill.

## Architecture
```text
Raw Public Data (FiveThirtyEight CSV over HTTPS)
        |
        v
/data/raw (runtime download, gitignored)
        |
        v
Bronze Parquet (/lakehouse/bronze)
  + metadata: _ingested_at, _source, _dataset, _file_path, _row_hash, _batch_id
        |
        v
Silver Parquet (/lakehouse/silver)
  typed + deduplicated + normalized keys
        |
        v
DuckDB + dbt-duckdb (/dbt/lakehouse_dbt)
  stg_* -> marts (dim_*, fact_*, kpi_*)
        |
        v
Gold Parquet (/lakehouse/gold) + docs + observability + DQ reports
```

## Notebook Storyline
- `notebooks/00_project_overview.ipynb`: architecture, conventions, run parameters
- `notebooks/01_data_sources.ipynb`: source inventory, licensing, ingestion design
- `notebooks/02_bronze_ingestion.ipynb`: download CSV, standardize schema, write Bronze + metadata
- `notebooks/03_silver_cleaning.ipynb`: cleaning, typing, dedupe, deterministic Silver outputs
- `notebooks/04_dbt_marts_gold.ipynb`: dbt run + dbt docs generate + Gold export
- `notebooks/05_data_quality.ipynb`: Great Expectations suite/checkpoint and report outputs
- `notebooks/06_observability.ipynb`: pipeline metrics summary and runtime chart
- `notebooks/07_portfolio_story.ipynb`: executive-level narrative and outcomes

## Datasets and Licensing
All datasets are downloaded at runtime from FiveThirtyEight (no committed raw data):
- `college-majors/recent-grads.csv`: https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/recent-grads.csv
- `bechdel/movies.csv`: https://raw.githubusercontent.com/fivethirtyeight/data/master/bechdel/movies.csv

Source repository: https://github.com/fivethirtyeight/data  
License: CC BY 4.0 (Attribution 4.0 International).

## Quickstart
```bash
make setup
make run-all
make dbt-test
make dq
```

To launch notebooks interactively:
```bash
make notebooks
```

## Medallion Layers
- Bronze: immutable standardized raw with ingestion metadata and row hashes.
- Silver: typed, deduplicated, null-handled datasets with business keys.
- Gold: curated analytics marts and KPI tables produced by dbt.

## dbt Docs
Generate docs after running models:
```bash
make dbt-run
```
Then serve docs locally:
```bash
.venv/bin/dbt docs serve --project-dir dbt/lakehouse_dbt --profiles-dir dbt/lakehouse_dbt
```

## Quality Gates
Great Expectations checkpoint validates Silver `bechdel_movies` data for:
- non-zero row count,
- key column null checks,
- type constraints,
- accepted values (`PASS`/`FAIL`).

Run quality gate:
```bash
make dq
```
Data Docs HTML output: `reports/great_expectations/`

## Add a New Dataset Connector
1. Add dataset metadata in `src/common/datasets.py` (URL, filename, description).
2. Extend cleaning logic in `src/common/pipeline.py`.
3. Update notebook logic in `02_bronze_ingestion.ipynb` and `03_silver_cleaning.ipynb`.
4. Add dbt staging/marts model and tests in `dbt/lakehouse_dbt/models/`.
5. Add or update Great Expectations suite/checkpoint for the new Silver table.
6. Re-run `make run-all && make dbt-test && make dq`.
