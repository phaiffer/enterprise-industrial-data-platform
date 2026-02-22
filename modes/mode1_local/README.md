# Mode 1: Notebook-First Local Lakehouse (Default)

This is the default workflow and primary portfolio artifact.

## Purpose
Run the full Medallion pipeline locally with deterministic outputs and minimal setup.

## Prerequisites
- Linux
- Python 3.11+
- `make`

## Run
```bash
make setup
make run-all
make dbt-test
make dq
```

## What Gets Produced
- Raw runtime downloads: `data/raw/` (gitignored)
- Bronze: `lakehouse/bronze/*/data.parquet`
- Silver: `lakehouse/silver/*/data.parquet`
- Gold: `lakehouse/gold/*/data.parquet`
- dbt docs and metadata: `dbt/lakehouse_dbt/target/`
- Great Expectations docs: `reports/great_expectations/`
- Pipeline metrics: `reports/metrics/pipeline_metrics.json`

## Regenerate Outputs
- Standard rerun: `make run-all`
- Regenerate dbt docs and marts: `make dbt-run`
- Run validations only: `make dbt-test && make dq`
- Clean generated artifacts: `make clean`

## Troubleshooting
- Import errors: run from repo root, then rerun `make setup`.
- dbt profile/path issues: use the Make targets instead of raw dbt commands.
- GE failures: inspect `reports/dq_result_summary.json` and `reports/great_expectations/`.
