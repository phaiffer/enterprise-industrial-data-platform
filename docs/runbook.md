# Runbook

## Standard Run
```bash
make setup
make run-all
make dbt-test
make dq
```

## Common Errors and Fixes

### 1) Notebook execution fails with import errors
- Symptom: `ModuleNotFoundError: src...`
- Fix: ensure commands are run from repository root and `make setup` completed.

### 2) dbt cannot find profile
- Symptom: dbt profile lookup error
- Fix: confirm `dbt/lakehouse_dbt/profiles.yml` exists and use provided Make targets.

### 3) Great Expectations checkpoint fails
- Symptom: `RuntimeError` in `scripts/run_dq.py`
- Fix:
  - inspect `reports/dq_result_summary.json`
  - open Data Docs in `reports/great_expectations/`
  - validate Silver table exists at `lakehouse/silver/bechdel_movies/data.parquet`

### 4) Raw download intermittently fails
- Symptom: network timeout during ingestion
- Fix: rerun pipeline; ingestion uses retry with backoff and idempotent deterministic file names.

## Rerun Strategy
- Normal rerun: execute `make run-all` (safe, idempotent overwrite behavior).
- Force re-download raw files: pass `--force-refresh` to `scripts/run_notebooks.py`.

## Backfill Strategy
- Run pipeline with a target `--run-date` using `scripts/run_notebooks.py`.
- Outputs remain deterministic because each layer overwrites stable table paths.
- Keep historical snapshots by extending scripts to write partitioned paths by `run_date` if needed.
