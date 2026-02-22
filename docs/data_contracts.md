# Data Contracts

## Naming Convention
- Physical layer paths: lowercase snake case (`lakehouse/silver/recent_grads/data.parquet`)
- Column naming: lowercase snake case
- Metadata fields reserved for Bronze and propagated downstream when useful:
  - `_ingested_at`
  - `_source`
  - `_dataset`
  - `_file_path`
  - `_row_hash`
  - `_batch_id`

## Bronze Contract
- Input format: CSV downloaded over HTTPS from FiveThirtyEight.
- Output format: Parquet, one deterministic file per dataset.
- Required metadata columns are appended to every Bronze row.
- Row hashes are SHA-256 on business payload columns for deterministic dedupe.

## Silver Contract
- Explicit typing for numeric and categorical fields.
- Dedupe strategy:
  - `recent_grads`: unique by `major_code`
  - `bechdel_movies`: unique by `imdb`
- Mandatory key checks:
  - `recent_grads`: `major_code`, `major`, `major_category`
  - `bechdel_movies`: `imdb`, `title`, `year`, `binary`

## Gold Contract
- Managed by dbt models in `dbt/lakehouse_dbt/models/marts`.
- Curated objects include:
  - `dim_major_category`
  - `fact_major_employment`
  - `kpi_major_outcomes`
  - `kpi_movie_representation`

## Schema Evolution Rules
- Additive changes are preferred.
- Breaking schema changes require:
  - notebook update,
  - dbt model/test update,
  - Great Expectations suite review,
  - runbook update.
