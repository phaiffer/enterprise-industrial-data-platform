# Technical Overview

This is a local technical demonstration using historical public FiveThirtyEight datasets. The diagrams below describe the implemented Mode 1 path; no customer environment or production service is represented.

## Architecture (Context + Containers)

```mermaid
flowchart LR
    source[FiveThirtyEight CSVs]
    notebooks[Papermill notebooks]
    lakehouse[Parquet + DuckDB]
    checks[dbt tests + Great Expectations]
    export[Local showcase export]

    source --> notebooks --> lakehouse --> checks --> export
```

```mermaid
flowchart LR
    notebooks[Notebook Orchestration<br/>Papermill + Python]
    lakehouse[Lakehouse Storage<br/>Bronze / Silver / Gold]
    dbt[Transformation Layer<br/>dbt]
    gx[Data Quality Layer<br/>Great Expectations]
    portfolio[Portfolio Exporter<br/>Python + Matplotlib]

    notebooks -->|Write/read layered data| lakehouse
    notebooks -->|Run models/tests| dbt
    notebooks -->|Run checkpoint| gx
    portfolio -->|Read curated outputs| lakehouse
    portfolio -->|Read DQ results| gx
    portfolio -->|Read runtime metrics| notebooks
```

## Data Flow (Sequence)

```mermaid
sequenceDiagram
    participant S as Source Data
    participant M as Make targets
    participant N as Notebook Runner
    participant L as Lakehouse (Bronze/Silver/Gold)
    participant D as dbt
    participant Q as Great Expectations
    participant P as Portfolio Exporter
    participant W as Local showcase bundle

    S->>N: Extract datasets
    N->>L: Write Bronze
    N->>L: Clean to Silver
    N->>D: Run dbt models
    D->>L: Materialize Gold marts/KPIs
    N->>Q: Run checkpoint
    Q-->>N: DQ results
    M->>D: Run dbt tests after notebooks
    M->>Q: Confirm checkpoint after notebooks
    N->>P: Provide metrics and artifacts
    P->>P: Build charts + manifest
    P->>W: Select verified local images + manifest
```

## Layered Model (Medallion)

- Bronze:
  - Raw-ish ingested records with metadata (`_ingested_at`, `_source`, `_dataset`, `_batch_id`, `_row_hash`).
  - Purpose: reproducible landing zone and lineage traceability.
- Silver:
  - Type-cleaned, deduplicated, standardized records.
  - Purpose: trusted analytical base with consistent schemas.
- Gold:
  - Business-facing marts and KPI tables.
  - Purpose: direct consumption by reporting, dashboards, and portfolio exports.

## Data Quality Strategy

- Rule engine: Great Expectations checkpoint on silver datasets.
- Rule types: not-null, accepted values, row-count bounds, and type checks.
- Threshold behavior:
  - Violations are counted and surfaced in `run.json`.
  - Pipeline can fail fast when checkpoint status is unsuccessful.
- Quarantine approach:
  - In this repo, violations are reported through validation results.
  - In production, failing records should be routed to quarantine tables/paths for triage and replay.

## Idempotency and Reproducibility

- Deterministic transformations:
  - Stable row hashing and sorted outputs during ingestion/cleaning.
  - The optional `throughput_trend_daily.png` uses a hash-derived day from year-level movie records. It does not measure actual daily processing and is excluded from the institutional showcase.
- Re-runnable exports:
  - `make portfolio-export RUN_ID=<id>` creates isolated run folders.
  - `latest.txt` points to the newest run without mutating prior runs.
- Traceability:
  - `manifests/run.json` captures `run_id`, timestamps, git SHA, sources, row counts, DQ summary, and exported file sizes.

## Observability

- Runtime metrics:
  - Stage runtimes are tracked in `reports/metrics/pipeline_metrics.json`.
  - The export compares these local runtimes with a 20-second reference, not an operational SLA.
- Quality metrics:
  - Evaluated rules, violations count, and category-level breakdown.
- Export checks:
  - Image size guardrails (`<= 500KB`) enforced during generation.
- Operational artifacts:
  - Executed notebooks and DQ reports are persisted in `reports/`.

## Security and PII Handling Guidance

- Current portfolio datasets are public and non-sensitive.
- For enterprise deployment:
  - Classify columns by sensitivity (PII/confidential/public).
  - Mask or tokenize PII before silver/gold publication.
  - Restrict access by role (least privilege).
  - Avoid exposing private source URLs or secrets in manifests and public artifacts.
  - Add encryption at rest/in transit and secrets management for connectors.
