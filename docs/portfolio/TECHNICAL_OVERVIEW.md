# Technical Overview

## Architecture (Context + Containers)

```mermaid
flowchart LR
    source[Source Systems]
    ops[Operations Team]
    analyst[Data Analyst]
    web[Website / Sales Materials]
    eidp[EIDP Platform]

    source -->|Batch source records| eidp
    eidp -->|Validated KPIs and manifests| ops
    eidp -->|Curated datasets| analyst
    eidp -->|Embed-ready exports| web
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
    participant N as Notebook Runner
    participant L as Lakehouse (Bronze/Silver/Gold)
    participant D as dbt
    participant Q as Great Expectations
    participant P as Portfolio Exporter
    participant W as Website/Sales

    S->>N: Extract datasets
    N->>L: Write Bronze
    N->>L: Clean to Silver
    N->>D: Run dbt models + tests
    D->>L: Materialize Gold marts/KPIs
    N->>Q: Run checkpoint
    Q-->>N: DQ results
    N->>P: Provide metrics and artifacts
    P->>P: Build charts + manifest
    P->>W: Publish embed-ready files
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
  - Deterministic daily bucketing logic for throughput charting from year-level records.
- Re-runnable exports:
  - `make portfolio-export RUN_ID=<id>` creates isolated run folders.
  - `latest.txt` points to the newest run without mutating prior runs.
- Traceability:
  - `manifests/run.json` captures `run_id`, timestamps, git SHA, sources, row counts, DQ summary, and exported file sizes.

## Observability

- Runtime metrics:
  - Stage runtimes are tracked in `reports/metrics/pipeline_metrics.json`.
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
