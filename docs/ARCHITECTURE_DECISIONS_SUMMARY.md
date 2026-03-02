# Architecture Decisions Summary

## Purpose
This document consolidates the key architecture decisions for the Enterprise Industrial Data Platform and explains why the current implementation favors fast local validation with optional enterprise breadth.

## ADR Snapshot
| ADR | Decision | Why It Matters |
|---|---|---|
| `0001` | DuckDB + `dbt-duckdb` as default analytics engine | Enables deterministic local execution, low friction onboarding, and CI portability. |
| `0002` | Two-Mode repository structure | Separates fast local proof (`Mode 1`) from infra demonstrations (`Mode 2`) without coupling them. |
| `0003` | Optional MySQL serving layer | Demonstrates downstream serving patterns for BI/API consumers while keeping Lakehouse storage primary. |
| `0004` | Lightweight formatting + Mode 1 CI policy | Keeps review quality high and CI runtime stable for portfolio and collaboration credibility. |

## DuckDB vs Spark Default Mode Trade-off
### Why DuckDB is the default
- Fast local feedback for interviews, code reviews, and CI.
- No cluster dependency for core Medallion and dbt validation.
- Easy reproducibility on a single machine.

### Why Spark is still present
- Spark appears in `Mode 2` to demonstrate distributed compute patterns and platform fluency.
- Streaming and orchestration demos remain possible without forcing cluster complexity into every developer workflow.

### Explicit trade-off
- Default mode optimizes for reproducibility and delivery speed.
- Optional mode optimizes for architectural breadth and enterprise narrative.

## Two-Mode Strategy
### Mode 1 (default)
- Notebook-first local lakehouse workflow.
- Core validation gates: `make run-all`, `make dbt-test`, `make dq`.
- CI target for deterministic checks.

### Mode 2 (optional)
- Docker-based infra stack (Kafka, MinIO, Spark, Airflow, Prometheus, Grafana).
- Supports infra smoke checks and platform demonstration.
- Isolated from mandatory CI path to prevent heavy operational coupling.

### Contract between modes
- Shared repo standards and docs.
- Independent runtime lifecycles.
- No requirement to run Mode 2 to validate Mode 1 correctness.

## Governance Model
Governance is implemented as layered controls instead of a single gate:
- Data contracts and naming conventions: `docs/data_contracts.md`
- Transformation tests and lineage discipline: `dbt test`, dbt model schema assertions.
- Data quality checkpointing: Great Expectations (`make dq`).
- Artifact and metrics traceability: `reports/` outputs and pipeline metrics JSON.
- Policy governance via ADRs: design choices are documented and versioned under `docs/adr/`.

## Architectural Positioning
This project intentionally balances two expectations of senior data/cloud roles:
- Production-minded reproducibility and quality enforcement.
- Demonstrable platform design choices with clear trade-offs and upgrade paths.
