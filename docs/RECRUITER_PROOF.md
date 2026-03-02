# Recruiter Proof

## Why This Project Signals Senior Data/Cloud Capability

### 1) Data Governance
- ADR-driven architecture decisions are explicit and versioned (`docs/adr/`).
- Data contracts and schema expectations are documented (`docs/data_contracts.md`).
- Layered Medallion structure enforces lifecycle boundaries from raw to curated outputs.

### 2) Reproducibility
- Deterministic local bootstrap and execution path (`make setup`, `make run-all`).
- CI executes the same core validation path used locally.
- Generated artifacts are excluded from version control for clean reruns and credible diffs.

### 3) Quality Enforcement
- dbt tests are first-class validation gates (`make dbt-test`).
- Great Expectations checkpoint enforces Silver-layer data quality (`make dq`).
- Linting is included in CI to preserve code quality standards (`make lint`).

### 4) Observability
- Mode 2 includes Airflow, Prometheus, and Grafana for orchestration and monitoring coverage.
- `infra_smoke` validates multi-service health (Kafka, MinIO, Spark, Airflow) and writes structured results to `reports/infra_smoke_result.json`.
- Smoke artifacts provide auditable evidence of environment health.

### 5) Platform Thinking
- Two-Mode strategy separates fast local delivery from enterprise infra breadth.
- Optional MySQL serving layer demonstrates downstream product-serving patterns.
- Production path is documented across cloud providers with governance and CI/CD promotion controls.

## Suggested Demo Script (Command Sequence)
Run from repository root:

```bash
make setup
make run-all
make dbt-test
make dq
make dbt-preview
```

Optional enterprise breadth demo:

```bash
make infra-up
make infra-status
make infra-smoke
cat reports/infra_smoke_result.json
make infra-down
```

## Talking Track for Interviews
- "Mode 1 proves fast, deterministic analytics engineering delivery with built-in quality gates."
- "Mode 2 demonstrates platform breadth without forcing infra complexity into every commit cycle."
- "The ADR and governance layer shows I optimize for explicit trade-offs, not ad hoc tooling choices."
