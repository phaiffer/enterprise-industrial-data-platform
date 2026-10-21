# Enterprise Industrial Data Platform

![CI](https://github.com/phaiffer/enterprise-industrial-data-platform/actions/workflows/ci.yml/badge.svg)

Technical demonstration of a local, notebook-first lakehouse. The optional enterprise infrastructure is a separate mode.

> **Demonstração técnica:** os dados são públicos e históricos (`recent_grads` e `bechdel_movies`, FiveThirtyEight). Este projeto não representa uma entrega a cliente, resultados industriais ou um sistema em produção. Veja o [resumo para a vitrine](docs/portfolio/DEMO_OVERVIEW.md).

## Two Execution Modes

### Mode 1 (Default): Local Lakehouse
Mode 1 is the primary path for development, interviews, and CI.
Use Python 3.11. On a fresh clone, create the environment with `make setup PYTHON=python3.11` (or pass the full path to a Python 3.11 executable). The setup registers the `notebook_lakehouse` kernel used by `make run-all`.

```bash
make setup
make run-all
make dbt-run
make dbt-test
make dq
make dbt-preview
```

What this proves:
- End-to-end local Medallion flow (Bronze -> Silver -> Gold) with DuckDB + Parquet
- dbt model/test discipline (`make dbt-test`)
- Great Expectations quality gate (`make dq`)
- Queryable KPI outputs (`make dbt-preview`)

Expected runtime outputs:
- `lakehouse/bronze/*`, `lakehouse/silver/*`, `lakehouse/gold/*`
- `lakehouse/warehouse/lakehouse.duckdb`
- `reports/great_expectations/`
- `reports/metrics/pipeline_metrics.json`

### Mode 2 (Optional): Enterprise Infra
Mode 2 is intentionally optional and **not required** to validate Mode 1 or pass CI.

```bash
make infra-up
make infra-status
make infra-logs
make infra-smoke
make infra-down
```

Primary URLs:
- Airflow: `http://localhost:8089` (`AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` from `.env`)
- Grafana: `http://localhost:3000` (`GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` from `.env`)
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8088`
- MinIO Console: `http://localhost:9001` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env`)
- Spark Master UI: `http://localhost:18080`
- Spark Worker UI: `http://localhost:18081`

Smoke demo:
1. Start services with `make infra-up`.
2. Open Airflow and trigger DAG `infra_smoke_dag`.
3. Confirm artifact exists at `reports/infra_smoke/ok.txt`.
4. Optional CLI verification: `make infra-smoke` (writes `reports/infra_smoke_result.json`).
5. Stop services with `make infra-down`.

## Local Validation
Run these commands to regenerate the local pipeline and checks:

```bash
make setup
make run-all
make dbt-test
make dq
```

Then inspect KPI outputs:

```bash
make dbt-preview
```

## Portfolio Pack
Generate charts and a run manifest from the Mode 1 outputs with:

```bash
make portfolio
```

Documentation:
- `docs/portfolio/README.md`
- `docs/portfolio/DEMO_OVERVIEW.md`
- `docs/portfolio/TECHNICAL_OVERVIEW.md`
- `docs/portfolio/WEBSITE_EMBED.md`
- `docs/portfolio/VALIDACAO_LOCAL_2026-10-20.md`

## Showcase Visualization Layer
The repository includes a local Flask visualization in `showcase/`. The committed evidence bundle in `showcase/static/evidence/` is a dated snapshot; it remains visible on a fresh clone. Regenerate it only after a successful local pipeline, dbt test and Great Expectations run:

```bash
make run-all
make dbt-test
make dq
make portfolio-export
make showcase-evidence
```

The manifest records the source, datasets, row counts and checks for this run. Figures with derived calendar days or a local 20-second reference are excluded from the institutional showcase because they do not measure operational throughput or a production SLA.

Run locally:

```bash
make showcase
```

Open:
- `http://localhost:8000/` (Overview)
- `http://localhost:8000/architecture` (Architecture)
- `http://localhost:8000/lineage` (dbt lineage summary)
- `http://localhost:8000/dq` (Data Quality summary)

### Showcase Validation
After starting the app, validate the routes:

```bash
curl -I http://localhost:8000/
curl -I http://localhost:8000/architecture
curl -I http://localhost:8000/lineage
curl -I http://localhost:8000/dq
```

The pages label this as a local technical demonstration and link to the committed snapshot manifest. If the bundle is absent, they show a preparation message.

## Visual Demo (Screenshots)
Use the [current showcase capture](docs/assets/showcase-overview.png), the [quality checks](showcase/static/evidence/quality_checks.png) and the [historical film chart](showcase/static/evidence/bechdel_pass_rate_by_year.png). The older SVGs in `docs/assets/` are illustrative placeholders, not execution evidence. See [publication guidance](docs/portfolio/WEBSITE_EMBED.md) for captions and exclusions.

## Architecture

### Mode 1 (Default)
```text
FiveThirtyEight CSV (HTTPS)
        |
        v
data/raw -> bronze parquet -> silver parquet -> dbt (DuckDB) -> gold marts/KPIs
                                                          |
                                                          v
                                       dbt tests + Great Expectations + reports
```

### Mode 2 (Optional)
```text
Kafka <-> Spark <-> MinIO
   |          |        |
   +----------+--------+--> Airflow (8089) --> reports/infra_smoke/ok.txt

Prometheus (9090) <- cAdvisor (8082) -> Grafana (3000)
```

## Repo Organization

Root contains Mode 1 core and docs:
- `notebooks/`, `src/`, `scripts/`, `dbt/`, `great_expectations/`
- `data/`, `lakehouse/`, `reports/`
- `docs/`, `tests/`, `Makefile`, `.github/workflows/ci.yml`

Enterprise-only assets are isolated under:
- `modes/mode2_enterprise/`

## Optional: Publish Gold to MySQL

```bash
make mysql-up
make mysql-publish
make mysql-preview
make mysql-down
```

## Tradeoffs and ADRs
- DuckDB + dbt for local Lakehouse speed and reproducibility: `docs/adr/0001_duckdb_dbt_choice.md`
- Two-Mode structure to separate fast local validation from enterprise demos: `docs/adr/0002_two_mode_repo.md`
- Optional MySQL serving layer for downstream BI/API consumption: `docs/adr/0003_mysql_serving_layer.md`
- Formatting and CI policy (fast Mode 1 CI with lightweight lint/format): `docs/adr/0004_formatting_and_ci_policy.md`

## CI and Data Policy
- CI runs Mode 1 only (`make setup`, `make run-all`, `make dbt-test`, `make dq`).
- Docker Compose is intentionally excluded from CI.
- Runtime outputs in `lakehouse/` and `reports/` are not committed.
- Raw datasets are downloaded at runtime and kept out of version control.
