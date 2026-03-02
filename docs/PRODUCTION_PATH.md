# Production Path

## Scope
Current repository is a demo/portfolio stack. This document outlines the minimum architecture changes required to promote it into a production deployment on AWS, Azure, or GCP.

## Cloud Landing Blueprint
### Common target shape
- Dev, staging, and production environments with isolated state.
- Managed object storage for Bronze/Silver/Gold datasets.
- Managed orchestration and metadata services.
- Centralized secrets and identity-based access.
- CI/CD with gated promotion across environments.

### Provider mapping
| Capability | AWS | Azure | GCP |
|---|---|---|---|
| Object storage | S3 | ADLS Gen2 | GCS |
| Spark runtime | EMR / Glue / Databricks | Synapse / Databricks | Dataproc / Databricks |
| Orchestration | MWAA / managed Airflow | Azure Data Factory / Airflow on AKS | Cloud Composer |
| Metadata store | RDS Postgres | Azure Database for PostgreSQL | Cloud SQL Postgres |
| Secrets | AWS Secrets Manager | Azure Key Vault | Secret Manager |
| Monitoring | CloudWatch + Prometheus/Grafana | Azure Monitor + Prometheus/Grafana | Cloud Monitoring + Prometheus/Grafana |

## Required Upgrades
### 1) Storage switch (S3/ADLS/GCS)
- Replace local paths and MinIO endpoints with cloud object store URIs.
- Standardize partitioning and retention by layer (`bronze`, `silver`, `gold`).
- Enforce encryption at rest and bucket/container lifecycle policies.

### 2) Metadata store upgrade
- Move Airflow metadata and serving metadata to managed Postgres.
- Enable automated backup, point-in-time restore, and high availability.
- Add migration policy for schema changes.

### 3) Secrets manager integration
- Remove plain credentials from runtime env files in deployed environments.
- Fetch runtime credentials from cloud-native secrets manager.
- Use workload identity/role-based auth instead of static long-lived keys.

### 4) Compute and orchestration hardening
- Run Spark workloads on managed cluster services with autoscaling.
- Replace local Docker Compose orchestration with managed schedulers (managed Airflow or equivalent).
- Introduce retry policies, SLAs, and alerting for critical DAG tasks.

### 5) Data governance expansion
- Add schema registry and dataset versioning strategy.
- Enforce quality thresholds as release gates (dbt tests + expectations).
- Add access controls by domain, layer, and consumer role.

## CI/CD Promotion Pipeline
### Recommended flow
1. Pull request checks: `make setup`, `make run-all`, `make dbt-test`, `make dq`, `make lint`.
2. Build stage: package code/assets and publish immutable artifact.
3. Deploy to `dev`: run integration smoke checks.
4. Promote to `staging`: run data backfill subset + quality gates.
5. Promote to `prod`: controlled rollout with approval and rollback plan.

### Deployment controls
- Environment-specific config via templated manifests.
- Branch protection + required checks.
- Drift detection for infra definitions.
- Post-deploy smoke test and observability validation.

## Two-Mode Continuity in Production
- Keep `Mode 1` as local deterministic developer workflow.
- Keep `Mode 2` as architecture reference and integration smoke sandbox.
- Production deployments should derive from the same contracts, tests, and governance policies, not from ad hoc runtime behavior.
