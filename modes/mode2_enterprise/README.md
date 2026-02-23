# Mode 2: Enterprise Stack (Optional)

This mode is optional and independent from the default local workflow.

## Purpose
Show enterprise platform breadth: orchestration, streaming, object storage, and observability.

## Prerequisites
- Docker + Docker Compose v2
- Minimum ~8 GB RAM recommended for all services

## Compose File
`modes/mode2_enterprise/docker-compose.enterprise.yml`

## Start / Stop
```bash
make infra-up
make infra-status
make infra-logs
make infra-down
```

## Service Endpoints
- Airflow: `http://localhost:8089` (`admin` / `admin`)
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8088`
- MinIO Console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- Spark Master UI: `http://localhost:8080`

## Infra Smoke Demo
Use the dedicated Airflow DAG:
```bash
make infra-smoke
```
This triggers `infra_smoke_dag` and verifies artifact creation at:
- `reports/infra_smoke/ok.txt`

Manual path:
1. Open Airflow UI.
2. Trigger DAG `infra_smoke_dag`.
3. Confirm `reports/infra_smoke/ok.txt` contains run metadata.

## Notes
- Mode 2 is not used by CI.
- Keep Mode 1 as the primary development path.

## Optional MySQL Serving Layer
MySQL can be started independently to host published Gold marts/KPIs:

```bash
make mysql-up
make mysql-publish
make mysql-preview
make mysql-down
```
