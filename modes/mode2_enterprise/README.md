# Mode 2: Enterprise Infra (Optional)

Mode 2 showcases orchestration, streaming, storage, and observability breadth. It is optional and separated from the default local Lakehouse workflow.

## Scope Boundary
- Mode 2 is not required for Mode 1 validation.
- CI remains Mode 1 only.
- Use this mode for infrastructure demos and smoke checks.

## Prerequisites
- Docker Engine + Docker Compose v2
- `make`
- Recommended: at least 6 GB free RAM for local containers
- Free local ports: `3000`, `8088`, `8089`, `9090`, `9000`, `9001`, `18080`, `18081`

## Core Commands
```bash
make infra-up
make infra-status
make infra-logs
make infra-smoke
make infra-down
```

## Optional MySQL Serving Commands
```bash
make mysql-up
make mysql-status
make mysql-publish
make mysql-preview
make mysql-down
```

## Service URLs
- Airflow UI: `http://localhost:8089` (`admin` / `admin`)
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8088`
- MinIO Console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- Spark Master UI: `http://localhost:18080`
- Spark Worker UI: `http://localhost:18081`

## Airflow Smoke Demo
1. Start the stack: `make infra-up`
2. Confirm containers: `make infra-status`
3. Open Airflow and trigger DAG `infra_smoke_dag`
4. Verify host artifact: `reports/infra_smoke/ok.txt`
5. Optional automated verification: `make infra-smoke`
6. Stop services: `make infra-down`

## Compose Mounts (Important)
- DAGs: `./orchestration/airflow/dags -> /opt/airflow/dags`
- Plugins: `./orchestration/airflow/plugins -> /opt/airflow/plugins`
- Host reports: `../../reports -> /opt/airflow/reports`

The smoke DAG writes `/opt/airflow/reports/infra_smoke/ok.txt` inside the container, which resolves to `reports/infra_smoke/ok.txt` on the host.
