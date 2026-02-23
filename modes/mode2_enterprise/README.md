# Mode 2: Enterprise Infra (Optional)

Mode 2 demonstrates infrastructure breadth and is intentionally optional.

## Important
- Mode 2 is not required for Mode 1 validation.
- CI remains Mode 1 only.

## Start / Stop
```bash
make infra-up
make infra-status
make infra-logs
make infra-smoke
make infra-down
```

## What Lives Here
- `docker-compose.enterprise.yml`: enterprise stack (Airflow, Spark, Kafka, MinIO, Prometheus, Grafana, Postgres)
- `orchestration/airflow/`: DAGs/plugins/logs mounts
- `observability/prometheus/`: Prometheus config
- `conf/spark/`: Spark runtime config for compose services
- `warehouse/`: local infra volumes (for optional services such as MySQL)

## Service URLs
- Airflow: `http://localhost:8089` (`admin` / `admin`)
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8088`
- MinIO Console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- Spark Master UI: `http://localhost:18080`
- Spark Worker UI: `http://localhost:18081`

## Airflow Smoke Demo
1. `make infra-up`
2. Open Airflow and trigger `infra_smoke_dag`
3. Verify `reports/infra_smoke/ok.txt` was written on the host
4. Optionally run `make infra-smoke` for command-based verification
5. `make infra-down`
