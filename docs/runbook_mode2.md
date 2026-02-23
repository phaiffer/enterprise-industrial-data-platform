# Runbook: Mode 2 Enterprise Stack

## Standard Flow
```bash
make infra-up
make infra-status
make infra-smoke
make infra-down
```

## Common Issues

### Docker daemon unavailable
- Symptom: `Cannot connect to the Docker daemon`
- Fix: start Docker service and retry.

### Port collisions
- Symptom: bind errors on `3000`, `8088`, `8089`, `9090`, `18080`, etc.
- Fix: stop conflicting processes or adjust port mappings in compose.

### Airflow UI opens but DAGs not visible
- Check `make infra-logs` for scheduler/webserver errors.
- Confirm DAG volume mount path exists: `modes/mode2_enterprise/orchestration/airflow/dags`.

### Smoke artifact not created
- Confirm scheduler is healthy in `make infra-status`.
- Re-trigger with `make infra-smoke`.
- Review scheduler logs for DAG/task failures.

### Slow startup on first run
- Image pulls and Airflow DB init can take several minutes.
- Wait for healthy status and retry smoke trigger.
