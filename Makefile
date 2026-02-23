PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
DBT_PROJECT_DIR := dbt/lakehouse_dbt
DBT_PROFILES_DIR := dbt/lakehouse_dbt
ENTERPRISE_COMPOSE_FILE := modes/mode2_enterprise/docker-compose.enterprise.yml
MYSQL_COMPOSE_FILE := modes/mode2_enterprise/docker-compose.mysql.yml
AIRFLOW_UID ?= 50000

.PHONY: help setup notebooks run-all dbt-run dbt-test dbt-preview dq clean infra-up infra-down infra-logs infra-status infra-smoke mysql-up mysql-down mysql-logs mysql-status mysql-publish mysql-preview

help:
	@echo "Mode 1 (default local lakehouse):"
	@echo "  make setup"
	@echo "  make run-all"
	@echo "  make dbt-run"
	@echo "  make dbt-test"
	@echo "  make dq"
	@echo "  make dbt-preview"
	@echo ""
	@echo "Mode 2 (optional enterprise infra):"
	@echo "  make infra-up"
	@echo "  make infra-status"
	@echo "  make infra-logs"
	@echo "  make infra-smoke"
	@echo "  make infra-down"

$(BIN)/python:
	$(PYTHON) -m venv $(VENV)

setup: $(BIN)/python
	$(BIN)/pip install --upgrade pip wheel
	$(BIN)/pip install -r requirements.txt
	$(BIN)/python -m ipykernel install --user --name notebook_lakehouse --display-name "Notebook Lakehouse" || true
	$(BIN)/dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

notebooks:
	$(BIN)/jupyter lab notebooks/

run-all:
	$(BIN)/python scripts/run_notebooks.py --source fivethirtyeight --datasets recent_grads bechdel_movies

dbt-run:
	EIDP_REPO_ROOT=$(PWD) $(BIN)/dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)
	EIDP_REPO_ROOT=$(PWD) $(BIN)/dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-test:
	EIDP_REPO_ROOT=$(PWD) $(BIN)/dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-preview:
	EIDP_REPO_ROOT=$(PWD) $(BIN)/python scripts/dbt_preview.py --profiles-path $(DBT_PROFILES_DIR)/profiles.yml --project-dir $(DBT_PROJECT_DIR)

dq:
	$(BIN)/python scripts/run_dq.py

clean:
	$(BIN)/python scripts/clean_artifacts.py

infra-up:
	mkdir -p reports/infra_smoke
	chmod 777 reports/infra_smoke
	AIRFLOW_UID=$(AIRFLOW_UID) docker compose -f $(ENTERPRISE_COMPOSE_FILE) up -d

infra-down:
	AIRFLOW_UID=$(AIRFLOW_UID) docker compose -f $(ENTERPRISE_COMPOSE_FILE) down -v

infra-logs:
	AIRFLOW_UID=$(AIRFLOW_UID) docker compose -f $(ENTERPRISE_COMPOSE_FILE) logs -f --tail=200

infra-status:
	AIRFLOW_UID=$(AIRFLOW_UID) docker compose -f $(ENTERPRISE_COMPOSE_FILE) ps

infra-smoke:
	AIRFLOW_UID=$(AIRFLOW_UID) python3 scripts/infra_smoke.py --compose-file $(ENTERPRISE_COMPOSE_FILE)

mysql-up:
	docker compose -f $(MYSQL_COMPOSE_FILE) up -d

mysql-down:
	docker compose -f $(MYSQL_COMPOSE_FILE) down -v

mysql-logs:
	docker compose -f $(MYSQL_COMPOSE_FILE) logs -f --tail=200

mysql-status:
	docker compose -f $(MYSQL_COMPOSE_FILE) ps

mysql-publish:
	$(BIN)/python scripts/publish_gold_to_mysql.py

mysql-preview:
	$(BIN)/python scripts/mysql_preview.py
