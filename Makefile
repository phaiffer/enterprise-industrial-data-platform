PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
DBT_PROJECT_DIR := dbt/lakehouse_dbt
DBT_PROFILES_DIR := dbt/lakehouse_dbt

.PHONY: setup notebooks run-all dbt-run dbt-test dq clean

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
	$(BIN)/dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)
	$(BIN)/dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-test:
	$(BIN)/dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dq:
	$(BIN)/python scripts/run_dq.py

clean:
	$(BIN)/python scripts/clean_artifacts.py
