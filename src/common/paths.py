from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"

LAKEHOUSE_DIR = ROOT_DIR / "lakehouse"
BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR = LAKEHOUSE_DIR / "gold"

REPORTS_DIR = ROOT_DIR / "reports"
METRICS_DIR = REPORTS_DIR / "metrics"
EXECUTED_NOTEBOOKS_DIR = REPORTS_DIR / "executed_notebooks"

WAREHOUSE_DIR = LAKEHOUSE_DIR / "warehouse"
WAREHOUSE_DB_PATH = WAREHOUSE_DIR / "lakehouse.duckdb"

DBT_PROJECT_DIR = ROOT_DIR / "dbt" / "lakehouse_dbt"
GE_DIR = ROOT_DIR / "great_expectations"

PIPELINE_METRICS_PATH = METRICS_DIR / "pipeline_metrics.json"


def ensure_project_dirs() -> None:
    """Create all runtime directories used by the pipeline."""
    for path in (
        RAW_DATA_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        REPORTS_DIR,
        METRICS_DIR,
        EXECUTED_NOTEBOOKS_DIR,
        WAREHOUSE_DIR,
        GE_DIR / "uncommitted" / "validations",
    ):
        path.mkdir(parents=True, exist_ok=True)
