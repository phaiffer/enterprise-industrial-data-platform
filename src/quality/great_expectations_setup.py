import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import great_expectations as gx

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.paths import GE_DIR


def utc_now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_batch_csv(csv_path: str) -> dict:
    """
    Run basic Great Expectations checks against a batch CSV file.
    This is a pragmatic MVP that can be wired into Airflow/CI.

    In production, you'd store expectation suites + checkpoints versioned.
    """
    df = pd.read_csv(csv_path)

    context = gx.get_context(context_root_dir=str(GE_DIR))
    validator = context.sources.pandas_default.read_dataframe(df)

    # Core schema checks
    validator.expect_column_values_to_not_be_null("sensor_id")
    validator.expect_column_values_to_not_be_null("site")
    validator.expect_column_values_to_not_be_null("event_time")

    # Range checks (adjust to your domain)
    validator.expect_column_values_to_be_between("temperature", min_value=0, max_value=150)
    validator.expect_column_values_to_be_between("vibration", min_value=0, max_value=20)
    validator.expect_column_values_to_be_between("pressure", min_value=0, max_value=50)

    result = validator.validate()
    return result.to_json_dict()


def main() -> None:
    csv_path = os.getenv("QUALITY_CSV_PATH", "data/raw/sensors_batch.csv")
    out_dir = Path(os.getenv("QUALITY_OUTPUT_DIR", "data/quality"))
    out_dir.mkdir(parents=True, exist_ok=True)

    result = validate_batch_csv(csv_path)

    out_file = out_dir / "ge_validation_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"validated_at": utc_now_iso(), "result": result}, f, indent=2)

    success = bool(result.get("success", False))
    print(f"Great Expectations validation success: {success}")
    print(f"Saved GE result to: {out_file}")

    # Fail fast for orchestration/CI if expectations fail
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
