import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def utc_now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_raw_sensor_csv(output_path: str, rows: int = 5000) -> str:
    """
    Generate batch data as CSV using the same schema contract as streaming.
    This is the 'batch ingestion source' used by the batch pipeline.
    """
    rng = pd.RangeIndex(start=0, stop=rows, step=1)

    df = pd.DataFrame(
        {
            "sensor_id": (rng % 25).map(lambda x: f"SENSOR_{x:02d}"),
            "site": (rng % 3).map(lambda x: ["BR-PR", "US-TX", "IN-PN"][x]),
            "temperature": (20 + (rng % 80)).astype(float),
            "vibration": (0.1 + (rng % 50) / 10.0).astype(float),
            "pressure": (1.0 + (rng % 90) / 10.0).astype(float),
            "event_time": [utc_now_iso()] * rows,
        }
    )

    Path(os.path.dirname(output_path)).mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    manifest = {"generated_at": utc_now_iso(), "rows": rows, "output": output_path}
    Path("data/manifests").mkdir(parents=True, exist_ok=True)
    with open("data/manifests/extract_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return output_path


if __name__ == "__main__":
    out = generate_raw_sensor_csv(
        "data/raw/sensors_batch.csv", rows=int(os.getenv("BATCH_ROWS", "5000"))
    )
    print(f"Generated batch file at: {out}")
