import html
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.common.hashing import dataframe_row_hashes
from src.common.logging import get_logger

LOGGER = get_logger("pipeline")


def to_snake_case(text: str) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", text.strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def standardize_column_names(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = {column: to_snake_case(column) for column in frame.columns}
    return frame.rename(columns=renamed)


def download_csv_with_retries(
    url: str,
    destination: Path,
    force_refresh: bool = False,
    max_attempts: int = 3,
    timeout_seconds: int = 30,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and not force_refresh:
        LOGGER.info("Using cached raw file: %s", destination)
        return destination

    for attempt in range(1, max_attempts + 1):
        try:
            LOGGER.info("Downloading %s (attempt %s/%s)", url, attempt, max_attempts)
            response = requests.get(url, timeout=timeout_seconds)
            response.raise_for_status()
            destination.write_bytes(response.content)
            return destination
        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise RuntimeError(f"Failed downloading {url}: {exc}") from exc
            sleep_seconds = attempt * 2
            LOGGER.warning("Download failed: %s. Retrying in %s seconds.", exc, sleep_seconds)
            time.sleep(sleep_seconds)

    return destination


def add_bronze_metadata(
    frame: pd.DataFrame,
    source: str,
    dataset: str,
    file_path: Path,
    batch_id: str,
) -> pd.DataFrame:
    enriched = frame.copy()
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    enriched["_ingested_at"] = ingested_at
    enriched["_source"] = source
    enriched["_dataset"] = dataset
    enriched["_file_path"] = str(file_path)
    enriched["_batch_id"] = batch_id
    enriched["_row_hash"] = dataframe_row_hashes(
        enriched,
        exclude_columns=["_ingested_at", "_source", "_dataset", "_file_path", "_batch_id"],
    )

    # Keep deterministic output ordering so runs are reproducible.
    return enriched.sort_values("_row_hash").reset_index(drop=True)


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def clean_recent_grads(frame: pd.DataFrame) -> pd.DataFrame:
    cleaned = frame.copy()

    integer_columns = [
        "rank",
        "major_code",
        "total",
        "men",
        "women",
        "sample_size",
        "employed",
        "full_time",
        "part_time",
        "full_time_year_round",
        "unemployed",
        "median",
        "p25th",
        "p75th",
        "college_jobs",
        "non_college_jobs",
        "low_wage_jobs",
    ]
    float_columns = ["sharewomen", "unemployment_rate"]

    for column in integer_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce").astype("Int64")

    for column in float_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    cleaned["major"] = cleaned["major"].astype(str).str.strip()
    cleaned["major_category"] = cleaned["major_category"].astype(str).str.strip()

    cleaned = cleaned.dropna(subset=["major_code", "major", "major_category"])
    cleaned = cleaned.drop_duplicates(subset=["major_code"], keep="first")
    cleaned = cleaned.sort_values("major_code").reset_index(drop=True)
    return cleaned


def clean_bechdel_movies(frame: pd.DataFrame) -> pd.DataFrame:
    cleaned = frame.copy()

    numeric_columns = [
        "year",
        "budget",
        "domgross",
        "intgross",
        "budget_2013",
        "domgross_2013",
        "intgross_2013",
        "period_code",
        "decade_code",
    ]

    for column in numeric_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    cleaned["imdb"] = cleaned["imdb"].astype(str).str.strip()
    cleaned["title"] = cleaned["title"].astype(str).map(html.unescape).str.strip()
    cleaned["clean_test"] = cleaned["clean_test"].astype(str).str.lower().str.strip()
    cleaned["binary"] = cleaned["binary"].astype(str).str.upper().str.strip()

    cleaned = cleaned[cleaned["binary"].isin(["PASS", "FAIL"])]
    cleaned = cleaned.dropna(subset=["imdb", "title", "year", "binary"])
    cleaned["year"] = cleaned["year"].astype("Int64")

    cleaned = cleaned.drop_duplicates(subset=["imdb"], keep="first")
    cleaned = cleaned.sort_values(["year", "imdb"]).reset_index(drop=True)
    return cleaned
