import hashlib
from typing import Iterable

import pandas as pd


def dataframe_row_hashes(
    frame: pd.DataFrame,
    exclude_columns: Iterable[str] | None = None,
) -> pd.Series:
    """Return deterministic SHA-256 hashes for each row in a DataFrame."""
    excluded = set(exclude_columns or [])
    selected_columns = [column for column in frame.columns if column not in excluded]

    normalized = (
        frame[selected_columns]
        .fillna("")
        .astype(str)
        .apply(lambda column: column.map(lambda value: value.strip()))
    )

    return normalized.apply(
        lambda row: hashlib.sha256("|".join(row.values).encode("utf-8")).hexdigest(),
        axis=1,
    )
