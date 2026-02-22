import pandas as pd

from src.common.hashing import dataframe_row_hashes
from src.common.pipeline import standardize_column_names


def test_standardize_column_names_snake_case() -> None:
    frame = pd.DataFrame(columns=["Major Code", "budget_2013$", "period code"])
    standardized = standardize_column_names(frame)
    assert list(standardized.columns) == ["major_code", "budget_2013", "period_code"]


def test_dataframe_row_hashes_are_deterministic() -> None:
    frame = pd.DataFrame(
        {
            "major_code": [1, 2],
            "major": ["A", "B"],
        }
    )

    first = dataframe_row_hashes(frame)
    second = dataframe_row_hashes(frame)
    assert first.tolist() == second.tolist()
