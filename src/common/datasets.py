SOURCE_NAME = "fivethirtyeight"
SOURCE_LICENSE = "Creative Commons Attribution 4.0 International (CC BY 4.0)"
SOURCE_REPOSITORY = "https://github.com/fivethirtyeight/data"

DATASETS: dict[str, dict[str, str]] = {
    "recent_grads": {
        "url": "https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/recent-grads.csv",
        "raw_filename": "recent_grads.csv",
        "description": "Recent U.S. graduates by major, employment, and salary outcomes.",
    },
    "bechdel_movies": {
        "url": "https://raw.githubusercontent.com/fivethirtyeight/data/master/bechdel/movies.csv",
        "raw_filename": "bechdel_movies.csv",
        "description": "Movie-level Bechdel test outcomes with inflation-adjusted gross and budget metrics.",
    },
}


def parse_dataset_argument(dataset_argument: str | None) -> list[str]:
    if not dataset_argument:
        return list(DATASETS.keys())

    parsed = [part.strip() for part in dataset_argument.split(",") if part.strip()]
    invalid = [item for item in parsed if item not in DATASETS]
    if invalid:
        valid = ", ".join(sorted(DATASETS.keys()))
        raise ValueError(f"Invalid dataset(s): {invalid}. Valid options: {valid}")

    return parsed
