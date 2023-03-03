"""
Module that contains logic to load data for the offline ETL process.

We use this to build our offline ETL featurization process.
"""

from typing import List

import pandas as pd

from hamilton.function_modifiers import extract_columns

# full set of available columns from the data source
data_columns = [
    "id",
    "reason_for_absence",
    "month_of_absence",
    "day_of_the_week",
    "seasons",
    "transportation_expense",
    "distance_from_residence_to_work",
    "service_time",
    "age",
    "work_load_average_per_day",
    "hit_target",
    "disciplinary_failure",
    "education",
    "son",
    "social_drinker",
    "social_smoker",
    "pet",
    "weight",
    "height",
    "body_mass_index",
    "absenteeism_time_in_hours",
]


def _sanitize_columns(df_columns: List[str]) -> List[str]:
    """Renames columns to be valid hamilton names -- and lower cases them.

    :param df_columns: the current column names.
    :return: sanitize column names that work with Hamilton
    """
    return [c.strip().replace("/", "_per_").replace(" ", "_").lower() for c in df_columns]


@extract_columns(*data_columns)
def raw_data(location: str) -> pd.DataFrame:
    """Extracts the raw data, renames the columns to be valid python variable names, and assigns an index.
    :param location: the location to load from
    :return:
    """
    df = pd.read_csv(location, sep=";")
    # rename columns to be valid hamilton names -- and lower case it
    df.columns = _sanitize_columns(df.columns)
    # create proper index -- ID-Month-Day - to be able to join features appropriately.
    index = (
        df["id"].astype(str)
        + "-"
        + df["month_of_absence"].astype(str)
        + "-"
        + df["day_of_the_week"].astype(str)
    )
    df.index = index
    return df
