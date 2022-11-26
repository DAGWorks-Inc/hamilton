"""
This module contains our data loading functions.
"""
from typing import List

import numpy as np
import pandas as pd
import pandera as pa
from sklearn import datasets
from sqlalchemy import engine

from hamilton.function_modifiers import check_output, config, extract_columns


def _sanitize_columns(df_columns: List[str]) -> List[str]:
    """Renames columns to be valid hamilton names -- and lower cases them.
    :param df_columns: the current column names.
    :return: sanitize column names that work with Hamilton
    """
    return [
        c.strip().replace(".", "_").replace("/", "_per_").replace(" ", "_").lower()
        for c in df_columns
    ]


# schema to validate against
passengers_df_schema = pa.DataFrameSchema(
    {
        "pclass": pa.Column(np.float, nullable=False),
        "sex": pa.Column(pa.Category, nullable=False),
        "age": pa.Column(np.float, nullable=True),
        "parch": pa.Column(np.float, nullable=False),
        "sibsp": pa.Column(np.float, nullable=False),
        "fare": pa.Column(np.float, nullable=True),
        "embarked": pa.Column(pa.Category, nullable=True),
        "name": pa.Column(str, nullable=False),
        "ticket": pa.Column(str, nullable=False),
        "boat": pa.Column(str, nullable=True),
        "body": pa.Column(np.float, nullable=True),
        "home_dest": pa.Column(str, nullable=True),
        "cabin": pa.Column(str, nullable=True),
        "survived": pa.Column(pa.Category, nullable=False),
    },
    strict=True,
    index=pa.Index(int, name="pid"),
)


@config.when(loader="sqllite")
@check_output(schema=passengers_df_schema)
def raw_passengers_df__sqllite(connection: engine.Connection) -> pd.DataFrame:
    """Pulls from the SQLlite DB.

    :param connection: the sqllite connection object to use.
    :return: dataframe of data.
    """
    query = """
                SELECT
                    tbl_passengers.pid,
                    tbl_passengers.pclass,
                    tbl_passengers.sex,
                    tbl_passengers.age,
                    tbl_passengers.parch,
                    tbl_passengers.sibsp,
                    tbl_passengers.fare,
                    tbl_passengers.embarked,
                    tbl_passengers.name,
                    tbl_passengers.ticket,
                    tbl_passengers.boat,
                    tbl_passengers.body,
                    tbl_passengers.home_dest,
                    tbl_passengers.cabin,
                    tbl_targets.is_survived as is_survived
                FROM
                    tbl_passengers
                JOIN
                    tbl_targets
                ON
                    tbl_passengers.pid=tbl_targets.pid
            """
    df = pd.read_sql(query, con=connection)
    df.pid = df.pid.astype(int)
    df.set_index("pid", inplace=True)  # required
    df.columns = _sanitize_columns(df.columns)
    return df


@config.when(loader="openml")
@check_output(schema=passengers_df_schema)
def raw_passengers_df__openml() -> pd.DataFrame:
    """Pulls data from the web.

    :return: dataframe of data.
    """
    base, targets = datasets.fetch_openml("titanic", version=1, as_frame=True, return_X_y=True)
    df = pd.concat([base, targets], axis=1)
    df.index.name = "pid"
    df.columns = _sanitize_columns(df.columns)
    return df


@extract_columns("pclass", "sex", "age", "parch", "sibsp", "fare", "embarked", "name", "survived")
def passengers_df(raw_passengers_df: pd.DataFrame) -> pd.DataFrame:
    """Due to the raw_passengers_df* functions checking_output, we need this function to enable @extract_columns.

    :param raw_passengers_df:
    :return:
    """
    return raw_passengers_df


def target(survived: pd.Series) -> pd.Series:
    """Just hardcoding this mapping that we want survived to be our target.

    :param survived:
    :return:
    """
    target_col = survived.copy()
    target_col.name = "target"
    return target_col
