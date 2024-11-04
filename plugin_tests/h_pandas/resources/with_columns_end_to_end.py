import pandas as pd

from hamilton.function_modifiers import config
from hamilton.plugins.h_pandas import with_columns


def upstream_factor() -> int:
    return 3


def initial_df() -> pd.DataFrame:
    return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14], "col_3": [1, 1, 1, 1]})


def subtract_1_from_2(col_1: pd.Series, col_2: pd.Series) -> pd.Series:
    return col_2 - col_1


@config.when(factor=5)
def multiply_3__by_5(col_3: pd.Series) -> pd.Series:
    return col_3 * 5


@config.when(factor=7)
def multiply_3__by_7(col_3: pd.Series) -> pd.Series:
    return col_3 * 7


def add_1_by_user_adjustment_factor(col_1: pd.Series, user_factor: int) -> pd.Series:
    return col_1 + user_factor


def multiply_2_by_upstream_3(col_2: pd.Series, upstream_factor: int) -> pd.Series:
    return col_2 * upstream_factor


@with_columns(
    subtract_1_from_2,
    multiply_3__by_5,
    multiply_3__by_7,
    add_1_by_user_adjustment_factor,
    multiply_2_by_upstream_3,
    columns_to_pass=["col_1", "col_2", "col_3"],
    select=[
        "subtract_1_from_2",
        "multiply_3",
        "add_1_by_user_adjustment_factor",
        "multiply_2_by_upstream_3",
    ],
    namespace="some_subdag",
)
def final_df(initial_df: pd.DataFrame) -> pd.DataFrame:
    return initial_df


def col_3(initial_df: pd.DataFrame) -> pd.Series:
    return pd.Series([0, 2, 4, 6])


@with_columns(
    col_3,
    pass_dataframe_as="initial_df",
    select=["col_3", "multiply_3"],
)
def final_df_2(initial_df: pd.DataFrame) -> pd.DataFrame:
    return initial_df
