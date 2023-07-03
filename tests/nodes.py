import logging

import pandas as pd

from hamilton.function_modifiers import tag


@tag(cache="str")
def lowercased(initial: str) -> str:
    logging.info("lowercased")
    return initial.lower()


@tag(cache="str")
def uppercased(initial: str) -> str:
    logging.info("uppercased")
    return initial.upper()


@tag(cache="json")
def both(lowercased: str, uppercased: str) -> dict:
    logging.info("both")
    return {"lower": lowercased, "upper": uppercased}


def b2(both: dict) -> dict:
    logging.info("b2")
    return both


@tag(cache="json")
def my_df() -> pd.DataFrame:
    logging.info("json df")
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@tag(cache="json")
def my_series() -> pd.Series:
    logging.info("json series")
    return pd.Series([7, 8, 9])


@tag(cache="parquet")
def my_df2(my_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("parquet df")
    return my_df


@tag(cache="parquet")
def my_series2(my_series: pd.Series) -> pd.Series:
    logging.info("parquet series")
    return my_series


def combined(my_df2: pd.DataFrame, my_series2: pd.Series) -> pd.DataFrame:
    logging.info("combined")
    _s = pd.Series(my_series2, name="c")
    _df = pd.concat([my_df2, _s], axis=1)
    return _df
