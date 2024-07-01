import narwhals as nw
import pandas as pd
import polars as pl

from hamilton.function_modifiers import config, tag


@config.when(load="pandas")
def df__pandas() -> nw.DataFrame:
    return pd.DataFrame({"a": [1, 1, 2, 2, 3], "b": [4, 5, 6, 7, 8]})


@config.when(load="pandas")
def series__pandas() -> nw.Series:
    return pd.Series([1, 3])


@config.when(load="polars")
def df__polars() -> nw.DataFrame:
    return pl.DataFrame({"a": [1, 1, 2, 2, 3], "b": [4, 5, 6, 7, 8]})


@config.when(load="polars")
def series__polars() -> nw.Series:
    return pl.Series([1, 3])


@tag(nw_kwargs=["eager_only"])
def example1(df: nw.DataFrame, series: nw.Series, col_name: str) -> int:
    return df.filter(nw.col(col_name).is_in(series.to_numpy())).shape[0]


def group_by_mean(df: nw.DataFrame) -> nw.DataFrame:
    return df.group_by("a").agg(nw.col("b").mean()).sort("a")
