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


if __name__ == "__main__":
    import __main__ as example
    from hamilton import base, driver
    from hamilton.plugins import h_narwhals, h_polars

    # pandas
    dr = (
        driver.Builder()
        .with_config({"load": "pandas"})
        .with_modules(example)
        .with_adapters(
            h_narwhals.NarwhalsAdapter(),
            h_narwhals.NarwhalsDataFrameResultBuilder(base.PandasDataFrameResult()),
        )
        .build()
    )
    r = dr.execute([example.group_by_mean, example.example1], inputs={"col_name": "a"})
    print(r)

    # polars
    dr = (
        driver.Builder()
        .with_config({"load": "polars"})
        .with_modules(example)
        .with_adapters(
            h_narwhals.NarwhalsAdapter(),
            h_narwhals.NarwhalsDataFrameResultBuilder(h_polars.PolarsDataFrameResult()),
        )
        .build()
    )
    r = dr.execute([example.group_by_mean, example.example1], inputs={"col_name": "a"})
    print(r)
    dr.display_all_functions("example.png")
