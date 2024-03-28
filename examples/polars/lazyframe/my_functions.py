import polars as pl

from hamilton.function_modifiers import load_from, value


@load_from.csv(file=value("./sample_data.csv"))
def raw_data(data: pl.LazyFrame) -> pl.LazyFrame:
    return data


def spend_per_signup(raw_data: pl.LazyFrame) -> pl.LazyFrame:
    """Computes cost per signup in relation to spend."""
    return raw_data.select("spend", "signups").with_columns(
        [(pl.col("spend") / pl.col("signups")).alias("spend_per_signup")]
    )
