import polars as pl

from hamilton.function_modifiers import config

"""
Notes:
  1. This file is used for all the [ray|dask|spark]/hello_world examples.
  2. It therefore show cases how you can write something once and not only scale it, but port it
     to different frameworks with ease!
"""


@config.when(case="millions")
def avg_3wk_spend__millions(spend: pl.Series) -> pl.Series:
    """Rolling 3 week average spend."""
    return (
        spend.to_frame("spend").select(pl.col("spend").rolling_mean(window_size=3) / 1e6)
    ).to_series(0)


@config.when(case="thousands")
def avg_3wk_spend__thousands(spend: pl.Series) -> pl.Series:
    """Rolling 3 week average spend."""
    return (
        spend.to_frame("spend").select(pl.col("spend").rolling_mean(window_size=3) / 1e3)
    ).to_series(0)


def spend_per_signup(spend: pl.Series, signups: pl.Series) -> pl.Series:
    """The cost per signup in relation to spend."""
    return spend / signups


def spend_mean(spend: pl.Series) -> float:
    """Shows function creating a scalar. In this case it computes the mean of the entire column."""
    return spend.mean()


def spend_zero_mean(spend: pl.Series, spend_mean: float) -> pl.Series:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend - spend_mean


def spend_std_dev(spend: pl.Series) -> float:
    """Function that computes the standard deviation of the spend column."""
    return spend.std()


def spend_zero_mean_unit_variance(spend_zero_mean: pl.Series, spend_std_dev: float) -> pl.Series:
    """Function showing one way to make spend have zero mean and unit variance."""
    return spend_zero_mean / spend_std_dev
