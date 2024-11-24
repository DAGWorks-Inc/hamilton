import polars as pl

from hamilton.function_modifiers import config

"""
Notes:
  1. This file is used for all the [ray|dask|spark]/hello_world examples.
  2. It therefore show cases how you can write something once and not only scale it, but port it
     to different frameworks with ease!
"""


@config.when(case="millions")
def avg_3wk_spend__millions(spend: pl.Expr) -> pl.Expr:
    """Rolling 3 week average spend."""
    return spend.rolling_mean(window_size=3) / 1e6


@config.when(case="thousands")
def avg_3wk_spend__thousands(spend: pl.Expr) -> pl.Expr:
    """Rolling 3 week average spend."""
    return spend.rolling_mean(window_size=3) / 1e3


def spend_per_signup(spend: pl.Expr, signups: pl.Expr) -> pl.Expr:
    """The cost per signup in relation to spend."""
    return spend / signups


def spend_mean(spend: pl.Expr) -> float:
    """Shows function creating a scalar. In this case it computes the mean of the entire column."""
    return spend.mean()


def spend_zero_mean(spend: pl.Expr, spend_mean: float) -> pl.Expr:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend - spend_mean


def spend_std_dev(spend: pl.Expr) -> float:
    """Function that computes the standard deviation of the spend column."""
    return spend.std()


def spend_zero_mean_unit_variance(spend_zero_mean: pl.Expr, spend_std_dev: float) -> pl.Expr:
    """Function showing one way to make spend have zero mean and unit variance."""
    return spend_zero_mean / spend_std_dev
