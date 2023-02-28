"""
Pandas UDFs.
Has to only contain map operations!
"""
import pandas as pd

from hamilton.function_modifiers import tag


@tag(return_type="float")
def spend_per_signup(spend: pd.Series, signups: pd.Series) -> pd.Series:
    """The cost per signup in relation to spend."""
    return spend / signups


def augmented_mean(foo: float, bar: float) -> float:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return foo + bar


@tag(return_type="float")
def spend_zero_mean(spend: pd.Series, spend_mean: pd.Series) -> pd.Series:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend - spend_mean


@tag(return_type="float")
def spend_zero_mean_unit_variance(
    spend_zero_mean: pd.Series, spend_std_dev: pd.Series
) -> pd.Series:
    """Function showing one way to make spend have zero mean and unit variance."""
    return spend_zero_mean / spend_std_dev
