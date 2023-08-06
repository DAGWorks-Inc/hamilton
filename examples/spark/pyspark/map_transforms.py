import pandas as pd

from hamilton.htypes import column


def spend_per_signup(spend: pd.Series, signups: pd.Series) -> column[pd.Series, float]:
    """The cost per signup in relation to spend."""
    return spend / signups


def spend_zero_mean(spend: pd.Series, spend_mean: float) -> column[pd.Series, float]:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend - spend_mean


def spend_zero_mean_unit_variance(
    spend_zero_mean: pd.Series, spend_std_dev: float
) -> column[pd.Series, float]:
    """Function showing one way to make spend have zero mean and unit variance."""
    return spend_zero_mean / spend_std_dev
