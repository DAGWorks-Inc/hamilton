import logging

import pandas as pd
# import pyspark.pandas as pd

logger = logging.getLogger(__name__)


def avg_3wk_spend(spend: pd.Series) -> pd.Series:
    """Rolling 3 week average spend."""
    return spend.rolling(3).mean()


def spend_per_signup(spend: pd.Series, signups: pd.Series) -> pd.Series:
    """The cost per signup in relation to spend."""
    return spend / signups
    # return spend.divide(signups)


def spend_mean(spend: pd.Series) -> float:
    """Shows function creating a scalar. In this case it computes the mean of the entire column."""
    return spend.mean()


# this has to be a map function -- can't do regular pandas_udf function here
#
def spend_zero_mean(spend: pd.Series, spend_mean: float) -> pd.Series:
# def spend_zero_mean(spend: pd.Series, spend_mean: pd.Series) -> pd.Series:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend - spend_mean


def spend_std_dev(spend: pd.Series) -> float:
    """Function that computes the standard deviation of the spend column."""
    return spend.std()


# this has to be a map function -- can't do regular pandas_udf function here
def spend_zero_mean_unit_variance(spend_zero_mean: pd.Series, spend_std_dev: float) -> pd.Series:
# def spend_zero_mean_unit_variance(spend_zero_mean: pd.Series, spend_std_dev: pd.Series) -> pd.Series:
    """Function showing one way to make spend have zero mean and unit variance."""
    return spend_zero_mean / spend_std_dev
    # return spend_zero_mean.divide(spend_std_dev)

