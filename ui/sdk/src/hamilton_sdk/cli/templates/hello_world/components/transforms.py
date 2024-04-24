import pandas as pd

"""A basic set of transforms for the classic Hamilton hello_world example.
This does a pretty simple set of column-wise operations on datasets.

You can also play around with a very similar example on tryhamilton.dev.
"""


def spend(spend_path: str) -> pd.Series:
    """Function that loads the spend column from a CSV."""
    return pd.read_csv(spend_path, index_col=0)["spend"]


def signups(signups_path: str) -> pd.Series:
    """Function that loads the signups column from a CSV."""
    return pd.read_csv(signups_path, index_col=0)["signups"]


def avg_3wk_spend(spend: pd.Series) -> pd.Series:
    """Rolling 3 week average spend."""
    return spend.rolling(3).mean()


def spend_per_signup(spend: pd.Series, signups: pd.Series) -> pd.Series:
    """The cost per signup in relation to spend."""
    return spend / signups


def spend_mean(spend: pd.Series) -> float:
    """Shows function creating a scalar. In this case it computes the mean of the entire column."""
    return spend.mean()


def spend_zero_mean(spend: pd.Series, spend_mean: float) -> pd.Series:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend - spend_mean


def spend_std_dev(spend: pd.Series) -> float:
    """Function that computes the standard deviation of the spend column."""
    return spend.std()


def spend_zero_mean_unit_variance(spend_zero_mean: pd.Series, spend_std_dev: float) -> pd.Series:
    """Function showing one way to make spend have zero mean and unit variance."""
    return spend_zero_mean / spend_std_dev
