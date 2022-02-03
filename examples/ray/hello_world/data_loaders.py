import pandas as pd


def spend(spend_location: str) -> pd.Series:
    """Dummy function showing how to wire through loading data.

    :param spend_location:
    :return:
    """
    return pd.Series([10, 10, 20, 40, 40, 50])


def signups(signups_location: str) -> pd.Series:
    """Dummy function showing how to wire through loading data.

    :param signups_location:
    :return:
    """
    return pd.Series([1, 10, 50, 100, 200, 400])
