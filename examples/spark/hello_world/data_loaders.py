import pandas as pd
import pyspark.pandas as ps


def spend(spend_location: str) -> ps.Series:
    """Dummy function showing how to wire through loading data.

    :param spend_location:
    :return:
    """
    return ps.from_pandas(pd.Series([10, 10, 20, 40, 40, 50], name="spend"))


def signups(signups_location: str) -> ps.Series:
    """Dummy function showing how to wire through loading data.

    :param signups_location:
    :return:
    """
    return ps.from_pandas(pd.Series([1, 10, 50, 100, 200, 400], name="signups"))
