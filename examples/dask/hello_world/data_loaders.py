import pandas as pd
from dask import dataframe


def spend(spend_location: str, spend_partitions: int) -> dataframe.Series:
    """Dummy function showing how to wire through loading data.

    :param spend_location:
    :param spend_partitions: number of partitions to segment the data into
    :return:
    """
    return dataframe.from_pandas(
        pd.Series([10, 10, 20, 40, 40, 50]), name="spend", npartitions=spend_partitions
    )


def signups(signups_location: str, signups_partitions: int) -> dataframe.Series:
    """Dummy function showing how to wire through loading data.

    :param signups_location:
    :param signups_partitions: number of partitions to segment the data into
    :return:
    """
    return dataframe.from_pandas(
        pd.Series([1, 10, 50, 100, 200, 400]), name="signups", npartitions=signups_partitions
    )
