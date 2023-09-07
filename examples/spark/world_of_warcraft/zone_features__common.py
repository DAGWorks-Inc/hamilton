import pandas as pd

from hamilton import htypes


def durotar_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
    return pd.Series((zone == " Durotar").astype(int), index=zone.index)


def darkshore_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
    return pd.Series((zone == " Darkshore").astype(int), index=zone.index)


def durotar_likelihood(
    durotar_count: pd.Series, total_count: pd.Series
) -> htypes.column[pd.Series, float]:
    return durotar_count / total_count


def darkshore_likelihood(
    darkshore_count: pd.Series, total_count: pd.Series
) -> htypes.column[pd.Series, float]:
    return darkshore_count / total_count
