import pandas as pd

from hamilton import htypes


def durotar_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
    return pd.Series((zone == " Durotar").astype(int), index=zone.index)


def darkshore_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
    return pd.Series((zone == " Darkshore").astype(int), index=zone.index)
