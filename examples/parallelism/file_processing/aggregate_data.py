import pandas as pd

from hamilton.htypes import Collect


def statistics_by_city(statistics: Collect[dict]) -> pd.DataFrame:
    """Joins all data together"""
    return pd.DataFrame.from_records(statistics).set_index("city")
