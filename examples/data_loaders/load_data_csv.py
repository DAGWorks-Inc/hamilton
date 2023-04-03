import pandas as pd

from hamilton.function_modifiers import load_from, value


@load_from.csv(path=value("test_data/marketing_spend.csv"))
def spend(data: pd.DataFrame) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01"""
    data["date"] = pd.date_range(start="2020-01-01", periods=len(data), freq="D")
    return data


@load_from.csv(path=value("test_data/churn.csv"))
def churn(data: pd.DataFrame) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01
    """
    data["date"] = pd.date_range(start="2020-01-01", periods=len(data), freq="D")
    return data


@load_from.csv(path=value("test_data/signups.csv"))
def signups(data: pd.DataFrame) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01
    """
    data["date"] = pd.date_range(start="2020-01-01", periods=len(data), freq="D")
    return data
