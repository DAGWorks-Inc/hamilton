import pandas as pd

from hamilton.function_modifiers import value
from hamilton.function_modifiers.adapters import load_from


@load_from.csv(path=value("marketing_spend.csv"))
def spend(data: pd.DataFrame) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01"""
    data["date"] = pd.date_range(start="2020-01-01", periods=len(data), freq="D")
    return data


@load_from.csv(path=value("churn.csv"))
def churn(data: pd.DataFrame) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01
    """
    data["date"] = pd.date_range(start="2020-01-01", periods=len(data), freq="D")
    return data


@load_from.csv(path=value("signups.csv"))
def signups(data: pd.DataFrame) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01
    """
    data["date"] = pd.date_range(start="2020-01-01", periods=len(data), freq="D")
    return data
