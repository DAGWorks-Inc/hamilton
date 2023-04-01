import os

import pandas as pd


def spend(db_path: str) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01"""
    df = pd.read_csv(os.path.join(db_path, "marketing_spend.csv"))
    df["date"] = pd.date_range(start="2020-01-01", periods=len(df), freq="D")
    return df


def churn(db_path: str) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01
    """
    df = pd.read_csv(os.path.join(db_path, "churn.csv"))
    df["date"] = pd.date_range(start="2020-01-01", periods=len(df), freq="D")
    return df


def signups(db_path: str) -> pd.DataFrame:
    """Takes in the dataframe and then generates a date index column to it,
    where each row is a day starting from 2020-01-01
    """
    df = pd.read_csv(os.path.join(db_path, "signups.csv"))
    df["date"] = pd.date_range(start="2020-01-01", periods=len(df), freq="D")
    return df
