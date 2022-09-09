import os

import pandas as pd


def spend(db_path: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(db_path, "marketing_spend.csv"))


def churn(db_path: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(db_path, "marketing_spend.csv"))


def signups(db_path: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(db_path, "marketing_spend.csv"))
