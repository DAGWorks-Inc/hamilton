import os

import generate_test_data
import pandas as pd


def index(start_date: str = "20220801", end_date: str = "20220801") -> pd.Series:
    return pd.Series(pd.date_range(start=start_date, end=end_date))


def spend(index: pd.Series) -> pd.DataFrame:
    return generate_test_data.marketing_spend_by_channel(index)


def churn(index: pd.Series) -> pd.DataFrame:
    return generate_test_data.churn_by_business_line(index)


def signups(index: pd.Series) -> pd.DataFrame:
    return generate_test_data.signups_by_business_line(index)
