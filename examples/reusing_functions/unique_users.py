import pandas as pd

_grain_mapping = {"day": "D", "week": "W", "month": "M"}


def _validate_grain(grain: str):
    assert grain in ["day", "week", "month"]


def filtered_interactions(website_interactions: pd.DataFrame, region: str) -> pd.DataFrame:
    return website_interactions[website_interactions.region == region]


def unique_users(filtered_interactions: pd.DataFrame, grain: str) -> pd.Series:
    """Gives the number of shares traded by the frequency"""
    _validate_grain(grain)
    return filtered_interactions.resample(_grain_mapping[grain])["user_id"].nunique()
