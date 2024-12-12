import pandas as pd


def raw_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def transformed_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data.dropna()
