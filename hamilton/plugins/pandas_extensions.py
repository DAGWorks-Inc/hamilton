from typing import Any

try:
    import pandas as pd
except ImportError:
    raise NotImplementedError("Pandas is not installed.")

from hamilton import base

# Required constants!
DATAFRAME_TYPE = pd.DataFrame
COLUMN_TYPE = pd.Series


@base.get_column.register(pd.DataFrame)
def get_column_pandas(df: pd.DataFrame, column_name: str) -> pd.Series:
    return df[column_name]


@base.fill_with_scalar.register(pd.DataFrame)
def fill_with_scalar_pandas(df: pd.DataFrame, column_name: str, value: Any) -> pd.DataFrame:
    df[column_name] = value
    return df
