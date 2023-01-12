from typing import Any

try:
    import pyspark.pandas as ps
except ImportError:
    raise NotImplementedError("Pyspark is not installed.")

from hamilton import base

# Required constants!
DATAFRAME_TYPE = ps.DataFrame
COLUMN_TYPE = ps.Series


@base.get_column.register(ps.DataFrame)
def get_column_pyspark_pandas(df: ps.DataFrame, column_name: str) -> ps.Series:
    return df[column_name]


@base.fill_with_scalar.register(ps.DataFrame)
def fill_with_scalar_pyspark_pandas(df: ps.DataFrame, column_name: str, value: Any) -> ps.DataFrame:
    df[column_name] = value
    return df
