import pandas as pd

from hamilton.function_modifiers import tag


@tag(cache="parquet")
def spend() -> pd.Series:
    """Emulates potentially expensive data extraction."""
    return pd.Series([10, 10, 20, 40, 40, 50])


@tag(cache="parquet")
def signups() -> pd.Series:
    """Emulates potentially expensive data extraction."""
    return pd.Series([1, 10, 50, 100, 200, 400])
