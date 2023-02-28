import pandas as pd

from hamilton import htypes


def base_func(a: pd.Series, b: pd.Series) -> htypes.column[pd.Series, int]:
    """Pandas UDF function."""
    return a + b


def base_func2(base_func: int, c: int) -> int:
    """Vanilla UDF function. This depends on a pandas UDF function."""
    return base_func + c


def base_func3(c: int, d: int) -> int:
    """This function is not satisfied by the dataframe. So is computed without using
    the columns in the dataframe."""
    return c + d
