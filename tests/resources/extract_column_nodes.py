import pandas as pd

from hamilton.function_modifiers import extract_columns


@extract_columns("col_1", "col_2")
def generate_df() -> pd.DataFrame:
    """Function that should be parametrized to form multiple functions"""
    return pd.DataFrame({"col_1": [1, 2, 3], "col_2": [4, 5, 6]})
