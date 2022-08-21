import collections

import pandas as pd

from hamilton.function_modifiers import extract_columns

outputs = collections.defaultdict(list)


@extract_columns("col_1", "col_2", "col_3")
def generate_df(unique_id: str) -> pd.DataFrame:
    """Function that should be parametrized to form multiple functions"""
    out = pd.DataFrame({"col_1": [1, 2, 3], "col_2": [4, 5, 6], "col_3": [7, 8, 9]})
    outputs[unique_id].append(out)
    return out
