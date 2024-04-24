"""
Module to load iris data.
"""

import pandas as pd
from sklearn import datasets, utils

from hamilton.function_modifiers import extract_columns

RAW_COLUMN_NAMES = [
    "sepal_length_cm",
    "sepal_width_cm",
    "petal_length_cm",
    "petal_width_cm",
]


def iris_data() -> utils.Bunch:
    return datasets.load_iris()


@extract_columns(*(RAW_COLUMN_NAMES + ["target_class"]))
def iris_df(iris_data: utils.Bunch) -> pd.DataFrame:
    _df = pd.DataFrame(iris_data.data, columns=RAW_COLUMN_NAMES)
    _df["target_class"] = [iris_data.target_names[t] for t in iris_data.target]
    return _df
