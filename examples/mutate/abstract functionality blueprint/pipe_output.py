from typing import Any, List

import pandas as pd

from hamilton.function_modifiers import pipe_output, source, step, value


# data1 and data2
def _filter(some_data: pd.DataFrame) -> pd.DataFrame:
    return some_data.dropna()


# data 2
# this is for value
def _add_missing_value(some_data: pd.DataFrame, missing_row: List[Any]) -> pd.DataFrame:
    some_data.loc[-1] = missing_row
    return some_data


# data 2
# this is for source
def _join(some_data: pd.DataFrame, other_data: pd.DataFrame) -> pd.DataFrame:
    return some_data.set_index("col_2").join(other_data.set_index("col_1"))


# data1 and data2
def _sort(some_data: pd.DataFrame) -> pd.DataFrame:
    columns = some_data.columns
    return some_data.sort_values(by=columns[0])


@pipe_output(
    step(_filter),
    step(_sort),
)
def data_1() -> pd.DataFrame:
    df = pd.DataFrame.from_dict({"col_1": [3, 2, pd.NA, 0], "col_2": ["a", "b", pd.NA, "d"]})
    return df


@pipe_output(
    step(_filter),
    step(_add_missing_value, missing_row=value(["c", 145])),
    step(_join, other_data=source("data_3")),
    step(_sort),
)
def data_2() -> pd.DataFrame:
    df = pd.DataFrame.from_dict(
        {"col_1": ["a", "b", pd.NA, "d", "e"], "col_2": [150, 155, 145, 200, 5000]}
    )
    return df


def data_3() -> pd.DataFrame:
    df = pd.DataFrame.from_dict({"col_1": [150, 155, 145, 200, 5000], "col_2": [10, 23, 32, 50, 0]})
    return df


def feat_A(data_1: pd.DataFrame, data_2: pd.DataFrame) -> pd.DataFrame:
    return (
        data_1.set_index("col_2").join(data_2.reset_index(names=["col_3"]).set_index("col_1"))
    ).reset_index(names=["col_0"])
