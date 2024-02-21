import numpy as np
import pandas as pd
import vaex

from hamilton.function_modifiers import extract_columns


@extract_columns("a", "b")
def generate_df() -> vaex.dataframe.DataFrame:
    return vaex.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [2, 3, 4]}))


def a_plus_b_expression(
    a: vaex.expression.Expression, b: vaex.expression.Expression
) -> vaex.expression.Expression:
    return a + b


def a_plus_b_nparray(a: vaex.expression.Expression, b: vaex.expression.Expression) -> np.ndarray:
    return (a + b).to_numpy()


def a_mean(a: vaex.expression.Expression) -> float:
    return a.mean()


def b_mean(b: vaex.expression.Expression) -> float:
    return b.mean()


def ab_as_df(
    a: vaex.expression.Expression, b: vaex.expression.Expression
) -> vaex.dataframe.DataFrame:
    return vaex.from_pandas(pd.DataFrame({"a_in_df": a.to_numpy(), "b_in_df": b.to_numpy()}))
