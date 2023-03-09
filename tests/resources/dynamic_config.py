import pandas as pd

from hamilton.function_modifiers import ResolveAt, extract_columns, parameterize, resolve, source


@extract_columns("a", "b", "c", "d", "e")
def df() -> pd.DataFrame:
    """Produces a dataframe with columns a, b, c, d, e consisting entirely of 1s"""
    return pd.DataFrame(
        {
            "a": [1],
            "b": [1],
            "c": [1],
            "d": [1],
            "e": [1],
        }
    )


@resolve(
    when=ResolveAt.CONFIG_AVAILABLE,
    decorate_with=lambda columns_to_sum_map: parameterize(
        **{
            key: {"col_1": source(value[0]), "col_2": source(value[1])}
            for key, value in columns_to_sum_map.items()
        }
    ),
)
def generic_summation(col_1: pd.Series, col_2: pd.Series) -> pd.Series:
    return col_1 + col_2


# TODO -- add grouping then we can test everything
