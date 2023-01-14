from typing import Any

try:
    import polars as pl
except ImportError:
    raise NotImplementedError("Polars is not installed.")

from hamilton import registry

DATAFRAME_TYPE = pl.DataFrame
COLUMN_TYPE = pl.Series


@registry.get_column.register(pl.DataFrame)
def get_column_polars(df: pl.DataFrame, column_name: str) -> pl.Series:
    return df[column_name]


@registry.fill_with_scalar.register(pl.DataFrame)
def fill_with_scalar_polars(df: pl.DataFrame, column_name: str, scalar_value: Any) -> pl.DataFrame:
    if not isinstance(scalar_value, pl.Series):
        scalar_value = [scalar_value]
    return df.with_column(pl.Series(name=column_name, values=scalar_value))


def register_types():
    """Function to register the types for this extension."""
    registry.register_types("polars", DATAFRAME_TYPE, COLUMN_TYPE)


register_types()
