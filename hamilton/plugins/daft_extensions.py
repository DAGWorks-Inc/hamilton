import dataclasses
from io import BytesIO, IOBase, TextIOWrapper
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Collection,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    TextIO,
    Tuple,
    Type,
    Union,
)

try:
    import daft
except ImportError:
    raise NotImplementedError("Daft is not installed.")


from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader, DataSaver

DATAFRAME_TYPE = daft.dataframe.dataframe.DataFrame
COLUMN_TYPE = daft.expressions.expressions.Expression


@registry.get_column.register(daft.dataframe.dataframe.DataFrame)
def get_column_daft(
    df: daft.dataframe.dataframe.DataFrame, column_name: str
) -> daft.dataframe.dataframe.DataFrame:
    return df.select(column_name)


@registry.fill_with_scalar.register(daft.dataframe.dataframe.DataFrame)
def fill_with_scalar_daft(
    df: daft.dataframe.dataframe.DataFrame, column_name: str, scalar_value: Any
) -> daft.dataframe.dataframe.DataFrame:
    if not isinstance(scalar_value, daft.expressions.expressions.Expression):
        scalar_value = [scalar_value]
    return df.with_column(
        daft.expressions.expressions.Expression(name=column_name, values=scalar_value)
    )


def register_types():
    """Function to register the types for this extension."""
    registry.register_types("daft", DATAFRAME_TYPE, COLUMN_TYPE)


register_types()
