import abc
import dataclasses
from typing import Any, Collection, Dict, Tuple, Type

from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader

try:
    import pandas as pd
except ImportError:
    raise NotImplementedError("Pandas is not installed.")

from hamilton import registry

DATAFRAME_TYPE = pd.DataFrame
COLUMN_TYPE = pd.Series


@registry.get_column.register(pd.DataFrame)
def get_column_pandas(df: pd.DataFrame, column_name: str) -> pd.Series:
    return df[column_name]


@registry.fill_with_scalar.register(pd.DataFrame)
def fill_with_scalar_pandas(df: pd.DataFrame, column_name: str, value: Any) -> pd.DataFrame:
    df[column_name] = value
    return df


def register_types():
    """Function to register the types for this extension."""
    registry.register_types("pandas", DATAFRAME_TYPE, COLUMN_TYPE)


register_types()


class DataFrameDataLoader(DataLoader, abc.ABC):
    """Base class for data loaders that load pandas dataframes."""

    @classmethod
    def load_targets(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    @abc.abstractmethod
    def load_data(self, type_: Type[DATAFRAME_TYPE]) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        pass


@dataclasses.dataclass
class CSVDataLoader(DataFrameDataLoader):
    """Data loader for CSV files. Note that this currently does not support the wide array of
    data loading functionality that pandas does. We will be adding this in over time, but for now
    you can subclass this or open up an issue if this doesn't have what you want."""

    path: str

    def load_data(self, type_: Type) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        df = pd.read_csv(self.path)
        metadata = utils.get_file_loading_metadata(self.path)
        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "csv"


@dataclasses.dataclass
class FeatherDataLoader(DataFrameDataLoader):
    """Data loader for feather files. Note that this currently does not support the wide array of
    data loading functionality that pandas does. We will be adding this in over time, but for now
    you can subclass this or open up an issue if this doesn't have what you want."""

    path: str

    def load_data(self, type_: Type[DATAFRAME_TYPE]) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pd.read_feather(self.path)
        metadata = utils.get_file_loading_metadata(self.path)
        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "feather"


@dataclasses.dataclass
class ParquetDataLoader(DataFrameDataLoader):
    """Data loader for feather files. Note that this currently does not support the wide array of
    data loading functionality that pandas does. We will be adding this in over time, but for now
    you can subclass this or open up an issue if this doesn't have what you want."""

    path: str

    def load_data(self, type_: Type[DATAFRAME_TYPE]) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pd.read_parquet(self.path)
        metadata = utils.get_file_loading_metadata(self.path)
        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "parquet"


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [CSVDataLoader, FeatherDataLoader, ParquetDataLoader]:
        registry.register_adapter(loader)


register_data_loaders()
