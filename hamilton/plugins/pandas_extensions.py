import abc
import dataclasses
from typing import Any, Collection, Dict, Tuple, Type

from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader, DataSaver

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


class DataFrameDataLoader(DataLoader, DataSaver, abc.ABC):
    """Base class for data loaders that saves/loads pandas dataframes.
    Note that these are currently grouped together, but this could change!
    We can change this as these are not part of the publicly exposed APIs.
    Rather, the fixed component is the keys (E.G. csv, feather, etc...) , which,
    when combined with types, correspond to a group of specific parameter. As such,
    the backwards-compatible invariance enables us to change the implementation
    (which classes), and so long as the set of parameters/load targets are compatible,
    we are good to go."""

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    @abc.abstractmethod
    def load_data(self, type_: Type[DATAFRAME_TYPE]) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        pass

    @abc.abstractmethod
    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        pass


@dataclasses.dataclass
class CSVDataAdapter(DataFrameDataLoader):
    """Data loader for CSV files. Note that this currently does not support the wide array of
    data loading functionality that pandas does. We will be adding this in over time, but for now
    you can subclass this or open up an issue if this doesn't have what you want.

    Note that, when saving, this does not currently save the index.
    We'll likely want to enable this in the future as an optional subclass,
    in which case we'll separate it out.
    """

    path: str
    sep: str = None

    def _get_loading_kwargs(self):
        kwargs = {}
        if self.sep is not None:
            kwargs["sep"] = self.sep
        return kwargs

    def _get_saving_kwargs(self):
        kwargs = {"index": False}
        if self.sep is not None:
            kwargs["sep"] = self.sep
        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_csv(self.path, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.path)

    def load_data(self, type_: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pd.read_csv(self.path, **self._get_loading_kwargs())
        # Pandas allows URLs for paths in load_csv...
        if str(self.path).startswith("https://"):
            metadata = {"path": self.path}
        else:
            metadata = utils.get_file_metadata(self.path)
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

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_feather(self.path)
        return utils.get_file_metadata(self.path)

    def load_data(self, type_: Type[DATAFRAME_TYPE]) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pd.read_feather(self.path)
        metadata = utils.get_file_metadata(self.path)
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
        metadata = utils.get_file_metadata(self.path)
        return df, metadata

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_parquet(self.path)
        return utils.get_file_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "parquet"


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [CSVDataAdapter, FeatherDataLoader, ParquetDataLoader]:
        registry.register_adapter(loader)


register_data_loaders()
