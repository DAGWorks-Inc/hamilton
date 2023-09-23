import abc
import dataclasses
import sys
from collections.abc import Hashable
from datetime import datetime
from io import BufferedReader, BytesIO, StringIO
from pathlib import Path
from typing import Any, Callable, Collection, Dict, Iterator, List, Optional, Tuple, Type, Union

try:
    import pandas as pd
except ImportError:
    raise NotImplementedError("Pandas is not installed.")

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

try:
    from collections.abc import Iterable, Sequence
except ImportError:
    from collections import Sequence, Iterable

from sqlite3 import Connection

from pandas._typing import NpDtype
from pandas.core.dtypes.dtypes import ExtensionDtype

from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader, DataSaver

DATAFRAME_TYPE = pd.DataFrame
COLUMN_TYPE = pd.Series

JSONSerializable = Optional[Union[str, float, bool, List, Dict]]
IndexLabel = Optional[Union[Hashable, Iterator[Hashable]]]
Dtype = Union[ExtensionDtype, NpDtype]


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


@dataclasses.dataclass
class PandasPickleReader(DataLoader):
    """Class for loading/reading pickle files with Pandas.
    Maps to https://pandas.pydata.org/docs/reference/api/pandas.read_pickle.html#pandas.read_pickle
    """

    filepath_or_buffer: Union[str, Path, BytesIO, BufferedReader]
    # kwargs:
    compression: Union[str, Dict[str, Any], None] = "infer"
    storage_options: Optional[Dict[str, Any]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        # Returns type for which data loader is available
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        # Puts kwargs in a dict
        kwargs = {}
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        return kwargs

    def load_data(self, type_: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        # Loads the data and returns the df and metadata of the pickle
        df = pd.read_pickle(self.filepath_or_buffer, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.filepath_or_buffer)

        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "pickle"


# for python 3.7 compatibility
if sys.version_info < (3, 8):
    pickle_protocol_default = 4
else:
    pickle_protocol_default = 5


@dataclasses.dataclass
class PandasPickleWriter(DataSaver):
    """Class that handles saving pickle files with pandas.
    Maps to https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_pickle.html#pandas.DataFrame.to_pickle
    """

    path: Union[str, Path, BytesIO, BufferedReader]
    # kwargs:
    compression: Union[str, Dict[str, Any], None] = "infer"
    protocol: int = pickle_protocol_default
    storage_options: Optional[Dict[str, Any]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self) -> Dict[str, Any]:
        # Puts kwargs in a dict
        kwargs = {}
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.protocol is not None:
            kwargs["protocol"] = self.protocol
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_pickle(self.path, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "pickle"


@dataclasses.dataclass
class PandasJsonReader(DataLoader):
    """Class specifically to handle loading JSON files/buffers with Pandas.

    Disclaimer: We're exposing all the *current* params from the Pandas read_json method.
    Some of these params may get deprecated or new params may be introduced. In the event that
    the params/kwargs below become outdated, please raise an issue or submit a pull request.

    Should map to https://pandas.pydata.org/docs/reference/api/pandas.read_json.html
    """

    filepath_or_buffer: Union[str, Path, BytesIO, BufferedReader]
    # kwargs
    chunksize: Optional[int] = None
    compression: Optional[Union[str, Dict[str, Any]]] = "infer"
    convert_axes: Optional[bool] = None
    convert_dates: Union[bool, List[str]] = True
    date_unit: Optional[str] = None
    dtype: Optional[Union[Dtype, Dict[Hashable, Dtype]]] = None
    dtype_backend: Optional[str] = None
    encoding: Optional[str] = None
    encoding_errors: Optional[str] = "strict"
    engine: str = "ujson"
    keep_default_dates: bool = True
    lines: bool = False
    nrows: Optional[int] = None
    orient: Optional[str] = None
    precise_float: bool = False
    storage_options: Optional[Dict[str, Any]] = None
    typ: str = "frame"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        if self.chunksize is not None:
            kwargs["chunksize"] = self.chunksize
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.convert_axes is not None:
            kwargs["convert_axes"] = self.convert_axes
        if self.convert_dates is not None:
            kwargs["convert_dates"] = self.convert_dates
        if self.date_unit is not None:
            kwargs["date_unit"] = self.date_unit
        if self.dtype is not None:
            kwargs["dtype"] = self.dtype
        if self.dtype_backend is not None:
            kwargs["dtype_backend"] = self.dtype_backend
        if self.encoding is not None:
            kwargs["encoding"] = self.encoding
        if self.encoding_errors is not None:
            kwargs["encoding_errors"] = self.encoding_errors
        if sys.version_info >= (3, 8) and self.engine is not None:
            kwargs["engine"] = self.engine
        if self.keep_default_dates is not None:
            kwargs["keep_default_dates"] = self.keep_default_dates
        if self.lines is not None:
            kwargs["lines"] = self.lines
        if self.nrows is not None:
            kwargs["nrows"] = self.nrows
        if self.orient is not None:
            kwargs["orient"] = self.orient
        if self.precise_float is not None:
            kwargs["precise_float"] = self.precise_float
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        if self.typ is not None:
            kwargs["typ"] = self.typ
        return kwargs

    def load_data(self, type_: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pd.read_json(self.filepath_or_buffer, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.filepath_or_buffer)
        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "json"


@dataclasses.dataclass
class PandasJsonWriter(DataSaver):
    """Class specifically to handle saving JSON files/buffers with Pandas.

    Disclaimer: We're exposing all the *current* params from the Pandas DataFrame.to_json method.
    Some of these params may get deprecated or new params may be introduced. In the event that
    the params/kwargs below become outdated, please raise an issue or submit a pull request.

    Should map to https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html
    """

    filepath_or_buffer: Union[str, Path, BytesIO, BufferedReader]
    # kwargs
    compression: str = "infer"
    date_format: str = "epoch"
    date_unit: str = "ms"
    default_handler: Optional[Callable[[Any], JSONSerializable]] = None
    double_precision: int = 10
    force_ascii: bool = True
    index: Optional[bool] = None
    indent: int = 0
    lines: bool = False
    mode: str = "w"
    orient: Optional[str] = None
    storage_options: Optional[Dict[str, Any]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self):
        kwargs = {}
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.date_format is not None:
            kwargs["date_format"] = self.date_format
        if self.date_unit is not None:
            kwargs["date_unit"] = self.date_unit
        if self.default_handler is not None:
            kwargs["default_handler"] = self.default_handler
        if self.double_precision is not None:
            kwargs["double_precision"] = self.double_precision
        if self.force_ascii is not None:
            kwargs["force_ascii"] = self.force_ascii
        if self.index is not None:
            kwargs["index"] = self.index
        if self.indent is not None:
            kwargs["indent"] = self.indent
        if self.lines is not False:
            kwargs["lines"] = self.lines
        if sys.version_info >= (3, 8) and self.mode is not None:
            kwargs["mode"] = self.mode
        if self.orient is not None:
            kwargs["orient"] = self.orient
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_json(self.filepath_or_buffer, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.filepath_or_buffer)

    @classmethod
    def name(cls) -> str:
        return "json"


@dataclasses.dataclass
class PandasSqlReader(DataLoader):
    """Class specifically to handle loading SQL data using Pandas.

    Disclaimer: We're exposing all the *current* params from the Pandas read_sql method.
    Some of these params may get deprecated or new params may be introduced. In the event that
    the params/kwargs below become outdated, please raise an issue or submit a pull request.

    Should map to https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
    Requires optional Pandas dependencies. See https://pandas.pydata.org/docs/getting_started/install.html#sql-databases.
    """

    query_or_table: str
    db_connection: Union[str, Connection]  # can pass in SQLAlchemy engine/connection
    # kwarg
    chunksize: Optional[int] = None
    coerce_float: bool = True
    columns: Optional[List[str]] = None
    dtype: Optional[Union[Dtype, Dict[Hashable, Dtype]]] = None
    dtype_backend: Optional[str] = None
    index_col: Optional[Union[str, List[str]]] = None
    params: Optional[Union[List, Tuple, Dict]] = None
    parse_dates: Optional[Union[List, Dict]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        if self.chunksize is not None:
            kwargs["chunksize"] = self.chunksize
        if self.coerce_float is not None:
            kwargs["coerce_float"] = self.coerce_float
        if self.columns is not None:
            kwargs["columns"] = self.columns
        if self.dtype is not None:
            kwargs["dtype"] = self.dtype
        if self.dtype_backend is not None:
            kwargs["dtype_backend"] = self.dtype_backend
        if self.index_col is not None:
            kwargs["index_col"] = self.index_col
        if self.params is not None:
            kwargs["params"] = self.params
        if self.parse_dates is not None:
            kwargs["parse_dates"] = self.parse_dates
        return kwargs

    def load_data(self, type_: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pd.read_sql(self.query_or_table, self.db_connection, **self._get_loading_kwargs())
        metadata = utils.get_sql_metadata(self.query_or_table, df)
        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "sql"


@dataclasses.dataclass
class PandasSqlWriter(DataSaver):
    """Class specifically to handle saving DataFrames to SQL databases using Pandas.

    Disclaimer: We're exposing all the *current* params from the Pandas DataFrame.to_sql method.
    Some of these params may get deprecated or new params may be introduced. In the event that
    the params/kwargs below become outdated, please raise an issue or submit a pull request.

    Should map to https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    Requires optional Pandas dependencies. See https://pandas.pydata.org/docs/getting_started/install.html#sql-databases.
    """

    table_name: str
    db_connection: Union[str, Connection]  # can pass in SQLAlchemy engine/connection
    # kwargs
    chunksize: Optional[int] = None
    dtype: Optional[Union[Dtype, Dict[Hashable, Dtype]]] = None
    if_exists: str = "fail"
    index: bool = True
    index_label: Optional[IndexLabel] = None
    method: Optional[Union[str, Callable]] = None
    schema: Optional[str] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        if self.chunksize is not None:
            kwargs["chunksize"] = self.chunksize
        if self.dtype is not None:
            kwargs["dtype"] = self.dtype
        if self.if_exists is not None:
            kwargs["if_exists"] = self.if_exists
        if self.index is not None:
            kwargs["index"] = self.index
        if self.index_label is not None:
            kwargs["index_label"] = self.index_label
        if self.method is not None:
            kwargs["method"] = self.method
        if self.schema is not None:
            kwargs["schema"] = self.schema
        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        results = data.to_sql(self.table_name, self.db_connection, **self._get_saving_kwargs())
        return utils.get_sql_metadata(self.table_name, results)

    @classmethod
    def name(cls) -> str:
        return "sql"


@dataclasses.dataclass
class PandasXmlReader(DataLoader):
    """Class for loading/reading xml files with Pandas.
    Maps to https://pandas.pydata.org/docs/reference/api/pandas.read_xml.html

    Requires `lxml`. See https://pandas.pydata.org/docs/getting_started/install.html#xml
    """

    path_or_buffer: Union[str, Path, BytesIO, BufferedReader]
    # kwargs
    xpath: Optional[str] = "./*"
    namespace: Optional[Dict[str, str]] = None
    elems_only: Optional[bool] = False
    attrs_only: Optional[bool] = False
    names: Optional[List[str]] = None
    dtype: Optional[Dict[str, Any]] = None
    converters: Optional[Dict[Union[int, str], Any]] = None
    parse_dates: Union[bool, List[Union[int, str, List[List], Dict[str, List[int]]]]] = False
    encoding: Optional[str] = "utf-8"
    parser: str = "lxml"
    stylesheet: Union[str, Path, BytesIO, BufferedReader] = None
    iterparse: Optional[Dict[str, List[str]]] = None
    compression: Union[str, Dict[str, Any], None] = "infer"
    storage_options: Optional[Dict[str, Any]] = None
    dtype_backend: str = "numpy_nullable"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        if self.xpath is not None:
            kwargs["xpath"] = self.xpath
        if self.namespace is not None:
            kwargs["namespace"] = self.namespace
        if self.elems_only is not None:
            kwargs["elems_only"] = self.elems_only
        if self.attrs_only is not None:
            kwargs["attrs_only"] = self.attrs_only
        if self.names is not None:
            kwargs["names"] = self.names
        if self.dtype is not None:
            kwargs["dtype"] = self.dtype
        if self.converters is not None:
            kwargs["converters"] = self.converters
        if sys.version_info >= (3, 8) and self.parse_dates is not None:
            kwargs["parse_dates"] = self.parse_dates
        if self.encoding is not None:
            kwargs["encoding"] = self.encoding
        if self.parser is not None:
            kwargs["parser"] = self.parser
        if self.encoding is not None:
            kwargs["encoding"] = self.encoding
        if self.parser is not None:
            kwargs["parser"] = self.parser
        if self.stylesheet is not None:
            kwargs["stylesheet"] = self.stylesheet
        if self.iterparse is not None:
            kwargs["iterparse"] = self.iterparse
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        if sys.version_info >= (3, 8) and self.dtype_backend is not None:
            kwargs["dtype_backend"] = self.dtype_backend
        return kwargs

    def load_data(self, type: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        # Loads the data and returns the df and metadata of the xml
        df = pd.read_xml(self.path_or_buffer, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.path_or_buffer)

        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "xml"


@dataclasses.dataclass
class PandasXmlWriter(DataSaver):
    """Class specifically to handle saving xml files/buffers with Pandas.
    Should map to https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_xml.html

    Requires `lxml`. See https://pandas.pydata.org/docs/getting_started/install.html#xml.
    """

    path_or_buffer: Union[str, Path, BytesIO, BufferedReader]
    # kwargs
    index: bool = True
    root_name: str = "data"
    row_name: str = "row"
    na_rep: Optional[str] = None
    attr_cols: Optional[List[str]] = None
    elems_cols: Optional[List[str]] = None
    namespaces: Optional[Dict[str, str]] = None
    prefix: Optional[str] = None
    encoding: str = "utf-8"
    xml_declaration: bool = True
    pretty_print: bool = True
    parser: str = "lxml"
    stylesheet: Optional[Union[str, Path, BytesIO, BufferedReader]] = None
    compression: Union[str, Dict[str, Any], None] = "infer"
    storage_options: Optional[Dict[str, Any]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self):
        kwargs = {}
        if self.index is not None:
            kwargs["index"] = self.index
        if self.root_name is not None:
            kwargs["root_name"] = self.root_name
        if self.row_name is not None:
            kwargs["row_name"] = self.row_name
        if self.na_rep is not None:
            kwargs["na_rep"] = self.na_rep
        if self.attr_cols is not None:
            kwargs["attr_cols"] = self.attr_cols
        if self.elems_cols is not None:
            kwargs["elems_cols"] = self.elems_cols
        if self.namespaces is not None:
            kwargs["namespaces"] = self.namespaces
        if self.prefix is not None:
            kwargs["prefix"] = self.prefix
        if self.encoding is not None:
            kwargs["encoding"] = self.encoding
        if self.xml_declaration is not None:
            kwargs["xml_declaration"] = self.xml_declaration
        if self.pretty_print is not None:
            kwargs["pretty_print"] = self.pretty_print
        if self.parser is not None:
            kwargs["parser"] = self.parser
        if self.stylesheet is not None:
            kwargs["stylesheet"] = self.stylesheet
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_xml(self.path_or_buffer, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.path_or_buffer)

    @classmethod
    def name(cls) -> str:
        return "xml"


@dataclasses.dataclass
class PandasHtmlReader(DataLoader):
    """Class for loading/reading xml files with Pandas.
    Maps to https://pandas.pydata.org/docs/reference/api/pandas.read_html.html
    """

    io: Union[str, Path, BytesIO, BufferedReader]
    # kwargs
    match: Optional[str] = ".+"
    flavor: Optional[Union[str, Sequence]] = None
    header: Optional[Union[int, Sequence]] = None
    index_col: Optional[Union[int, Sequence]] = None
    skiprows: Optional[Union[int, Sequence, slice]] = None
    attrs: Optional[Dict[str, str]] = None
    parse_dates: Optional[bool] = None
    thousands: Optional[str] = ","
    encoding: Optional[str] = None
    decimal: str = "."
    converters: Optional[Dict[Any, Any]] = None
    na_values: Iterable = None
    keep_default_na: bool = True
    displayed_only: bool = True
    extract_links: Optional[Literal["header", "footer", "body", "all"]] = None
    dtype_backend: Literal["pyarrow", "numpy_nullable"] = "numpy_nullable"
    storage_options: Optional[Dict[str, Any]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        if self.match is not None:
            kwargs["match"] = self.match
        if self.flavor is not None:
            kwargs["flavor"] = self.flavor
        if self.header is not None:
            kwargs["header"] = self.header
        if self.index_col is not None:
            kwargs["index_col"] = self.index_col
        if self.skiprows is not None:
            kwargs["skiprows"] = self.skiprows
        if self.attrs is not None:
            kwargs["attrs"] = self.attrs
        if sys.version_info >= (3, 8) and self.parse_dates is not None:
            kwargs["parse_dates"] = self.parse_dates
        if self.thousands is not None:
            kwargs["thousands"] = self.thousands
        if self.encoding is not None:
            kwargs["encoding"] = self.encoding
        if self.decimal is not None:
            kwargs["decimal"] = self.decimal
        if self.converters is not None:
            kwargs["converters"] = self.converters
        if self.na_values is not None:
            kwargs["na_values"] = self.na_values
        if self.keep_default_na is not None:
            kwargs["keep_default_na"] = self.keep_default_na
        if self.displayed_only is not None:
            kwargs["displayed_only"] = self.displayed_only
        if self.extract_links is not None:
            kwargs["extract_links"] = self.extract_links
        if sys.version_info >= (3, 8) and self.dtype_backend is not None:
            kwargs["dtype_backend"] = self.dtype_backend
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options

        return kwargs

    def load_data(self, type: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        # Loads the data and returns the df and metadata of the xml
        df = pd.read_html(self.io, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.io)

        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "html"


@dataclasses.dataclass
class PandasHtmlWriter(DataSaver):
    """Class specifically to handle saving xml files/buffers with Pandas.
    Should map to https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_html.html#pandas.DataFrame.to_html
    """

    buf: Union[str, Path, StringIO, None] = None
    # kwargs
    columns: Optional[List[str]] = None
    col_space: Optional[Union[str, int, List, Dict]] = None
    header: Optional[bool] = True
    index: Optional[bool] = True
    na_rep: Optional[str] = "NaN"
    formatters: Optional[Union[List, Tuple, Dict]] = None
    float_format: Optional[str] = None
    sparsify: Optional[bool] = True
    index_names: Optional[bool] = True
    justify: str = None
    max_rows: Optional[int] = None
    max_cols: Optional[int] = None
    show_dimensions: bool = False
    decimal: str = "."
    bold_rows: bool = True
    classes: Union[str, List[str], Tuple, None] = None
    escape: Optional[bool] = True
    notebook: Literal[True, False] = False
    border: int = None
    table_id: Optional[str] = None
    render_links: bool = False
    encoding: Optional[str] = "utf-8"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self):
        kwargs = {}
        if self.columns is not None:
            kwargs["columns"] = self.columns
        if self.col_space is not None:
            kwargs["col_space"] = self.col_space
        if self.header is not None:
            kwargs["header"] = self.header
        if self.index is not None:
            kwargs["index"] = self.index
        if self.na_rep is not None:
            kwargs["na_rep"] = self.na_rep
        if self.formatters is not None:
            kwargs["formatters"] = self.formatters
        if self.float_format is not None:
            kwargs["float_format"] = self.float_format
        if self.sparsify is not None:
            kwargs["sparsify"] = self.sparsify
        if self.index_names is not None:
            kwargs["index_names"] = self.index_names
        if self.justify is not None:
            kwargs["justify"] = self.justify
        if self.max_rows is not None:
            kwargs["max_rows"] = self.max_rows
        if self.max_cols is not None:
            kwargs["max_cols"] = self.max_cols
        if self.show_dimensions is not None:
            kwargs["show_dimensions"] = self.show_dimensions
        if self.decimal is not None:
            kwargs["decimal"] = self.decimal
        if self.bold_rows is not None:
            kwargs["bold_rows"] = self.bold_rows
        if self.classes is not None:
            kwargs["classes"] = self.classes
        if self.escape is not None:
            kwargs["escape"] = self.escape
        if self.notebook is not None:
            kwargs["notebook"] = self.notebook
        if self.border is not None:
            kwargs["border"] = self.border
        if self.table_id is not None:
            kwargs["table_id"] = self.table_id
        if self.render_links is not None:
            kwargs["render_links"] = self.render_links
        if self.encoding is not None:
            kwargs["encoding"] = self.encoding

        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_html(self.buf, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.buf)

    @classmethod
    def name(cls) -> str:
        return "html"


@dataclasses.dataclass
class PandasStataReader(DataLoader):
    """Class for loading/reading xml files with Pandas.
    Maps to https://pandas.pydata.org/docs/reference/api/pandas.read_stata.html#pandas.read_stata
    """

    filepath_or_buffer: Union[str, Path, BytesIO, BufferedReader]
    # kwargs
    convert_dates: bool = True
    convert_categoricals: bool = True
    index_col: Optional[str] = None
    convert_missing: bool = False
    preserve_dtypes: bool = True
    columns: Optional[Sequence] = None
    order_categoricals: bool = True
    chunksize: Optional[int] = None
    iterator: bool = False
    compression: Union[
        Dict[str, Any], Literal["infer", "gzip", "bz2", "zip", "xz", "zstd", "tar"]
    ] = "infer"
    storage_options: Optional[Dict[str, Any]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        if self.convert_dates is not None:
            kwargs["convert_dates"] = self.convert_dates
        if self.convert_categoricals is not None:
            kwargs["convert_categoricals"] = self.convert_categoricals
        if self.index_col is not None:
            kwargs["index_col"] = self.index_col
        if self.convert_missing is not None:
            kwargs["convert_missing"] = self.convert_missing
        if self.preserve_dtypes is not None:
            kwargs["preserve_dtypes"] = self.preserve_dtypes
        if self.columns is not None:
            kwargs["columns"] = self.columns
        if self.order_categoricals is not None:
            kwargs["order_categoricals"] = self.order_categoricals
        if self.chunksize is not None:
            kwargs["chunksize"] = self.chunksize
        if self.iterator is not None:
            kwargs["iterator"] = self.iterator
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options

        return kwargs

    def load_data(self, type: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        # Loads the data and returns the df and metadata of the xml
        df = pd.read_stata(self.filepath_or_buffer, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.filepath_or_buffer)

        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "stata"


@dataclasses.dataclass
class PandasStataWriter(DataSaver):
    """Class specifically to handle saving xml files/buffers with Pandas.
    Should map to https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_stata.html
    """

    path: Union[str, Path, BufferedReader] = None
    # kwargs
    convert_dates: Optional[Dict[Hashable, str]] = None
    write_index: bool = True
    byteorder: Optional[str] = None
    time_stamp: Optional[datetime] = None
    data_label: Optional[str] = None
    variable_labels: Optional[Dict[Hashable, str]] = None
    version: Literal["114", "117", "118", "119", None] = "114"
    convert_strl: Optional[str] = None
    compression: Union[
        Dict[str, Any], Literal["infer", "gzip", "bz2", "zip", "xz", "zstd", "tar"]
    ] = "infer"
    storage_options: Optional[Dict[str, Any]] = None
    value_labels: Optional[Dict[Hashable, str]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self):
        kwargs = {}
        if self.convert_dates is not None:
            kwargs["convert_dates"] = self.convert_dates
        if self.write_index is not None:
            kwargs["write_index"] = self.write_index
        if self.byteorder is not None:
            kwargs["byteorder"] = self.byteorder
        if self.time_stamp is not None:
            kwargs["time_stamp"] = self.time_stamp
        if self.data_label is not None:
            kwargs["data_label"] = self.data_label
        if self.variable_labels is not None:
            kwargs["variable_labels"] = self.variable_labels
        if self.version is not None:
            kwargs["version"] = self.version
        if self.convert_strl is not None and self.version == "117":
            kwargs["convert_strl"] = self.convert_strl
        if self.compression is not None:
            kwargs["compression"] = self.compression
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        if self.value_labels is not None:
            kwargs["value_labels"] = self.value_labels

        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_stata(self.path, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.buf)

    @classmethod
    def name(cls) -> str:
        return "stata"


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [
        CSVDataAdapter,
        FeatherDataLoader,
        ParquetDataLoader,
        PandasPickleReader,
        PandasPickleWriter,
        PandasJsonReader,
        PandasJsonWriter,
        PandasSqlReader,
        PandasSqlWriter,
        PandasXmlReader,
        PandasXmlWriter,
        PandasHtmlReader,
        PandasHtmlWriter,
        PandasStataReader,
        PandasStataWriter,
    ]:
        registry.register_adapter(loader)


register_data_loaders()
