import dataclasses
from io import BytesIO, TextIOWrapper
from pathlib import Path
from typing import Any, BinaryIO, Collection, Dict, Mapping, Sequence, TextIO, Tuple, Type, Union

try:
    import polars as pl
except ImportError:
    raise NotImplementedError("Polars is not installed.")

from polars import PolarsDataType

# for polars <0.16.0 we need to determine whether type_aliases exist.
has_alias = False
if hasattr(pl, "type_aliases"):
    has_alias = True

# for polars 0.18.0 we need to check what to do.
if has_alias and hasattr(pl.type_aliases, "CsvEncoding"):
    from polars.type_aliases import CsvEncoding
else:
    CsvEncoding = Type
if has_alias and hasattr(pl.type_aliases, "CsvQuoteStyle"):
    from polars.type_aliases import CsvQuoteStyle
else:
    CsvQuoteStyle = Type

from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader, DataSaver

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


@dataclasses.dataclass
class PolarsCSVReader(DataLoader):
    """Class specifically to handle loading CSV files with Polars.
    Should map to https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.read_csv.html
    """

    file: Union[str, TextIO, BytesIO, Path, BinaryIO, bytes]
    # kwargs:
    has_header: bool = True
    columns: Union[Sequence[int], Sequence[str]] = None
    new_columns: Sequence[str] = None
    separator: str = ","
    comment_char: str = None
    quote_char: str = '"'
    skip_rows: int = 0
    dtypes: Union[Mapping[str, PolarsDataType], Sequence[PolarsDataType]] = None
    null_values: Union[str, Sequence[str], Dict[str, str]] = None
    missing_utf8_is_empty_string: bool = False
    ignore_errors: bool = False
    try_parse_dates: bool = False
    n_threads: int = None
    infer_schema_length: int = 100
    batch_size: int = 8192
    n_rows: int = None
    encoding: Union[CsvEncoding, str] = "utf8"
    low_memory: bool = False
    rechunk: bool = True
    use_pyarrow: bool = False
    storage_options: Dict[str, Any] = None
    skip_rows_after_header: int = 0
    row_count_name: str = None
    row_count_offset: int = 0
    sample_size: int = 1024
    eol_char: str = "\n"
    raise_if_empty: bool = True

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self):
        kwargs = {}
        if self.has_header is not None:
            kwargs["has_header"] = self.has_header
        if self.columns is not None:
            kwargs["columns"] = self.columns
        if self.new_columns is not None:
            kwargs["new_columns"] = self.new_columns
        if self.separator is not None:
            kwargs["separator"] = self.separator
        if self.comment_char is not None:
            kwargs["comment_char"] = self.comment_char
        if self.quote_char is not None:
            kwargs["quote_char"] = self.quote_char
        if self.skip_rows is not None:
            kwargs["skip_rows"] = self.skip_rows
        if self.dtypes is not None:
            kwargs["dtypes"] = self.dtypes
        if self.null_values is not None:
            kwargs["null_values"] = self.null_values
        if self.missing_utf8_is_empty_string is not None:
            kwargs["missing_utf8_is_empty_string"] = self.missing_utf8_is_empty_string
        if self.ignore_errors is not None:
            kwargs["ignore_errors"] = self.ignore_errors
        if self.try_parse_dates is not None:
            kwargs["try_parse_dates"] = self.try_parse_dates
        if self.n_threads is not None:
            kwargs["n_threads"] = self.n_threads
        if self.infer_schema_length is not None:
            kwargs["infer_schema_length"] = self.infer_schema_length
        if self.batch_size is not None:
            kwargs["batch_size"] = self.batch_size
        if self.n_rows is not None:
            kwargs["n_rows"] = self.n_rows
        if self.encoding is not None:
            kwargs["encoding"] = self.encoding
        if self.low_memory is not None:
            kwargs["low_memory"] = self.low_memory
        if self.rechunk is not None:
            kwargs["rechunk"] = self.rechunk
        if self.use_pyarrow is not None:
            kwargs["use_pyarrow"] = self.use_pyarrow
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        if self.skip_rows_after_header is not None:
            kwargs["skip_rows_after_header"] = self.skip_rows_after_header
        if self.row_count_name is not None:
            kwargs["row_count_name"] = self.row_count_name
        if self.row_count_offset is not None:
            kwargs["row_count_offset"] = self.row_count_offset
        if self.sample_size is not None:
            kwargs["sample_size"] = self.sample_size
        if self.eol_char is not None:
            kwargs["eol_char"] = self.eol_char
        if self.raise_if_empty is not None:
            kwargs["raise_if_empty"] = self.raise_if_empty
        return kwargs

    def load_data(self, type_: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pl.read_csv(self.file, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.file)
        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "csv"


@dataclasses.dataclass
class PolarsCSVWriter(DataSaver):
    """Class specifically to handle saving CSV files with Polars.
    Should map to https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.write_csv.html
    """

    file: Union[BytesIO, TextIOWrapper, str, Path]
    # kwargs:
    has_header: bool = True
    separator: str = ","
    line_terminator: str = "\n"
    quote: str = '"'
    batch_size: int = 1024
    datetime_format: str = None
    date_format: str = None
    time_format: str = None
    float_precision: int = None
    null_value: str = None
    quote_style: CsvQuoteStyle = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self):
        kwargs = {}
        if self.separator is not None:
            kwargs["separator"] = self.separator
        if self.has_header is not None:
            kwargs["has_header"] = self.has_header
        if self.separator is not None:
            kwargs["separator"] = self.separator
        if self.line_terminator is not None:
            kwargs["line_terminator"] = self.line_terminator
        if self.quote is not None:
            kwargs["quote"] = self.quote
        if self.batch_size is not None:
            kwargs["batch_size"] = self.batch_size
        if self.datetime_format is not None:
            kwargs["datetime_format"] = self.datetime_format
        if self.date_format is not None:
            kwargs["date_format"] = self.date_format
        if self.time_format is not None:
            kwargs["time_format"] = self.time_format
        if self.float_precision is not None:
            kwargs["float_precision"] = self.float_precision
        if self.null_value is not None:
            kwargs["null_value"] = self.null_value
        if self.quote_style is not None:
            kwargs["quote_style"] = self.quote_style
        return kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.write_csv(self.file, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.file)

    def load_data(self, type_: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        df = pl.read_csv(self.file, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.file)
        return df, metadata

    @classmethod
    def name(cls) -> str:
        return "csv"


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [PolarsCSVReader, PolarsCSVWriter]:
        registry.register_adapter(loader)


register_data_loaders()
