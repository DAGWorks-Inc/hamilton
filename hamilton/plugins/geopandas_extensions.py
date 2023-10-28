import dataclasses
import os
from io import BufferedReader, BytesIO
from pathlib import Path
from typing import IO, Any, Collection, Dict, List, Optional, Tuple, Type, Union

from pyproj import CRS
from shapely import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

try:
    import geopandas as gpd
except ImportError:
    raise NotImplementedError("geopandas is not installed.")

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader, DataSaver

DATAFRAME_TYPE = gpd.GeoDataFrame
COLUMN_TYPE = gpd.GeoSeries

Geometry = Optional[
    Union[
        Point,
        LineString,
        LinearRing,
        Polygon,
        MultiPoint,
        MultiLineString,
        MultiPolygon,
        GeometryCollection,
    ]
]


@registry.get_column.register(gpd.GeoDataFrame)
def get_column_geopandas(df: gpd.GeoDataFrame, column_name: str) -> gpd.GeoSeries:
    return df[column_name]


@registry.fill_with_scalar.register(gpd.GeoDataFrame)
def fill_with_scalar_geopandas(
    df: gpd.GeoDataFrame, column_name: str, value: Any
) -> gpd.GeoDataFrame:
    df[column_name] = value
    return df


def register_types():
    """Function to register the types for this extension."""
    registry.register_types("geopandas", DATAFRAME_TYPE, COLUMN_TYPE)


register_types()


@dataclasses.dataclass
class GeopandasFileWriter(DataSaver):
    """
    Class that handles saving a GeoDataFrame to a file.
    Maps to https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_file.html
    """

    filename: Union[str, os.PathLike, IO]
    # kwargs
    driver: Optional[str] = None
    schema: Optional[Dict] = None
    index: Optional[bool] = None
    mode: str = "w"
    crs: Optional[CRS] = None
    engine: Literal["fiona", "pyogrio"] = None

    # TODO Allow additional arguments via kwargs

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_saving_kwargs(self) -> Dict[str, Any]:
        saving_kwargs = {}
        if self.driver is not None:
            saving_kwargs["driver"] = self.driver
        if self.schema is not None:
            saving_kwargs["schema"] = self.schema
        if self.index is not None:
            saving_kwargs["index"] = self.index
        if self.mode is not None:
            saving_kwargs["mode"] = self.mode
        if self.crs is not None:
            saving_kwargs["crs"] = self.crs
        if self.engine is not None:
            saving_kwargs["engine"] = self.engine

        return saving_kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_file(self.filename, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.filename)

    @classmethod
    def name(cls) -> str:
        return ["shp", "shx", "dbf"]


@dataclasses.dataclass
class GeopandasFileReader(DataLoader):
    """
    Class that handles reading files or URLs with Geopandas
    Maps to https://geopandas.org/en/stable/docs/reference/api/geopandas.read_file.html
    """

    filename: Union[str, Path, BytesIO, BufferedReader]
    # kwargs
    bbox: Optional[Union[Tuple, DATAFRAME_TYPE, COLUMN_TYPE, Geometry]] = None
    mask: Optional[Union[Dict, DATAFRAME_TYPE, COLUMN_TYPE, Geometry]] = None
    rows: Optional[Union[int, slice]] = None
    engine: Literal["fiona", "pyogrio"] = None

    # TODO: allow additional arguments via kwargs
    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        loading_kwargs = {}

        if self.bbox is not None:
            loading_kwargs["bbox"] = self.bbox
        if self.mask is not None:
            loading_kwargs["mask"] = self.mask
        if self.rows is not None:
            loading_kwargs["rows"] = self.rows
        if self.engine is not None:
            loading_kwargs["engine"] = self.engine

        return loading_kwargs

    def load_data(self, type: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        gdf = gpd.read_file(self.filename, **self._get_loading_kwargs())
        metedata = utils.get_file_metadata(self.filename)

        return gdf, metedata

    @classmethod
    def name(cls) -> str:
        return ["shp", "shx", "dbf"]


@dataclasses.dataclass
class GeopandasParquetWriter(DataSaver):
    """
    Class that handles writing a GeoDataFrame to Parquet File.
    Maps to: https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_parquet.html
    """

    path: Union[str, Path]
    # kwargs
    index: Optional[bool] = None
    compression: Union[Literal["snappy", "gzip", "brotli"], None] = "snappy"
    schema_version: Optional[Union[Literal["0.1.0", "0.4.0"], None]] = None
    # TO DO: allow additional arguments via the kwargs keyword

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return DATAFRAME_TYPE

    def _get_saving_kwargs(self) -> Dict[str, Any]:
        saving_kwargs = {}
        if self.index is not None:
            saving_kwargs["index"] = self.index
        if self.compression is not None:
            saving_kwargs["compression"] = self.compression
        if self.schema_version is not None:
            saving_kwargs["schema_version"] = self.schema_version

        return saving_kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_parquet(self.path, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "parquet"


@dataclasses.dataclass
class GeopandasParquetReader(DataLoader):
    """
    Class that handles reading Parquet Files and outputs a GeoDataFrame.
    Maps to: https://geopandas.org/en/stable/docs/reference/api/geopandas.read_parquet.html
    """

    path: Union[str, Path]
    # kwargs
    columns: Optional[List] = None
    storage_options: Optional[Dict] = None

    # TO DO: allow additional arguments via kwargs

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return DATAFRAME_TYPE

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        loading_kwargs = {}
        if self.columns is not None:
            loading_kwargs["columns"] = self.columns
        if self.storage_options is not None:
            loading_kwargs["storage_options"] = self.storage_options

        return loading_kwargs

    def load_data(self, type: Type) -> Tuple[DATAFRAME_TYPE, Dict[str, Any]]:
        gdf = gpd.read_parquet(self.path, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.path)

        return gdf, metadata

    @classmethod
    def name(cls) -> str:
        return "parquet"


@dataclasses.dataclass
class GeopandasFeatherWriter(DataSaver):
    """
    Class that handles writing a GeoDataFrame to a Feather File.
    Maps to: https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_feather.html
    """

    path: Union[str, Path]
    # kwargs
    index: Optional[bool] = None
    compression: Optional[Union[Literal["zstd", "lz4", "uncompressed"], None]] = None
    schema_version: Optional[Union[Literal["0.1.0", "0.4.0"], None]] = None
    # TO DO: allow additional arguments via the kwargs keyword

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return DATAFRAME_TYPE

    def _get_saving_kwargs(self) -> Dict[str, Any]:
        saving_kwargs = {}
        if self.index is not None:
            saving_kwargs["index"] = self.index
        if self.compression is not None:
            saving_kwargs["compression"] = self.compression
        if self.schema_version is not None:
            saving_kwargs["schema_version"] = self.schema_version

        return saving_kwargs

    def save_data(self, data: DATAFRAME_TYPE) -> Dict[str, Any]:
        data.to_feather(self.path, **self._get_saving_kwargs())
        return utils.get_file_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "feather"


@dataclasses.dataclass
class GeopandasFeatherReader(DataLoader):
    """
    Class that handles reading Feather Files and outputs a GeoDataFrame.
    Maps to: https://geopandas.org/en/stable/docs/reference/api/geopandas.read_feather.html
    """

    path: Union[str, Path]
    # kwargs
    columns: Optional[List] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [DATAFRAME_TYPE]

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        loading_kwargs = {}
        if self.columns is not None:
            loading_kwargs["columns"] = self.columns

        return loading_kwargs

    def load_data(self, type: Type) -> Tuple[Any, Dict[str, Any]]:
        gdf = gpd.read_feather(self.path, **self._get_loading_kwargs())
        metadata = utils.get_file_metadata(self.path)

        return gdf, metadata

    @classmethod
    def name(cls) -> str:
        return "feather"


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [
        GeopandasFileReader,
        GeopandasFileWriter,
        GeopandasParquetReader,
        GeopandasParquetWriter,
        GeopandasFeatherReader,
        GeopandasFeatherWriter,
    ]:
        registry.register_adapter(loader)
