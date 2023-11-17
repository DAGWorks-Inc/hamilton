import pathlib

import geopandas as gpd
from geopandas.testing import assert_geodataframe_equal

from hamilton.plugins.geopandas_extensions import (
    GeopandasFeatherReader,
    GeopandasFeatherWriter,
    GeopandasFileReader,
    GeopandasFileWriter,
    GeopandasParquetReader,
    GeopandasParquetWriter,
)

try:
    # load in the example data used for python >= 3.8
    import geodatasets

    example_data = gpd.read_file(geodatasets.get_path("nybb"))
except ImportError:
    # create example for python 3.7
    from shapely.geometry import Point

    gdf = {"col1": ["name1", "name2"], "geometry": [Point(1, 2), Point(2, 1)]}
    example_data = gpd.GeoDataFrame(gdf, crs="EPSG:4326")


def test_geopandas_file(tmp_path: pathlib.Path) -> None:

    # write the data to a shapefile
    file_path = tmp_path / "ShapeFileTest"
    writer = GeopandasFileWriter(filename=file_path)
    writer.save_data(data=example_data)

    # read in the multiple files that we output
    dbf_file_path = tmp_path / "ShapeFileTest/ShapeFileTest.dbf"
    shp_file_path = tmp_path / "ShapeFileTest/ShapeFileTest.shp"
    shx_file_path = tmp_path / "ShapeFileTest/ShapeFileTest.shx"

    dbf_reader = GeopandasFileReader(dbf_file_path)
    dbf_data, dbf_metadata = dbf_reader.load_data(gpd.GeoDataFrame)
    shp_reader = GeopandasFileReader(shp_file_path)
    shp_data, shp_metadata = shp_reader.load_data(gpd.GeoDataFrame)
    shx_reader = GeopandasFileReader(shx_file_path)
    shx_data, shx_metadata = shx_reader.load_data(gpd.GeoDataFrame)

    # check that each is the same as the original
    assert_geodataframe_equal(example_data, dbf_data)
    assert_geodataframe_equal(example_data, shp_data)
    assert_geodataframe_equal(example_data, shx_data)


def test_geopandas_parquet(tmp_path: pathlib.Path) -> None:

    # write the data to a shapefile
    file_path = tmp_path / "ParquetFileTest.parquet"
    writer = GeopandasParquetWriter(path=file_path)
    writer.save_data(data=example_data)

    # read in the multiple files that we output
    parquet_file_path = file_path

    parquet_reader = GeopandasParquetReader(parquet_file_path)
    parquet_data, parquet_metadata = parquet_reader.load_data(gpd.GeoDataFrame)

    # check that each is the same as the original
    assert_geodataframe_equal(example_data, parquet_data)


def test_geopandas_feather(tmp_path: pathlib.Path) -> None:

    # write the data to a shapefile
    file_path = tmp_path / "FeatherFileTest.feather"
    writer = GeopandasFeatherWriter(path=file_path)
    writer.save_data(data=example_data)

    # read in the multiple files that we output
    feather_file_path = file_path

    feather_reader = GeopandasFeatherReader(feather_file_path)
    feather_data, feather_metadata = feather_reader.load_data(gpd.GeoDataFrame)

    # check that each is the same as the original
    assert_geodataframe_equal(example_data, feather_data)
