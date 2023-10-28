import pathlib

import geodatasets
import geopandas as gpd
from geopandas.testing import assert_geodataframe_equal

from hamilton.plugins.geopandas_extensions import GeopandasFileReader, GeopandasFileWriter


def test_geopandas_file(tmp_path: pathlib.Path) -> None:
    # load in the example data
    new_york_example_data = gpd.read_file(geodatasets.get_path("nybb"))

    # write the data to a shapefile
    file_path = tmp_path / "ShapeFileTest"
    writer = GeopandasFileWriter(filename=file_path)
    writer.save_data(data=new_york_example_data)

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
    assert_geodataframe_equal(new_york_example_data, dbf_data)
    assert_geodataframe_equal(new_york_example_data, shp_data)
    assert_geodataframe_equal(new_york_example_data, shx_data)
