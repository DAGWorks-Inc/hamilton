import pathlib
import sys

import polars as pl  # isort: skip
import pytest  # isort: skip

from hamilton.plugins.polars_extensions import (  # isort: skip
    PolarsAvroReader,
    PolarsAvroWriter,
    PolarsCSVReader,
    PolarsCSVWriter,
    PolarsDatabaseReader,
    PolarsDatabaseWriter,
    PolarsFeatherReader,
    PolarsFeatherWriter,
    PolarsJSONReader,
    PolarsJSONWriter,
    PolarsParquetReader,
    PolarsParquetWriter,
)


@pytest.fixture
def df():
    yield pl.DataFrame({"a": [1, 2], "b": [3, 4]})


def test_polars_csv(df: pl.DataFrame, tmp_path: pathlib.Path) -> None:
    file = tmp_path / "test.csv"

    writer = PolarsCSVWriter(file=file)
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PolarsCSVReader(file=file)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pl.DataFrame)

    assert PolarsCSVWriter.applicable_types() == [pl.DataFrame]
    assert PolarsCSVReader.applicable_types() == [pl.DataFrame]
    assert kwargs1["separator"] == ","
    assert kwargs2["has_header"] is True
    assert df.frame_equal(df2)


def test_polars_parquet(df: pl.DataFrame, tmp_path: pathlib.Path) -> None:
    file = tmp_path / "test.parquet"

    writer = PolarsParquetWriter(file=file)
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PolarsParquetReader(file=file, n_rows=2)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pl.DataFrame)

    assert PolarsParquetWriter.applicable_types() == [pl.DataFrame]
    assert PolarsParquetReader.applicable_types() == [pl.DataFrame]
    assert kwargs1["compression"] == "zstd"
    assert kwargs2["n_rows"] == 2
    assert df.frame_equal(df2)


def test_polars_feather(tmp_path: pathlib.Path) -> None:
    test_data_file_path = "tests/resources/data/test_load_from_data.feather"
    reader = PolarsFeatherReader(source=test_data_file_path)
    read_kwargs = reader._get_loading_kwargs()
    df, _ = reader.load_data(pl.DataFrame)

    file_path = tmp_path / "test.dta"
    writer = PolarsFeatherWriter(file=file_path)
    write_kwargs = writer._get_saving_kwargs()
    metadata = writer.save_data(df)

    assert PolarsFeatherReader.applicable_types() == [pl.DataFrame]
    assert "n_rows" not in read_kwargs
    assert df.shape == (4, 3)

    assert PolarsFeatherWriter.applicable_types() == [pl.DataFrame]
    assert "compression" in write_kwargs
    assert file_path.exists()
    assert metadata["file_metadata"]["path"] == str(file_path)
    assert metadata["dataframe_metadata"]["column_names"] == ["animal", "points", "environment"]
    assert metadata["dataframe_metadata"]["datatypes"] == ["String", "Int64", "String"]


def test_polars_json(df: pl.DataFrame, tmp_path: pathlib.Path) -> None:
    file = tmp_path / "test.json"
    writer = PolarsJSONWriter(file=file, pretty=True)
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PolarsJSONReader(source=file)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pl.DataFrame)

    assert PolarsJSONWriter.applicable_types() == [pl.DataFrame]
    assert PolarsJSONReader.applicable_types() == [pl.DataFrame]
    assert kwargs1["pretty"]
    assert df2.shape == (2, 2)
    assert "schema" not in kwargs2
    assert df.frame_equal(df2)


def test_polars_avro(df: pl.DataFrame, tmp_path: pathlib.Path) -> None:
    file = tmp_path / "test.avro"

    writer = PolarsAvroWriter(file=file)
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PolarsAvroReader(file=file, n_rows=2)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pl.DataFrame)

    assert PolarsAvroWriter.applicable_types() == [pl.DataFrame]
    assert PolarsAvroReader.applicable_types() == [pl.DataFrame]
    assert kwargs1["compression"] == "uncompressed"
    assert kwargs2["n_rows"] == 2
    assert df.frame_equal(df2)


@pytest.mark.skipif(
    sys.version_info.major == 3 and sys.version_info.minor == 12,
    reason="weird connectorx error on 3.12",
)
def test_polars_database(df: pl.DataFrame, tmp_path: pathlib.Path) -> None:
    conn = f"sqlite:///{tmp_path}/test.db"
    table_name = "test_table"

    writer = PolarsDatabaseWriter(table_name=table_name, connection=conn, if_table_exists="replace")
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PolarsDatabaseReader(query=f"SELECT * FROM {table_name}", connection=conn)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pl.DataFrame)

    assert PolarsDatabaseWriter.applicable_types() == [pl.DataFrame]
    assert PolarsDatabaseReader.applicable_types() == [pl.DataFrame]
    assert kwargs1["if_table_exists"] == "replace"
    assert "batch_size" not in kwargs2
    assert df2.shape == (2, 2)
    assert df.frame_equal(df2)
