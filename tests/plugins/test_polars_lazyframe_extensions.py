import pathlib

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from hamilton.plugins.polars_extensions import (
    PolarsCSVWriter,
    PolarsFeatherWriter,
    PolarsParquetWriter,
)

from hamilton.plugins.polars_lazyframe_extensions import (
    PolarsScanCSVReader,
    PolarsScanParquetReader,
    PolarsScanFeatherReader,
)


@pytest.fixture
def df():
    yield pl.LazyFrame({"a": [1, 2], "b": [3, 4]})


def test_polars_lazyframe_csv(df: pl.LazyFrame, tmp_path: pathlib.Path) -> None:
    file = tmp_path / "test.csv"

    writer = PolarsCSVWriter(file=file)
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PolarsScanCSVReader(file=file)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pl.LazyFrame)

    assert PolarsCSVWriter.applicable_types() == [pl.DataFrame, pl.LazyFrame]
    assert PolarsScanCSVReader.applicable_types() == [pl.LazyFrame]
    assert kwargs1["separator"] == ","
    assert kwargs2["has_header"] is True
    assert_frame_equal(df.collect(), df2.collect())


def test_polars_parquet(df: pl.LazyFrame, tmp_path: pathlib.Path) -> None:
    file = tmp_path / "test.parquet"

    writer = PolarsParquetWriter(file=file)
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PolarsScanParquetReader(file=file, n_rows=2)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pl.DataFrame)

    assert PolarsParquetWriter.applicable_types() == [pl.DataFrame, pl.LazyFrame]
    assert PolarsScanParquetReader.applicable_types() == [pl.LazyFrame]
    assert kwargs1["compression"] == "zstd"
    assert kwargs2["n_rows"] == 2
    assert_frame_equal(df.collect(), df2.collect())


def test_polars_feather(tmp_path: pathlib.Path) -> None:
    test_data_file_path = "tests/resources/data/test_load_from_data.feather"
    reader = PolarsScanFeatherReader(source=test_data_file_path)
    read_kwargs = reader._get_loading_kwargs()
    df, _ = reader.load_data(pl.DataFrame)

    file_path = tmp_path / "test.dta"
    writer = PolarsFeatherWriter(file=file_path)
    write_kwargs = writer._get_saving_kwargs()
    metadata = writer.save_data(df)

    assert PolarsScanFeatherReader.applicable_types() == [pl.DataFrame]
    assert "n_rows" not in read_kwargs
    assert df.shape == (4, 3)

    assert PolarsFeatherWriter.applicable_types() == [pl.DataFrame]
    assert "compression" in write_kwargs
    assert file_path.exists()
    assert metadata["file_metadata"]["path"] == str(file_path)
    assert metadata["dataframe_metadata"]["column_names"] == [
        "animal",
        "points",
        "environment",
    ]
    assert metadata["dataframe_metadata"]["datatypes"] == ["String", "Int64", "String"]

