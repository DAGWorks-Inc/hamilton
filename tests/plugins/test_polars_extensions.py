import pathlib

import polars as pl
import pytest

from hamilton.plugins.polars_extensions import PolarsCSVReader, PolarsCSVWriter


@pytest.fixture
def df():
    yield pl.DataFrame({"foo": ["bar"]})


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
