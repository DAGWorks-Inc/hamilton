import pathlib
import sqlite3
import sys
from typing import Union

import pandas as pd
import pytest
from sqlalchemy import create_engine

from hamilton.plugins.pandas_extensions import (
    PandasJsonReader,
    PandasJsonWriter,
    PandasPickleReader,
    PandasPickleWriter,
    PandasSqlReader,
    PandasSqlWriter,
)


@pytest.fixture
def df():
    yield pd.DataFrame({"foo": ["bar"]})


def test_pandas_pickle(df: pd.DataFrame, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "sample_df.pkl"
    writer = PandasPickleWriter(path=file_path)
    writer.save_data(df)

    reader = PandasPickleReader(filepath_or_buffer=file_path)
    read_df, metadata = reader.load_data(pd.DataFrame)

    assert read_df.equals(df), "DataFrames do not match"
    assert len(list(tmp_path.iterdir())) == 1, "Unexpected number of files in tmp_path directory."


def test_pandas_json(df: pd.DataFrame, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "test.json"

    writer = PandasJsonWriter(filepath_or_buffer=file_path, indent=4)
    kwargs1 = writer._get_saving_kwargs()
    writer.save_data(df)

    reader = PandasJsonReader(filepath_or_buffer=file_path, encoding="utf-8")
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pd.DataFrame)

    assert PandasJsonReader.applicable_types() == [pd.DataFrame]
    assert PandasJsonWriter.applicable_types() == [pd.DataFrame]
    assert kwargs1["indent"] == 4
    assert kwargs2["encoding"] == "utf-8"
    assert file_path.exists()
    assert df.equals(df2)


@pytest.mark.parametrize(
    "conn",
    [
        sqlite3.connect(":memory:"),
        create_engine("sqlite://"),
    ],
)
def test_pandas_sql(df: pd.DataFrame, conn: Union[str, sqlite3.Connection]) -> None:
    writer = PandasSqlWriter(table_name="bar", db_connection=conn)
    kwargs1 = writer._get_saving_kwargs()
    metadata1 = writer.save_data(df)

    reader = PandasSqlReader(query_or_table="SELECT foo FROM bar", db_connection=conn)
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata2 = reader.load_data(pd.DataFrame)

    assert PandasSqlReader.applicable_types() == [pd.DataFrame]
    assert PandasSqlWriter.applicable_types() == [pd.DataFrame]
    assert kwargs1["if_exists"] == "fail"
    assert kwargs2["coerce_float"] is True
    assert df.equals(df2)

    if sys.version_info >= (3, 8):
        # py37 pandas 1.3.5 doesn't return rows inserted
        assert metadata1["rows"] == 1
        assert metadata2["rows"] == 1

    if hasattr(conn, "close"):
        conn.close()
