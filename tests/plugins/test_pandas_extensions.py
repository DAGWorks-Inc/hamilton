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

DB_RELATIVE_PATH = "tests/resources/data/test.db"
DB_MEMORY_PATH = "sqlite://"
DB_DISK_PATH = f"{DB_MEMORY_PATH}/"
DB_ABSOLUTE_PATH = f"{DB_DISK_PATH}{DB_RELATIVE_PATH}"


def test_pandas_pickle(tmp_path: pathlib.Path) -> None:
    data = {
        "name": ["ladybird", "butterfly", "honeybee"],
        "num_caught": [4, 5, 6],
        "stings": [0, 0, 3],
    }
    sample_df = pd.DataFrame(data)

    save_path = tmp_path / "sample_df.pkl"
    writer = PandasPickleWriter(path=save_path)
    writer.save_data(sample_df)

    reader = PandasPickleReader(filepath_or_buffer=save_path)
    read_df, metadata = reader.load_data(type(sample_df))

    # same contents
    assert read_df.equals(sample_df), "DataFrames do not match"

    # correct number of files returned
    assert len(list(tmp_path.iterdir())) == 1, "Unexpected number of files in tmp_path directory."


def test_pandas_json_reader() -> None:
    file_path = "tests/resources/data/test_load_from_data.json"
    reader = PandasJsonReader(filepath_or_buffer=file_path, encoding="utf-8")
    kwargs = reader._get_loading_kwargs()
    df, metadata = reader.load_data(pd.DataFrame)

    assert PandasJsonReader.applicable_types() == [pd.DataFrame]
    assert kwargs["encoding"] == "utf-8"
    assert df.shape == (3, 1)


def test_pandas_json_writer(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "test.json"
    writer = PandasJsonWriter(filepath_or_buffer=file_path, indent=4)
    kwargs = writer._get_saving_kwargs()
    writer.save_data(pd.DataFrame({"foo": ["bar"]}))

    assert PandasJsonWriter.applicable_types() == [pd.DataFrame]
    assert kwargs["indent"] == 4
    assert file_path.exists()


@pytest.mark.parametrize(
    "conn",
    [
        DB_ABSOLUTE_PATH,
        sqlite3.connect(DB_RELATIVE_PATH),
        create_engine(DB_ABSOLUTE_PATH),
    ],
)
def test_pandas_sql_reader(conn: Union[str, sqlite3.Connection]) -> None:
    reader = PandasSqlReader(query_or_table="SELECT foo FROM bar", db_connection=conn)
    kwargs = reader._get_loading_kwargs()
    df, metadata = reader.load_data(pd.DataFrame)

    assert PandasSqlReader.applicable_types() == [pd.DataFrame]
    assert kwargs["coerce_float"] is True
    assert metadata["rows"] == 1
    assert df.shape == (1, 1)

    if hasattr(conn, "close"):
        conn.close()


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Test requires Python 3.8 or higher")
@pytest.mark.parametrize(
    "conn",
    [
        DB_MEMORY_PATH,
        sqlite3.connect(":memory:"),
        create_engine(DB_MEMORY_PATH),
    ],
)
def test_pandas_sql_writer(conn: Union[str, sqlite3.Connection]) -> None:
    df = pd.DataFrame({"foo": ["bar"]})
    writer = PandasSqlWriter(table_name="test", db_connection=conn)
    kwargs = writer._get_saving_kwargs()
    metadata = writer.save_data(df)

    assert PandasSqlWriter.applicable_types() == [pd.DataFrame]
    assert kwargs["if_exists"] == "fail"
    assert metadata["rows"] == 1

    if hasattr(conn, "close"):
        conn.close()
