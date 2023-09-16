import pathlib
import sqlite3
from typing import Union

import pandas as pd
import pytest
from sqlalchemy import Connection, Engine, create_engine

from hamilton.plugins.pandas_extensions import (
    PandasJsonReader,
    PandasJsonWriter,
    PandasPickleReader,
    PandasPickleWriter,
    PandasSqlReader,
)

DB_PATH = "tests/resources/data/test.db"
SQLITE_DB_PATH = f"sqlite:///{DB_PATH}"


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
        SQLITE_DB_PATH,
        sqlite3.connect(DB_PATH),
        create_engine(SQLITE_DB_PATH),
    ],
)
def test_pandas_sql_reader(conn: Union[str, Connection, Engine, sqlite3.Connection]) -> None:
    reader = PandasSqlReader(
        query_or_table="SELECT foo FROM bar", db_connection=conn, coerce_float=False
    )
    kwargs = reader._get_loading_kwargs()
    df, metadata = reader.load_data(pd.DataFrame)

    assert PandasSqlReader.applicable_types() == [pd.DataFrame]
    assert kwargs["coerce_float"] is False
    assert df.shape == (1, 1)

    if hasattr(conn, "close"):
        conn.close()
