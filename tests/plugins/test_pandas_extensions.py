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


def test_pandas_json(tmp_path: pathlib.Path) -> None:
    df1 = pd.DataFrame({"foo": ["bar"]})
    file_path = tmp_path / "test.json"
    writer = PandasJsonWriter(filepath_or_buffer=file_path, indent=4)
    reader = PandasJsonReader(filepath_or_buffer=file_path, encoding="utf-8")
    writer.save_data(df1)
    kwargs1 = writer._get_saving_kwargs()
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata = reader.load_data(pd.DataFrame)

    assert PandasJsonReader.applicable_types() == [pd.DataFrame]
    assert PandasJsonWriter.applicable_types() == [pd.DataFrame]
    assert kwargs1["indent"] == 4
    assert kwargs2["encoding"] == "utf-8"
    assert file_path.exists()
    assert df1.equals(df2)


@pytest.mark.parametrize(
    "conn",
    [
        sqlite3.connect(":memory:"),
        create_engine("sqlite://"),
    ],
)
def test_pandas_sql(conn: Union[str, sqlite3.Connection]) -> None:
    df1 = pd.DataFrame({"foo": ["bar"]})
    reader = PandasSqlReader(query_or_table="SELECT foo FROM test", db_connection=conn)
    writer = PandasSqlWriter(table_name="test", db_connection=conn)
    metadata1 = writer.save_data(df1)
    kwargs1 = writer._get_saving_kwargs()
    kwargs2 = reader._get_loading_kwargs()
    df2, metadata2 = reader.load_data(pd.DataFrame)

    assert PandasSqlReader.applicable_types() == [pd.DataFrame]
    assert PandasSqlWriter.applicable_types() == [pd.DataFrame]
    assert kwargs1["if_exists"] == "fail"
    assert kwargs2["coerce_float"] is True
    assert df1.equals(df2)

    if sys.version_info >= (3, 8):
        # py37 pandas 1.3.5 doesn't return rows inserted
        assert metadata1["rows"] == 1
        assert metadata2["rows"] == 1

    if hasattr(conn, "close"):
        conn.close()
