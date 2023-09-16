import pathlib
import sqlite3

import pandas as pd
from sqlalchemy import create_engine

from hamilton.plugins.pandas_extensions import (
    PandasJsonReader,
    PandasJsonWriter,
    PandasPickleReader,
    PandasPickleWriter,
    PandasSqlReader,
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


def test_pandas_sql_reader() -> None:
    query = "SELECT foo FROM bar"
    db_path = "tests/resources/data/test.db"
    conn = sqlite3.connect(db_path)
    engine = create_engine(f"sqlite:///{db_path}")
    reader1 = PandasSqlReader(query_or_table=query, db_connection=conn, coerce_float=False)
    reader2 = PandasSqlReader(query_or_table=query, db_connection=engine)
    kwargs = reader1._get_loading_kwargs()
    df1, metadata = reader1.load_data(pd.DataFrame)
    df2, metadata = reader2.load_data(pd.DataFrame)
    assert PandasSqlReader.applicable_types() == [pd.DataFrame]
    assert kwargs["coerce_float"] is False
    assert df1.shape == (1, 1)
    assert df2.shape == (1, 1)
    conn.close()
