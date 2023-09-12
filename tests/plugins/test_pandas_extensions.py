import pathlib

import pandas as pd

from hamilton.plugins.pandas_extensions import (
    PandasJsonDataSaver,
    PandasPickleReader,
    PandasPickleWriter,
)


def test_pandas_pickle(tmp_path):
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


def test_pandas_json_data_saver(tmp_path: pathlib.Path) -> None:
    data = {"foo": ["bar"]}
    df = pd.DataFrame(data)
    path = tmp_path / "test.json"
    saver = PandasJsonDataSaver(filepath_or_buffer=path, indent=4)
    kwargs = saver._get_saving_kwargs()
    metadata = saver.save_data(df)
    assert PandasJsonDataSaver.applicable_types() == [pd.DataFrame]
    assert kwargs["indent"] == 4
    assert metadata["path"] == path
