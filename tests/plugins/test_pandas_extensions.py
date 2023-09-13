import pathlib

import pandas as pd

from hamilton.plugins.pandas_extensions import (
    PandasJsonDataLoader,
    PandasJsonDataSaver,
    PandasPickleReader,
    PandasPickleWriter,
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


def test_pandas_json_data_loader(tmp_path: pathlib.Path) -> None:
    file_path = "tests/resources/data/test_load_from_data.json"
    loader = PandasJsonDataLoader(filepath_or_buffer=file_path, encoding="utf-8")
    kwargs = loader._get_loading_kwargs()
    df, metadata = loader.load_data(pd.DataFrame)
    assert PandasJsonDataLoader.applicable_types() == [pd.DataFrame]
    assert kwargs["encoding"] == "utf-8"
    assert df.shape == (3, 1)
    assert metadata["path"] == file_path


def test_pandas_json_data_saver(tmp_path: pathlib.Path) -> None:
    df = pd.DataFrame({"foo": ["bar"]})
    file_path = tmp_path / "test.json"
    saver = PandasJsonDataSaver(filepath_or_buffer=file_path, indent=4)
    kwargs = saver._get_saving_kwargs()
    assert not file_path.exists()
    metadata = saver.save_data(df)
    assert file_path.exists()
    assert PandasJsonDataSaver.applicable_types() == [pd.DataFrame]
    assert kwargs["indent"] == 4
    assert metadata["path"] == file_path
