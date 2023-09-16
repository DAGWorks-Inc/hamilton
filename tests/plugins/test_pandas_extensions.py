from importlib import metadata
import pathlib
import lmxl 
import pandas as pd

from hamilton.plugins.pandas_extensions import (
    PandasJsonReader,
    PandasJsonWriter,
    PandasPickleReader,
    PandasPickleWriter,
    PandasXmlReader,
    PandasXmlWriter
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


def test_pandas_json_reader(tmp_path: pathlib.Path) -> None:
    file_path = "tests/resources/data/test_load_from_data.json"
    reader = PandasJsonReader(filepath_or_buffer=file_path, encoding="utf-8")
    kwargs = reader._get_loading_kwargs()
    df, metadata = reader.load_data(pd.DataFrame)
    assert PandasJsonReader.applicable_types() == [pd.DataFrame]
    assert kwargs["encoding"] == "utf-8"
    assert df.shape == (3, 1)
    assert metadata["path"] == file_path

def test_pandas_json_writer(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "test.json"
    writer = PandasJsonWriter(filepath_or_buffer=file_path, indent=4)
    kwargs = writer._get_saving_kwargs()
    metadata = writer.save_data(pd.DataFrame({"foo": ["bar"]}))
    assert PandasJsonWriter.applicable_types() == [pd.DataFrame]
    assert kwargs["indent"] == 4
    assert file_path.exists()
    assert metadata["path"] == file_path

def test_pandas_xml_reader(tmp_path: pathlib.Path) -> None:
    path_to_test = "tests/resources/data/test_load_from_data.xml"
    reader = PandasXmlReader(path_or_buffer = path_to_test)
    kwargs = reader._get_loading_kwargs()
    df, metadata = reader.load_data(pd.DataFrame)

    assert PandasXmlReader.applicable_types() == [pd.DataFrame]
    assert df.shape == (5, 4)

def test_pandas_xml_writer(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "test.xml"
    writer = PandasXmlWriter(path_or_buffer = file_path)
    kwargs = writer._get_saving_kwargs()
    metadata = writer.save_data(pd.DataFrame({"foo": ["bar"]}))

    assert PandasXmlWriter.applicable_types() == [pd.DataFrame]
    assert file_path.exists()
    assert metadata["path"] == file_path