import io
import json
import pathlib
from typing import List

import pytest

from hamilton.io.default_data_loaders import JSONDataLoader, JSONDataSaver, RawFileDataSaverBytes
from hamilton.io.utils import FILE_METADATA


@pytest.mark.parametrize(
    "data",
    [
        b"test",
        io.BytesIO(b"test"),
    ],
)
def test_raw_file_adapter(data, tmp_path: pathlib.Path) -> None:
    path = tmp_path / "test"

    writer = RawFileDataSaverBytes(path=path)
    writer.save_data(data)

    with open(path, "rb") as f:
        data2 = f.read()

    data_processed = data if type(data) is bytes else data.getvalue()
    assert data_processed == data2


@pytest.mark.parametrize("data", [{"key": "value"}, [{"key": "value1"}, {"key": "value2"}]])
def test_json_save_object_and_array(data, tmp_path: pathlib.Path):
    """Test that `from_.json` and `to.json` can handle JSON objects where
    the top-level is an object `{ }` -> dict or an array `[ ]` -> list[dict]
    """
    data_path = tmp_path / "data.json"
    saver = JSONDataSaver(path=data_path)

    metadata = saver.save_data(data)
    loaded_data = json.loads(data_path.read_text())

    assert JSONDataSaver.applicable_types() == [dict, List[dict]]
    assert data_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(data_path)
    assert data == loaded_data


@pytest.mark.parametrize("data", [{"key": "value"}, [{"key": "value1"}, {"key": "value2"}]])
def test_json_load_object_and_array(data, tmp_path: pathlib.Path):
    """Test that `from_.json` and `to.json` can handle JSON objects where
    the top-level is an object `{ }` -> dict or an array `[ ]` -> list[dict]
    """
    data_path = tmp_path / "data.json"
    loader = JSONDataLoader(path=data_path)

    json.dump(data, data_path.open("w"))
    loaded_data, metadata = loader.load_data(type(data))

    assert JSONDataLoader.applicable_types() == [dict, List[dict]]
    assert data == loaded_data
