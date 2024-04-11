import io
import json
import pathlib

import pytest
import yaml

from hamilton.io.default_data_loaders import (
    JSONDataLoader,
    JSONDataSaver,
    RawFileDataSaverBytes,
    YAMLDataLoader,
    YAMLDataSaver,
)
from hamilton.io.utils import FILE_METADATA

TEST_DATA_FOR_YAML = [
    (1, "int.yaml"),
    ("string", "string.yaml"),
    (True, "bool.yaml"),
    ({"key": "value"}, "test.yaml"),
    ([1, 2, 3], "data.yaml"),
    ({"nested": {"a": 1, "b": 2}}, "config.yaml"),
]


@pytest.mark.parametrize("data, file_name", TEST_DATA_FOR_YAML)
def test_yaml_loader(tmp_path: pathlib.Path, data, file_name):
    path = tmp_path / pathlib.Path(file_name)
    with path.open(mode="w") as f:
        yaml.dump(data, f)
    assert path.exists()
    loader = YAMLDataLoader(path)
    loaded_data = loader.load_data(type(data))
    assert loaded_data[0] == data


@pytest.mark.parametrize("data, file_name", TEST_DATA_FOR_YAML)
def test_yaml_saver(tmp_path: pathlib.Path, data, file_name):
    path = tmp_path / pathlib.Path(file_name)
    saver = YAMLDataSaver(path)
    saver.save_data(data)
    assert path.exists()
    with path.open("r") as f:
        loaded_data = yaml.safe_load(f)
    assert data == loaded_data


@pytest.mark.parametrize("data, file_name", TEST_DATA_FOR_YAML)
def test_yaml_loader_and_saver(tmp_path: pathlib.Path, data, file_name):
    path = tmp_path / pathlib.Path(file_name)
    saver = YAMLDataSaver(path)
    saver.save_data(data)
    assert path.exists()
    loader = YAMLDataLoader(path)
    loaded_data = loader.load_data(type(data))
    assert data == loaded_data[0]


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


@pytest.mark.parametrize(
    "data", [{"key": "value"}, [{"key": "value1"}, {"key": "value2"}], ["value1", "value2"], [0, 1]]
)
def test_json_save_object_and_array(data, tmp_path: pathlib.Path):
    """Test that `from_.json` and `to.json` can handle JSON objects where
    the top-level is an object `{ }` -> dict or an array `[ ]` -> list
    """
    data_path = tmp_path / "data.json"
    saver = JSONDataSaver(path=data_path)

    metadata = saver.save_data(data)
    loaded_data = json.loads(data_path.read_text())

    assert JSONDataSaver.applicable_types() == [dict, list]
    assert data_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(data_path)
    assert data == loaded_data


@pytest.mark.parametrize(
    "data", [{"key": "value"}, [{"key": "value1"}, {"key": "value2"}], ["value1", "value2"], [0, 1]]
)
def test_json_load_object_and_array(data, tmp_path: pathlib.Path):
    """Test that `from_.json` and `to.json` can handle JSON objects where
    the top-level is an object `{ }` -> dict or an array `[ ]` -> list
    """
    data_path = tmp_path / "data.json"
    loader = JSONDataLoader(path=data_path)

    json.dump(data, data_path.open("w"))
    loaded_data, metadata = loader.load_data(type(data))

    assert JSONDataLoader.applicable_types() == [dict, list]
    assert data == loaded_data
