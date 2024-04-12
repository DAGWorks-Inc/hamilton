import pathlib

import pytest
import yaml

from hamilton.plugins.yaml_extensions import YAMLDataLoader, YAMLDataSaver

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
