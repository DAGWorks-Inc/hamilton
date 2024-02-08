import pathlib

import numpy as np
import pytest

from hamilton.io.utils import FILE_METADATA
from hamilton.plugins.numpy_extensions import NumpyNpyReader, NumpyNpyWriter


@pytest.fixture
def array():
    yield np.ones((3, 3, 3))


def test_numpy_file_writer(array: np.ndarray, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "array.npy"

    writer = NumpyNpyWriter(path=file_path)
    metadata = writer.save_data(array)

    assert file_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(file_path)


def test_numpy_file_reader(array: np.ndarray, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "array.npy"
    np.save(file_path, array)

    reader = NumpyNpyReader(path=file_path)
    loaded_array, metadata = reader.load_data(np.ndarray)

    assert np.equal(array, loaded_array).all()
    assert NumpyNpyReader.applicable_types() == [np.ndarray]
