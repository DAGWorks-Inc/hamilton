import io
import pathlib

import pytest

from hamilton.io.default_data_loaders import RawFileDataSaverBytes


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
