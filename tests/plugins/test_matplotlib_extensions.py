import pathlib

import matplotlib
import matplotlib.pyplot as plt
import pytest

from hamilton.io.utils import FILE_METADATA
from hamilton.plugins.matplotlib_extensions import MatplotlibWriter


def figure1():
    fig = plt.figure()
    plt.scatter([0, 1, 3], [0, 2.2, 2])
    return fig


def figure2():
    fig, ax = plt.subplots()
    ax.scatter([0, 1, 3], [0, 2.2, 2])
    return fig


@pytest.mark.parametrize("figure", [figure1(), figure2()])
def test_plotly_static_writer(figure: matplotlib.figure.Figure, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "figure.png"

    writer = MatplotlibWriter(path=file_path)
    metadata = writer.save_data(figure)

    assert file_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(file_path)
