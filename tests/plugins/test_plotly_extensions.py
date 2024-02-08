import pathlib

import plotly.graph_objects as go
import pytest

from hamilton.io.utils import FILE_METADATA
from hamilton.plugins.plotly_extensions import PlotlyInteractiveWriter, PlotlyStaticWriter


@pytest.fixture
def figure():
    yield go.Figure(data=go.Scatter(x=[1, 2, 3, 4, 5], y=[10, 14, 18, 24, 30], mode="markers"))


def test_plotly_static_writer(figure: go.Figure, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "figure.png"

    writer = PlotlyStaticWriter(path=file_path)
    metadata = writer.save_data(figure)

    assert file_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(file_path)


def test_plotly_interactive_writer(figure: go.Figure, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "figure.html"

    writer = PlotlyInteractiveWriter(path=file_path, auto_open=False)
    metadata = writer.save_data(figure)

    assert file_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(file_path)
