import sys
from pathlib import Path

import pytest

PY38_OR_BELOW = sys.version_info < (3, 9)
pytestmark = pytest.mark.skipif(
    PY38_OR_BELOW, reason="Breaks for python 3.8 and below due to backports dependency."
)

if not PY38_OR_BELOW:
    import dlt
    from dlt.destinations import filesystem

    from hamilton.plugins.dlt_extensions import DltDestinationSaver, DltResourceLoader

import pandas as pd
import pyarrow as pa


def pandas_df():
    return pd.DataFrame({"a": [1, 2], "b": [1, 2]})


def iterable():
    return [{"a": 1, "b": 3}, {"a": 2, "b": 4}]


def pyarrow_table():
    col_a = pa.array([1, 2])
    col_b = pa.array([3, 4])
    return pa.Table.from_arrays([col_a, col_b], names=["a", "b"])


@pytest.mark.parametrize("data", [iterable(), pandas_df(), pyarrow_table()])
def test_dlt_destination_saver(data, tmp_path):
    save_pipe = dlt.pipeline(destination=filesystem(bucket_url=tmp_path.as_uri()))
    saver = DltDestinationSaver(pipeline=save_pipe, table_name="test_table")

    metadata = saver.save_data(data)

    assert len(metadata["dlt_metadata"]["load_packages"]) == 1
    assert metadata["dlt_metadata"]["load_packages"][0]["state"] == "loaded"
    assert Path(metadata["dlt_metadata"]["load_packages"][0]["jobs"][0]["file_path"]).exists()


def test_dlt_source_loader():
    resource = dlt.resource([{"a": 1, "b": 3}, {"a": 2, "b": 4}], name="mock_resource")
    loader = DltResourceLoader(resource=resource)

    loaded_data, metadata = loader.load_data(pd.DataFrame)

    assert len(loaded_data) == len([row for row in resource])
    assert "_dlt_load_id" in loaded_data.columns
