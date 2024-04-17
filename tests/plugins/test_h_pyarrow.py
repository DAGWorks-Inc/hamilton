import pandas as pd
import pyarrow
import pytest

from hamilton.plugins import h_pyarrow


@pytest.fixture()
def pandas():
    return pd.DataFrame({"a": [0, 1, 2], "b": ["a", "b", "c"]})


def test_pandas_to_pyarrow(pandas):
    result_builder = h_pyarrow.PyarrowTableResult()
    data = {"df": pandas}
    # ResultBuilder receive unpacked dict as arg, i.e., kwargs only
    table = result_builder.build_result(**data)
    assert isinstance(table, pyarrow.Table)


def test_fail_for_multiple_outputs(pandas):
    result_builder = h_pyarrow.PyarrowTableResult()
    data = {"df": pandas, "df2": pandas}
    with pytest.raises(AssertionError):
        result_builder.build_result(**data)
