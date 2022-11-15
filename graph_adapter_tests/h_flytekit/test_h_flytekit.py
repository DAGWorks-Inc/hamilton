import sys

import numpy as np
import pandas as pd
import pytest

from hamilton import driver
from hamilton.experimental import h_flytekit

from . import unsupported_modules
from .resources import example_module


def test_flytekit_graph_adapter():
    initial_columns = {
        "signups": pd.Series([1, 10, 50, 100, 200, 400], name="signups"),
        "spend": pd.Series([10, 10, 20, 40, 40, 50], name="spend"),
    }
    dr = driver.Driver(
        initial_columns, example_module, adapter=h_flytekit.FlyteKitAdapter("my_workflow")
    )
    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
    ]
    df = dr.execute(output_columns)
    assert set(df) == set(output_columns)
    expected_column = pd.Series(
        [0.0, 0.0, 13.33333, 23.33333, 33.33333, 43.33333],
        index=[0, 1, 2, 3, 4, 5],
        name="avg_3wk_spend",
    )
    pd.testing.assert_series_equal(df.avg_3wk_spend.fillna(0.0).sort_index(), expected_column)


def test_flytekit_graph_adapter_unsupported_type():
    columns = {
        "a": unsupported_modules.CustomType(1),
        "b": unsupported_modules.CustomType(2),
    }
    dr = driver.Driver(
        columns, unsupported_modules, adapter=h_flytekit.FlyteKitAdapter("my_workflow")
    )
    with pytest.raises(ValueError):
        assert dr.execute(["a", "b", "add_custom_type"])
