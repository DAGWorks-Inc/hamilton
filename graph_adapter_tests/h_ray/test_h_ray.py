import pandas as pd
import pytest

import ray

from hamilton import driver, base
from hamilton.experimental import h_ray

from .resources import example_module

"""
For reference https://docs.ray.io/en/latest/auto_examples/testing-tips.html
"""


@pytest.fixture(scope='module')
def init():
    ray.init(local_mode=True)  # need local mode, else it can't seem to find the h_ray module.
    yield 'initialized'
    ray.shutdown()


def test_ray_graph_adapter(init):
    initial_columns = {
        'signups': pd.Series([1, 10, 50, 100, 200, 400]),
        'spend': pd.Series([10, 10, 20, 40, 40, 50]),
    }
    dr = driver.Driver(initial_columns, example_module, adapter=h_ray.RayGraphAdapter(base.PandasDataFrameResult()))
    output_columns = [
        'spend',
        'signups',
        'avg_3wk_spend',
        'spend_per_signup',
    ]
    df = dr.execute(output_columns)
    assert set(df) == set(output_columns)
    expected_column = pd.Series([0.0, 0.0, 13.33333, 23.33333, 33.33333, 43.33333], name='avg_3wk_spend')
    pd.testing.assert_series_equal(df.avg_3wk_spend.fillna(0.0), expected_column)  # fill na to get around NaN
    # TODO: do some more asserting?
