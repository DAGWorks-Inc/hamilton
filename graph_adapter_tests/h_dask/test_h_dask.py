import numpy as np
import pandas as pd
import pytest

from dask.delayed import delayed
from distributed import Client

from hamilton import driver
from hamilton.experimental import h_dask

from .resources import example_module


@pytest.fixture
def client():
    with Client(set_as_default=False).as_current() as client:
        yield client


def test_dask_graph_adapter(client):
    initial_columns = {
        # NOTE: here you could load individual columns from a parquet file
        # in delayed manner, e.g., using sth. along the lines of
        #     delayed(lambda: pd.read_parquet(path, columns=[col])[col])()
        'signups': delayed(lambda: pd.Series([1, 10, 50, 100, 200, 400]))(),
        'spend': delayed(lambda: pd.Series([10, 10, 20, 40, 40, 50]))(),
    }

    dr = driver.Driver(initial_columns, example_module, adapter=h_dask.DaskGraphAdapter(client, visualize=False))

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
