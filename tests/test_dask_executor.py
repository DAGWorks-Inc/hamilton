import pandas as pd
from hamilton.dask_executor import DaskExecutor
import pytest

from dask import compute
from dask.delayed import delayed
from distributed import Client

from hamilton.driver import Driver

from . import example_module


@pytest.fixture
def client():
    with Client(set_as_default=False).as_current() as client:
        yield client


def test_dask_executor(client):
    initial_columns = {
        # NOTE: here you could load individual columns from a parquet file
        # in delayed manner, e.g., using sth. along the lines of
        #     delayed(lambda: pd.read_parquet(path, columns=[col])[col])()
        'signups': delayed(lambda: pd.Series([1, 10, 50, 100, 200, 400]))(),
        'spend': delayed(lambda: pd.Series([10, 10, 20, 40, 40, 50]))(),
    }

    dr = Driver(initial_columns, example_module, executor=DaskExecutor())

    output_columns = [
        'spend',
        'signups',
        'avg_3wk_spend',
        'spend_per_signup',
    ]
    df = dr.execute(output_columns)

    assert set(df) == set(output_columns)
