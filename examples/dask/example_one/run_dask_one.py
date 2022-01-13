import importlib
import logging
import sys

import pandas as pd
from hamilton import driver, dask_executor, base

from dask import dataframe

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    module_name = 'my_functions'
    module = importlib.import_module(module_name)

    from dask.distributed import Client, LocalCluster
    # Setup a local cluster.
    # By default this sets up 1 worker per core
    cluster = LocalCluster()
    client = Client(cluster)
    print(client.cluster)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    """Option 1:
    
    1. People pass in an "executor".
    2. The executor dictates what is returned at the end of computing the Hamilton DAG.
    3. To get data to be loaded lazily, if needed, is up to the user to do that -- either in the initial data, or
    via Hamilton functions from data loading modules.
    """

    initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
        'signups': dataframe.from_pandas(pd.Series([1, 10, 50, 100, 200, 400]), name='signups', npartitions=2),
        # 'signups': pd.Series([1, 10, 50, 100, 200, 400]),
        'spend': dataframe.from_pandas(pd.Series([10, 10, 20, 40, 40, 50]), name='spend', npartitions=2),
        # 'spend': pd.Series([10, 10, 20, 40, 40, 50]),
    }
    de = dask_executor.DaskExecutor(client)
    dr = driver.Driver(initial_columns, module, executor=de)  # can pass in multiple modules
    # we need to specify what we want in the final dataframe.
    output_columns = [
        'spend',
        'signups',
        'avg_3wk_spend',
        'spend_per_signup',
        'spend_zero_mean_unit_variance'
    ]
    # let's create the dataframe!
    df = dr.execute(output_columns, display_graph=True)
    print(df)
    client.shutdown()
