import importlib
import logging
import sys

import pandas as pd
from hamilton import driver, dask_executor, base

from dask import dataframe

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    module_name = 'my_functions'

    from dask.distributed import Client, LocalCluster
    # Setup a local cluster.
    # By default this sets up 1 worker per core
    cluster = LocalCluster()
    client = Client(cluster)
    print(client.cluster)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    module = importlib.import_module(module_name)
    initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
        'signups': pd.Series([1, 10, 50, 100, 200, 400]),
        'spend': pd.Series([10, 10, 20, 40, 40, 50]),
    }

    """Option 2(a):
    
    1. People instantiate a specific Driver.
    2. The Driver can contain as much or as little logic as it wants, e.g. it can convert input sets into the dask
    data type.
    3. The Driver dictates the return type and is fixed. E.g. only returns pandas DF. 
    """
    dask_parameters = {
        'default_npartitions': 2,
        'convert_to_dask_data_type': True
    }
    dr = dask_executor.DaskDriver(
        initial_columns,
        module,
        dask_client=client,
        dask_parameters=dask_parameters)

    """Option 2(b):

    1. People instantiate a specific Driver.
    2. The Driver can contain as much or as little logic as it wants, e.g. it can convert input sets into the dask
    data type.
    3. The user tells the Driver a return type that they want.    
    """
    dask_parameters = {
        'default_npartitions': 2,
        'convert_to_dask_data_type': True
    }
    dr = dask_executor.DaskDriver(
        initial_columns,
        module,
        dask_client=client,
        dask_parameters=dask_parameters,
        result_builder=base.PandasDataFrameResult()
    )

    output_columns = [
        'spend',
        'signups',
        'avg_3wk_spend',
        'spend_per_signup',
        'spend_zero_mean_unit_variance'
    ]
    df = dr.execute(output_columns, display_graph=True)
    print(df)
    client.shutdown()
