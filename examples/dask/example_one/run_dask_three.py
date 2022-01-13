import importlib
import logging
import sys

import pandas as pd
from hamilton import driver

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
    initial_data = {  # load from actuals or wherever -- this is our initial data we use as input.
        'signups': pd.Series([1, 10, 50, 100, 200, 400]),
        'spend': pd.Series([10, 10, 20, 40, 40, 50]),
    }
    """Option 3:
    
    1. The user creates a specific Driver via a special function. 
    2. The point of the special function is that when people cut and past code, and with minimal changes get things
    to work locally, and then scale to dask/spark/ray, etc.
    3. This means that this script doesn't import anything about "hamilton.dask" -- which is good from a platform
     standpoint perhaps? E.g. we could replace where the Dask driver is imported from very easily -- it's hidden by
     the magicly create_driver function. 
    """

    driver_configuration = {  # drivers should come with batteries included, but you can also tweak values.
        'default_npartitions': 2,
        'convert_to_dask_data_type': True
    }

    dr = driver.create_driver(
        module,
        driver_type='Dask',
        driver_configuration=driver_configuration,
        return_type='pandas_df',
        initial_data=initial_data,
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
