"""
This script runs a Hamilton DAG whose intent is to create some features for input to a model.
The way the code is set up, is that you can do two things:

1. It uses Dask to parallelize computation. Use {"execution": "normal"} for this mode.
2. It uses Dask to parallelize computation and scale the data. It does this latter part by changing the way data
is loaded. Use {"execution": "dask"} for this mode. Note: the current validators implemented do not support dask
data types just yet.

The model fitting steps are not represented here, just the feature ingestion and transformation logic.

Use (1) if all your data can fit in memory, and you want to parallelize execution.
Use (2) if you need to scale to "big data" set sizes.

To run:
> python run_dask.py
"""

import logging
import sys

from dask.distributed import Client
from dask.distributed import LocalCluster
from hamilton import base
from hamilton import driver
from hamilton.experimental import h_dask
# we need to tell hamilton where to load function definitions from
import feature_logic
import data_loaders

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger(__name__)
    # Setup a local cluster.
    # By default this sets up 1 worker per core
    cluster = LocalCluster()
    client = Client(cluster)
    logger.info(client.cluster)
    # passing in execution to help set up the right nodes for the DAG
    config = {'location': 'Absenteeism_at_work.csv', 'execution': 'normal'}
    dga = h_dask.DaskGraphAdapter(client, base.PandasDataFrameResult())
    dr = driver.Driver(config, data_loaders, feature_logic, adapter=dga)  # can pass in multiple modules
    # we need to specify what we want in the final dataframe.
    output_columns = [
        'age',
        'age_zero_mean_unit_variance',
        'has_children',
        'is_summer',
        'has_pet',
        'day_of_the_week_2',
        'day_of_the_week_3',
        'day_of_the_week_4',
        'day_of_the_week_5',
        'day_of_the_week_6',
        'seasons_1',
        'seasons_2',
        'seasons_3',
        'seasons_4',
        'absenteeism_time_in_hours'
    ]
    # To visualize do `pip install sf-hamilton[visualization]` if you want these to work
    # dr.visualize_execution(output_columns, './my_dag.dot', {})
    # dr.display_all_functions('./my_full_dag.dot')

    # let's create the dataframe!
    df = dr.execute(output_columns)
    print(df.head().to_string())
