import importlib
import logging
import sys

import pandas as pd

from hamilton import base, driver
from hamilton.plugins import h_dask

logger = logging.getLogger(__name__)

"""
This example shows you how to run a Hamilton DAG and farm each function out to dask using
dask.delayed.
"""
if __name__ == "__main__":
    from dask.distributed import Client, LocalCluster

    # Setup a local cluster.
    # By default this sets up 1 worker per core
    cluster = LocalCluster()
    client = Client(cluster)
    logger.info(client.cluster)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    # easily replace how data is loaded by replacing the data_loaders module
    module_names = ["business_logic"]  # or do `import business_logic`
    modules = [importlib.import_module(m) for m in module_names]
    dga = h_dask.DaskGraphAdapter(
        client,
        base.PandasDataFrameResult(),
        visualize_kwargs={"filename": "run_with_delayed", "format": "png"},
        use_delayed=True,
        compute_at_end=True,
    )
    # will output Dask's execution graph run_with_delayed.png -- requires sf-hamilton[visualization] to be installed.

    initial_config_and_data = {
        "signups": pd.Series([1, 10, 50, 100, 200, 400]),
        "spend": pd.Series([10, 10, 20, 40, 40, 50]),
    }
    dr = driver.Driver(initial_config_and_data, *modules, adapter=dga)

    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
        "spend_mean",
        "spend_zero_mean_unit_variance",
    ]
    pandas_df = dr.execute(output_columns)
    logger.info(pandas_df.to_string())
    # To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
    # dr.visualize_execution(output_columns, './hello_world_dask', {"format": "png"})
    # dr.display_all_functions('./my_full_dag.dot')
    client.shutdown()
