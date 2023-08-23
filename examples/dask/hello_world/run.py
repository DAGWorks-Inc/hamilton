import importlib
import logging
import sys

from hamilton import driver
from hamilton.plugins import h_dask

logger = logging.getLogger(__name__)

"""
This example shows you how to run a Hamilton DAG and simply replace how you load data,
and how the result is built by delegating to dask to do any distributed work.
"""

if __name__ == "__main__":
    from hamilton import log_setup

    log_setup.setup_logging()
    # easily replace how data is loaded by replacing the data_loaders module
    module_names = ["business_logic", "data_loaders"]

    from dask.distributed import Client, LocalCluster

    # Setup a local cluster.
    # By default this sets up 1 worker per core
    cluster = LocalCluster()
    client = Client(cluster)
    logger.info(client.cluster)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    modules = [importlib.import_module(m) for m in module_names]
    dga = h_dask.DaskGraphAdapter(
        client,
        h_dask.DaskDataFrameResult(),
        visualize_kwargs={"filename": "run_dask.png", "format": "png"},
        use_delayed=False,
        compute_at_end=False,
    )
    # will output Dask's execution graph  -- requires sf-hamilton[visualization] to be installed.

    initial_config_and_data = {
        "spend_location": "some file path",
        "spend_partitions": 2,
        "signups_location": "some file path",
        "signups_partitions": 2,
        "foobar": "some_other_data",
    }
    dr = driver.Driver(initial_config_and_data, *modules, adapter=dga)

    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
        "spend_mean",
        "spend_zero_mean_unit_variance",
        "foobar",
    ]
    dask_df = dr.execute(output_columns)  # it's dask dataframe -- it hasn't been evaluated yet.
    df = dask_df.compute()
    # To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
    # dr.visualize_execution(output_columns, './hello_world_dask', {"format": "png"})
    dr.display_all_functions("./my_full_dag.dot")
    logger.info(df.to_string())
    client.shutdown()
