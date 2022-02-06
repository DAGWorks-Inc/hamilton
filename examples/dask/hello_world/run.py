import importlib
import logging
import sys

from hamilton import base
from hamilton import driver
from hamilton.experimental import h_dask


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    from hamilton import log_setup
    log_setup.setup_logging()
    # easily replace how data is loaded by replacing the data_loaders module
    module_names = ['business_logic', 'data_loaders']

    from dask.distributed import Client, LocalCluster
    # Setup a local cluster.
    # By default this sets up 1 worker per core
    cluster = LocalCluster()
    client = Client(cluster)
    logger.info(client.cluster)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    modules = [importlib.import_module(m) for m in module_names]
    dga = h_dask.DaskGraphAdapter(client, base.PandasDataFrameResult())
    # will output Dask's execution graph mydask.png -- requires sf-hamilton[visualization] to be installed.

    initial_config_and_data = {
        'spend_location': 'some file path',
        'spend_partitions': 2,
        'signups_location': 'some file path',
        'signups_partitions': 2
    }
    dr = driver.Driver(initial_config_and_data, *modules, adapter=dga)

    output_columns = [
        'spend',
        'signups',
        'avg_3wk_spend',
        'spend_per_signup',
        'spend_zero_mean_unit_variance'
    ]
    df = dr.execute(output_columns)
    # To visualize do `pip install sf-hamilton[visualization]` if you want these to work
    # dr.visualize_execution(output_columns, './my_dag.dot', {})
    # dr.display_all_functions('./my_full_dag.dot')
    logger.info(df.to_string())
    client.shutdown()
