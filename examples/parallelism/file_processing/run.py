import logging

import aggregate_data
import click
import list_data
import process_data
import ray
from dask import distributed

from hamilton import driver, log_setup
from hamilton.execution import executors
from hamilton.plugins import h_dask, h_ray

log_setup.setup_logging(logging.INFO)


@click.command()
@click.option(
    "--mode",
    type=click.Choice(["local", "multithreading", "dask", "ray"]),
    required=True,
    help="Where to run remote tasks.",
)
def main(mode: str):
    shutdown = None
    if mode == "local":
        remote_executor = executors.SynchronousLocalTaskExecutor()
    elif mode == "multithreading":
        remote_executor = executors.MultiThreadingExecutor(max_tasks=100)
    elif mode == "dask":
        cluster = distributed.LocalCluster()
        client = distributed.Client(cluster)
        remote_executor = h_dask.DaskExecutor(client=client)
        shutdown = cluster.close
    else:
        remote_executor = h_ray.RayTaskExecutor(num_cpus=4)
        shutdown = ray.shutdown
    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(remote_executor)  # We only need to specify remote exeecutor
        # The local executor just runs it synchronously
        .with_modules(aggregate_data, list_data, process_data)
        .build()
    )
    print(
        dr.execute(final_vars=["statistics_by_city"], inputs={"data_dir": "data"})[
            "statistics_by_city"
        ]
    )
    if shutdown:
        shutdown()


if __name__ == "__main__":
    main()
