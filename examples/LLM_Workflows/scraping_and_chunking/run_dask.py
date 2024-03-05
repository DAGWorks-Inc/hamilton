"""
Shows how to run document chunking using dask.
"""

import logging

import doc_pipeline
from dask import distributed

from hamilton import driver, log_setup
from hamilton.plugins import h_dask

log_setup.setup_logging(logging.INFO)

if __name__ == "__main__":
    cluster = distributed.LocalCluster()
    client = distributed.Client(cluster)
    remote_executor = h_dask.DaskExecutor(client=client)

    dr = (
        driver.Builder()
        .with_modules(doc_pipeline)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_config({})
        # Choose a backend to process the parallel parts of the pipeline
        # .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=5))
        # .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))
        .with_remote_executor(h_dask.DaskExecutor(client=client))
        .build()
    )
    dr.display_all_functions("pipeline.png")
    result = dr.execute(
        ["collect_chunked_url_text"],
        inputs={"chunk_size": 256, "chunk_overlap": 32},
    )
    # do something with the result...
    import pprint

    for chunk in result["collect_chunked_url_text"]:
        pprint.pprint(chunk)

    client.shutdown()
