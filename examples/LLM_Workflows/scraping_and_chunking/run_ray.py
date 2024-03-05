"""
Shows how to run document chunking using ray.
"""

import logging

import doc_pipeline
import ray

from hamilton import driver, log_setup
from hamilton.plugins import h_ray

if __name__ == "__main__":
    log_setup.setup_logging(logging.INFO)
    ray.init()

    dr = (
        driver.Builder()
        .with_modules(doc_pipeline)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_config({})
        # Choose a backend to process the parallel parts of the pipeline
        # .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=5))
        # .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))
        .with_remote_executor(
            h_ray.RayTaskExecutor()
        )  # be sure to run ray.init() or pass in config.
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

    ray.shutdown()
