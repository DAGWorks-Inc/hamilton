"""
A basic script to run the pipeline defined by Hamilton.
"""

import pipeline

from hamilton import driver
from hamilton.execution import executors

dr = (
    driver.Builder()
    .with_modules(pipeline)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_config({})
    .with_local_executor(executors.SynchronousLocalTaskExecutor())
    # could be Ray or Dask
    .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))
    .build()
)
dr.display_all_functions("pipeline.png")
result = dr.execute(
    ["collect_chunked_url_text"],
    inputs={"chunk_size": 256, "chunk_overlap": 32},
)
# do something with the result...
# import pprint
#
# for chunk in result["collect_chunked_url_text"]:
#     pprint.pprint(chunk)
