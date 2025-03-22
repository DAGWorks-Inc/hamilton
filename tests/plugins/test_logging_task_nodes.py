# NOTE: This file contains nodes for the test 'test_logging_parallel_nodes' in
# test_logging_task_nodes.py. They are required to be in a separate file in order to properly
# test the multi-processing executor.

from hamilton.htypes import Collect, Parallelizable
from hamilton.plugins.h_logging import get_logger


def b() -> int:
    return 5


def c(b: int) -> Parallelizable[int]:
    for i in range(b):
        yield i


def d(c: int) -> int:
    logger = get_logger("test_logging_parallel_nodes")
    logger.warning("Context aware message")
    return 2 * c


def e(d: Collect[int]) -> int:
    return sum(d)


def f(e: int) -> int:
    return e
