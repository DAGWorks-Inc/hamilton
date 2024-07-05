from hamilton import ad_hoc_utils, driver
from hamilton.execution.executors import SynchronousLocalTaskExecutor
from hamilton.htypes import Collect, Parallelizable
from hamilton.lifecycle import GracefulErrorAdapter


def distro(n: int, allow_fail: bool) -> Parallelizable[int]:
    for x in range(n):
        if x > 4 and allow_fail:
            raise Exception("bad")
        yield x * 3


def some_math(distro: int) -> float:
    if distro > 15:
        raise Exception("No no no")
    return distro * 2.0


def distro_gather(some_math: Collect[float]) -> list[float]:
    ans = [x for x in some_math]
    return ans


def test_parallel_error() -> None:
    local_executor = SynchronousLocalTaskExecutor()
    temp_module = ad_hoc_utils.create_temporary_module(
        distro,
        some_math,
        distro_gather,
        module_name="parallel_example",
    )
    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(temp_module)
        .with_remote_executor(local_executor)
        .with_adapters(
            GracefulErrorAdapter(
                error_to_catch=Exception,
                sentinel_value=None,
            )
        )
        .build()
    )
    _ = dr.execute(
        ["distro_gather"],
        inputs={
            "n": 10,
            "allow_fail": True,
        },
    )
