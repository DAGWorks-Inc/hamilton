from hamilton import ad_hoc_utils, driver
from hamilton.execution.executors import SynchronousLocalTaskExecutor
from hamilton.function_modifiers import config
from hamilton.htypes import Collect, Parallelizable
from hamilton.lifecycle import GracefulErrorAdapter


@config.when(test_state="middle")
def distro__middle(n: int) -> Parallelizable[int]:
    for x in range(n):
        if x > 4:
            raise Exception("bad")
        yield x * 3


@config.when(test_state="early")
def distro__early(n: int) -> Parallelizable[int]:
    raise Exception("bad")
    for x in range(n):
        yield x * 3


@config.when(test_state="pass")
def distro__pass(n: int) -> Parallelizable[int]:
    for x in range(n):
        yield x * 3


def some_math(distro: int) -> float:
    if distro > 15:
        raise Exception("No no no")
    return distro * 2.0


def distro_gather(some_math: Collect[float]) -> list[float]:
    ans = [x for x in some_math]
    return ans


# TODO: How does this break extract_fields, e.g.?

temp_module = ad_hoc_utils.create_temporary_module(
    distro__early,
    distro__middle,
    distro__pass,
    some_math,
    distro_gather,
    module_name="parallel_example",
)


def _make_driver(test_state: str, fail_parallel=False):
    local_executor = SynchronousLocalTaskExecutor()
    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(temp_module)
        .with_remote_executor(local_executor)
        .with_adapters(
            GracefulErrorAdapter(
                error_to_catch=Exception,
                sentinel_value=None,
                fail_all_parallel=fail_parallel,
            )
        )
        .with_config(
            {"test_state": test_state},
        )
        .build()
    )
    return dr


def test_parallel_error() -> None:
    # Fail before the anything is yielded: should get 1 failure and skip all
    dr = _make_driver("early")
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [None]

    # Fail in the middle of the parallel: ends early
    dr = _make_driver("middle")
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [0.0, 6.0, 12.0, 18.0, 24.0, None]

    # Fail in the middle of the parallel, but we tell it to act as 1 failure
    dr = _make_driver("middle", True)
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [None]

    # Parallelized block doesn't fail, we see the collect work on the internal failures
    dr = _make_driver("pass")
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [0.0, 6.0, 12.0, 18.0, 24.0, 30.0, None, None, None, None]
