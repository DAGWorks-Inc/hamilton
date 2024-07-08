from typing import List

from hamilton import ad_hoc_utils, driver
from hamilton.execution.executors import SynchronousLocalTaskExecutor
from hamilton.function_modifiers import config
from hamilton.htypes import Collect, Parallelizable
from hamilton.lifecycle.default import GracefulErrorAdapter, accept_error_sentinels


@config.when(test_front="good")
def input_maker__good(n: int) -> int:
    return n


@config.when(test_front="bad")
def input_maker__bad(n: int) -> int:
    raise Exception("Went wrong")


@config.when(test_state="middle")
def distro__middle(input_maker: int) -> Parallelizable[int]:
    for x in range(input_maker):
        if x > 4:
            raise Exception("bad")
        yield x * 3


@config.when(test_state="early")
def distro__early(input_maker: int) -> Parallelizable[int]:
    raise Exception("bad")
    for x in range(input_maker):
        yield x * 3


@config.when(test_state="pass")
def distro__pass(input_maker: int) -> Parallelizable[int]:
    for x in range(input_maker):
        yield x * 3


def some_math(distro: int) -> float:
    if distro > 15:
        raise Exception("No no no")
    return distro * 2.0


def other_math(distro: int) -> float:
    if distro < 10:
        raise Exception("Not allowed")
    return distro + 1


@accept_error_sentinels
def gather_math(some_math: float, other_math: float) -> List[float]:
    return [some_math, other_math]


def distro_end(gather_math: Collect[List[float]]) -> List[float]:
    ans = [x for x in gather_math]
    return ans


def distro_gather(some_math: Collect[float]) -> List[float]:
    ans = [x for x in some_math]
    return ans


# TODO: How does this break extract_fields, e.g.?

temp_module_simple = ad_hoc_utils.create_temporary_module(
    input_maker__good,
    input_maker__bad,
    distro__early,
    distro__middle,
    distro__pass,
    some_math,
    distro_gather,
    module_name="parallel_example",
)

temp_module = ad_hoc_utils.create_temporary_module(
    input_maker__good,
    input_maker__bad,
    distro__early,
    distro__middle,
    distro__pass,
    some_math,
    other_math,
    gather_math,
    distro_end,
    module_name="parallel_example_complex",
)


def _make_driver(
    module,
    test_state: str,
    try_parallel=True,
    fail_first=False,
    allow_injection=True,
):
    local_executor = SynchronousLocalTaskExecutor()
    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(module)
        .with_remote_executor(local_executor)
        .with_adapters(
            GracefulErrorAdapter(
                error_to_catch=Exception,
                sentinel_value=None,
                try_all_parallel=try_parallel,
                allow_injection=allow_injection,
            )
        )
        .with_config(
            {
                "test_state": test_state,
                "test_front": "bad" if fail_first else "good",
            },
        )
        .build()
    )
    return dr


def test_parallel_graceful_simple() -> None:
    # Fail before the anything is yielded: should get 1 failure and skip all
    dr = _make_driver(
        temp_module_simple,
        "early",
    )
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [None]

    # Fail in the middle of the parallel: ends early
    dr = _make_driver(
        temp_module_simple,
        "middle",
    )
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [0.0, 6.0, 12.0, 18.0, 24.0, None]

    # Fail in the middle of the parallel, but we tell it to act as 1 failure
    dr = _make_driver(
        temp_module_simple,
        "middle",
        try_parallel=False,
    )
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [None]

    # Parallelized block doesn't fail, we see the collect work on the internal failures
    dr = _make_driver(temp_module_simple, "pass")
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [0.0, 6.0, 12.0, 18.0, 24.0, 30.0, None, None, None, None]

    # Check what happens when the parallel block gets a failure.
    dr = _make_driver(temp_module_simple, "pass", try_parallel=False, fail_first=True)
    ans = dr.execute(
        ["distro_gather"],
        inputs={"n": 10},
    )
    assert ans["distro_gather"] == [None]


def test_parallel_gather_injection() -> None:
    dr = _make_driver(
        temp_module,
        "pass",
        try_parallel=True,
        fail_first=False,
    )
    ans = dr.execute(
        ["distro_end"],
        inputs={"n": 10},
    )
    assert ans["distro_end"] == [
        [0.0, None],
        [6.0, None],
        [12.0, None],
        [18.0, None],
        [24.0, 13],
        [30.0, 16],
        [None, 19],
        [None, 22],
        [None, 25],
        [None, 28],
    ]

    # show that disallowing injection skips over the gathering step
    dr = _make_driver(
        temp_module,
        "pass",
        try_parallel=True,
        fail_first=False,
        allow_injection=False,
    )
    ans = dr.execute(
        ["distro_end"],
        inputs={"n": 10},
    )
    assert ans["distro_end"] == [
        None,
        None,
        None,
        None,
        [24.0, 13],
        [30.0, 16],
        None,
        None,
        None,
        None,
    ]
