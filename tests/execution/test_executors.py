import time

import pytest

from hamilton import base, driver
from hamilton.ad_hoc_utils import create_temporary_module
from hamilton.execution.executors import (
    DefaultExecutionManager,
    MultiProcessingExecutor,
    MultiThreadingExecutor,
    SynchronousLocalTaskExecutor,
)
from hamilton.execution.grouping import (
    GroupByRepeatableBlocks,
    GroupNodesAllAsOne,
    GroupNodesByLevel,
    GroupNodesIndividually,
    NodeGroupPurpose,
    TaskImplementation,
)
from hamilton.htypes import Collect, Parallelizable
from tests.resources.dynamic_parallelism import (
    no_parallel,
    parallel_complex,
    parallel_delayed,
    parallel_linear_basic,
)

ADAPTER = base.DefaultAdapter()


def multi_processing_executor_factory():
    return MultiProcessingExecutor(max_tasks=10)


def multi_threading_executor_factory():
    return MultiThreadingExecutor(max_tasks=10)


@pytest.mark.parametrize(
    "executor_factory",
    [
        multi_processing_executor_factory,
        multi_threading_executor_factory,
        SynchronousLocalTaskExecutor,
    ],
)
def test_end_to_end_parallel_execute_group_individually(executor_factory):
    dr = (
        driver.Builder()
        .with_modules(no_parallel)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executor_factory())
        .with_grouping_strategy(GroupNodesIndividually())
        .build()
    )
    results = dr.execute(["final"])
    assert results["final"] == no_parallel._calc()


@pytest.mark.parametrize(
    "executor_factory",
    [
        multi_processing_executor_factory,
        multi_threading_executor_factory,
        SynchronousLocalTaskExecutor,
    ],
)
def test_executors_group_all_as_one(executor_factory):
    dr = (
        driver.Builder()
        .with_modules(no_parallel)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executor_factory())
        .with_grouping_strategy(GroupNodesAllAsOne())
        .build()
    )

    results = dr.execute(["final"])
    assert results["final"] == no_parallel._calc()


@pytest.mark.parametrize(
    "executor_factory",
    [
        multi_processing_executor_factory,
        multi_threading_executor_factory,
        SynchronousLocalTaskExecutor,
    ],
)
def test_executors_group_by_level(executor_factory):
    dr = (
        driver.Builder()
        .with_modules(no_parallel)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executor_factory())
        .with_grouping_strategy(GroupNodesByLevel())
        .build()
    )
    result = dr.execute(["final"])
    assert result["final"] == no_parallel._calc()


@pytest.mark.parametrize(
    "executor_factory",
    [
        multi_processing_executor_factory,
        multi_threading_executor_factory,
        SynchronousLocalTaskExecutor,
    ],
)
def test_end_to_end_parallel_execute_group_by_repeatable_blocks(executor_factory):
    dr = (
        driver.Builder()
        .with_modules(parallel_linear_basic)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executor_factory())
        .with_grouping_strategy(GroupByRepeatableBlocks())
        .build()
    )
    result = dr.execute(["final"])
    assert result["final"] == parallel_linear_basic._calc()


@pytest.mark.parametrize(
    "executor_factory", [multi_processing_executor_factory, multi_threading_executor_factory]
)
def test_parallelism_would_take_too_long_with_no_parallelism(executor_factory):
    dr = (
        driver.Builder()
        .with_modules(parallel_delayed)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executor_factory())
        .with_grouping_strategy(GroupByRepeatableBlocks())
        .build()
    )
    t0 = time.time()
    delay_seconds = 1.0
    number_of_steps = 30
    result = dr.execute(
        ["final"], inputs={"delay_seconds": delay_seconds, "number_of_steps": number_of_steps}
    )
    t1 = time.time()
    assert result["final"] == 197780
    time_taken = t1 - t0
    assert time_taken < 10


@pytest.mark.parametrize(
    "executor_factory",
    [
        multi_processing_executor_factory,
        multi_threading_executor_factory,
        SynchronousLocalTaskExecutor,
    ],
)
def test_end_to_end_block_has_external_dependencies(executor_factory):
    dr = (
        driver.Builder()
        .with_modules(parallel_complex)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executor_factory())
        .with_grouping_strategy(GroupByRepeatableBlocks())
        .build()
    )
    result = dr.execute(["final"])
    assert result["final"] == 165


@pytest.mark.parametrize(
    "executor_factory",
    [
        multi_processing_executor_factory,
        multi_threading_executor_factory,
        SynchronousLocalTaskExecutor,
    ],
)
def test_end_to_end_with_overrides(executor_factory):
    dr = (
        driver.Builder()
        .with_modules(parallel_complex)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executor_factory())
        .with_grouping_strategy(GroupByRepeatableBlocks())
        .build()
    )
    result = dr.execute(
        ["final"],
        overrides={
            "number_of_steps": 2,
            "param_external_to_block": 0,
            "second_param_external_to_block": 0,
        },
    )
    assert result["final"] == 5


def create_dummy_task(task_purpose: NodeGroupPurpose):
    return TaskImplementation(
        base_id="foo",
        spawning_task_base_id="foo",
        nodes=[],
        purpose=task_purpose,
        outputs_to_compute=[],
        overrides={},
        adapters=[],
        base_dependencies=[],
        group_id=None,
        realized_dependencies={},
        spawning_task_id=None,
        dynamic_inputs={},
    )


@pytest.mark.parametrize(
    "purpose, check",
    [
        (NodeGroupPurpose.EXECUTE_BLOCK, lambda x: isinstance(x, MultiProcessingExecutor)),
        (NodeGroupPurpose.EXECUTE_SINGLE, lambda x: isinstance(x, SynchronousLocalTaskExecutor)),
        (NodeGroupPurpose.EXPAND_UNORDERED, lambda x: isinstance(x, SynchronousLocalTaskExecutor)),
        (NodeGroupPurpose.GATHER, lambda x: isinstance(x, SynchronousLocalTaskExecutor)),
    ],
)
def test_get_execotor_for_tasks_default_execution_manager(purpose, check):
    execution_manager = DefaultExecutionManager(
        local_executor=SynchronousLocalTaskExecutor(),
        remote_executor=MultiProcessingExecutor(max_tasks=10),
    )
    assert check(execution_manager.get_executor_for_task(create_dummy_task(purpose)))


def test_end_to_end_parallelizable_with_input_in_collect():
    def par() -> Parallelizable[int]:
        for i in range(10):
            yield i

    def identity(par: int) -> int:
        return par

    def foo() -> int:
        return 1

    def collect(identity: Collect[int], foo: int) -> int:
        return sum(identity) + foo

    tmp_module = create_temporary_module(par, identity, collect, foo)
    dr = (
        driver.Builder()
        .with_modules(tmp_module)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(SynchronousLocalTaskExecutor())
    ).build()
    assert dr.execute(["collect"])["collect"] == 46
