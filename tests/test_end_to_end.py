import importlib
import json
import sys
from typing import Any, Callable, Dict, List, Type

import pytest

import tests.resources.data_quality
import tests.resources.dynamic_config
import tests.resources.overrides
from hamilton import base, driver, settings
from hamilton.base import DefaultAdapter
from hamilton.data_quality.base import DataValidationError, ValidationResult
from hamilton.execution import executors, grouping
from hamilton.io.materialization import to


@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: driver.Driver({}, tests.resources.data_quality, adapter=DefaultAdapter())),
        (
            lambda: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.data_quality)
            .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .with_local_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .build()
        ),
        (
            lambda: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_modules(tests.resources.data_quality)
            .build()
        ),
    ],
    ids=["basic_driver", "driver_v2_multithreading", "driver_v2_synchronous"],
)
def test_data_quality_workflow_passes(driver_factory: Callable[[], driver.Driver]):
    dr = driver_factory()
    all_vars = dr.list_available_variables()
    result = dr.execute([var.name for var in all_vars], inputs={"data_quality_should_fail": False})
    dq_nodes = [
        var.name
        for var in all_vars
        if var.tags.get("hamilton.data_quality.contains_dq_results", False)
    ]
    assert len(dq_nodes) == 1
    dq_result = result[dq_nodes[0]]
    assert isinstance(dq_result, ValidationResult)
    assert dq_result.passes is True


@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: driver.Driver({}, tests.resources.data_quality)),
        (
            lambda: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.data_quality)
            .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .with_local_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .build()
        ),
        (
            lambda: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_modules(tests.resources.data_quality)
            .build()
        ),
    ],
    ids=["basic_driver", "driver_v2_multithreading", "driver_v2_synchronous"],
)
def test_data_quality_workflow_fails(driver_factory):
    dr = driver_factory()
    all_vars = dr.list_available_variables()
    with pytest.raises(DataValidationError):
        dr.execute([var.name for var in all_vars], inputs={"data_quality_should_fail": True})


# Adapted from https://stackoverflow.com/questions/41858147/how-to-modify-imported-source-code-on-the-fly
# This is needed to decide whether to import annotations...
def modify_and_import(module_name, package, modification_func):
    spec = importlib.util.find_spec(module_name, package)
    source = spec.loader.get_source(module_name)
    new_source = modification_func(source)
    module = importlib.util.module_from_spec(spec)
    codeobj = compile(new_source, module.__spec__.origin, "exec")
    exec(codeobj, module.__dict__)
    sys.modules[module_name] = module
    return module


@pytest.mark.parametrize(
    "driver_factory,future_import_annotations",
    [
        (lambda modules: driver.Driver({"region": "US"}, *modules), False),
        (
            lambda modules: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(*modules)
            .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .with_local_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .with_config({"region": "US"})
            .build(),
            False,
        ),
        (
            lambda modules: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_config({"region": "US"})
            .with_modules(*modules)
            .build(),
            False,
        ),
        (lambda modules: driver.Driver({"region": "US"}, *modules), True),
        (
            lambda modules: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(*modules)
            .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .with_local_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .with_config({"region": "US"})
            .build(),
            True,
        ),
        (
            lambda modules: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_config({"region": "US"})
            .with_modules(*modules)
            .build(),
            True,
        ),
        (
            lambda modules: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_grouping_strategy(grouping.GroupNodesAllAsOne())
            .with_config({"region": "US"})
            .with_modules(*modules)
            .build(),
            False,
        ),
        (
            lambda modules: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_grouping_strategy(grouping.GroupNodesByLevel())
            .with_config({"region": "US"})
            .with_modules(*modules)
            .build(),
            False,
        ),
        (
            lambda modules: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_grouping_strategy(grouping.GroupNodesIndividually())
            .with_config({"region": "US"})
            .with_modules(*modules)
            .build(),
            False,
        ),
    ],
    ids=[
        "basic_driver_no_future_import",
        "driver_v2_multithreading_no_future_import",
        "driver_v2_synchronous_no_future_import",
        "basic_driver_future_import",
        "driver_v2_multithreading_future_import",
        "driver_v2_synchronous_future_import",
        "driver_v2_group_all_as_one",
        "driver_v2_group_by_level",
        "driver_v2_group_nodes_individually",
    ],
)
def test_smoke_screen_module(driver_factory, future_import_annotations):
    # Monkeypatch the env
    # This tells the smoke screen module whether to use the future import
    modification_func = (
        lambda source: "\n".join(["from __future__ import annotations"] + source.splitlines())
        if future_import_annotations
        else source
    )
    module = modify_and_import(
        "tests.resources.smoke_screen_module", tests.resources, modification_func
    )
    dr = driver_factory([module])
    output_columns = [
        "raw_acquisition_cost",
        "pessimistic_net_acquisition_cost",
        "neutral_net_acquisition_cost",
        "optimistic_net_acquisition_cost",
        "series_with_start_date_end_date",
    ]
    res = dr.execute(
        inputs={"date_range": {"start_date": "20200101", "end_date": "20220801"}},
        final_vars=output_columns,
    )
    epsilon = 0.00001
    assert abs(res["raw_acquisition_cost"].mean() - 0.393808) < epsilon
    assert abs(res["pessimistic_net_acquisition_cost"].mean() - 0.420769) < epsilon
    assert abs(res["neutral_net_acquisition_cost"].mean() - 0.405582) < epsilon
    assert abs(res["optimistic_net_acquisition_cost"].mean() - 0.399363) < epsilon
    assert res["series_with_start_date_end_date"].iloc[0] == "date_20200101_date_20220801"


_dynamic_config = {
    "columns_to_sum_map": {
        "ab": ["a", "b"],
        "cd": ["c", "d"],
        "ed": ["e", "d"],
        "ae": ["a", "e"],
        # You can create some crazy self-referential stuff
        "abcd": ["ab", "cd"],
        "abcdcd": ["abcd", "cd"],
    },
    settings.ENABLE_POWER_USER_MODE: True,
}


@pytest.mark.parametrize(
    "driver_factory",
    [
        (
            lambda: driver.Driver(
                _dynamic_config, tests.resources.dynamic_config, adapter=DefaultAdapter()
            )
        ),
        (
            lambda: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.dynamic_config)
            .with_config(_dynamic_config)
            .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .with_local_executor(executors.MultiThreadingExecutor(max_tasks=3))
            .build()
        ),
        (
            lambda: driver.Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.dynamic_config)
            .with_config(_dynamic_config)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .build()
        ),
    ],
    ids=["basic_driver", "driver_v2_multithreading", "driver_v2_synchronous"],
)
def test_end_to_end_with_dynamic_config(driver_factory):
    dr = driver_factory()
    out = dr.execute(final_vars=list(_dynamic_config["columns_to_sum_map"].keys()))
    assert out["ab"][0] == 2
    assert out["cd"][0] == 2
    assert out["ed"][0] == 2
    assert out["ae"][0] == 2
    assert out["abcd"][0] == 4
    assert out["abcdcd"][0] == 6


class JoinBuilder(base.ResultMixin):
    @staticmethod
    def build_result(**outputs: Dict[str, Any]) -> Any:
        out = {}
        for output in outputs.values():
            out.update(output)
        return out

    def output_type(self) -> Type:
        return dict

    def input_types(self) -> List[Type]:
        return [dict]


def test_materialize_driver_call_end_to_end(tmp_path_factory):
    dr = driver.Driver({}, tests.resources.test_for_materialization)
    path_1 = tmp_path_factory.mktemp("home") / "test_materialize_driver_call_end_to_end_1.json"
    path_2 = tmp_path_factory.mktemp("home") / "test_materialize_driver_call_end_to_end_2.json"

    materialization_result, result = dr.materialize(
        to.json(id="materializer_1", dependencies=["json_to_save_1"], path=path_1),
        to.json(
            id="materializer_2",
            dependencies=["json_to_save_1", "json_to_save_2"],
            path=path_2,
            combine=JoinBuilder(),
        ),
        additional_vars=["json_to_save_1", "json_to_save_2"],
    )
    assert result == {
        "json_to_save_1": tests.resources.test_for_materialization.json_to_save_1(),
        "json_to_save_2": tests.resources.test_for_materialization.json_to_save_2(),
    }
    assert "materializer_1" in materialization_result
    assert "materializer_2" in materialization_result
    with open(path_1) as f_1, open(path_2) as f_2:
        assert json.load(f_1) == tests.resources.test_for_materialization.json_to_save_1()
        assert json.load(f_2) == {
            **tests.resources.test_for_materialization.json_to_save_2(),
            **tests.resources.test_for_materialization.json_to_save_1(),
        }


def test_driver_validate_with_overrides():
    dr = (
        driver.Builder()
        .with_modules(tests.resources.overrides)
        .with_adapter(base.DefaultAdapter())
        .build()
    )
    assert dr.execute(["c"], overrides={"b": 1})["c"] == 2


def test_driver_validate_with_overrides_2():
    dr = (
        driver.Builder()
        .with_modules(tests.resources.overrides)
        .with_adapter(base.DefaultAdapter())
        .build()
    )
    assert dr.execute(["d"], overrides={"b": 1})["d"] == 3
