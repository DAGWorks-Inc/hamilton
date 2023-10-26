from unittest import mock

import pandas as pd
import pytest

import tests.resources.cyclic_functions
import tests.resources.dummy_functions
import tests.resources.dynamic_parallelism.parallel_linear_basic
import tests.resources.tagging
import tests.resources.test_default_args
import tests.resources.very_simple_dag
from hamilton import base
from hamilton.driver import (
    Builder,
    Driver,
    InvalidExecutorException,
    TaskBasedGraphExecutor,
    Variable,
)
from hamilton.execution import executors
from hamilton.io.materialization import to

"""This file tests driver capabilities.
Anything involving execution is tested for multiple executors/driver configuration.
Anything not involving execution is tested for just the single driver configuration.

TODO -- move any execution tests to tests the graph executor capabilities on their own.
"""


@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({"a": 1})),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_config({"a": 1})
            .build()
        ),
    ],
)
def test_driver_validate_input_types(driver_factory):
    dr = driver_factory()
    results = dr.raw_execute(["a"])
    assert results == {"a": 1}


@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({}, tests.resources.very_simple_dag)),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.very_simple_dag)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .build()
        ),
    ],
)
def test_driver_validate_runtime_input_types(driver_factory):
    dr = driver_factory()
    results = dr.raw_execute(["b"], inputs={"a": 1})
    assert results == {"b": 1}


@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({}, tests.resources.cyclic_functions)),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.cyclic_functions)
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .build()
        ),
    ],
)
def test_driver_has_cycles_true(driver_factory):
    """Tests that we don't break when detecting cycles from the driver."""
    dr = driver_factory()
    assert dr.has_cycles(["C"])


# This is possible -- but we don't want to officially support it. Here for documentation purposes.
# def test_driver_cycles_execute_override():
#     """Tests that we short circuit a cycle by passing in overrides."""
#     dr = Driver({}, tests.resources.cyclic_functions, adapter=base.DefaultAdapter())
#     result = dr.execute(['C'], overrides={'D': 1}, inputs={'b': 2, 'c': 2})
#     assert result['C'] == 34


@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({}, tests.resources.cyclic_functions)),
        # TODO -- fix erroring out when we try to run a driver with cycles
        # should display a better error
        # (lambda: Builder()
        #     .enabble_parallelizable_type(allow_experimental_mode=True)
        #     .with_modules(tests.resources.cyclic_functions)
        #     .with_remote_executor(executors.SynchronousLocalTaskExecutor())
        #     .with_adapter(base.DefaultAdapter())
        #     .build())
    ],
)
def test_driver_cycles_execute_recursion_error(driver_factory):
    """Tests that we throw a recursion error when we try to execute over a DAG that isn't a DAG."""
    dr = driver_factory()
    with pytest.raises(RecursionError):
        dr.execute(["C"], inputs={"b": 2, "c": 2})


def test_driver_variables_exposes_tags():
    dr = Driver({}, tests.resources.tagging)
    tags = {var.name: var.tags for var in dr.list_available_variables()}
    assert tags["a"] == {"module": "tests.resources.tagging", "test": "a"}
    assert tags["b"] == {"module": "tests.resources.tagging", "test": "b_c"}
    assert tags["c"] == {"module": "tests.resources.tagging", "test": "b_c"}
    assert tags["d"] == {"module": "tests.resources.tagging"}


def test_driver_variables_external_input():
    dr = Driver({}, tests.resources.very_simple_dag)
    input_types = {var.name: var.is_external_input for var in dr.list_available_variables()}
    assert input_types["a"] is True
    assert input_types["b"] is False


def test_driver_variables_exposes_original_function():
    dr = Driver({}, tests.resources.very_simple_dag)
    originating_functions = {
        var.name: var.originating_functions for var in dr.list_available_variables()
    }
    assert originating_functions["b"] == (tests.resources.very_simple_dag.b,)
    assert originating_functions["a"] is None


@mock.patch("hamilton.telemetry.send_event_json")
def test_capture_constructor_telemetry_disabled(send_event_json):
    """Tests that we don't do anything if telemetry is disabled."""
    send_event_json.return_value = ""
    Driver({}, tests.resources.tagging)  # this will exercise things underneath.
    assert send_event_json.called is False


@mock.patch("hamilton.telemetry.get_adapter_name")
@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
def test_capture_constructor_telemetry_error(send_event_json, get_adapter_name):
    """Tests that we don't error if an exception occurs"""
    get_adapter_name.side_effect = ValueError("TELEMETRY ERROR")
    Driver({}, tests.resources.tagging)  # this will exercise things underneath.
    assert send_event_json.called is False


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
def test_capture_constructor_telemetry_none_values(send_event_json):
    """Tests that we don't error if there are none values"""
    Driver({}, None, None)  # this will exercise things underneath.
    assert send_event_json.called is True


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
def test_capture_constructor_telemetry(send_event_json):
    """Tests that we send an event if we could. Validates deterministic parts."""
    Driver({}, tests.resources.very_simple_dag)
    # assert send_event_json.called is True
    assert len(send_event_json.call_args_list) == 1  # only called once
    # check contents of what it was called with:
    send_event_json_call = send_event_json.call_args_list[0]
    actual_event_dict = send_event_json_call[0][0]
    assert actual_event_dict["api_key"] == "phc_mZg8bkn3yvMxqvZKRlMlxjekFU5DFDdcdAsijJ2EH5e"
    assert actual_event_dict["event"] == "os_hamilton_run_start"
    # validate schema
    expected_properites = {
        "os_type",
        "os_version",
        "python_version",
        "distinct_id",
        "hamilton_version",
        "telemetry_version",
        "number_of_nodes",
        "number_of_modules",
        "number_of_config_items",
        "decorators_used",
        "graph_adapter_used",
        "result_builder_used",
        "driver_run_id",
        "error",
        "graph_executor_class",
    }
    actual_properties = actual_event_dict["properties"]
    assert set(actual_properties.keys()) == expected_properites
    # validate static parts
    assert actual_properties["error"] is None
    assert actual_properties["number_of_nodes"] == 2  # b, and input a
    assert actual_properties["number_of_modules"] == 1
    assert actual_properties["number_of_config_items"] == 0
    assert actual_properties["number_of_config_items"] == 0
    assert (
        actual_properties["graph_adapter_used"] == "hamilton.base.SimplePythonDataFrameGraphAdapter"
    )
    assert actual_properties["result_builder_used"] == "hamilton.base.PandasDataFrameResult"


@mock.patch("hamilton.telemetry.send_event_json")
@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({}, tests.resources.very_simple_dag)),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.very_simple_dag)
            .with_adapter(base.SimplePythonGraphAdapter(base.PandasDataFrameResult()))
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .build()
        ),
    ],
)
def test_capture_execute_telemetry_disabled(send_event_json, driver_factory):
    """Tests that we don't do anything if telemetry is disabled."""
    dr = driver_factory()
    results = dr.execute(["b"], inputs={"a": 1})
    expected = pd.DataFrame([{"b": 1}])
    pd.testing.assert_frame_equal(results, expected)
    assert send_event_json.called is False


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({}, tests.resources.very_simple_dag)),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.very_simple_dag)
            .with_adapter(base.SimplePythonGraphAdapter(base.PandasDataFrameResult()))
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .build()
        ),
    ],
)
def test_capture_execute_telemetry_error(send_event_json, driver_factory):
    """Tests that we don't error if an exception occurs"""
    send_event_json.side_effect = [None, ValueError("FAKE ERROR"), None]
    dr = driver_factory()
    results = dr.execute(["b"], inputs={"a": 1})
    expected = pd.DataFrame([{"b": 1}])
    pd.testing.assert_frame_equal(results, expected)
    assert send_event_json.called is True
    assert len(send_event_json.call_args_list) == 2


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({}, tests.resources.very_simple_dag)),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.very_simple_dag)
            .with_adapter(base.SimplePythonGraphAdapter(base.PandasDataFrameResult()))
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .build()
        ),
    ],
)
def test_capture_execute_telemetry(send_event_json, driver_factory):
    """Happy path with values passed."""
    dr = driver_factory()
    results = dr.execute(["b"], inputs={"a": 1}, overrides={"b": 2})
    expected = pd.DataFrame([{"b": 2}])
    pd.testing.assert_frame_equal(results, expected)
    assert send_event_json.called is True
    assert len(send_event_json.call_args_list) == 2


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
@pytest.mark.parametrize(
    "driver_factory",
    [
        (lambda: Driver({"a": 1}, tests.resources.very_simple_dag)),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.very_simple_dag)
            .with_adapter(base.SimplePythonGraphAdapter(base.PandasDataFrameResult()))
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_config({"a": 1})
            .build()
        ),
    ],
)
def test_capture_execute_telemetry_none_values(send_event_json, driver_factory):
    """Happy path with none values."""
    dr = driver_factory()
    results = dr.execute(["b"])
    expected = pd.DataFrame([{"b": 1}])
    pd.testing.assert_frame_equal(results, expected)
    assert len(send_event_json.call_args_list) == 2


@pytest.mark.parametrize(
    "driver_factory",
    [
        (
            lambda: Driver(
                {"required": 1},
                tests.resources.test_default_args,
                adapter=base.DefaultAdapter(),
            )
        ),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.test_default_args)
            .with_adapter(base.DefaultAdapter())
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_config({"required": 1})
            .build()
        ),
    ],
)
def test_node_is_required_by_anything(driver_factory):
    """Tests that default args are correctly interpreted.

    Specifically, if it's not in the execution path then things should
    just work. Here I'm being lazy and rather than specifically testing
    _node_is_required_by_anything() directly, I'm doing it via
    execute(), which calls it via validate_inputs().

    To understand what's going on see the functions in `test_default_args`.
    """
    dr = driver_factory()
    # D is not in the execution path, but requires defaults_to_zero
    # so this should work.
    results = dr.execute(["C"])
    assert results["C"] == 2
    with pytest.raises(ValueError):
        # D is now in the execution path, but requires defaults_to_zero
        # this should error
        dr.execute(["D"])


@pytest.mark.parametrize(
    "driver_factory",
    [
        (
            lambda: Driver(
                {"required": 1},
                tests.resources.test_default_args,
                adapter=base.DefaultAdapter(),
            )
        ),
        (
            lambda: Builder()
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_modules(tests.resources.test_default_args)
            .with_adapter(base.DefaultAdapter())
            .with_remote_executor(executors.SynchronousLocalTaskExecutor())
            .with_config({"required": 1})
            .build()
        ),
    ],
)
def test_using_callables_to_execute(driver_factory):
    """Test that you can pass a function reference and it will work fine."""
    dr = driver_factory()
    results = dr.execute(
        [tests.resources.test_default_args.C, tests.resources.test_default_args.B, "A"]
    )
    assert results["C"] == 2
    assert results["B"] == 1
    assert results["A"] == 1
    with pytest.raises(ValueError):
        dr.execute([tests.resources.cyclic_functions.B])


def test_create_final_vars():
    """Tests that the final vars are created correctly."""
    dr = Driver({"required": 1}, tests.resources.test_default_args)
    actual = dr._create_final_vars(
        [
            "C",
            tests.resources.test_default_args.B,
            tests.resources.test_default_args.A,
            Variable("D", int, {}, False),
        ]
    )
    expected = ["C", "B", "A", "D"]
    assert actual == expected


def test_create_final_vars_errors():
    """Tests that we catch functions pointed to in modules that aren't part of the DAG."""
    dr = Driver({"required": 1}, tests.resources.test_default_args)
    with pytest.raises(ValueError):
        dr._create_final_vars(
            ["C", tests.resources.cyclic_functions.A, tests.resources.cyclic_functions.B]
        )


def test_v2_driver_builder():
    dr = (
        Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_adapter(base.DefaultAdapter())
        .with_modules(tests.resources.very_simple_dag)
        .build()
    )
    assert isinstance(dr.graph_executor, TaskBasedGraphExecutor)
    assert list(dr.graph_modules) == [tests.resources.very_simple_dag]


def test_executor_validates_happy_default_executor():
    dr = Driver({}, tests.resources.very_simple_dag)
    nodes, user_nodes = dr.graph.get_upstream_nodes(["b"])
    dr.graph_executor.validate(nodes | user_nodes)


def test_executor_validates_sad_default_executor():
    dr = Driver({}, tests.resources.dynamic_parallelism.parallel_linear_basic)
    nodes, user_nodes = dr.graph.get_upstream_nodes(["final"])
    with pytest.raises(InvalidExecutorException):
        dr.graph_executor.validate(nodes | user_nodes)


def test_executor_validates_happy_parallel_executor():
    dr = (
        Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(tests.resources.dynamic_parallelism.parallel_linear_basic)
        .build()
    )

    nodes, user_nodes = dr.graph.get_upstream_nodes(["final"])
    dr.graph_executor.validate(nodes | user_nodes)


def test_builder_defaults_to_dict_result():
    dr = Builder().with_modules(tests.resources.dummy_functions).build()

    result = dr.execute(["C"], inputs={"b": 1, "c": 1})
    assert result == {"C": 4}


def test_materialize_checks_required_input(tmp_path):
    dr = Builder().with_modules(tests.resources.dummy_functions).build()

    with pytest.raises(ValueError):
        dr.materialize(additional_vars=["C"], inputs={"c": 1})
    with pytest.raises(ValueError):
        dr.materialize(
            to.pickle(id="1", path=f"{tmp_path}/foo.pkl", dependencies=["C"]), inputs={"c": 1}
        )


def test_validate_execution_happy():
    dr = Builder().with_modules(tests.resources.very_simple_dag).build()
    dr.validate_execution(["b"], inputs={"a": 1})


def test_validate_execution_sad():
    dr = Builder().with_modules(tests.resources.very_simple_dag).build()
    with pytest.raises(ValueError):
        dr.validate_execution(["b"], inputs={})


def test_validate_materialization_happy(tmp_path):
    dr = Builder().with_modules(tests.resources.very_simple_dag).build()
    dr.validate_materialization(
        to.pickle(id="1", path=f"{tmp_path}/foo.pkl", dependencies=["b"]), inputs={"a": 1}
    )


def test_validate_materialization_sad(tmp_path):
    dr = Builder().with_modules(tests.resources.very_simple_dag).build()
    with pytest.raises(ValueError):
        dr.validate_materialization(
            # c does not exist
            # no inputs either
            to.pickle(id="1", path=f"{tmp_path}/foo.pkl", dependencies=["c"]),
            inputs={},
        )
