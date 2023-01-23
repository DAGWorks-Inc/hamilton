from unittest import mock

import pandas as pd
import pytest

import tests.resources.cyclic_functions
import tests.resources.tagging
import tests.resources.very_simple_dag
from hamilton import base
from hamilton.driver import Driver


def test_driver_validate_input_types():
    dr = Driver({"a": 1})
    results = dr.raw_execute(["a"])
    assert results == {"a": 1}


def test_driver_validate_runtime_input_types():
    dr = Driver({}, tests.resources.very_simple_dag)
    results = dr.raw_execute(["b"], inputs={"a": 1})
    assert results == {"b": 1}


def test_driver_has_cycles_true():
    """Tests that we don't break when detecting cycles from the driver."""
    dr = Driver({}, tests.resources.cyclic_functions)
    assert dr.has_cycles(["C"])


# This is possible -- but we don't want to officially support it. Here for documentation purposes.
# def test_driver_cycles_execute_override():
#     """Tests that we short circuit a cycle by passing in overrides."""
#     dr = Driver({}, tests.resources.cyclic_functions, adapter=base.SimplePythonGraphAdapter(base.DictResult()))
#     result = dr.execute(['C'], overrides={'D': 1}, inputs={'b': 2, 'c': 2})
#     assert result['C'] == 34


def test_driver_cycles_execute_recursion_error():
    """Tests that we throw a recursion error when we try to execute over a DAG that isn't a DAG."""
    dr = Driver(
        {},
        tests.resources.cyclic_functions,
        adapter=base.SimplePythonGraphAdapter(base.DictResult()),
    )
    with pytest.raises(RecursionError):
        dr.execute(["C"], inputs={"b": 2, "c": 2})


def test_driver_variables():
    dr = Driver({}, tests.resources.tagging)
    tags = {var.name: var.tags for var in dr.list_available_variables()}
    assert tags["a"] == {"module": "tests.resources.tagging", "test": "a"}
    assert tags["b"] == {"module": "tests.resources.tagging", "test": "b_c"}
    assert tags["c"] == {"module": "tests.resources.tagging", "test": "b_c"}
    assert tags["d"] == {"module": "tests.resources.tagging"}


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
def test_capture_execute_telemetry_disabled(send_event_json):
    """Tests that we don't do anything if telemetry is disabled."""
    dr = Driver({}, tests.resources.very_simple_dag)
    results = dr.execute(["b"], inputs={"a": 1})
    expected = pd.DataFrame([{"b": 1}])
    pd.testing.assert_frame_equal(results, expected)
    assert send_event_json.called is False


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
def test_capture_execute_telemetry_error(send_event_json):
    """Tests that we don't error if an exception occurs"""
    send_event_json.side_effect = [None, ValueError("FAKE ERROR")]
    dr = Driver({}, tests.resources.very_simple_dag)
    results = dr.execute(["b"], inputs={"a": 1})
    expected = pd.DataFrame([{"b": 1}])
    pd.testing.assert_frame_equal(results, expected)
    assert send_event_json.called is True
    assert len(send_event_json.call_args_list) == 2


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
def test_capture_execute_telemetry(send_event_json):
    """Happy path with values passed."""
    dr = Driver({}, tests.resources.very_simple_dag)
    results = dr.execute(["b"], inputs={"a": 1}, overrides={"b": 2})
    expected = pd.DataFrame([{"b": 2}])
    pd.testing.assert_frame_equal(results, expected)
    assert send_event_json.called is True
    assert len(send_event_json.call_args_list) == 2


@mock.patch("hamilton.telemetry.send_event_json")
@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
def test_capture_execute_telemetry_none_values(send_event_json):
    """Happy path with none values."""
    dr = Driver({"a": 1}, tests.resources.very_simple_dag)
    results = dr.execute(["b"])
    expected = pd.DataFrame([{"b": 1}])
    pd.testing.assert_frame_equal(results, expected)
    assert len(send_event_json.call_args_list) == 2


def test__node_is_required_by_anything():
    """Tests that default args are correctly interpreted.

    Specifically, if it's not in the execution path then things should
    just work. Here I'm being lazy and rather than specifically testing
    _node_is_required_by_anything() directly, I'm doing it via
    execute(), which calls it via validate_inputs().

    To understand what's going on see the functions in `test_default_args`.
    """
    dr = Driver({"required": 1}, tests.resources.test_default_args)
    # D is not in the execution path, but requires defaults_to_zero
    # so this should work.
    results = dr.execute(["C"])
    pd.testing.assert_series_equal(results["C"], pd.Series([2], name="C"))
    with pytest.raises(ValueError):
        # D is now in the execution path, but requires defaults_to_zero
        # this should error
        dr.execute(["D"])
