import importlib
import os
import sys
from unittest import mock

import pytest

import hamilton.driver
import tests.resources.data_quality
import tests.resources.dynamic_config
from hamilton import settings
from hamilton.data_quality.base import DataValidationError, ValidationResult


@pytest.fixture(autouse=True)
def power_user_mode():
    with mock.patch.dict(os.environ, {"HAMILTON_POWER_USER_MODE": "true"}):
        yield


def test_data_quality_workflow_passes():
    driver = hamilton.driver.Driver({}, tests.resources.data_quality)
    all_vars = driver.list_available_variables()
    result = driver.raw_execute(
        [var.name for var in all_vars], inputs={"data_quality_should_fail": False}
    )
    dq_nodes = [
        var.name
        for var in all_vars
        if var.tags.get("hamilton.data_quality.contains_dq_results", False)
    ]
    assert len(dq_nodes) == 1
    dq_result = result[dq_nodes[0]]
    assert isinstance(dq_result, ValidationResult)
    assert dq_result.passes is True


def test_data_quality_workflow_fails():
    driver = hamilton.driver.Driver({}, tests.resources.data_quality)
    all_vars = driver.list_available_variables()
    with pytest.raises(DataValidationError):
        driver.raw_execute(
            [var.name for var in all_vars], inputs={"data_quality_should_fail": True}
        )


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
    "future_import_annotations",
    [
        True,
        False,
    ],
)
def test_smoke_screen_module(future_import_annotations, monkeypatch):
    # Monkeypatch the env
    # This tells the smoke screen module whether to use the future import
    modification_func = (
        lambda source: "\n".join(["from __future__ import annotations"] + source.splitlines())
        if future_import_annotations
        else source
    )
    # module = importlib.reload(tests.resources.smoke_screen_module)
    module = modify_and_import(
        "tests.resources.smoke_screen_module", tests.resources, modification_func
    )
    config = {"region": "US"}
    dr = hamilton.driver.Driver(config, module)
    output_columns = [
        "raw_acquisition_cost",
        "pessimistic_net_acquisition_cost",
        "neutral_net_acquisition_cost",
        "optimistic_net_acquisition_cost",
        "series_with_start_date_end_date",
    ]
    df = dr.execute(
        inputs={"date_range": {"start_date": "20200101", "end_date": "20220801"}},
        final_vars=output_columns,
    )
    epsilon = 0.00001
    assert abs(df["raw_acquisition_cost"].mean() - 0.393808) < epsilon
    assert abs(df["pessimistic_net_acquisition_cost"].mean() - 0.420769) < epsilon
    assert abs(df["neutral_net_acquisition_cost"].mean() - 0.405582) < epsilon
    assert abs(df["optimistic_net_acquisition_cost"].mean() - 0.399363) < epsilon
    assert df["series_with_start_date_end_date"].iloc[0] == "date_20200101_date_20220801"


def test_end_to_end_with_dynamic_config():
    config = {
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
    module = tests.resources.dynamic_config
    dr = hamilton.driver.Driver(config, module)

    out = dr.execute(final_vars=list(config["columns_to_sum_map"].keys()))
    assert out["ab"][0] == 2
    assert out["cd"][0] == 2
    assert out["ed"][0] == 2
    assert out["ae"][0] == 2
    assert out["abcd"][0] == 4
    assert out["abcdcd"][0] == 6
