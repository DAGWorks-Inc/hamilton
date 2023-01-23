import sys

import pandas as pd
import pytest

import hamilton.driver
import tests.resources.data_quality
import tests.resources.smoke_screen_module
from hamilton import ad_hoc_utils, base
from hamilton.base import SimplePythonGraphAdapter
from hamilton.data_quality.base import DataValidationError, ValidationResult


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


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7")
def test_smoke_screen_module():
    config = {"region": "US"}
    dr = hamilton.driver.Driver(config, tests.resources.smoke_screen_module)
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
    assert abs(df.mean()["raw_acquisition_cost"] - 0.393808) < epsilon
    assert abs(df.mean()["pessimistic_net_acquisition_cost"] - 0.420769) < epsilon
    assert abs(df.mean()["neutral_net_acquisition_cost"] - 0.405582) < epsilon
    assert abs(df.mean()["optimistic_net_acquisition_cost"] - 0.399363) < epsilon
    assert df["series_with_start_date_end_date"].iloc[0] == "date_20200101_date_20220801"


def test_configs_work_as_inputs():
    # Taken from user-reported bug:
    # https://hamilton-opensource.slack.com/archives/C03M33QB4M8
    # /p1674466068367019

    def load_sat_data(satellite_product: str) -> pd.DataFrame:
        return pd.DataFrame.from_records([{satellite_product: 1}])

    config = {"satellite_product": "sm"}
    dr = hamilton.driver.Driver(
        config,
        ad_hoc_utils.create_temporary_module(load_sat_data),
        adapter=SimplePythonGraphAdapter(base.DictResult()),
    )
    results = dr.execute(["load_sat_data"])
    pd.testing.assert_frame_equal(results["load_sat_data"], pd.DataFrame.from_records([{"sm": 1}]))


def test_inputs_work_as_inputs():
    # Taken from user-reported bug:
    # https://hamilton-opensource.slack.com/archives/C03M33QB4M8
    # /p1674466068367019

    def load_sat_data(satellite_product: str) -> pd.DataFrame:
        return pd.DataFrame.from_records([{satellite_product: 1}])

    dr = hamilton.driver.Driver(
        {},
        ad_hoc_utils.create_temporary_module(load_sat_data),
        adapter=SimplePythonGraphAdapter(base.DictResult()),
    )
    inputs = {"satellite_product": "sm"}
    results = dr.execute(["load_sat_data"], inputs=inputs)
    pd.testing.assert_frame_equal(results["load_sat_data"], pd.DataFrame.from_records([{"sm": 1}]))
