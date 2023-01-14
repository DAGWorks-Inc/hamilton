import sys

import pytest

import hamilton.driver
import tests.resources.data_quality
import tests.resources.smoke_screen_module
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
