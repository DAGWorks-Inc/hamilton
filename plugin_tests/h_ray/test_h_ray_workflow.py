import pandas as pd
import pytest
import ray
from ray import workflow

from hamilton import base, driver
from hamilton.plugins import h_ray
from plugin_tests.h_ray.resources import example_module, smoke_screen_module


@pytest.fixture(scope="module")
def init():
    # Do not need to call ray.init() when using a workflow now it seems?
    yield "initialized"
    ray.shutdown()


# This does not work locally -- will ask Ray slack for support.
def test_ray_workflow_graph_adapter(init):
    workflow.init()
    initial_columns = {
        "signups": pd.Series([1, 10, 50, 100, 200, 400]),
        "spend": pd.Series([10, 10, 20, 40, 40, 50]),
    }
    dr = driver.Driver(
        initial_columns,
        example_module,
        adapter=h_ray.RayWorkflowGraphAdapter(
            base.PandasDataFrameResult(), "test-test_ray_workflow_graph_adapter"
        ),
    )
    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
    ]
    df = dr.execute(output_columns)
    assert set(df) == set(output_columns)
    expected_column = pd.Series(
        [0.0, 0.0, 13.33333, 23.33333, 33.33333, 43.33333], name="avg_3wk_spend"
    )
    pd.testing.assert_series_equal(
        df.avg_3wk_spend.fillna(0.0), expected_column
    )  # fill na to get around NaN
    # TODO: do some more asserting?


def test_smoke_screen_module(init):
    workflow.init()
    config = {"region": "US"}
    dr = driver.Driver(
        config,
        smoke_screen_module,
        adapter=h_ray.RayWorkflowGraphAdapter(
            base.PandasDataFrameResult(), "test-test_smoke_screen_module"
        ),
    )
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
