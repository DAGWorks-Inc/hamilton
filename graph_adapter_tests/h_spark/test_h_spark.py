import pandas as pd
import pyspark.pandas as ps
import pytest
from pyspark.sql import SparkSession

from hamilton import base, driver
from hamilton.experimental import h_spark

from .resources import example_module, smoke_screen_module


@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()


def test_koalas_spark_graph_adapter(spark_session):
    initial_columns = {
        "signups": ps.Series([1, 10, 50, 100, 200, 400], name="signups"),
        "spend": ps.Series([10, 10, 20, 40, 40, 50], name="signups"),
    }
    ps.set_option(
        "compute.ops_on_diff_frames", True
    )  # we should play around here on how to correctly initialize data.
    ps.set_option("compute.default_index_type", "distributed")  # this one doesn't seem to work?
    dr = driver.Driver(
        initial_columns,
        example_module,
        adapter=h_spark.SparkKoalasGraphAdapter(
            spark_session, result_builder=base.PandasDataFrameResult(), spine_column="spend"
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
        [0.0, 0.0, 13.33333, 23.33333, 33.33333, 43.33333],
        index=[0, 1, 2, 3, 4, 5],
        name="avg_3wk_spend",
    )
    pd.testing.assert_series_equal(
        df.avg_3wk_spend.fillna(0.0).sort_index(), expected_column
    )  # fill na to get around NaN
    # TODO: do some more asserting?


def test_smoke_screen_module(spark_session):
    config = {"region": "US", "pandas_on_spark": True}
    ps.set_option(
        "compute.ops_on_diff_frames", True
    )  # we should play around here on how to correctly initialize data.
    ps.set_option("compute.default_index_type", "distributed")  # this one doesn't seem to work?
    dr = driver.Driver(
        config,
        smoke_screen_module,
        adapter=h_spark.SparkKoalasGraphAdapter(
            spark_session, result_builder=base.PandasDataFrameResult(), spine_column="weeks"
        ),
    )
    output_columns = [
        "raw_acquisition_cost",
        "pessimistic_net_acquisition_cost",
        "neutral_net_acquisition_cost",
        "optimistic_net_acquisition_cost",
        "weeks",
    ]
    df = dr.execute(
        inputs={"start_date": "20200101", "end_date": "20220801"}, final_vars=output_columns
    )
    epsilon = 0.00001
    assert abs(df.mean()["raw_acquisition_cost"] - 0.393808) < epsilon
    assert abs(df.mean()["pessimistic_net_acquisition_cost"] - 0.420769) < epsilon
    assert abs(df.mean()["neutral_net_acquisition_cost"] - 0.405582) < epsilon
    assert abs(df.mean()["optimistic_net_acquisition_cost"] - 0.399363) < epsilon
