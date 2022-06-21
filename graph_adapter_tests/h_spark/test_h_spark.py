import pandas as pd
import pytest

from pyspark.sql import SparkSession
import pyspark.pandas as ps

from hamilton import driver, base
from hamilton.experimental import h_spark

from .resources import example_module


@pytest.fixture(scope='module')
def spark_session():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()


def test_koalas_spark_graph_adapter(spark_session):
    initial_columns = {
        'signups': ps.Series([1, 10, 50, 100, 200, 400], name='signups'),
        'spend': ps.Series([10, 10, 20, 40, 40, 50], name='signups'),
    }
    ps.set_option('compute.ops_on_diff_frames', True)  # we should play around here on how to correctly initialize data.
    ps.set_option('compute.default_index_type', 'distributed')  # this one doesn't seem to work?
    dr = driver.Driver(
        initial_columns, example_module,
        adapter=h_spark.SparkKoalasGraphAdapter(
            spark_session,
            result_builder=base.PandasDataFrameResult(),
            spine_column='spend')
    )
    output_columns = [
        'spend',
        'signups',
        'avg_3wk_spend',
        'spend_per_signup',
    ]
    df = dr.execute(output_columns)
    assert set(df) == set(output_columns)
    expected_column = pd.Series([0.0, 0.0, 13.33333, 23.33333, 33.33333, 43.33333], index=[0, 1, 2, 3, 4, 5], name='avg_3wk_spend')
    pd.testing.assert_series_equal(df.avg_3wk_spend.fillna(0.0).sort_index(), expected_column)  # fill na to get around NaN
    # TODO: do some more asserting?
