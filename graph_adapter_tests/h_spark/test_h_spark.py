import pandas as pd
import pyspark.pandas as ps
import pytest
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import column

from hamilton import base, driver, htypes, node
from hamilton.experimental import h_spark

from .resources import example_module, pyspark_udfs, smoke_screen_module


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


spark = SparkSession.builder.master("local[1]").getOrCreate()

pandas_df = pd.DataFrame({"spend": [10, 10, 20, 40, 40, 50], "signups": [1, 10, 50, 100, 200, 400]})
spark_df = spark.createDataFrame(pandas_df)


@pytest.mark.parametrize(
    "input, expected",
    [
        ({}, (None, {})),
        ({"a": 1}, (None, {"a": 1})),
        ({"a": spark_df}, (spark_df, {})),
        ({"a": spark_df, "b": 1}, (spark_df, {"b": 1})),
    ],
    ids=["no_kwargs", "one_plain_kwarg", "one_df_kwarg", "one_df_kwarg_and_one_plain_kwarg"],
)
def test__inspect_kwargs(input, expected):
    """A unit test for inspect_kwargs."""
    assert h_spark._inspect_kwargs(input) == expected


def test__get_pandas_annotations():
    """Unit test for _get_pandas_annotations()."""

    def no_pandas(a: int, b: float) -> float:
        return a * b

    def with_pandas(a: pd.Series) -> pd.Series:
        return a * 2

    def with_pandas_and_other_default(a: pd.Series, b: int = 2) -> pd.Series:
        return a * b

    def with_pandas_and_other_default_with_one_more(a: pd.Series, c: int, b: int = 2) -> pd.Series:
        return a * b

    assert h_spark._get_pandas_annotations(no_pandas) == {"a": False, "b": False}
    assert h_spark._get_pandas_annotations(with_pandas) == {"a": True}
    assert h_spark._get_pandas_annotations(with_pandas_and_other_default) == {"a": True}
    assert h_spark._get_pandas_annotations(with_pandas_and_other_default_with_one_more) == {
        "a": True,
        "c": False,
    }


def test__bind_parameters_to_callable():
    """Unit test for _bind_parameters_to_callable()."""

    def base_func(a: int, b: int) -> int:
        return a + b

    actual_kwargs = {"a": 1, "b": 2}
    df_columns = {"b"}
    node_input_types = {"a": (int,), "b": (int,)}
    mod_func, df_params = h_spark._bind_parameters_to_callable(
        actual_kwargs, df_columns, base_func, node_input_types, "test"
    )
    import inspect

    sig = inspect.signature(mod_func)
    assert sig.parameters["a"].default == 1
    assert sig.parameters["b"].default == inspect.Parameter.empty
    assert str(df_params["b"]) == str(column("b"))  # hacky, but compare string representation.


def test__lambda_udf_plain_func(spark_session):
    """Tests plain UDF function"""

    def base_func(a: int, b: int) -> int:
        return a + b

    base_spark_df = spark_session.createDataFrame(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))
    node_ = node.Node(
        "test",
        int,
        "",
        base_func,
        input_types={
            "a": (int, node.DependencyType.REQUIRED),
            "b": (int, node.DependencyType.REQUIRED),
        },
    )

    new_df = h_spark._lambda_udf(base_spark_df, node_, base_func, {})
    assert new_df.collect() == [Row(a=1, b=4, test=5), Row(a=2, b=5, test=7), Row(a=3, b=6, test=9)]


def test__lambda_udf_pandas_func(spark_session):
    """Tests pandas UDF function"""

    def base_func(a: pd.Series, b: pd.Series) -> htypes.column[pd.Series, int]:
        return a + b

    base_spark_df = spark_session.createDataFrame(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))
    node_ = node.Node(
        "test",
        htypes.column[pd.Series, int],
        "",
        base_func,
        input_types={
            "a": (int, node.DependencyType.REQUIRED),
            "b": (int, node.DependencyType.REQUIRED),
        },
    )

    new_df = h_spark._lambda_udf(base_spark_df, node_, base_func, {})
    assert new_df.collect() == [Row(a=1, b=4, test=5), Row(a=2, b=5, test=7), Row(a=3, b=6, test=9)]


def test__lambda_udf_pandas_func_error(spark_session):
    """Tests it errors on a bad pandas UDF function"""

    def base_func(a: pd.Series, b: int) -> htypes.column[pd.Series, int]:
        return a + b

    base_spark_df = spark_session.createDataFrame(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))
    node_ = node.Node(
        "test",
        htypes.column[pd.Series, int],
        "",
        base_func,
        input_types={
            "a": (int, node.DependencyType.REQUIRED),
            "b": (int, node.DependencyType.REQUIRED),
        },
    )

    with pytest.raises(ValueError):
        h_spark._lambda_udf(base_spark_df, node_, base_func, {"a": 1})


def test_smoke_screen_udf_graph_adatper(spark_session):
    """Tests that we can run the PySparkUDFGraphAdapter on a simple graph.

    THe graph has a pandas UDF, a plain UDF that depends on the output of the pandas UDF, and
    also has a parameter bound to it, and then an extra function that isn't satisfied by the
    dataframe, so we add the result as a literal. This should exercise all the code paths
    at least in the result_builder.

    """
    input_df = spark_session.createDataFrame(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))
    dr = driver.Driver({}, pyspark_udfs, adapter=h_spark.PySparkUDFGraphAdapter())
    inputs = {"a": input_df, "b": input_df, "c": 4, "d": 5}
    output_df = dr.execute(["base_func", "base_func2", "base_func3"], inputs=inputs)
    assert output_df.collect() == [
        Row(a=1, b=4, base_func=5, base_func2=9, base_func3=9),
        Row(a=2, b=5, base_func=7, base_func2=11, base_func3=9),
        Row(a=3, b=6, base_func=9, base_func2=13, base_func3=9),
    ]
