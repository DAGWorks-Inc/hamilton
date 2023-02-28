"""Spark driver and Hamilton driver code."""

import implementation
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from hamilton import driver, log_setup


def use_pandas_udfs(df):
    agg_values = df.agg(F.mean("spend"), F.stddev("spend"))
    agg_values = agg_values.withColumnRenamed("avg(spend)", "spend_mean")
    agg_values = agg_values.withColumnRenamed("stddev_samp(spend)", "spend_std_dev")
    config = {"aggs": "columns"}  # or "columns" or "literals"
    # this works:
    df = df.withColumn("spend_mean", F.lit(agg_values.first()["spend_mean"]))
    df = df.withColumn("spend_std_dev", F.lit(agg_values.first()["spend_std_dev"]))
    other_inputs = {
        # "spend_mean": agg_values.first()["spend_mean"],
        # "spend_std_dev": agg_values.first()["spend_std_dev"],
        "foo": 1.0,
        "bar": 2.0,
    }
    import pandas_udfs

    modules = [pandas_udfs]
    adapter = implementation.PySparkGraphAdapter()

    dr = driver.Driver(config, *modules, adapter=adapter)  # can pass in multiple modules

    inputs = {col: df for col in df.columns}
    if other_inputs:
        inputs.update(other_inputs)
    # we need to specify what we want the dataframe schema to look like
    output_columns = [
        "spend",
        "signups",
        "spend_mean",
        "spend_per_signup",
        "spend_zero_mean_unit_variance",
        "foo",
        "bar",
        "augmented_mean",
    ]
    dr.visualize_execution(output_columns, "./my_spark_udf.dot", {"format": "png"}, inputs=inputs)
    # let's create the dataframe!
    df = dr.execute(output_columns, inputs=inputs)
    # To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
    return df


def use_vanilla_udfs(df):
    agg_values = df.agg(F.mean("spend"), F.stddev("spend"))
    agg_values = agg_values.withColumnRenamed("avg(spend)", "spend_mean")
    agg_values = agg_values.withColumnRenamed("stddev_samp(spend)", "spend_std_dev")
    # this works:
    # df = df.withColumn("spend_mean", F.lit(agg_values.first()["spend_mean"]))
    # df = df.withColumn("spend_std_dev", F.lit(agg_values.first()["spend_std_dev"]))
    other_inputs = {}
    other_inputs = {
        "spend_mean": agg_values.first()["spend_mean"],
        "spend_std_dev": agg_values.first()["spend_std_dev"],
        "foo": 1.0,
        "bar": 2.0,
    }
    import vanilla_udfs

    modules = [vanilla_udfs]
    adapter = implementation.PySparkGraphAdapter()

    dr = driver.Driver({}, *modules, adapter=adapter)  # can pass in multiple modules

    inputs = {col: df for col in df.columns}
    if other_inputs:
        inputs.update(other_inputs)
    # we need to specify what we want the dataframe schema to look like
    output_columns = [
        "spend",
        "signups",
        "spend_mean",
        "spend_per_signup",
        "spend_zero_mean_unit_variance",
        "foo",
        "bar",
        "augmented_mean",
    ]
    # let's create the dataframe!
    df = dr.execute(output_columns, inputs=inputs)
    # To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
    dr.visualize_execution(output_columns, "./my_spark_udf.dot", {"format": "png"}, inputs=inputs)
    return df


if __name__ == "__main__":
    log_setup.setup_logging(log_level=log_setup.LOG_LEVELS["INFO"])
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("info")

    # replace this with SQL or however you'd get the data you need in.
    pandas_df = pd.DataFrame(
        {"spend": [10, 10, 20, 40, 40, 50], "signups": [1, 10, 50, 100, 200, 400]}
    )

    df = spark.createDataFrame(pandas_df)
    df = use_vanilla_udfs(df)
    # df = use_pandas_udfs(df)
    # dr.display_all_functions('./my_full_dag.dot')
    # print(type(df))
    # df.show()
    w = Window.rowsBetween(-2, 0)
    df = df.withColumn("avg_3wk_spend", F.mean("spend").over(w))
    df = df.select(
        [
            "spend",
            "signups",
            "avg_3wk_spend",
            "spend_per_signup",
            "spend_zero_mean_unit_variance",
            "foo",
            "bar",
            "augmented_mean",
        ]
    )
    df.show()
    spark.stop()
