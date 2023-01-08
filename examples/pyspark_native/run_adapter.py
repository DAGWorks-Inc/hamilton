import possible_api
import pyspark_adapter
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

from hamilton import driver, log_setup


def disc_price(l_extendedprice, l_discount) -> float:
    """Computes the discounted price"""
    return l_extendedprice * (1 - l_discount)


def grr():
    path = "/Users/stefankrawczyk/Downloads/tpc-h-public/lineitem.tbl"
    sc = SparkSession.builder.getOrCreate()
    sc.sparkContext.setLogLevel("WARN")
    log_setup.setup_logging()
    df: DataFrame = sc.read.option("inferSchema", True).option("header", True).csv(path, sep="|")
    df = df.filter("l_shipdate <= date '1998-12-01' - interval '90' day")
    print(df.schema)
    spark_udf = F.udf(disc_price, types.FloatType())
    df.withColumn("disc_price", spark_udf(df["l_extendedprice"], df["l_discount"])).show()


def main():
    log_setup.setup_logging(log_level=log_setup.LOG_LEVELS["INFO"])
    spark = SparkSession.builder.getOrCreate()
    path = "/Users/stefankrawczyk/Downloads/tpc-h-public/lineitem.tbl"
    adapter = pyspark_adapter.PySparkGraphAdapter()
    dr = driver.Driver({"sc": spark, "path": path}, possible_api, adapter=adapter)
    outputs = [
        "al_returnflag",  # can comment one of these out and they are dropped
        "al_linestatus",
        "row_count",
        "charge_sum",
        "disc_price_sum",
        "l_discount_mean",
        "l_extendedprice_mean",
        "l_extendedprice_sum",
        "l_quantity_mean",
        "l_quantity_sum",
        # "grouped_lineitem" -- calling collect on groups doesn't work it seems...
        # "l_returnflag",  # can also get intermediate results
        # "charge",
        # "disc_price",
    ]
    dr.visualize_execution(outputs, "./my_dag.dot", {})
    df = dr.execute(outputs)
    df.show()
    spark.stop()


if __name__ == "__main__":
    main()
    # grr()
