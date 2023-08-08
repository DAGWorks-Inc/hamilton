from typing import Callable, List

import pandas as pd
import pyspark.sql as ps

from hamilton.experimental import h_spark
from hamilton.htypes import column as _

IntSeries = _[pd.Series, int]
FloatSeries = _[pd.Series, float]


def to_add() -> int:
    return 1


def _module() -> List[Callable]:
    def a(a_raw: ps.DataFrame, to_add: int) -> ps.DataFrame:
        return a_raw.withColumn("a", a_raw.a_raw + to_add)

    def b(b_raw: IntSeries) -> IntSeries:
        return b_raw + 3

    def c(c_raw: IntSeries) -> FloatSeries:
        return c_raw * 3.5

    @h_spark.transforms("a", "key")
    def a_times_key(a_key: ps.DataFrame) -> ps.Column:
        return a_key.a * a_key.key

    def b_times_key(b: IntSeries, key: IntSeries) -> IntSeries:
        return b * key

    @h_spark.transforms("a", "b", "c")
    def a_plus_b_plus_c(a_b_c: ps.DataFrame) -> ps.Column:
        return a_b_c.a + a_b_c.b + a_b_c.c

    return [a, b, c, a_times_key, b_times_key, a_plus_b_plus_c]


def df_1(spark_session: ps.SparkSession) -> ps.DataFrame:
    df = pd.DataFrame.from_records(
        [
            {"key": 1, "a_raw": 1, "b_raw": 2, "c_raw": 1},
            {"key": 2, "a_raw": 4, "b_raw": 5, "c_raw": 2},
            {"key": 3, "a_raw": 7, "b_raw": 8, "c_raw": 3},
            {"key": 4, "a_raw": 10, "b_raw": 11, "c_raw": 4},
            {"key": 5, "a_raw": 13, "b_raw": 14, "c_raw": 5},
        ]
    )
    return spark_session.createDataFrame(df)


@h_spark.with_columns(
    *_module(),
    select=["a_times_key", "b_times_key", "a_plus_b_plus_c"],
    initial_schema=["a_raw", "b_raw", "c_raw", "key"],
)
def processed_df_as_pandas(df_1: ps.DataFrame) -> pd.DataFrame:
    return df_1.select("a_times_key", "b_times_key", "a_plus_b_plus_c").toPandas()
