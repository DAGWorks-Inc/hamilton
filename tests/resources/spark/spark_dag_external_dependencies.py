import pandas as pd
import pyspark.sql as ps

from hamilton.htypes import column as _
from hamilton.plugins import h_spark

IntSeries = _[pd.Series, int]


def to_multiply() -> int:
    return 2


def a(initial_column: IntSeries, to_add: int = 1) -> IntSeries:
    return initial_column + to_add


def b(a: IntSeries, to_multiply: int) -> IntSeries:
    return a * to_multiply


def df_input(spark_session: ps.SparkSession) -> ps.DataFrame:
    df = pd.DataFrame.from_records(
        [
            {"initial_column": 1},
            {"initial_column": 2},
            {"initial_column": 3},
            {"initial_column": 4},
        ]
    )
    return spark_session.createDataFrame(df)


@h_spark.with_columns(
    a,
    b,
    columns_to_pass=["initial_column"],
)
def processed_df_as_pandas(df_input: ps.DataFrame) -> pd.DataFrame:
    return df_input.select("a", "b").toPandas()
