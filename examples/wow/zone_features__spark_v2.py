import pandas as pd
import pyspark.sql as ps
from pyspark.sql import functions as sf

from hamilton import htypes
from hamilton.plugins.h_spark import with_columns


def spark_session() -> ps.SparkSession:
    return ps.SparkSession.builder.master("local[1]").getOrCreate()


def world_of_warcraft(spark_session: ps.SparkSession) -> ps.DataFrame:
    return spark_session.read.parquet("data/wow.parquet")


def _flag_functions():
    """Hidden from the DAG by using this wrapper function"""

    def durotar_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
        return (zone == " Durotar").astype(int)

    def darkshore_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
        return (zone == " Darkshore").astype(int)

    return durotar_flag, darkshore_flag


@with_columns(*_flag_functions(), columns_to_pass=["zone"])
def with_flags(world_of_warcraft: ps.DataFrame, aggregation_level: str) -> ps.DataFrame:
    return world_of_warcraft


def zone_counts(with_flags: ps.DataFrame, aggregation_level: str) -> ps.DataFrame:
    return with_flags.groupby(aggregation_level).agg(
        sf.count("*").alias("total_count"),
        sf.sum("darkshore_flag").alias("darkshore_count"),
        sf.sum("durotar_flag").alias("durotar_count"),
    )


def _likelihood_functions():
    def durotar_likelihood(
        durotar_count: pd.Series, total_count: pd.Series
    ) -> htypes.column[pd.Series, float]:
        return durotar_count / total_count

    def darkshore_likelihood(
        darkshore_count: pd.Series, total_count: pd.Series
    ) -> htypes.column[pd.Series, float]:
        return darkshore_count / total_count

    return durotar_likelihood, darkshore_likelihood


@with_columns(
    *_likelihood_functions(), columns_to_pass=["durotar_count", "darkshore_count", "total_count"]
)
def zone_likelihoods(zone_counts: ps.DataFrame) -> ps.DataFrame:
    return zone_counts
