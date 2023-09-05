import pyspark.sql as ps
from pyspark.sql import functions as sf
from zone_features import darkshore_flag, darkshore_likelihood, durotar_flag, durotar_likelihood

from hamilton.plugins.h_spark import with_columns


def spark_session() -> ps.SparkSession:
    return ps.SparkSession.builder.master("local[1]").getOrCreate()


def world_of_warcraft(spark_session: ps.SparkSession) -> ps.DataFrame:
    return spark_session.read.parquet("data/wow.parquet")


@with_columns(darkshore_flag, durotar_flag, columns_to_pass=["zone"])
def with_flags(world_of_warcraft: ps.DataFrame, aggregation_level: str) -> ps.DataFrame:
    return world_of_warcraft


def zone_counts(with_flags: ps.DataFrame, aggregation_level: str) -> ps.DataFrame:
    return with_flags.groupby(aggregation_level).agg(
        sf.count("*").alias("total_count"),
        sf.sum("darkshore_flag").alias("darkshore_count"),
        sf.sum("durotar_flag").alias("durotar_count"),
    )


@with_columns(
    darkshore_likelihood,
    durotar_likelihood,
    columns_to_pass=["durotar_count", "darkshore_count", "total_count"],
)
def zone_likelihoods(zone_counts: ps.DataFrame) -> ps.DataFrame:
    return zone_counts
