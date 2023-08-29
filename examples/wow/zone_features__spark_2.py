import pandas as pd
import pyspark.sql as ps
from pyspark.sql import functions as sf

from hamilton import htypes
from hamilton.plugins.h_spark import with_columns


def spark_session() -> ps.SparkSession:
    return ps.SparkSession.builder.master("local[1]").getOrCreate()


def world_of_warcraft(spark_session: ps.SparkSession) -> ps.DataFrame:
    return spark_session.read.parquet("data/wow.parquet")


def durotar_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
    return (zone == ' Durotar').astype(int)

def darkshore_flag(zone: pd.Series) -> htypes.column[pd.Series, int]:
    return (zone == ' Darkshore').astype(int)

@with_columns(
    durotar_flag, darkshore_flag,
    columns_to_pass=['zone']
)
def zone_flags(world_of_warcraft: ps.DataFrame) -> ps.DataFrame:
    return world_of_warcraft


def durotar_count(durotar_flag: pd.Series, aggregation_level: str) -> htypes.column[pd.Series, int]:
    return (
        durotar_flag
        .groupby(aggregation_level)
        .agg(durotar_count=('durotar_flag', 'sum'))
    )

def darkshore_count(darkshore_flag: pd.Series, aggregation_level: str) -> htypes.column[pd.Series, int]:
    return (
        darkshore_flag
        .groupby(aggregation_level)
        .agg(darkshore_count=('darkshore_flag', 'sum'))
    )

def total_count(durotar_flag: pd.Series, aggregation_level: str) -> htypes.column[pd.Series, int]:
    return (
        durotar_flag
        .groupby(aggregation_level)
        .agg(total_count=('durotar_flag', 'count'))
    )

@with_columns(
    durotar_count, darkshore_count, total_count,
    columns_to_pass=['durotar_flag', 'darkshore_flag']
)
def zone_counts(zone_flags: ps.DataFrame) -> ps.DataFrame:
    return zone_flags


def durotar_likelihood(durotar_count: pd.Series, total_count: pd.Series) -> htypes.column[pd.Series, float]:
    return durotar_count / total_count

def darkshore_likelihood(darkshore_count: pd.Series, total_count: pd.Series) -> htypes.column[pd.Series, float]:
    return darkshore_count / total_count

@with_columns(
    durotar_likelihood, darkshore_likelihood,
    columns_to_pass=['durotar_count', 'darkshore_count', 'total_count']
)
def zone_likelyhoods(zone_counts: ps.DataFrame) -> ps.DataFrame:
    return zone_counts