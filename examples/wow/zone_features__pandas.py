import pandas as pd

from hamilton.function_modifiers import extract_columns


def with_flags(avatarId: pd.Series, zone: pd.Series) -> pd.DataFrame:
    return (
        pd.concat([avatarId, zone], axis=1)
        .assign(darkshore_flag=lambda x: (x["zone"] == " Darkshore").astype(int))
        .assign(durotar_flag=lambda x: (x["zone"] == " Durotar").astype(int))
    )


@extract_columns("total_count", "darkshore_count", "durotar_count")
def zone_counts(with_flags: pd.DataFrame, aggregation_level: str) -> pd.DataFrame:
    return with_flags.groupby(aggregation_level).agg(
        total_count=("darkshore_flag", "count"),
        darkshore_count=("darkshore_flag", "sum"),
        durotar_count=("durotar_flag", "sum"),
    )


@extract_columns("darkshore_likelihood", "durotar_likelihood")
def zone_likelihoods(zone_counts: pd.DataFrame) -> pd.DataFrame:
    return zone_counts.assign(
        darkshore_likelihood=lambda x: x["darkshore_count"] / x["total_count"]
    ).assign(durotar_likelihood=lambda x: x["durotar_count"] / x["total_count"])
