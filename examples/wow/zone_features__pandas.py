import pandas as pd

from hamilton.function_modifiers import extract_columns


def with_flags(
    avatarId: pd.Series, darkshore_flag: pd.Series, durotar_flag: pd.Series
) -> pd.DataFrame:
    _df = pd.concat([avatarId, darkshore_flag, durotar_flag], axis=1)
    _df.columns = ["avatarId", "darkshore_flag", "durotar_flag"]
    return _df


@extract_columns("total_count", "darkshore_count", "durotar_count")
def zone_counts(with_flags: pd.DataFrame, aggregation_level: str) -> pd.DataFrame:
    return with_flags.groupby(aggregation_level).agg(
        total_count=("darkshore_flag", "count"),
        darkshore_count=("darkshore_flag", "sum"),
        durotar_count=("durotar_flag", "sum"),
    )
