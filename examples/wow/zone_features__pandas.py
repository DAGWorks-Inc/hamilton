import pandas as pd

from hamilton.function_modifiers import extract_columns


@extract_columns("total_count", "darkshore_count")
def zone_counts(avatarId: pd.Series, zone: pd.Series, aggregation_level: str) -> pd.DataFrame:    
    return (
        pd.concat([avatarId, zone], axis=1)
        .assign(darkshore_flag=lambda x: (x['zone'] == ' Darkshore').astype(int))            
        .groupby(aggregation_level)
        .agg({'avatarId': 'count', 'darkshore_flag': 'sum'})
        .rename(columns={'avatarId': 'total_count', 'darkshore_flag': 'darkshore_count'})
    )


def darkshore_likelyhood(darkshore_count: pd.Series, total_count: pd.Series) -> pd.Series:
    return (
        darkshore_count / total_count        
    )
