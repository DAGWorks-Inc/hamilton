import pandas as pd

from hamilton.function_modifiers import source
from hamilton.function_modifiers.adapters import load_from


@load_from.csv(path=source("data_path"), sep=",")
def sales_data_set(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(by=["store", "item", "date"], ascending=True, inplace=True)
    return df
