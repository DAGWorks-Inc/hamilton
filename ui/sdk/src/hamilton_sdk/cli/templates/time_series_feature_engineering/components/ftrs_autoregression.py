import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from hamilton.function_modifiers import parameterize_values, tag


@tag(stage="production")
@parameterize_values(
    parameter="n",
    assigned_output={
        ("lag_sales_1", ""): 1,
        ("lag_sales_2", ""): 2,
        ("lag_sales_3", ""): 3,
        ("lag_sales_4", ""): 4,
        ("lag_sales_5", ""): 5,
        ("lag_sales_6", ""): 6,
        ("lag_sales_7", ""): 7,
        ("lag_sales_29", ""): 29,
        ("lag_sales_30", ""): 30,
    },
)
def lag_sales_n(grp_date_store_item: DataFrameGroupBy, output_idx: pd.Index, n: int) -> pd.Series:
    df = grp_date_store_item.shift(n)
    return df.loc[output_idx]["sales"].dropna()
